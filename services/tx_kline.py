# -*- coding: utf-8 -*-
"""
腾讯行情 K 线 HTTP 数据源 (无天勤账号可用的兜底源)

为什么需要它:
  天勤 TqSdk 的指数覆盖不全 —— 科创50(上证科创板50成份指数, 代码 000688)在天勤
  合约库里【根本不存在】: 无论 SSE.000688 还是 CSI.000688, graphql 都直接回
  "contains non-existent instrument"(2026-09-11 实测, zhubajie 账号)。
  这类标的无论换什么账号都拿不到, 只能走公开行情接口。

设计要点:
  1. 与 TqApi 会话【完全隔离】—— 走普通 HTTP, 不碰 AccountSlot, 不会毒化天勤会话。
  2. 只做 K 线(OHLCV), 不涉及交易/持仓, 因此复用同一套指标计算即可。
  3. 内置 2s 内存缓存 + 【并发预取】: 单周期 HTTP 往返约 120ms, 6 个周期串行要 700ms+,
     会拖慢整屏 0.5s 轮询; prefetch() 用线程池一次并发拉齐, 冷启动降到 ~150ms。
  4. 输出列与 futures_kline._fetch_kline_df 对齐(open/high/low/close/volume/datetime),
     上层 compute_period 无需区分来源。

接口(实测 2026-09-14 可用):
  日线   : https://ifzq.gtimg.cn/appstock/app/fqkline/get?param=<code>,day,,,<n>,qfq
           (web.ifzq.gtimg.cn 同路径 2026-09-14 起返回 HTTP 501, 故改用无反查域名 ifzq 优先)
           data.<code>.day = [[date, open, close, high, low, volume], ...]
  分钟线 : https://ifzq.gtimg.cn/appstock/app/kline/mkline?param=<code>,<m1|m5|m15|m30|m60>,,<n>
           data.<code>.<tag> = [[YYYYMMDDHHMM, open, close, high, low, volume, {}, ...], ...]
  两者前 6 列顺序一致 = 时间/开/收/高/低/量。
  注: 日线必须带 ",qfq" 参数才返回数据(不带时 data.<code> 里连 day 字段都没有)。
"""
import time
import json
import threading
import urllib.request
import concurrent.futures

import pandas as pd

# 天勤代码前缀 -> 腾讯代码前缀
_EXCH_PREFIX = {"SSE": "sh", "SZSE": "sz"}

# 技术分析周期 key -> 腾讯周期 tag
_PERIOD_MAP = {
    "1m": "m1", "5m": "m5", "15m": "m15", "30m": "m30", "60m": "m60",
    "day": "day",
    # 2026-09-28: 周/月线改为【直取腾讯前复权端点】(qfqweek/qfqmonth, 历史 801/301 根),
    # 不再由日线重采样 —— 日线最多 800 根, 重采样出的月线仅 40 根, MACD(12,26,9) 的
    # EMA 种子误差无法收敛(实测平安银行月线 DIF 正负号颠倒 -> 金叉/死叉判定反向)。
    "week": "week", "month": "month",
}

_UA = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Referer": "https://gu.qq.com/",
}

_CACHE = {}          # (code, tag, n) -> (ts, df)
_LOCK = threading.Lock()
_TTL = 2.0           # 秒; 页面 0.5s 轮询 -> 每 2s 才真正打一次上游
_MAX_STALE = 30.0    # 秒; 旧值最多可用这么久(期间后台异步刷新, 绝不阻塞页面)
_TIMEOUT = 6
_MAX_WORKERS = 6

_EXEC = None
_EXEC_LOCK = threading.Lock()
_REFRESHING = set()  # 正在后台刷新的 key(去重, 避免同一 key 并发打上游)


def _executor():
    global _EXEC
    if _EXEC is None:
        with _EXEC_LOCK:
            if _EXEC is None:
                _EXEC = concurrent.futures.ThreadPoolExecutor(
                    max_workers=_MAX_WORKERS, thread_name_prefix="txk")
    return _EXEC


def to_tx_code(symbol):
    """SSE.000688 -> sh000688 ; SZSE.399006 -> sz399006 ; 不支持的交易所返回 None。"""
    if not symbol or "." not in symbol:
        return None
    ex, _, num = symbol.partition(".")
    pre = _EXCH_PREFIX.get(ex.upper())
    if not pre or not num:
        return None
    return pre + num


def _norm(tag, count):
    """规范化根数(腾讯单次上限: 日线 800 / 周月 800 / 分钟 320)。"""
    n = max(int(count or 90), 30)
    if tag in ("day", "week", "month"):
        return min(n, 800)
    return min(n, 320)


# 新浪日线兜底（2026-09-23）: 腾讯 fqkline 日线端点持续 501（实测 6/6），分钟线 mkline 正常。
_SINA_URL = ("https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
             "CN_MarketData.getKLineData?symbol=%s&scale=240&ma=no&datalen=%d")
_UA_SINA = {
    "User-Agent": _UA["User-Agent"],
    "Referer": "https://finance.sina.com.cn/",
}


def _get(url):
    req = urllib.request.Request(url, headers=_UA)
    return urllib.request.urlopen(req, timeout=_TIMEOUT).read().decode("utf-8", "ignore")


def _fetch_raw_sina(code, n):
    """新浪日线兜底: 返回 [[day, open, close, high, low, volume], ...]（与 _fetch_raw 同序/同列义）。

    2026-09-23 新增: 腾讯 appstock/app/fqkline/get 对日线/周线稳定返回 HTTP 501（分钟线 mkline 正常），
    导致所有走 HTTP 源的【日线】全空。新浪该接口实测可用，个股 / 深市 / 指数（含科创50 sh000688）均覆盖。
    ⚠️ 新浪为【不复权】价，与腾讯 qfq 在除权日之后存在差异；因此仅在腾讯取不到时才使用。
    """
    n = max(30, min(int(n or 260), 1023))
    try:
        raw = urllib.request.urlopen(
            urllib.request.Request(_SINA_URL % (code, n), headers=_UA_SINA),
            timeout=_TIMEOUT).read().decode("utf-8", "ignore")
        arr = json.loads(raw)
    except Exception:
        return []
    out = []
    for it in (arr or []):
        try:
            out.append([str(it.get("day")), float(it.get("open")), float(it.get("close")),
                        float(it.get("high")), float(it.get("low")), float(it.get("volume") or 0)])
        except Exception:
            continue
    return out


def _fetch_raw(code, tag, n):
    """向上游取原始行, 返回 [[time, o, c, h, l, v], ...]; 失败/空返回 []。

    2026-09-14: 日线原用 web.ifzq.gtimg.cn, 实测该子域返回 HTTP 501(全部代码, 分钟线同源的
    ifzq.gtimg.cn 正常) => 科创50 日线取不到("暂无K线数据")。改为 ifzq.gtimg.cn 优先,
    并保留 web. 作兜底(两域名互备, 哪个通用哪个)。两者返回结构一致(data.<code>.day)。"""
    if tag in ("day", "week", "month"):
        # 2026-09-28: 周/月线同走 fqkline 端点(返回 qfqweek / qfqmonth), 历史远长于日线重采样。
        # (2026-09-23 曾实测该端点对日/周返回 501, 现均已恢复 200; 保留两域名互备。)
        urls = ["https://ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,%s,,,%d,qfq" % (code, tag, n),
                "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,%s,,,%d,qfq" % (code, tag, n)]
    else:
        urls = ["https://ifzq.gtimg.cn/appstock/app/kline/mkline?param=%s,%s,,%d" % (code, tag, n)]
    raw, rows = None, []
    for url in urls:
        try:
            raw = json.loads(_get(url))
        except Exception:
            raw = None
            continue
        node = (raw.get("data") or {}).get(code) or {}
        # 2026-09-14: 【个股】请求带 qfq 时, 返回键名是 qfqday 而不是 day ---- 指数/无需复权
        # 的标的才用 day(如 sh000688/sh000001 走 day, sz000001/sh600000 走 qfqday)。
        # 原先只取 node.get(tag), 导致所有【个股】日线恒为 0 行(静默失败, 表现为"暂无K线数据")。
        rows = node.get(tag) or node.get("qfq" + tag) or node.get("hfq" + tag) or []
        if rows:                       # 该域名有数据即用, 否则试下一个
            break
    # 2026-09-23: 腾讯 fqkline 端点对日线稳定返回 HTTP 501(分钟线 mkline 正常) => 日线全空。
    #   改用新浪同源接口兜底（个股/深市/指数均可）；新浪为不复权价, 仅作兜底。
    if not rows and tag == "day":
        rows = _fetch_raw_sina(code, n)
    out = []
    for r in rows:
        if not isinstance(r, (list, tuple)) or len(r) < 6:
            continue
        try:
            out.append([r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])])
        except Exception:
            continue
    return out


def _to_df(rows, tag):
    if not rows:
        return None
    fmt = "%Y-%m-%d" if tag in ("day", "week", "month") else "%Y%m%d%H%M"
    # 2026-09-28 性能改版: 原先逐行 pd.to_datetime(str, format) —— 800 行 × 纯 Python 调用开销,
    #   在技术分析轮询 + 股票K线并发加载时加剧 GIL 饱和(实锤: py-spy 多线程卡死在 strptime)。
    #   改为一次向量化解析, 单次 load 的解析 CPU 降 ~10 倍以上。列序与原版一致。
    ts = pd.to_datetime([str(r[0]) for r in rows], format=fmt, errors="coerce")
    df = pd.DataFrame({"datetime": ts,
                       "open": [r[1] for r in rows],
                       "high": [r[3] for r in rows],
                       "low": [r[4] for r in rows],
                       "close": [r[2] for r in rows],
                       "volume": [r[5] for r in rows]})
    df = df[df["datetime"].notna()]
    if df.empty:
        return None
    return df


# ---- 实时报价(用于"当前价/涨跌"显示) ----
# K 线柱收尾刷新太粗(约 7~10s 一跳), 导致页面"跳几下就停"; 腾讯 qt 接口约 3s 一拍, 更贴近 tick。
# 指标(MA/BOLL/MACD 等)仍基于 K 线柱计算, 不受影响。
_QT_URL = "https://qt.gtimg.cn/q="
_QT_TTL = 0.5  # 2026-10-08: 前端实时价改为 1s 轮询, TTL 须 < 轮询间隔, 否则每次轮询命中 2s 前旧缓存而不刷新
_QT_CACHE = {}
_QT_LOCK = threading.Lock()


def _parse_qt(text, code):
    """解析 v_sh000001="1~名称~代码~现价~昨收~今开~..." 取 [现价, 昨收]; 失败返回 (None, None)。"""
    import re
    m = re.search(r'v_%s="([^"]*)"' % re.escape(code), text or "")
    if not m:
        return None, None
    f = m.group(1).split("~")
    try:
        last = float(f[3])
        prev = float(f[4])
    except (IndexError, ValueError):
        return None, None
    if last <= 0 or prev <= 0:
        return None, None
    return last, prev


def fetch_quote(symbol):
    """返回 (last, prev_close) 实时报价; 失败/无值返回 (None, None)。

    2026-10-08 提速改版: TTL 收紧到 0.5s / MAX_STALE 0.6s(见常量定义)。
      前端实时价已改为 1s 轮询(stock_kline.js skTickDetail); 若 TTL>=轮询间隔, 每次轮询都命中
      2s 前的旧缓存、永远不刷新 —— 故 TTL 必须 < 1s。腾讯 qt 接口实测网络仅 ~10ms, 同步取最新
      快照无阻塞风险, 不必再走"旧值窗口"策略。本函数仅作为天勤主源失败时的兜底源。
    """
    code = to_tx_code(symbol)
    if not code:
        return None, None
    with _QT_LOCK:
        hit = _QT_CACHE.get(code)
    if hit is not None:
        age = time.time() - hit[0]
        if age < _QT_TTL:
            return hit[1]
        if age < _QT_MAX_STALE:
            _qt_refresh_bg(code)
            return hit[1]
    try:
        raw = _get(_QT_URL + code)
    except Exception:
        with _QT_LOCK:
            h = _QT_CACHE.get(code)
        return h[1] if h else (None, None)
    last, prev = _parse_qt(raw, code)
    if last is None:
        with _QT_LOCK:
            h = _QT_CACHE.get(code)
        return h[1] if h else (None, None)
    with _QT_LOCK:
        _QT_CACHE[code] = (time.time(), (last, prev))
    return last, prev


_QT_MAX_STALE = 0.6  # 必须小于轮询间隔; 保证 1s 轮询每次都同步取最新腾讯快照(网络仅 ~10ms, 无阻塞)
_QT_REFRESHING = set()


def _qt_refresh_bg(code):
    """报价后台异步刷新(同一 code 去重), 请求线程永不等待。"""
    with _QT_LOCK:
        if code in _QT_REFRESHING:
            return
        _QT_REFRESHING.add(code)

    def _work():
        try:
            raw = _get(_QT_URL + code)
            last, prev = _parse_qt(raw, code)
            if last is not None:
                with _QT_LOCK:
                    _QT_CACHE[code] = (time.time(), (last, prev))
        except Exception:
            pass
        finally:
            with _QT_LOCK:
                _QT_REFRESHING.discard(code)

    try:
        _executor().submit(_work)
    except Exception:
        with _QT_LOCK:
            _QT_REFRESHING.discard(code)


# ---- 批量报价(2026-09-28 股票监控迁移 tx 源) ----
_QT_BATCH_SIZE = 50        # 单请求最多带 50 个代码(实测 200 只=4 请求 0.15s)
_QT_BATCH_GAP = 0.05       # 相邻批量请求最小间隔(限流护栏)


def fetch_quotes_batch(symbols):
    """批量实时报价: symbols -> {symbol: {"last": float|None, "pre_close": float|None}}。

    走 qt.gtimg.cn 多代码接口(一次 50 只), 供监控列表/分钟引擎替代天勤订阅快照。
    不做 SWR 缓存(调用方轮询间隔 >= 3s, 直取即可, 实测单批 ~40ms);
    单批失败不影响其余, 单只解析失败仅该标的为 None。"""
    out = {}
    code_map = {}          # tx_code -> symbol
    for sym in (symbols or []):
        c = to_tx_code(sym)
        if not c:
            continue
        code_map[c] = sym
        out[sym] = {"last": None, "pre_close": None}
    codes = list(code_map.keys())
    for i in range(0, len(codes), _QT_BATCH_SIZE):
        chunk = codes[i:i + _QT_BATCH_SIZE]
        try:
            raw = _get(_QT_URL + ",".join(chunk))
        except Exception:
            continue
        for c in chunk:
            last, prev = _parse_qt(raw, c)
            out[code_map[c]] = {"last": last, "pre_close": prev}
        if i + _QT_BATCH_SIZE < len(codes):
            time.sleep(_QT_BATCH_GAP)
    return out


def _load(code, tag, n):
    """取数(不查缓存): 成功写缓存; 失败退回旧数据(页面不闪空)。"""
    df = _to_df(_fetch_raw(code, tag, n), tag)
    if df is not None and not df.empty:
        with _LOCK:
            _CACHE[(code, tag, n)] = (time.time(), df)
        return df
    with _LOCK:
        hit = _CACHE.get((code, tag, n))
    return hit[1] if hit else None


def _refresh_bg(code, tag, n):
    """后台异步刷新(同一 key 去重)。页面永不因它等待。"""
    key = (code, tag, n)
    with _LOCK:
        if key in _REFRESHING:
            return
        _REFRESHING.add(key)

    def _work():
        try:
            _load(code, tag, n)
        except Exception:
            pass
        finally:
            with _LOCK:
                _REFRESHING.discard(key)

    try:
        _executor().submit(_work)
    except Exception:
        with _LOCK:
            _REFRESHING.discard(key)


def fetch_df(symbol, period_key, count):
    """取 K 线 DataFrame(open/high/low/close/volume/datetime), 失败返回 None。

    策略 = stale-while-revalidate:
      新鲜(<=2s)    -> 直接返回;
      旧值(<=30s)   -> 【立即返回旧值】并把刷新丢到后台线程 —— 页面永远秒回, 不因网络抖动卡壳;
      无值/过期太久 -> 阻塞取一次(仅首次)。
    与 futures_kline._fetch_kline_df 的产出结构对齐, 上层可无差别使用。
    """
    code = to_tx_code(symbol)
    tag = _PERIOD_MAP.get(period_key)
    if not code or not tag:
        return None
    n = _norm(tag, count)
    with _LOCK:
        hit = _CACHE.get((code, tag, n))
    if hit is not None:
        age = time.time() - hit[0]
        if age < _TTL:
            return hit[1]
        if age < _MAX_STALE:
            _refresh_bg(code, tag, n)
            return hit[1]
    return _load(code, tag, n)


def prefetch(symbol, period_counts):
    """并发把【完全没有值】的周期拉进缓存(冷启动/进程刚起时用)。

    period_counts: {period_key: count} —— 6 个周期串行 700ms+, 并发后 ~150ms。
    已有值(哪怕已过期但在 _MAX_STALE 内)的周期不在这里阻塞拉取, 由 fetch_df 后台刷新。
    """
    code = to_tx_code(symbol)
    if not code:
        return
    now = time.time()
    misses = []
    for pk, cnt in (period_counts or {}).items():
        tag = _PERIOD_MAP.get(pk)
        if not tag:
            continue
        n = _norm(tag, cnt)
        with _LOCK:
            hit = _CACHE.get((code, tag, n))
        if hit is not None and (now - hit[0]) < _MAX_STALE:
            continue
        misses.append((tag, n))
    if not misses:
        return
    try:
        list(_executor().map(lambda a: _load(code, a[0], a[1]), misses))
    except Exception:
        for tag, n in misses:          # 线程池异常时退化为串行
            _load(code, tag, n)
