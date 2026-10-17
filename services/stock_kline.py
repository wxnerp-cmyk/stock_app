# -*- coding: utf-8 -*-
"""
股票模块 (stock_kline)

功能:
- 全部 A 股列表 (经天勤 TqSdk query_quotes(ins_class="STOCK") 拉取, 后台线程缓存)
- 列表页: 可搜索/翻页, 双击个股 -> 下钻明细
- 明细页: ECharts 蜡烛图 + 成交量 + MACD 副图(标注金叉) + 金叉/死叉事件表
- 周期: 周线(week) / 月线(month) —— 复用 futures_kline 的 _resample_daily 重采样
- MACD 金叉判定: 复用 indicators.macd + ta_patterns.detect_macd_cross (周期无关)

数据来源: 天勤 TqSdk (经 stock_app 的 AccountSlot 单一连接, 不新建连接)
网关路径约定(与 futures_kline 一致):
- 前端 JS 用 skBase()=location.pathname 截到 /stockkline/ 做前缀, 自动兼容 直连(:5088) 与 网关(:9000/api/gw/stockkline)
- ECharts 用相对路径 "static/echarts.min.js", 经网关时自动走 /api/gw/stockkline/static/...
"""
import os
import time
import html
import json
import threading
from datetime import datetime, date, timedelta

import pandas as pd
import numpy as np
from flask import request, jsonify, send_from_directory

from services import indicators   # MACD 计算 (周期无关)
from services import ta_patterns  # MACD 金叉/死叉检测
from services import futures_kline  # 复用 _fetch_kline_df / _resample_daily / serve_static
from services import tx_kline     # 腾讯 HTTP 实时源(行情/报价, 不占天勤会话, 不受账户权限/到期限制)


# ===================== 全局状态 =====================
_SLOTS = None
_DEFAULT_KEY = None
_GET_SLOT = None  # 请求感知的账号解析器（来自 main.py 的 _resolve_slot）
_STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")

# 全部 A 股缓存: [{symbol, exchange, code, name}]
_STOCK_LIST = []
_STOCK_MAP = {}        # symbol -> 同一 dict 引用(便于名称渐进回填)
_STOCK_LOCK = threading.Lock()
_STOCK_READY = False
_STOCK_ERROR = ""

# 周期配置: 周/月由日线重采样得到; 日内周期直接向天勤取。
# align = 自动刷新的"周期时间点"对齐秒数(以 9:00 为基轴), 见 skNextPeriodTick()
PERIODS = {
    "m5":    {"label": "5分",  "duration": 300,   "count": 500, "freq": None, "fmt": "%m-%d %H:%M", "align": 300},
    "m15":   {"label": "15分", "duration": 900,   "count": 500, "freq": None, "fmt": "%m-%d %H:%M", "align": 900},
    "m30":   {"label": "30分", "duration": 1800,  "count": 500, "freq": None, "fmt": "%m-%d %H:%M", "align": 1800},
    "m60":   {"label": "60分", "duration": 3600,  "count": 500, "freq": None, "fmt": "%m-%d %H:%M", "align": 3600},
    "day":   {"label": "日线", "duration": 86400, "count": 500, "freq": None, "fmt": "%Y-%m-%d", "align": 86400},
    "week":  {"label": "周线", "duration": 86400, "count": 300, "freq": "W",  "fmt": "%Y-%m-%d", "align": 604800},
    "month": {"label": "月线", "duration": 86400, "count": 500, "freq": "MS", "fmt": "%Y-%m", "align": 2592000},
}
# 页面周期按钮顺序
PERIOD_ORDER = ["m5", "m15", "m30", "m60", "day", "week", "month"]
DEFAULT_STOCK_PERIOD = "day"

_MAX_KLINE = 1500

# K 线/MACD/金叉计算结果缓存 (构建成本高, 相同 symbol+period 短时间内复用)
_KLINE_CACHE = {}          # (symbol, period) -> {"ts": float, "data": payload_without_quote}
_KLINE_CACHE_LOCK = threading.Lock()
_KLINE_CACHE_TTL = 60      # 秒
_KLINE_CACHE_MAX = 200     # 最大条目数, 超限淘汰最旧


# ===================== 工具函数 =====================
def _r(v):
    try:
        if v is None:
            return None
        v = float(v)
        if np.isnan(v) or np.isinf(v):
            return None
        return round(v, 4)
    except Exception:
        return None


def _get_slot():
    """解析当前请求应选用的账户 Slot（优先网关注入的选中账号，回落默认）。"""
    if _GET_SLOT is not None:
        try:
            s = _GET_SLOT()
            if s is not None:
                return s
        except Exception:
            pass
    if _SLOTS and _DEFAULT_KEY:
        return _SLOTS.get(_DEFAULT_KEY)
    return None


def _get_cached_kline(symbol, period):
    """取 K 线缓存(不含实时 quote)。返回缓存 payload 或 None。"""
    key = (symbol, period)
    with _KLINE_CACHE_LOCK:
        item = _KLINE_CACHE.get(key)
        now = time.time()
        if not item:
            return None
        if now - item["ts"] > _KLINE_CACHE_TTL:
            _KLINE_CACHE.pop(key, None)
            return None
        return item["data"]


def _set_cached_kline(symbol, period, payload):
    """缓存 K 线 payload(不含 quote)。容量超限时淘汰最旧条目。"""
    key = (symbol, period)
    with _KLINE_CACHE_LOCK:
        if len(_KLINE_CACHE) >= _KLINE_CACHE_MAX and key not in _KLINE_CACHE:
            oldest_key = min(_KLINE_CACHE, key=lambda k: _KLINE_CACHE[k]["ts"])
            _KLINE_CACHE.pop(oldest_key, None)
        _KLINE_CACHE[key] = {"ts": time.time(), "data": payload}


# ===================== 腾讯 tx 实时源(股票专用, 不占天勤会话) =====================
# 背景(2026-09-28): 股票原先与「技术分析」共用同一 pro 账户的天勤会话取 A 股 K 线/报价,
#   既消耗技术分析的订阅容量(影响技术分析优先级), 又因前端只在"周期对齐点"刷新(日线一天只刷一次)
#   导致【实时价/当日 forming bar 全天冻结 = 看着像历史数据】。
#   现统一改走腾讯 HTTP 源(与 stock_tech 指数同源): 实时(约 3s)、免费、不占天勤会话、
#   不受账户权限/到期限制; 彻底与期货线 / 技术分析线解耦。
_TX_PERIOD_TAG = {"m1": "1m", "m5": "5m", "m15": "15m", "m30": "30m", "m60": "60m", "day": "day"}


def _norm_tx_dt(df):
    """把腾讯返回的"北京时间 naive" datetime 归一为 UTC 纳秒 int64, 与天勤 _fetch_kline_df
    输出结构对齐, 使下游 _build_stock_kline / _dt_local 无需区分来源。"""
    d = df.copy()
    ts = pd.to_datetime(d["datetime"], errors="coerce")
    utc = ts.dt.tz_localize("Asia/Shanghai", ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")
    d["datetime"] = utc.astype("int64")
    d = d[~utc.isna()]            # 丢弃时区归一失败的异常行
    return d


def _align_tx_period_start(df, period):
    """腾讯周/月线的日期标的是该周期【末】交易日(周五 / 月末), 而本地重采样口径是【首】
    (周一 / 月初)。两者前端 categories 与金叉落库日期会差 4~30 天, 故统一归一到周期首日,
    与既有 _resample_tx_daily 输出保持一致(前端展示/历史记录口径不变)。"""
    try:
        d = _norm_tx_dt(df)
        dt = futures_kline._dt_local(d["datetime"])
        if period == "week":
            dt = dt - pd.to_timedelta(dt.dt.weekday, unit="D")
        else:
            dt = dt - pd.to_timedelta(dt.dt.day - 1, unit="D")
        out = d.copy()
        out["datetime"] = dt.dt.tz_convert("UTC").astype("int64")
        return out
    except Exception as e:
        print(f"[股票模块] 周/月日期归一失败: {e}")
        return df


def _resample_tx_daily(df, freq, count):
    """把腾讯日线 df 按北京时间重采样为周/月线(结构与 _resample_daily 输出对齐)。"""
    try:
        d = _norm_tx_dt(df)
        d["dt"] = futures_kline._dt_local(d["datetime"])
        d = d.set_index("dt")
        agg_map = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
        agg = d.resample(freq).agg(agg_map).dropna(subset=["close"])
        if freq.upper().startswith("W"):
            agg.index = agg.index - pd.Timedelta(days=6)   # 中国习惯: 周一为周起点
        agg = agg.tail(count).reset_index()
        agg["datetime"] = agg["dt"].dt.tz_convert("UTC").astype("int64")
        cols = ["datetime", "open", "high", "low", "close", "volume"]
        return agg[cols]
    except Exception as e:
        print(f"[股票模块] tx 周/月重采样失败: {e}")
        return df


_EM_KLT = {"day": 101, "week": 102, "month": 103}
_EM_FAIL_UNTIL = 0.0  # 东财连续失败后的临时熔断截止时间戳(防反爬时拖垮全市场扫描)


def _em_kline_df(symbol, period, count):
    """东财 push2his 前复权(fqt=1) K线 -> DataFrame(datetime, close)。

    🔴 2026-09-30 实锤(688557 兰剑智能, 用户 WH6 对照): 腾讯 qfqweek 对【有除权事件】的票
    实际返回未复权数据 —— 未复权序列上 MACD 交叉位置被推错数周(688557 周金叉真实位置=
    2026-09-07 那根周K(WH6 标 9-11), 未复权却算成"本周金叉");
    东财 fqt=1 前复权与 WH6(向前复权) 表头逐位一致(dif=-0.7401/dea=-0.9226 ↔ WH6 -0.74/-0.92)。
    故 周/月线优先走东财前复权, 腾讯直取降为回落。失败熔断 5 分钟, 期间直接走腾讯, 不拖垮扫描。
    """
    global _EM_FAIL_UNTIL
    klt = _EM_KLT.get(period)
    if not klt:
        return None
    if time.time() < _EM_FAIL_UNTIL:
        return None
    try:
        import requests as _rq
        code = symbol.split(".")[-1]
        secid = ("1." if symbol.startswith("SSE") else "0.") + code
        u = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=%s"
             "&klt=%d&fqt=1&lmt=%d&end=20500101&fields1=f1,f2,f3&fields2=f51,f53"
             % (secid, klt, max(int(count), 80)))
        r = _rq.get(u, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        ks = ((r.json() or {}).get("data") or {}).get("klines") or []
        if len(ks) < 60:
            return None
        rows = [(x.split(",")[0], float(x.split(",")[1])) for x in ks]
        df = pd.DataFrame(rows, columns=["dtstr", "close"])
        df["datetime"] = pd.to_datetime(df["dtstr"])
        return _align_tx_period_start(df, period)
    except Exception:
        _EM_FAIL_UNTIL = time.time() + 300.0  # 熔断 5 分钟
        return None


def _tx_kline_df(symbol, period, count):
    """从腾讯 tx 源取 A 股 K 线 DataFrame(结构与天勤 _fetch_kline_df 对齐)。返回 df 或 None。

    - 日/分钟: 直接走 tx;
    - 周/月: 【优先】直取腾讯 qfqweek/qfqmonth(前复权, 历史 801/301 根);
             取不到再回落"日线重采样"(历史短, 仅作兜底);
    ⚠️ 2026-09-28 实锤: 原"日线重采样"方案下月线只有 40 根, MACD(12,26,9) 的 EMA 种子
       误差未收敛 —— 实测平安银行月线 DIF 由 -0.127(长史) 变成 +0.018(40根), 与 DEA 的
       上下关系反转, 导致【月线金叉/死叉判定方向颠倒】(= 用户反馈"指标都是错的")。
       直取腾讯月线后历史 301 根, 与长史结果偏差 < 0.01。
    - A股 60m: 取 tx 30m 后合成"标准交易时段"4 根/天(与主流看盘一致, 沿用期货侧 _resample_60m_std)。
    """
    try:
        if period in ("week", "month"):
            # 2026-09-30: 优先东财前复权(与 WH6 对齐)。腾讯周/月线对有除权事件的票实际未复权,
            # MACD 交叉位置会错周(688557 实锤), 详见 _em_kline_df 注释。
            em = _em_kline_df(symbol, period, count)
            if em is not None and not em.empty and len(em) >= 60:
                return em
            direct = tx_kline.fetch_df(symbol, period, count)
            if direct is not None and not direct.empty and len(direct) >= 60:
                return _align_tx_period_start(direct, period)
            # 直取失败/过短 -> 回落重采样(兜底, 历史较短)
            daily = tx_kline.fetch_df(symbol, "day", min(800, max(count * (30 if period == "month" else 7), 320)))
            if daily is None or daily.empty:
                return None
            return _resample_tx_daily(daily, "MS" if period == "month" else "W", count)
        if period == "m60" and futures_kline.uses_ashare_60m_sessions(symbol):
            d30 = tx_kline.fetch_df(symbol, "30m", count * 8)
            if d30 is None or d30.empty:
                return None
            return futures_kline._resample_60m_std(_norm_tx_dt(d30))
        tag = _TX_PERIOD_TAG.get(period)
        if not tag:
            return None
        # 注意: 必须传转换后的腾讯 tag("5m"), 而非 stock 内部周期 key("m5");
        # tx_kline.fetch_df 的 _PERIOD_MAP 只认 "5m"/"15m"/... 否则返回 None -> 误报"暂无K线数据"
        df = tx_kline.fetch_df(symbol, tag, count)
        if df is None or df.empty:
            return None
        return _norm_tx_dt(df)
    except Exception as e:
        print(f"[股票模块] tx 取 K 线失败 {symbol} {period}: {e}")
        return None


def _fetch_fresh_quote(slot, symbol):
    """单独获取实时报价(轻量, 命中缓存时补充用)。

    数据源(2026-10-08 改版, 需求: 主源切回天勤, 为专业版铺路):
      1) 优先天勤 get_quote(需有效 slot) —— 推送式, 盘中实时性最佳, 且是未来专业版统一接口;
         无权限标的(如上证指数 000001 在免费/试用账户下天勤无 A 股指数权限) get_quote 会抛错或
         返回 0/空, 此时回落腾讯。
      2) 回落腾讯 HTTP 实时报价(tx.gtimg.cn) —— 不占天勤订阅容量、不受账户权限与到期限制,
         作为天勤无权限/会话异常时的兜底, 保证所有标的(尤其指数)实时价不空白。
        ⚠️ 资源保护: 天勤 get_quote 会隐式占用订阅容量; 当前股票扫描账号(Mr qia 等)若与
          期货/技术分析共用同一 pro 账户, 高频轮询可能互相挤占, 建议股票线单独配专业版账户。"""
    quote = {"last": None, "pre_close": None, "change": None, "change_pct": None, "name": ""}
    with _STOCK_LOCK:
        cached = _STOCK_MAP.get(symbol)
        if cached and cached.get("name"):
            quote["name"] = cached["name"]
    # 1) 优先天勤实时报价(推送式, 实时性最佳)
    if slot is not None and slot.client is not None:
        try:
            q = slot.client.get_quote(symbol)
            last = _r(getattr(q, "last_price", None))
            pre = _r(getattr(q, "pre_close", None))
            name = getattr(q, "instrument_name", None) or getattr(q, "name", None) or ""
            if last is not None and last > 0:
                quote["last"] = last
                if pre and pre > 0:
                    quote["pre_close"] = pre
                    quote["change"] = round(last - pre, 4)
                    quote["change_pct"] = round((last - pre) / pre * 100, 2)
                if name:
                    quote["name"] = str(name)
                    with _STOCK_LOCK:
                        if symbol in _STOCK_MAP:
                            _STOCK_MAP[symbol]["name"] = str(name)
                return quote
        except Exception as e:
            print(f"[股票模块] 天勤 get_quote({symbol}) 失败, 回落腾讯: {e}")
    # 2) 回落腾讯实时报价(独立 HTTP, 不碰天勤会话)
    try:
        last, prev = tx_kline.fetch_quote(symbol)
        if last is not None and last > 0:
            quote["last"] = last
            if prev and prev > 0:
                quote["pre_close"] = prev
                quote["change"] = round(last - prev, 4)
                quote["change_pct"] = round((last - prev) / prev * 100, 2)
            return quote
    except Exception:
        pass
    return quote


def _parse_symbol(sym):
    parts = str(sym).split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    return "", sym


# ===================== A 股列表后台初始化 =====================
def _load_from_db():
    """从本地主数据库 t_stock_list 读取全部 A 股(含名称)。
    该表由系统同步维护(8487 行, 带中文名), 不依赖天勤行情元数据,
    即使试用账户到期 / query_quotes 返回空也能正常出列表。"""
    try:
        from services import futures_db
        import pymysql
        cfg = futures_db.DB_CONFIG
        conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                               password=cfg["password"], database=cfg["database"], connect_timeout=5)
        cur = conn.cursor()
        try:
            cur.execute("SELECT symbol, name, exchange_id FROM t_stock_list "
                        "WHERE ins_class='STOCK' OR ins_class IS NULL OR ins_class='' "
                        "ORDER BY sort_order, id")
            rows = cur.fetchall()
        except Exception:
            # 表结构兼容: 若字段不同则降级取全部
            cur.execute("SELECT symbol, name, exchange_id FROM t_stock_list ORDER BY id")
            rows = cur.fetchall()
        conn.close()
        lst = []
        for sym, name, ex in rows:
            sym = str(sym)
            ex = (ex or (sym.split(".")[0] if "." in sym else ""))
            code = sym.split(".")[-1] if "." in sym else sym
            lst.append({"symbol": sym, "exchange": ex, "code": code, "name": (name or "")})
        return lst
    except Exception as e:
        print("[股票模块] 从 t_stock_list 读取失败:", e)
        return None


def _load_from_query(api):
    """实时从天勤拉取(需有效账户 + 行情元数据已同步)。作为 DB 为空时的兜底。"""
    try:
        syms = api.query_quotes(ins_class="STOCK") or []
    except Exception:
        syms = []
    lst = []
    for s in syms:
        s = str(s)
        ex, code = _parse_symbol(s)
        lst.append({"symbol": s, "exchange": ex, "code": code, "name": ""})
    return lst


def _init_stock_list():
    """后台线程加载全部 A 股代码(含名称)。
    优先级: 本地 t_stock_list(含中文名, 稳定) -> 天勤 query_quotes(有效账户实时)。"""
    def _run():
        global _STOCK_LIST, _STOCK_MAP, _STOCK_READY, _STOCK_ERROR
        try:
            lst = _load_from_db()
            if not lst:
                # DB 为空, 退回天勤实时拉取(需 api 就绪)
                api = None
                for _ in range(1, 61):
                    slot = _get_slot()
                    if slot and getattr(getattr(slot, "client", None), "api", None) is not None:
                        api = slot.client.api
                        break
                    time.sleep(2)
                if api is not None:
                    lst = _load_from_query(api)
            with _STOCK_LOCK:
                _STOCK_LIST = lst or []
                _STOCK_MAP = {x["symbol"]: x for x in _STOCK_LIST}
                _STOCK_READY = True
            if not _STOCK_LIST:
                _STOCK_ERROR = "A 股列表为空(t_stock_list 无数据且天勤 query_quotes 未返回)"
            print(f"[股票模块] A 股列表初始化完成: 共 {len(_STOCK_LIST)} 支")
        except Exception as e:
            _STOCK_ERROR = str(e)
            print("[股票模块] 初始化失败:", e)
    threading.Thread(target=_run, daemon=True).start()


# ===================== K 线 + MACD 金叉构建 =====================
def _build_stock_kline(slot, symbol, period):
    cfg = PERIODS.get(period, PERIODS[DEFAULT_STOCK_PERIOD])

    # 1) 优先读缓存 (K 线/MACD/金叉 构建成本高, 相同 symbol+period 复用)
    cached_payload = _get_cached_kline(symbol, period)
    if cached_payload is not None:
        # 命中缓存时只补实时 quote, 避免重复拉 K 线/MACD 计算
        payload = cached_payload.copy()
        payload["quote"] = _fetch_fresh_quote(slot, symbol)
        if payload["quote"].get("name"):
            payload["name"] = payload["quote"]["name"]
        return payload

    # 2) 缓存未命中: 完整构建 —— 走腾讯 tx 实时源(不占天勤会话 / 不受账户权限与到期限制)
    real = symbol
    df = _tx_kline_df(symbol, period, cfg["count"])
    if df is None or df.empty:
        return {"ok": False, "error": "暂无K线数据(该标的腾讯源暂不可用或无行情)", "symbol": symbol, "period": period}

    df["dt"] = futures_kline._dt_local(df["datetime"])
    categories = [d.strftime(cfg["fmt"]) for d in df["dt"]]
    kline = [[_r(o), _r(c), _r(l), _r(h)] for o, c, l, h in zip(df["open"], df["close"], df["low"], df["high"])]
    volume = [_r(v) for v in df["volume"]]
    close = df["close"].astype(float).tolist()

    dif, dea, bar = indicators.macd(close, fast=12, slow=26, signal=9)

    # 金叉/死叉检测 (idx 对齐到后一根 bar, 与期货模块一致)
    crosses = ta_patterns.detect_macd_cross(dif, dea, close)
    cross_rows = []
    for c in crosses:
        idx = c.get("idx")
        if idx is None or idx < 0 or idx >= len(categories):
            continue
        cross_rows.append({
            "type": c["type"],
            "idx": idx,
            "date": categories[idx],
            "ts": df["dt"].iloc[idx].to_pydatetime() if hasattr(df["dt"].iloc[idx], "to_pydatetime") else df["dt"].iloc[idx],
            "dif": c.get("dif"),
            "dea": c.get("dea"),
            "price": c.get("price"),
        })
    # ① 实时精确记录(优先): forming bar(未走完的最后一根)上出现的新交叉 = 盘中实时金叉/死叉,
    #    用 src='rt' 记检测时刻(ts=now)。scan(②) 对同 bar 窗自动跳过, 避免重复/漂移。
    try:
        _record_tail_rt(symbol, period, crosses, df["dt"], cfg.get("duration"))
    except Exception:
        pass
    # ② 沉淀金叉/死叉事件(供首页"最近一周金叉记录"); skip-if-rt 由 _record_events 内部完成
    try:
        _record_events(symbol, period, cross_rows)
    except Exception as _e:
        pass
    # ③ 展示合并: 同 symbol/period 的实时精确记录(src='rt')若落在这批 K 线 bar 窗口内,
    #    用精确时间点替换对应的回溯交叉行(避免"实时14:15"与"回溯14:00"并列), 无对应则保留回溯。
    try:
        _merge_rt_crosses(symbol, period, cross_rows, df["dt"], cfg.get("duration"), cfg.get("fmt", "%m-%d %H:%M"))
    except Exception:
        pass

    quote = _fetch_fresh_quote(slot, symbol)
    ex, code = _parse_symbol(symbol)
    payload = {
        "ok": True,
        "symbol": symbol,
        "real_symbol": real,
        "exchange": ex,
        "code": code,
        "name": quote.get("name") or "",
        "period": period,
        "period_label": cfg["label"],
        "loaded_count": len(df),
        "categories": categories,
        "kline": kline,
        "volume": volume,
        "macd": {"dif": dif, "dea": dea, "bar": bar},
        "crosses": cross_rows,
        "quote": quote,
    }

    # 3) 缓存不含 quote 的 payload, 供后续复用
    cache_payload = payload.copy()
    cache_payload["quote"] = {"last": None, "pre_close": None, "change": None, "change_pct": None, "name": ""}
    _set_cached_kline(symbol, period, cache_payload)
    return payload


# ===================== API =====================
def api_stocks():
    q = (request.args.get("q", "") or "").strip().lower()
    recent_only = request.args.get("recent") == "1"
    try:
        offset = max(0, int(request.args.get("offset", "0")))
    except Exception:
        offset = 0
    try:
        limit = min(500, max(20, int(request.args.get("limit", "200"))))
    except Exception:
        limit = 200

    with _STOCK_LOCK:
        ready = _STOCK_READY
        err = _STOCK_ERROR
        items = list(_STOCK_LIST)
    with _CROSS_LOCK:
        cache = dict(_CROSS_CACHE)

    total = len(items)
    if q:
        items = [x for x in items if q in x["symbol"].lower() or q in x["code"].lower()
                 or q in (x["name"] or "").lower() or q in x["exchange"].lower()]
    if recent_only:
        items = [x for x in items if cache.get(x["symbol"], {}).get("w_recent")]
    filtered_total = len(items)
    page = items[offset:offset + limit]

    # 标注近期金叉(来自缓存表), 列表红点用
    out = []
    for s in page:
        c = cache.get(s["symbol"])
        if c:
            s = dict(s)
            s["recent_golden"] = bool(c.get("w_recent"))
            s["last_golden_date"] = c.get("w_golden") or ""
            s["recent_golden_month"] = bool(c.get("m_recent"))
        out.append(s)

    return jsonify({
        "ok": True,
        "ready": ready,
        "error": err,
        "total": total,
        "filtered_total": filtered_total,
        "offset": offset,
        "limit": limit,
        "has_more": (offset + limit) < filtered_total,
        "stocks": out,
    })


def api_stock_kline():
    symbol = (request.args.get("symbol", "") or "").strip()
    period = (request.args.get("period", DEFAULT_STOCK_PERIOD) or "").strip()
    if not symbol:
        return jsonify({"ok": False, "error": "缺少 symbol 参数"}), 400
    if period not in PERIODS:
        period = DEFAULT_STOCK_PERIOD
    # 股票 K 线/报价已统一改走腾讯 tx 源(不依赖天勤会话/账户), 故不再因"无天勤 slot"而 503。
    # 仍优先取一个 slot 作为 tx 失败时的回落通道(get_quote 兜底), 但 slot 缺失不再阻断 tx 主路径。
    slot = _get_scan_slot() or _get_slot() or None
    try:
        payload = _build_stock_kline(slot, symbol, period)
    except Exception as e:
        return jsonify({"ok": False, "error": f"K线构建失败: {e}", "symbol": symbol}), 500
    return jsonify(payload)


def api_stock_quote():
    """轻量实时报价接口: 只返回最新价/前收/涨跌。

    数据源(2026-10-08): 主源天勤 get_quote(经 slot), 失败/无权限回落腾讯 tx(见 _fetch_fresh_quote)。
    供前端高频轮询(默认 1s), 让股票详情页"最新价"实时跳动, 而不必每次重建 K 线/MACD。"""
    symbol = (request.args.get("symbol", "") or "").strip()
    if not symbol:
        return jsonify({"ok": False, "error": "缺少 symbol 参数"}), 400
    # 带上天勤 slot(扫描账号/默认账号), 使主源 get_quote 可用; 无 slot 时自动回落腾讯。
    slot = _get_scan_slot() or _get_slot() or None
    q = _fetch_fresh_quote(slot, symbol)
    q["symbol"] = symbol
    return jsonify({"ok": True, **q})


# ===================== 金叉扫描缓存(列表红点) =====================
_CROSS_CACHE = {}      # symbol -> {w_golden,w_golden_idx,w_death,w_recent,m_golden,m_golden_idx,m_death,m_recent}
_CROSS_LOCK = threading.Lock()
_SCAN = {"total": 0, "done": 0, "running": False, "last": "", "started": None}


# ===================== A 股扫描账号(可配置) =====================
# 背景(2026-09-04 实测): 天勤 A 股数据需要企业版权限。
# 2026-09-08 需求: 去掉硬编码默认扫描账号(原 Mr qia)。扫描账号改为动态从 tq_accounts
# 选【有效】账户(启用且未过期), 优先付费版本(能看 A 股); 管理员仍可通过 t_stock_scan_account
# 表手动指定(优先级更高)。后台定时任务(全市场遍历)没有 request 上下文, 只能用扫描账号。
_SCAN_ACCOUNT = None          # 运行时生效的扫描账号(动态从 DB 选, 或管理员手动指定)
_ACCT_CRED = {}               # tq_username -> tq_password (由网关注入的凭据被动记忆)
_ACCT_CRED_LOCK = threading.Lock()
_MAKE_SLOT = None             # main.py 传入的 get_slot(user, pass)


def remember_account(user, pwd):
    """记住网关注入的账号凭据, 供后台任务按用户名建 Slot。

    网关(stock_main)转发请求时会把解密后的 tq_user/tq_pass 注入查询参数,
    stock_app 本身拿不到明文密码, 只能这样被动收集。"""
    if not user or not pwd:
        return
    with _ACCT_CRED_LOCK:
        if _ACCT_CRED.get(user) != pwd:
            _ACCT_CRED[user] = pwd


def known_accounts():
    """返回已记忆的账号用户名列表。"""
    with _ACCT_CRED_LOCK:
        return sorted(_ACCT_CRED.keys())


def _scan_cfg_conn():
    from services import futures_db
    return futures_db.get_connection()


def _ensure_scan_cfg_table():
    try:
        conn = _scan_cfg_conn(); cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS t_stock_scan_account (
            id TINYINT PRIMARY KEY,
            username VARCHAR(64) NOT NULL,
            update_time DATETIME DEFAULT NOW()
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        conn.commit(); conn.close()
        return True
    except Exception as e:
        print("[股票模块] 扫描账号配置表初始化失败:", e)
        return False


def _pick_valid_account():
    """从 tq_accounts 动态选一个【有效】账户(启用且未过期)作为扫描账号, 不再硬编码默认。

    优先选付费版本(能看 A 股的): product_type 含 企业/pro/ent/专业 特征; 否则取第一个
    有效账户。都失败返回 None(股票扫描/技术分析将无数据, 由页面提示用户切换账户)。"""
    try:
        conn = _scan_cfg_conn(); cur = conn.cursor()
        cur.execute(
            "SELECT tq_username FROM tq_accounts "
            "WHERE status=1 AND (expiry IS NULL OR expiry > NOW()) "
            "ORDER BY (CASE WHEN product_type LIKE '%企业%' OR product_type LIKE '%专业%' "
            "OR product_type LIKE '%pro%' OR product_type LIKE '%ent%' THEN 0 ELSE 1 END), id "
            "LIMIT 1")
        row = cur.fetchone(); conn.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        print("[股票模块] 动态选扫描账号失败:", e)
        return None


def _load_scan_account():
    global _SCAN_ACCOUNT
    # 1) 管理员手动指定的扫描账号(若存在), 优先级最高
    try:
        conn = _scan_cfg_conn(); cur = conn.cursor()
        cur.execute("SELECT username FROM t_stock_scan_account WHERE id=1")
        row = cur.fetchone(); conn.close()
        if row and row[0]:
            _SCAN_ACCOUNT = row[0]
            return _SCAN_ACCOUNT
    except Exception:
        pass
    # 2) 动态从 tq_accounts 选有效账户(无默认)
    _SCAN_ACCOUNT = _pick_valid_account()
    return _SCAN_ACCOUNT


def _decrypt_tq(cipher):
    """用与 stock_main 完全相同的 Fernet 密钥解密天勤密码(密钥取 env TQ_PASS_KEY, 缺省开发默认值)。"""
    if not cipher:
        return ''
    import os, hashlib, base64
    from cryptography.fernet import Fernet
    raw = os.environ.get('TQ_PASS_KEY') or 'stock-dev-default-passkey-change-me'
    key = base64.urlsafe_b64encode(hashlib.sha256(raw.encode()).digest())
    try:
        return Fernet(key).decrypt(cipher.encode()).decode()
    except Exception:
        return cipher


def _load_scan_credential(user):
    """从 tq_accounts 读取扫描账号(如 Mr qia)的加密密码并解密, 存入 _ACCT_CRED。

    使 stock_app 重启后无需用户先浏览一次即可主动拿到有 A 股权限的账号凭据,
    从而浏览 K 线 / 全市场扫描都能稳定取到 A 股数据。"""
    if not user:
        return
    try:
        conn = _scan_cfg_conn(); cur = conn.cursor()
        cur.execute("SELECT tq_password FROM tq_accounts WHERE tq_username=%s", (user,))
        row = cur.fetchone(); conn.close()
        if row and row[0]:
            pwd = _decrypt_tq(row[0])
            if pwd:
                remember_account(user, pwd)
                if _MAKE_SLOT is not None:
                    try:
                        _MAKE_SLOT(user, pwd)
                    except Exception:
                        pass
    except Exception as e:
        print(f"[股票模块] 加载扫描账号 {user} 凭据失败: {e}")


def get_scan_account():
    """当前生效的 A 股扫描账号。"""
    return _SCAN_ACCOUNT or _load_scan_account()


def set_scan_account(username):
    """配置 A 股扫描账号。"""
    username = (username or "").strip()
    if not username:
        return {"ok": False, "error": "账号不能为空"}
    _ensure_scan_cfg_table()
    try:
        conn = _scan_cfg_conn(); cur = conn.cursor()
        cur.execute("""INSERT INTO t_stock_scan_account (id, username, update_time)
                       VALUES (1, %s, NOW())
                       ON DUPLICATE KEY UPDATE username=%s, update_time=NOW()""",
                    (username, username))
        conn.commit(); conn.close()
        global _SCAN_ACCOUNT
        _SCAN_ACCOUNT = username
        return {"ok": True, "account": username}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _get_scan_slot():
    """后台任务专用: 取"扫描账号"对应的 Slot, 绝不回落到默认账号。

    与 _get_slot() 的区别: _get_slot() 依赖 request 上下文, 在 daemon 线程里会
    回落 DEFAULT_ACCT_KEY(zhubajie), 而 zhubajie 没有 A 股权限 -> 遍历全空。"""
    user = get_scan_account()
    # 惰性加载扫描账号凭据(从 tq_accounts 解密密码), 使进程重启后也能自动拿到有 A 股权限的账号
    _load_scan_credential(user)
    # 1) 已在线的 Slot 直接复用
    with _ACCT_CRED_LOCK:
        pwd = _ACCT_CRED.get(user)
    if pwd and _SLOTS is not None:
        slot = _SLOTS.get(f"{user}:{pwd}")
        if slot is not None and getattr(slot, "status", "") == "online":
            return slot
    # 1.5) 遍历所有在线 Slot, 按用户名匹配(即便被动记忆的凭据缺失也能命中 Mr qia)
    if _SLOTS is not None:
        for s in _SLOTS.values():
            if getattr(s, "tq_user", "") == user and getattr(s, "status", "") == "online":
                return s
    # 2) 有凭据但未建 Slot -> 请求主线程建(与请求线程同路径)
    if pwd and _MAKE_SLOT is not None:
        try:
            return _MAKE_SLOT(user, pwd)
        except Exception as e:
            print(f"[股票模块] 扫描账号 {user} 建 Slot 失败: {e}")
            return None
    print(f"[股票模块] 未拿到扫描账号 {user} 的凭据"
          f"(请用该账号在页面浏览一次, 让网关注入凭据); 已知账号: {known_accounts()}")
    return None


def _cross_conn():
    from services import futures_db
    import pymysql
    cfg = futures_db.DB_CONFIG
    return pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                           password=cfg["password"], database=cfg["database"], connect_timeout=5)


def _ensure_cross_table():
    try:
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS t_stock_cross_cache (
            symbol VARCHAR(32) NOT NULL PRIMARY KEY,
            w_golden VARCHAR(20), w_golden_idx INT, w_death VARCHAR(20), w_recent TINYINT DEFAULT 0,
            m_golden VARCHAR(20), m_golden_idx INT, m_death VARCHAR(20), m_recent TINYINT DEFAULT 0,
            updated_at DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        conn.commit(); conn.close()
        _ensure_cross_table_minute()
        return True
    except Exception as e:
        print("[股票模块] 金叉缓存表初始化失败:", e); return False


def _ensure_cross_table_minute():
    """幂等补齐分钟级字段(min_*)与日线字段(d_*)。不影响既有 w_/m_ 列与全局功能。"""
    cols = ["d_golden VARCHAR(20)", "d_golden_idx INT", "d_death VARCHAR(20)",
            "d_death_idx INT", "d_recent TINYINT DEFAULT 0",
            "min_golden VARCHAR(20)", "min_golden_idx INT", "min_death VARCHAR(20)",
            "min_death_idx INT", "min_recent TINYINT DEFAULT 0"]
    try:
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute("SHOW COLUMNS FROM t_stock_cross_cache")
        exist = {r[0] for r in cur.fetchall()}
        for i, c in enumerate(cols):
            name = c.split()[0]
            if name not in exist:
                cur.execute(f"ALTER TABLE t_stock_cross_cache ADD COLUMN {c} AFTER m_recent")
                print(f"[股票模块] 已补齐分钟字段: {name}")
        conn.commit(); conn.close()
    except Exception as e:
        print("[股票模块] 分钟字段补齐失败(可忽略, 引擎将降级):", e)


def _ensure_cross_table_live():
    """幂等补齐 live_state 列(2026-09-29 实时当前K线判定)。

    live_state = JSON {"d":{...},"w":{...},"m":{...}}, 每周期存:
      p=[ema12,ema26,dea]  上一根【完成】K线后的 EMA 基态
      c=[ema12,ema26,dea]  最近一根K线(扫描时刻)后的 EMA 状态
      dt=最近一根K线的日期标签(日=当日/周=周内最后交易日/月=月内最后交易日)
    监控刷新时用实时报价递推当前K线的 DIF/DEA 判定 dif>dea, 无需重算整条K线。"""
    try:
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute("SHOW COLUMNS FROM t_stock_cross_cache")
        exist = {r[0] for r in cur.fetchall()}
        if "live_state" not in exist:
            cur.execute("ALTER TABLE t_stock_cross_cache ADD COLUMN live_state TEXT")
            print("[股票模块] 已补齐实时判定字段: live_state")
        conn.commit(); conn.close()
    except Exception as e:
        print("[股票模块] live_state 字段补齐失败(引擎将回落交叉日期口径):", e)


def _load_cross_cache():
    try:
        conn = _cross_conn(); cur = conn.cursor()
        base_sel = ("SELECT symbol,"
                    "w_golden,w_golden_idx,w_death,w_recent,"
                    "m_golden,m_golden_idx,m_death,m_recent,"
                    "d_golden,d_golden_idx,d_death,d_recent,"
                    "min_golden,min_golden_idx,min_death,min_death_idx,min_recent")
        try:
            cur.execute(base_sel + ",live_state FROM t_stock_cross_cache")
        except Exception:
            cur.execute(base_sel + " FROM t_stock_cross_cache")   # 老库无 live_state 列 -> 降级
        rows = cur.fetchall(); conn.close()
        n_col = len(rows[0]) if rows else 18
        d = {}
        for r in rows:
            d[r[0]] = {"w_golden": r[1], "w_golden_idx": r[2], "w_death": r[3], "w_recent": bool(r[4]),
                       "m_golden": r[5], "m_golden_idx": r[6], "m_death": r[7], "m_recent": bool(r[8]),
                       "d_golden": r[9], "d_golden_idx": r[10], "d_death": r[11], "d_recent": bool(r[12]),
                       "min_golden": r[13], "min_golden_idx": r[14], "min_death": r[15],
                       "min_death_idx": r[16], "min_recent": bool(r[17])}
            if n_col > 18:
                d[r[0]]["live_state"] = r[18]
        with _CROSS_LOCK:
            _CROSS_CACHE.clear(); _CROSS_CACHE.update(d)
        print(f"[股票模块] 金叉缓存载入: {len(d)} 支")
        return len(d)
    except Exception as e:
        print("[股票模块] 加载金叉缓存失败:", e); return 0


def _scan_one(symbol):
    """计算某股票 周/月/日 MACD 最近一次金叉信息, 并把金叉/死叉事件沉淀到事件表。
    数据源=腾讯 tx(2026-09-28 迁移, 不依赖天勤扫描账号)。返回 {week:{...}, month:{...}, day:{...}} 或 None。"""
    out = {}
    for period in ("week", "month", "day"):
        cfg = PERIODS[period]
        try:
            df = _tx_kline_df(symbol, period, cfg["count"])
        except Exception:
            df = None
        if df is None or getattr(df, "empty", True):
            out[period] = {"ok": False}; continue
        close = df["close"].astype(float).tolist()
        # 2026-09-30 修复(600072 中船科技误标"月:金叉"实锤): 腾讯 qfqmonth 直取偶发失败
        #   (fqkline 反爬 501/空)时 _tx_kline_df 会静默回落"800根日线重采样月线" —— 只有
        #   ~40 根(2023-06 起), MACD(12,26,9) 的 EMA 种子误差把 dif-dea 差距从真实 -0.25
        #   压缩到 -0.05, 当日大阳线即可假性翻转 dif/dea 上下关系 -> 写入假"本月金叉"毒化
        #   cross_cache(与 K线页直取 349 根的正确结果及 WH6 直接矛盾)。
        #   直取月线恒 >=200 根, 重采样月线恒 <=45 根 -> 扫描侧月线 <60 根一律拒绝写库。
        #   (周线重采样已 2026-09-28 实测与直取偏差 0, 不受影响。)
        _min_bars = 60 if period == "month" else 35
        if len(close) < _min_bars:
            out[period] = {"ok": False}; continue
        try:
            dif, dea, _bar = indicators.macd(close, 12, 26, 9)
            crosses = ta_patterns.detect_macd_cross(dif, dea, close)
            dts = futures_kline._dt_local(df["datetime"])
            # 2026-09-29: 采集 EMA 基态供监控「实时当前K线」递推判定。
            # 注意: 不能用 indicators.macd 的返回值当基态 —— to_list 会 round 到 4 位小数,
            # 累积递推会带 ~1e-5 偏差; 这里用 pandas 全精度序列(indicators.macd 同一套 ewm 递归)。
            cl = df["close"].astype(float)
            e12 = cl.ewm(span=12, adjust=False).mean()
            e26 = cl.ewm(span=26, adjust=False).mean()
            dea_fp = (e12 - e26).ewm(span=9, adjust=False).mean()
            live = None
            if len(close) >= 2:
                prev3 = [e12.iloc[-2], e26.iloc[-2], dea_fp.iloc[-2]]
                cur3 = [e12.iloc[-1], e26.iloc[-1], dea_fp.iloc[-1]]
                if all(x is not None and x == x for x in prev3 + cur3):
                    live = {"p": [float(x) for x in prev3], "c": [float(x) for x in cur3],
                            "pc": float(close[-1]),
                            "dt": dts[-1].strftime("%Y-%m-%d") if dts and dts[-1] is not None else ""}
        except Exception:
            out[period] = {"ok": False}; continue
        golds = [c for c in crosses if c["type"] == "golden"]
        deaths = [c for c in crosses if c["type"] == "death"]
        n = len(close)
        # 2026-09-29 口径收口(需求人最终澄清): 监控「金叉」标签只标【当前这根K线内刚发生金叉】(历史金叉不标)。
        # gxd = 最近一次金叉距扫描时末根K线的根数(0=末根当根刚金叉); 查询时跨入新K线再 +1。
        gxd = (n - 1 - max(golds, key=lambda c: c["idx"])["idx"]) if golds else -1
        if live is not None:
            live["gxd"] = gxd
        rec_n = 12 if period == "week" else (6 if period == "month" else 20)
        rec_crosses = []
        dd = dts[deaths[-1]["idx"]].strftime(cfg["fmt"]) if deaths and 0 <= deaths[-1]["idx"] < len(dts) else ""
        if golds:
            g = max(golds, key=lambda c: c["idx"])
            gd = dts[g["idx"]].strftime(cfg["fmt"]) if 0 <= g["idx"] < len(dts) else ""
            recent = 1 if (n - g["idx"]) <= rec_n else 0
            out[period] = {"ok": True, "golden_date": gd, "golden_idx": g["idx"], "recent": recent,
                           "death_date": dd, "live": live}
            gi = g["idx"]
            rec_crosses.append({"type": "golden", "date": gd,
                                "ts": (dts[gi] if 0 <= gi < len(dts) else None),
                                "price": close[gi], "dif": dif[gi], "dea": dea[gi]})
        else:
            out[period] = {"ok": True, "golden_date": "", "golden_idx": -1, "recent": 0,
                           "death_date": dd, "live": live}
        if deaths:
            d = deaths[-1]
            di = d["idx"]
            rec_crosses.append({"type": "death", "date": dd,
                                "ts": (dts[di] if 0 <= di < len(dts) else None),
                                "price": close[di], "dif": dif[di], "dea": dea[di]})
        if rec_crosses:
            try:
                _record_events(symbol, period, rec_crosses)
            except Exception:
                pass
    return out if out else None


def _upsert_cross(symbol, res):
    try:
        conn = _cross_conn(); cur = conn.cursor()
        w = res.get("week", {}); m = res.get("month", {}); d = res.get("day", {})
        # 仅对"确有数据"的周期写入, 避免某次扫描因账户/网络临时失败(df 为空)时,
        # 把之前已算出的金叉标记(recent)清零 —— 否则日线等新增周期报警几乎永远不会触发。
        cols = ["symbol"]; vparts = ["%s"]; vals = [symbol]; dup = []
        def _add(p, pfx):
            if p.get("golden_date") or p.get("death_date"):
                cols.append(f"{pfx}_golden"); vals.append(p.get("golden_date") or None); vparts.append("%s")
                cols.append(f"{pfx}_golden_idx"); vals.append(p.get("golden_idx")); vparts.append("%s")
                cols.append(f"{pfx}_death"); vals.append(p.get("death_date") or None); vparts.append("%s")
                cols.append(f"{pfx}_recent"); vals.append(int(p.get("recent", 0) or 0)); vparts.append("%s")
                dup.append(f"{pfx}_golden=VALUES({pfx}_golden),{pfx}_golden_idx=VALUES({pfx}_golden_idx),{pfx}_death=VALUES({pfx}_death),{pfx}_recent=VALUES({pfx}_recent)")
        _add(w, "w"); _add(m, "m"); _add(d, "d")
        # live_state 合并写入(2026-09-29): 仅覆盖本次"确有数据"的周期, 其余沿用旧值;
        # 老库无该列时 SELECT/INSERT 会抛异常 -> 静默降级(监控回落交叉日期口径)。
        live = {}
        for p in ("week", "month", "day"):
            lv = (res.get(p) or {}).get("live")
            if lv:
                live[p[0]] = lv
        if live:
            try:
                cur.execute("SELECT live_state FROM t_stock_cross_cache WHERE symbol=%s", (symbol,))
                row = cur.fetchone()
                old = {}
                if row and row[0]:
                    try:
                        old = json.loads(row[0])
                    except Exception:
                        old = {}
                old.update(live)
                cols.append("live_state"); vals.append(json.dumps(old, ensure_ascii=False)); vparts.append("%s")
                dup.append("live_state=VALUES(live_state)")
            except Exception:
                pass
        cols.append("updated_at"); vparts.append("NOW()")
        # 注意: 必须用 ON DUPLICATE KEY UPDATE 而非 REPLACE, 否则会清空分钟引擎写的 min_* 列
        # 2026-09-30 修复: 全周期无数据(次新股<35根)时 dup 为空, 旧拼接在 ON DUPLICATE 子句
        #   末尾留下悬空逗号 -> MySQL 1064 -> 行永远写不进去(605577/宇树科技等新股监控行无状态)。
        #   旧代码(回滚用):
        #   cur.execute(
        #       "INSERT INTO t_stock_cross_cache (%s) VALUES (%s) ON DUPLICATE KEY UPDATE updated_at=NOW(),%s"
        #       % (",".join(cols), ",".join(vparts), ",".join(dup)), vals)
        _sql = ("INSERT INTO t_stock_cross_cache (%s) VALUES (%s) ON DUPLICATE KEY UPDATE updated_at=NOW()"
                % (",".join(cols), ",".join(vparts)))
        if dup:
            _sql += "," + ",".join(dup)
        cur.execute(_sql, vals)
        conn.commit(); conn.close()
        with _CROSS_LOCK:
            _CROSS_CACHE[symbol] = {
                "w_golden": w.get("golden_date"), "w_golden_idx": w.get("golden_idx"),
                "w_death": w.get("death_date"), "w_recent": bool(w.get("recent", 0)),
                "m_golden": m.get("golden_date"), "m_golden_idx": m.get("golden_idx"),
                "m_death": m.get("death_date"), "m_recent": bool(m.get("recent", 0)),
                "d_golden": d.get("golden_date"), "d_golden_idx": d.get("golden_idx"),
                "d_death": d.get("death_date"), "d_recent": bool(d.get("recent", 0))}
            # 同步 live_state(合并旧值), 监控接口无需重读库即可拿到最新基态
            if live:
                try:
                    prev_raw = (_CROSS_CACHE.get(symbol) or {}).get("live_state")
                    merged = json.loads(prev_raw) if prev_raw else {}
                    merged.update(live)
                    _CROSS_CACHE[symbol]["live_state"] = json.dumps(merged, ensure_ascii=False)
                except Exception:
                    pass
    except Exception as e:
        print("[股票模块] 写入金叉缓存失败", symbol, e)


def _ensure_event_table():
    try:
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS t_stock_cross_event (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(32) NOT NULL,
            period VARCHAR(8) NOT NULL,
            cross_type VARCHAR(8) NOT NULL,
            cross_date VARCHAR(20) NOT NULL,
            price DECIMAL(18,4) DEFAULT NULL,
            dif DOUBLE DEFAULT NULL,
            dea DOUBLE DEFAULT NULL,
            created_at DATETIME DEFAULT NULL,
            ts DATETIME DEFAULT NULL,
            src VARCHAR(8) NOT NULL DEFAULT 'scan',
            UNIQUE KEY uq_evt (symbol, period, cross_type, cross_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        # 幂等补列/索引: ts(精确交叉时刻) + src(scan=回溯沉淀 / rt=实时精确检测)
        cur.execute("SHOW COLUMNS FROM t_stock_cross_event")
        exist = {r[0] for r in cur.fetchall()}
        if "ts" not in exist:
            cur.execute("ALTER TABLE t_stock_cross_event ADD COLUMN ts DATETIME DEFAULT NULL")
        if "src" not in exist:
            cur.execute("ALTER TABLE t_stock_cross_event ADD COLUMN src VARCHAR(8) NOT NULL DEFAULT 'scan'")
        cur.execute("SHOW INDEX FROM t_stock_cross_event")
        idxs = {r[2] for r in cur.fetchall()}
        if "idx_evt_ts" not in idxs:
            cur.execute("CREATE INDEX idx_evt_ts ON t_stock_cross_event(symbol, period, cross_type, ts)")
        # 历史回填: 老记录无 ts(精确时刻) 时, 从 cross_date 解析补上(幂等, 仅处理可解析行)
        cur.execute("""UPDATE t_stock_cross_event SET ts = STR_TO_DATE(cross_date, '%Y-%m-%d %H:%i')
                       WHERE ts IS NULL AND cross_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}'""")
        cur.execute("""UPDATE t_stock_cross_event SET ts = STR_TO_DATE(cross_date, '%Y-%m-%d')
                       WHERE ts IS NULL AND cross_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'""")
        cur.execute("""UPDATE t_stock_cross_event SET ts = STR_TO_DATE(cross_date, '%Y-%m')
                       WHERE ts IS NULL AND cross_date REGEXP '^[0-9]{4}-[0-9]{2}$'""")
        conn.commit(); conn.close(); return True
    except Exception as e:
        print("[股票模块] 金叉事件表初始化失败:", e); return False


def _merge_rt_crosses(symbol, period, cross_rows, dts, dur_sec, fmt):
    """把库里 src='rt' 的实时精确交叉合并进 K 线展示用的 cross_rows:
    rt 记录 ts 落进某根 bar 窗口 [bar_i, bar_i+dur) 时, 若该 bar 已有同类型回溯交叉行则用
    rt 精确时间替换该行 date/ts(标记 rt=true); 否则保留回溯行(历史照旧)。
    这样盘中实时检测到的金叉(如 m30 的 14:15)在图上/列表展示时以精确位置为准。"""
    if not cross_rows or not dur_sec or dts is None or len(dts) == 0:
        return
    try:
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute("SELECT cross_type, ts FROM t_stock_cross_event "
                    "WHERE symbol=%s AND period=%s AND src='rt' AND ts IS NOT NULL "
                    "ORDER BY ts", (symbol, period))
        rt_rows = [(r[0], r[1]) for r in cur.fetchall()]
        conn.close()
    except Exception:
        return
    if not rt_rows:
        return
    n = len(dts)
    try:
        ends = []
        for i in range(n):
            ends.append(dts[i] + timedelta(seconds=dur_sec))
    except Exception:
        return
    by_idx_type = {}
    for r in cross_rows:
        by_idx_type.setdefault((r.get("idx"), r.get("type")), r)
    now = datetime.now()
    for ctype, ts in rt_rows:
        if ts is None:
            continue
        # 找 rt 所属 bar 窗口
        bar_idx = None
        for i in range(n):
            if dts[i] <= ts < ends[i]:
                bar_idx = i
                break
        if bar_idx is None:
            continue                      # 超出本批 K 线范围
        row = by_idx_type.get((bar_idx, ctype))
        if row is not None:
            # 同 bar 同类型回溯行存在 -> 打上实时精确标记(rt_ts 精确到秒; date 保留 bar 起点,
            # 供前端图表/双击定位用 categories 匹配, 展示文本优先用 rt_ts)
            row["rt"] = True
            row["rt_ts"] = ts.strftime("%Y-%m-%d %H:%M:%S")
    return


def _record_tail_rt(symbol, period, crosses, dts, dur_sec):
    """实时精确记录(2026-09-09 需求): 若最新交叉落在「最后一根 forming bar」上
    (idx==n-1 且该周期 bar 尚未走完), 说明是盘中实时出现的金叉/死叉 ——
    用 src='rt' 记录检测时刻 ts=now(如 30 分钟线 14:15 出现即记 14:15, 而非收盘后的 bar 对齐点)。

    后续 scan 回溯(_record_events 内 skip-if-rt)会对同一 bar 窗自动跳过, 保证库里一个金叉只有
    「实时精确条目」权威存在, 不出现"实时 14:15 / 回溯 14:00"两条漂移记录。"""
    if not crosses or not dur_sec or dts is None or len(dts) == 0:
        return False
    n_last = len(dts)
    try:
        last_dt = dts.iloc[-1]
    except Exception:
        try:
            last_dt = dts[-1]
        except Exception:
            return False
    if last_dt is None:
        return False
    if not isinstance(last_dt, datetime):
        try:
            last_dt = datetime.strptime(str(last_dt)[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                last_dt = datetime.strptime(str(last_dt)[:16], "%Y-%m-%d %H:%M")
            except Exception:
                return False
    now = datetime.now()
    try:
        if now >= last_dt + timedelta(seconds=dur_sec):
            return False          # 最后一根 bar 已收盘: 属稳定历史, 交给 scan 回溯即可
        latest = max(crosses, key=lambda c: c.get("idx", -1))
        if latest.get("idx") != n_last - 1:
            return False          # 最新交叉不落在 forming bar 上(旧交叉, 已由 scan 沉淀)
    except Exception:
        return False
    try:
        _record_events(symbol, period,
                       [{"type": latest["type"], "ts": now,
                         "price": latest.get("price"),
                         "dif": latest.get("dif"), "dea": latest.get("dea")}],
                       src="rt")
        return True
    except Exception:
        return False


def _record_events(symbol, period, crosses, src='scan'):
    """把一批金叉/死叉事件写入 t_stock_cross_event (按唯一键去重)。

    每次构建 K 线(api_stock_kline) 或 全市场扫描(_scan_one) 时调用,
    用户浏览 + 定时扫描会自然把各周期金叉事件沉淀下来, 供首页"最近一周金叉记录"查询。

    src: 'scan'=回溯沉淀(bar 起点时刻); 'rt'=实时精确检测(ts=检测到的精确时刻)。
    回溯写库前做"实时记录优先": 若同 symbol/period/type 在该 bar 时间窗内已有 src='rt'
    的精确记录, 则跳过该条回溯(避免同一金叉出现"实时 14:15 / 回溯 14:00"两条漂移记录)。
    """
    if not crosses:
        return
    dur = PERIODS.get(period, {}).get("duration")

    def _to_dt(v):
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    return datetime.strptime(v, f)
                except Exception:
                    continue
        return None

    rows = []
    for c in crosses:
        ct = c.get("type")
        ts = _to_dt(c.get("ts") or c.get("date"))
        if ct not in ("golden", "death") or ts is None:
            continue
        rows.append((symbol, period, ct, ts.strftime("%Y-%m-%d %H:%M"),
                     _r(c.get("price")), _r(c.get("dif")), _r(c.get("dea")), ts))
    if not rows:
        return
    try:
        conn = _cross_conn(); cur = conn.cursor()
        rt_list = []
        try:
            cur.execute("SELECT cross_type, ts FROM t_stock_cross_event "
                        "WHERE symbol=%s AND period=%s AND src='rt' AND ts IS NOT NULL",
                        (symbol, period))
            rt_list = [(r[0], r[1]) for r in cur.fetchall()]
        except Exception:
            rt_list = []
        final = []
        for (sym, p, ct, cd, price, dif, dea, ts) in rows:
            hit = False
            if rt_list and dur:
                for (rct, rts) in rt_list:
                    if rct == ct and (rts - ts).total_seconds() >= 0 and (rts - ts).total_seconds() < dur:
                        hit = True
                        break
            if not hit:
                final.append((sym, p, ct, cd, price, dif, dea, ts))
        if final:
            cur.executemany(
                """INSERT INTO t_stock_cross_event
                   (symbol, period, cross_type, cross_date, price, dif, dea, created_at, ts, src)
                   VALUES (%s,%s,%s,%s,%s,%s,%s, NOW(), %s, %s)
                   ON DUPLICATE KEY UPDATE price=VALUES(price), dif=VALUES(dif),
                                           dea=VALUES(dea), created_at=NOW(), ts=VALUES(ts), src=VALUES(src)""",
                [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], src) for r in final])
        conn.commit(); conn.close()
    except Exception as e:
        print("[股票模块] 写入金叉事件失败", symbol, period, e)


def api_recent_crosses():
    """首页"最近一周金叉记录": 返回最近 N 天内的金叉事件。

    排序/过滤以精确时刻 ts 为准(实时记录 src='rt' 的检测时刻; 回溯 scan 为 bar 起点),
    无 ts 的旧行回退用 cross_date。返回项附带 ts 与 src, 供前端区分/展示精确时间。"""
    days = request.args.get("days", 7, type=int)
    days = max(1, min(60, days))
    try:
        # 用 Python 算 cutoff 日期, 避免 SQL % 占位符与日期格式串冲突
        cutoff_dt = (datetime.now() - timedelta(days=days))
        cutoff = cutoff_dt.strftime('%Y-%m-%d')
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute("""SELECT e.symbol, e.period, e.cross_type, e.cross_date,
                              e.price, s.name, e.ts, e.src
                       FROM t_stock_cross_event e
                       LEFT JOIN t_stock_list s ON s.symbol = e.symbol
                       WHERE e.cross_type='golden'
                         AND ((e.ts IS NOT NULL AND e.ts >= %s) OR (e.ts IS NULL AND e.cross_date >= %s))
                       ORDER BY COALESCE(e.ts, e.cross_date) DESC LIMIT 300""", (cutoff, cutoff))
        rows = cur.fetchall(); conn.close()
        items = [{"symbol": r[0], "period": r[1], "type": r[2], "date": r[3],
                  "price": (float(r[4]) if r[4] is not None else None),
                  "name": (r[5] or r[0]),
                  "ts": (r[6].strftime("%Y-%m-%d %H:%M:%S") if r[6] else None),
                  "src": r[7] or "scan"} for r in rows]
        return jsonify({"ok": True, "days": days, "count": len(items), "items": items})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def api_recent_crosses_for():
    """分钟金叉触发时, 关联展示该股票各周期(日/周/月/分钟/盘中各档)的近期金叉。

    各周期"近期"窗口(由服务端设定, 与用户需求对齐):
      - 分钟: 近 30 根 1 分钟 K 线内(≈30 分钟)
      - 5分:  近 1 天
      - 15分: 近 2 天
      - 30分: 近 3 天
      - 60分: 近 5 天
      - 日线: 近 90 天
      - 周线: 近 365 天
      - 月线: 近 730 天(2 年)
    返回每周期的 最近金叉日期 / 是否近期 / 提示说明, 供前端"在旁边"汇总展示。"""
    symbol = (request.args.get("symbol") or "").strip()
    if not symbol:
        return jsonify({"ok": False, "error": "missing symbol"}), 400
    # period -> (标签, 近期窗口天数, 提示文案); minute 用 bars
    # 聚焦用户关注的 日/周/月/分钟 四档(盘中各分钟档 m5/m15/m30/m60 不单独扫描, 故不列入)
    WINDOWS = {
        "day":   {"label": "日线", "days": 90,   "hint": "近90天内"},
        "week":  {"label": "周线", "days": 365,  "hint": "近1年内"},
        "month": {"label": "月线", "days": 730,  "hint": "近2年内"},
        "minute":{"label": "分钟", "bars": 30,   "hint": "近30根1分钟K线内"},
    }
    try:
        conn = _cross_conn(); cur = conn.cursor()
        # 每周期最近的一次金叉
        cur.execute("""SELECT period, cross_date, price, dif, dea
                       FROM t_stock_cross_event
                       WHERE symbol=%s AND cross_type='golden'
                       ORDER BY cross_date DESC""", (symbol,))
        rows = cur.fetchall()
        # 每周期最近的一次死叉(仅用于对照展示)
        cur.execute("""SELECT period, cross_date
                       FROM t_stock_cross_event
                       WHERE symbol=%s AND cross_type='death'
                       ORDER BY cross_date DESC""", (symbol,))
        deaths = {r[0]: r[1] for r in cur.fetchall()}
        conn.close()

        latest = {}
        for r in rows:
            p = r[0]
            if p not in latest:
                latest[p] = {"date": r[1], "price": (float(r[2]) if r[2] is not None else None),
                             "dif": r[3], "dea": r[4]}

        name = symbol
        with _STOCK_LOCK:
            name = (_STOCK_MAP.get(symbol, {}) or {}).get("name") or symbol

        now = datetime.now()
        today = date.today()
        periods = []
        for p, cfg in WINDOWS.items():
            g = latest.get(p)
            if not g:
                periods.append({"period": p, "label": cfg["label"], "has": False,
                                "recent": False, "date": "", "death_date": deaths.get(p, ""),
                                "hint": cfg["hint"], "price": None})
                continue
            gd = g["date"] or ""
            recent = False
            if p == "minute":
                # 分钟金叉精确到分钟, 用时间差判定(近 30 分钟)
                try:
                    gdt = datetime.strptime(gd, "%Y-%m-%d %H:%M")
                    recent = (now - gdt).total_seconds() <= cfg["bars"] * 60
                except Exception:
                    recent = False
            else:
                # 日/周/月/盘中各档用日期差判定
                try:
                    if len(gd) >= 10:
                        cdt = datetime.strptime(gd[:10], "%Y-%m-%d").date()
                    else:
                        cdt = datetime.strptime(gd[:7], "%Y-%m").date()
                    recent = (today - cdt).days <= cfg["days"]
                except Exception:
                    recent = False
            periods.append({"period": p, "label": cfg["label"], "has": True, "recent": recent,
                            "date": gd, "death_date": deaths.get(p, ""), "hint": cfg["hint"],
                            "price": g["price"]})
        # 排序: 日 / 周 / 月 / 分钟
        order = ["day", "week", "month", "minute"]
        periods.sort(key=lambda x: order.index(x["period"]) if x["period"] in order else 99)
        return jsonify({"ok": True, "symbol": symbol, "name": name, "periods": periods})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _cross_state(c, period):
    """某周期当前状态: "金叉" / "死叉" / ""(无数据)。

    判据 = 最近一次交叉的类型(比较金叉/死叉日期字符串, 两种日期格式都可字典序比较)。
    """
    m = {"week": ("w_golden", "w_death"), "month": ("m_golden", "m_death"),
         "day": ("d_golden", "d_death"), "minute": ("min_golden", "min_death")}.get(period)
    if not m:
        return ""
    g = str(c.get(m[0]) or "")
    d = str(c.get(m[1]) or "")
    if not g and not d:
        return ""
    if not d:
        return "金叉"
    if not g:
        return "死叉"
    return "金叉" if g >= d else "死叉"


def _golden_today(c, td):
    """该股今天是否出现过金叉(任一周期); td = 'YYYY-MM-DD'。月线按"本月"判定。"""
    for k in ("d_golden", "w_golden", "min_golden"):
        v = str(c.get(k) or "")
        if v and v[:10] == td:
            return True
    mg = str(c.get("m_golden") or "")
    return bool(mg) and mg[:7] == td[:7]


# ---- 实时「当前K线」判定(2026-09-29, 需求人口径: 实时=只看当前K线及其指标) ----
#   月/周/日各取【此刻正在走的那根K线】(今日日K/本周周K/本月月K, 带实时价)算 MACD, dif>dea=金叉。
#   判定不重算整条K线: EMA(adjust=False) 是递推式 —— 已知上一根完成K线的 ema12/ema26/dea 基态,
#   代入实时价一步递推即得当前K线的精确 DIF/DEA(与 indicators.macd 全量重算数学等价, 已抽样对拍)。

_LIVE_PERIOD_KEY = {"day": "d", "week": "w", "month": "m"}


def _iso_week(dstr):
    """'YYYY-MM-DD' -> (ISO年, ISO周), 用于判定是否同一根周K。"""
    try:
        return datetime.strptime(dstr[:10], "%Y-%m-%d").isocalendar()[:2]
    except Exception:
        return None


def _live_golden(lv, period, price, now=None):
    """用实时价递推当前K线的 MACD 状态。

    lv = 解析后的 live_state JSON; period ∈ day/week/month; price = 此刻最新价。
    返回 True/False(可判定 dif>dea) 或 None(无基态/数据异常, 调用方回落交叉日期口径)。"""
    if not lv or price is None or price <= 0:
        return None
    st = lv.get(_LIVE_PERIOD_KEY.get(period) or "")
    if not st:
        return None
    try:
        p_s, c_s, dt = st.get("p"), st.get("c"), str(st.get("dt") or "")
        if not p_s or not c_s or len(p_s) != 3 or len(c_s) != 3:
            return None
        now = now or datetime.now()
        if period == "day":
            same = dt[:10] == now.strftime("%Y-%m-%d")
        elif period == "week":
            same = bool(dt) and _iso_week(dt) == _iso_week(now.strftime("%Y-%m-%d"))
        else:
            same = dt[:7] == now.strftime("%Y-%m")
        base = p_s if same else c_s        # 同一根K线 -> 用上一根完成K线的基态; 跨入新K线 -> 用最近一根的状态
        e12, e26, dea = (float(x) for x in base)
        a1, a2, a3 = 2.0 / 13.0, 2.0 / 27.0, 2.0 / 10.0
        n12 = a1 * price + (1 - a1) * e12
        n26 = a2 * price + (1 - a2) * e26
        dif = n12 - n26
        ndea = a3 * dif + (1 - a3) * dea
        return dif > ndea
    except Exception:
        return None


def _same_bucket(dt, period, now):
    """扫描时末根K线的日期标签 dt 与 now 是否仍在同一根K线内。"""
    dt = str(dt or "")
    if not dt:
        return False
    if period == "day":
        return dt[:10] == now.strftime("%Y-%m-%d")
    if period == "week":
        return _iso_week(dt) == _iso_week(now.strftime("%Y-%m-%d"))
    return dt[:7] == now.strftime("%Y-%m")


# 监控「金叉」新鲜度口径(2026-09-30 需求人最终裁定: 实时只保留【当前这根K线】):
#   「周线金叉」等标签只标【当前正在走的这根K线内刚发生金叉】的标的 —— 即金叉必须发生在
#   本周/本月/今日的那一根K线里; 上一根或更早的金叉(如华电国际 9-04、600135 9-21)一律不再标。
# 主路径: 扫描落库 gxd(最近金叉距当时末根的根数), 查询时已跨入新K线则 +1;
#   判定 = gxd + (跨桶?1:0) <= GOLDEN_RECENT_BARS; GOLDEN_RECENT_BARS=0 → 仅 gxd=0 且仍在当前桶才标。
# 回落(老数据无 gxd / live_state 空): 严格按金叉日期所在【当前】桶判定(日=今天, 周=本周, 月=本月),
#   不再放宽到"上周/上月"——这是 600135(9-21, 属上周)被误标的历史根因。
GOLDEN_RECENT_BARS = 0


def _golden_recent(lv, c, period, now):
    """该周期最近一次金叉是否发生在最近 GOLDEN_RECENT_BARS 根K线内(布尔, 回落日期桶口径)。"""
    st = (lv or {}).get(_LIVE_PERIOD_KEY.get(period) or "")
    if st and st.get("gxd") is not None:
        try:
            eff = int(st["gxd"]) + (0 if _same_bucket(st.get("dt"), period, now) else 1)
            return eff <= GOLDEN_RECENT_BARS
        except Exception:
            pass
    gfield = {"week": "w_golden", "month": "m_golden", "day": "d_golden",
              "minute": "min_golden"}.get(period)
    gd = str((c or {}).get(gfield) or "")[:10]
    if not gd:
        return False
    td = now.strftime("%Y-%m-%d")
    if period in ("day", "minute"):
        return gd == td
    if period == "week":
        return _iso_week(gd) == _iso_week(td)
    return gd[:7] == td[:7]


def _rt_cross_chip(lv, c, period, price, now=None):
    """【全实时】某周期「当前K线内刚发生」的交叉事件 chip: "金叉" / "死叉" / ""(本根无交叉)。

    2026-09-30 需求人裁定: 监控页状态列全部实时算 —— 不用缓存交叉日期的"最近交叉类型"
    (持仓态语义), 那会把 6 月的周金叉(至今未死叉)一直标成 周:金叉(浦发银行 600000 实锤)。
    主路径: live_state EMA 基态(= 当前K线的前一根完成K线收盘) + 实时价一步递推当前K线
      dif/dea —— 前收 dif<=dea 且实时 dif>dea = 当前K线内刚金叉; 反向穿越 = 刚死叉。
    回落(无基态/无报价/分钟周期): 交叉日期落在当前桶(日=今天/周=本周/月=本月)才标;
      同桶金叉死叉并存取较晚者(最后一次事件)。
    """
    m = {"week": ("w_golden", "w_death"), "month": ("m_golden", "m_death"),
         "day": ("d_golden", "d_death"), "minute": ("min_golden", "min_death")}.get(period)
    if not m:
        return ""
    st = (lv or {}).get(_LIVE_PERIOD_KEY.get(period) or "")
    if st and st.get("p") and st.get("c") and price:
        try:
            now = now or datetime.now()
            dt = str(st.get("dt") or "")
            if period == "day":
                same = dt[:10] == now.strftime("%Y-%m-%d")
            elif period == "week":
                same = bool(dt) and _iso_week(dt) == _iso_week(now.strftime("%Y-%m-%d"))
            else:
                same = dt[:7] == now.strftime("%Y-%m")
            e12, e26, dea0 = (float(x) for x in (st["p"] if same else st["c"]))
            if not same:
                # 扫描时那根K线现已完成: 用其收盘价(pc)把基态精确滚算到「当前K线的前一根收盘」,
                # 消除旧实现直接拿 c(扫描时盘中态)当基态的近似误差。
                pc = st.get("pc")
                if pc:
                    _a1, _a2, _a3 = 2.0 / 13.0, 2.0 / 27.0, 2.0 / 10.0
                    e12 = _a1 * float(pc) + (1 - _a1) * e12
                    e26 = _a2 * float(pc) + (1 - _a2) * e26
                    dea0 = _a3 * (e12 - e26) + (1 - _a3) * dea0
            dif0 = e12 - e26                       # 前一根完成K线收盘的 dif
            a1, a2, a3 = 2.0 / 13.0, 2.0 / 27.0, 2.0 / 10.0
            n12 = a1 * price + (1 - a1) * e12
            n26 = a2 * price + (1 - a2) * e26
            dif1 = n12 - n26                       # 实时 dif
            ndea = a3 * dif1 + (1 - a3) * dea0     # 实时 dea
            if dif0 <= dea0 and dif1 > ndea:
                return "金叉"
            if dif0 >= dea0 and dif1 < ndea:
                return "死叉"
            return ""
        except Exception:
            pass
    gd = str(c.get(m[0]) or "")
    dd = str(c.get(m[1]) or "")
    if not gd and not dd:
        return ""
    td = (now or datetime.now()).strftime("%Y-%m-%d")
    if period in ("day", "minute"):
        g_in = gd[:10] == td
        d_in = dd[:10] == td
    elif period == "week":
        g_in = bool(gd) and _iso_week(gd) == _iso_week(td)
        d_in = bool(dd) and _iso_week(dd) == _iso_week(td)
    else:
        g_in = gd[:7] == td[:7]
        d_in = dd[:7] == td[:7]
    if g_in and d_in:
        return "金叉" if gd >= dd else "死叉"
    if g_in:
        return "金叉"
    if d_in:
        return "死叉"
    return ""


_QSNAP = {"ts": 0.0, "data": {}, "lock": threading.Lock()}
_QSNAP_TTL = 10.0   # 全市场报价快照 TTL(秒): 监控 3s 轮询共享一份, 避免每次刷新都打 112 个批量请求


def _quote_snapshot(symbols):
    """全市场实时价快照 {symbol: last}, TTL 内复用。失败返回 {}(调用方回落缓存口径)。"""
    now = time.time()
    with _QSNAP["lock"]:
        if _QSNAP["data"] and now - _QSNAP["ts"] < _QSNAP_TTL:
            return _QSNAP["data"]
    try:
        qmap = tx_kline.fetch_quotes_batch(symbols)
        data = {s: (q or {}).get("last") for s, q in (qmap or {}).items()}
    except Exception:
        return {}
    if data:
        with _QSNAP["lock"]:
            _QSNAP["ts"] = now
            _QSNAP["data"] = data
    return data


# ---- 监控表「MACD 状态」列的数据源(2026-09-23) ----
#   五条件不在监控接口里重算(天勤会话会被拖垮), 直接读每日筛选结果表 t_stock_screen_result。
#   监控表每 3s 轮询 → 不能每次查库, 用「全表载入 + 60s TTL」缓存。
#   口径(2026-09-30 用户裁定「不要历史」): 只认【当天 scan_date】的筛选结果。
#   当天首轮筛选(11:35)跑完前状态列为空 —— 旧一天的残留(如 09-28 的 c2=1)一律不展示,
#   否则几天前金叉的票会顶着过期标签出现(600383/600032 实锤)。
_SCREEN_FLAG_CACHE = {"ts": 0.0, "map": {}}
_SCREEN_FLAG_TTL = 60.0


def _screen_flag_map():
    """symbol -> {c1..c5, detail, date}（仅当天筛选结果; 2026-09-30 起「不要历史」）。"""
    now = time.time()
    if _SCREEN_FLAG_CACHE["map"] and (now - _SCREEN_FLAG_CACHE["ts"]) < _SCREEN_FLAG_TTL:
        return _SCREEN_FLAG_CACHE["map"]
    out = {}
    try:
        conn = _cross_conn()
        cur = conn.cursor()
        cur.execute("""SELECT symbol, c1, c2, c3, c4, c5, detail, scan_date, updated_at
                       FROM t_stock_screen_result
                       WHERE scan_date = CURDATE()
                       ORDER BY scan_date ASC""")
        _today = date.today().strftime("%Y-%m-%d")
        for sym, c1, c2, c3, c4, c5, detail, sd, ua in cur.fetchall():
            # 2026-09-29 用户要求: 每条状态后面带上「抓取到的时间」= 筛选结果落库时间(updated_at)。
            #   当天只显示 HH:MM, 非当天带日期前缀, 便于一眼看出数据新鲜度。
            tstr = ""
            if ua:
                sd_str = sd.strftime("%Y-%m-%d") if sd else ""
                tstr = ua.strftime("%H:%M") if sd_str == _today else ua.strftime("%m-%d %H:%M")
            out[str(sym)] = {"c1": int(c1 or 0), "c2": int(c2 or 0), "c3": int(c3 or 0),
                             "c4": int(c4 or 0), "c5": int(c5 or 0),
                             "detail": str(detail or ""),
                             "date": sd.strftime("%Y-%m-%d") if sd else "",
                             "time": tstr}
        conn.close()
    except Exception as e:
        print("[股票监控] 读每日筛选结果失败(状态列沿用旧缓存):", e)
        return _SCREEN_FLAG_CACHE["map"]
    _SCREEN_FLAG_CACHE["map"] = out
    _SCREEN_FLAG_CACHE["ts"] = time.time()
    return out


def api_monitor():
    """轻量监控接口: 批量返回实时价 + 最近金叉/死叉(读缓存表, 不建 K 线)。
    参数:
      symbols: 自选股票列表(默认模式)
      all_golden: true 时返回缓存中所有当前金叉股票
      watch_symbols: all_golden 模式下用于置顶+标记自选股
      golden_period: all_golden 模式下的周期过滤: 'week'/'month'(空=全部)
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}
    symbols = data.get("symbols") or []
    if not isinstance(symbols, list):
        symbols = []
    all_golden = bool(data.get("all_golden"))
    watch_symbols = set(str(s) for s in (data.get("watch_symbols") or []))
    golden_period = str(data.get("golden_period") or "").strip().lower()
    # 把自选股(报警目标)告诉分钟引擎, 优先用历史 1 分钟 K 线预热 MACD。
    # 同时作为"有人在看监控"的心跳: 引擎未启动则惰性启动, 空闲超时会自动停止。
    try:
        from services import stock_minute_engine as _mengine
        _mengine.touch_minute_engine(watch_symbols or None)
    except Exception:
        pass

    # 2026-09-28 迁移: 实时价改走腾讯批量报价(qt.gtimg.cn 一次50只), 不再依赖天勤扫描账号。
    # (原实现 slot.ensure_subscribed + slot.latest_data, zhaotest trial 10-06 到期即断。)
    with _CROSS_LOCK:
        cache = dict(_CROSS_CACHE)
    # 合并分钟引擎缓存(隔离写入, 不影响 w_/m_)
    try:
        from services import stock_minute_engine as _mengine
        for sym, mc in _mengine.get_minute_cache().items():
            cache.setdefault(sym, {}).update(mc)
    except Exception:
        pass
    with _STOCK_LOCK:
        stock_map = dict(_STOCK_MAP)

    if all_golden:
        # 口径定版(2026-09-30 需求人最终裁定): 「金叉」标签 = 【当前这根K线内刚发生金叉】
        #   且当前K线仍处金叉态(实时价递推 dif>dea); 上一根或更早的金叉(历史)一律不标。
        #   全部金叉(空 golden_period) = 各周期并集。
        #   实现: live_state EMA 基态 + 实时价递推当前K线 DIF/DEA(状态) + gxd(新鲜度);
        #         无基态/无报价/分钟周期 -> 回落 _cross_state + 金叉日期桶判定。
        periods_chk = (golden_period,) if golden_period in ("week", "month", "day", "minute") \
            else ("day", "week", "month", "minute")
        qsnap = _quote_snapshot(list(cache.keys()))
        now_dt = datetime.now()
        golden = []
        for sym, c in cache.items():
            lv = None
            raw = c.get("live_state")
            if raw:
                try:
                    lv = json.loads(raw)
                except Exception:
                    lv = None
            price = qsnap.get(sym)
            hit = False
            for p in periods_chk:
                lg = _live_golden(lv, p, price, now_dt) if (lv and price) else None
                if lg is None:
                    if _cross_state(c, p) != "金叉":
                        continue
                elif not lg:
                    continue
                # 新鲜度(2026-09-29 需求人最终口径): 只标最近一两根K线内刚发生金叉的
                if not _golden_recent(lv, c, p, now_dt):
                    continue
                hit = True
                break
            if hit:
                golden.append(sym)
        symbols = golden[:800]
    else:
        symbols = [str(s) for s in symbols][:100]

    if not symbols:
        return jsonify({"ok": True, "items": []})

    # 批量拉实时报价(同步, 200 只 ≈ 4 请求 ≈ 0.1s; 无需等待天勤订阅快照)
    try:
        qmap = tx_kline.fetch_quotes_batch(symbols)
    except Exception:
        qmap = {}

    scmap = _screen_flag_map()
    out = []
    for sym in symbols:
        base = stock_map.get(sym, {})
        c = cache.get(sym, {})
        sc = scmap.get(sym) or {}
        q = {"last": None, "change": None, "change_pct": None}
        qb = qmap.get(sym) or {}
        last = _r(qb.get("last"))
        pre = _r(qb.get("pre_close"))
        if last is not None:
            q["last"] = last
            if pre and pre > 0:
                q["change"] = round(last - pre, 4)
                q["change_pct"] = round((last - pre) / pre * 100, 2)
        ex, code = _parse_symbol(sym)
        # 2026-09-29 口径收口: c4/c5 标签与「实时金叉」同口径 —— 只标【最近一两根K线内刚金叉】。
        # 每日筛选表里落的结果还是旧宽窗口(SCREEN_C4_BARS 旧值 12), 要到 15:05 重算才刷;
        # 这里在下发时实时降级(不改表): 监控页标签立即符合新口径, 筛选页历史回看不受影响。
        if sc.get("c4") or sc.get("c5"):
            sc = dict(sc)
            _lv = None
            _raw = c.get("live_state")
            if _raw:
                try:
                    _lv = json.loads(_raw)
                except Exception:
                    _lv = None
            _now = datetime.now()
            if sc.get("c4") and not (_cross_state(c, "week") == "金叉"
                                     and _golden_recent(_lv, c, "week", _now)):
                sc["c4"] = 0
            if sc.get("c5") and not (_cross_state(c, "month") == "金叉"
                                     and _golden_recent(_lv, c, "month", _now)):
                sc["c5"] = 0
        # 2026-09-30 需求人裁定: 状态列 chips 全实时算 —— live_state 基态 + 实时价递推,
        #   判定「当前K线内刚发生」的交叉事件; 无基态/分钟周期回落交叉日期当前桶。
        _lv2 = None
        _raw2 = c.get("live_state")
        if _raw2:
            try:
                _lv2 = json.loads(_raw2)
            except Exception:
                _lv2 = None
        _rt_price = q["last"]
        w_chip = _rt_cross_chip(_lv2, c, "week", _rt_price)
        m_chip = _rt_cross_chip(_lv2, c, "month", _rt_price)
        d_chip = _rt_cross_chip(_lv2, c, "day", _rt_price)
        min_chip = _rt_cross_chip(None, c, "minute", None)
        out.append({
            "symbol": sym, "code": code, "exchange": ex,
            "name": base.get("name") or "",
            "last": q["last"], "change": q["change"], "change_pct": q["change_pct"],
            "w_golden": c.get("w_golden") or "", "w_death": c.get("w_death") or "",
            "w_recent": bool(c.get("w_recent")),
            "m_golden": c.get("m_golden") or "", "m_death": c.get("m_death") or "",
            "m_recent": bool(c.get("m_recent")),
            "d_golden": c.get("d_golden") or "", "d_death": c.get("d_death") or "",
            "d_recent": bool(c.get("d_recent")),
            "min_golden": c.get("min_golden") or "", "min_death": c.get("min_death") or "",
            "min_recent": bool(c.get("min_recent")),
            # 2026-09-23: 前端周期列改为显示【当前状态】(金叉红 / 死叉绿)
            "w_state": _cross_state(c, "week"), "m_state": _cross_state(c, "month"),
            "d_state": _cross_state(c, "day"), "min_state": _cross_state(c, "minute"),
            # 2026-09-30 全实时 chips(前端状态列用这个, 不再用 _state 的"最近交叉类型"):
            "w_chip": w_chip, "m_chip": m_chip, "d_chip": d_chip, "min_chip": min_chip,
            "is_watch": sym in watch_symbols,
            # 2026-09-23(二次): 监控表后 4 列(周/月/日/分钟状态) → 单个「MACD 状态」列。
            #   五条件来自每日筛选结果表; sc_known 用于区分「已扫过但没命中(无)」/「还没扫到(—)」。
            #   w_state/m_state/d_state/min_state 保留(「全部金叉」下拉的周期过滤仍靠它, 且便于回退)。
            "c1": int(sc.get("c1") or 0), "c2": int(sc.get("c2") or 0),
            "c3": int(sc.get("c3") or 0), "c4": int(sc.get("c4") or 0),
            "c5": int(sc.get("c5") or 0),
            "sc_known": bool(sc), "sc_date": sc.get("date") or "",
            "sc_time": sc.get("time") or "",
            "sc_detail": sc.get("detail") or "",
        })

    if all_golden:
        # 2026-09-30 需求人裁定(终版): 「状态」= 任一 MACD 条件(c1-c5) 或 任一周期
        #   【当前K线内刚金叉】(全实时 chip 口径, 与列表准入一致 → 每一行至少有一个 chip)。
        #   旧口径用 _cross_state("最近交叉类型") 会把 6 月的周金叉也算状态(浦发银行实锤)。
        def _has_status(o):
            if o["c1"] or o["c2"] or o["c3"] or o["c4"] or o["c5"]:
                return True
            return (o["w_chip"] == "金叉" or o["m_chip"] == "金叉"
                    or o["d_chip"] == "金叉" or o["min_chip"] == "金叉")
        out = [o for o in out if _has_status(o)]
        # 自选股置顶; 其次按 symbol 稳定排序
        out.sort(key=lambda x: (0 if x.get("is_watch") else 1, x.get("symbol") or ""))

    return jsonify({"ok": True, "items": out})


def api_minute_status():
    """分钟级引擎健康/状态(只读, 不依赖全局功能)。"""
    try:
        from services import stock_minute_engine as _mengine
        return jsonify({"ok": True, "engine": _mengine.minute_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


def api_minute_engine():
    """分钟引擎惰性启停(2026-09-08): 监控页打开 -> start(心跳); 页面关闭 -> stop。

    引擎不再随服务常驻, 避免后台长期空转订阅把天勤会话灌到限流;
    即使前端没发 stop, 连续 300s 无请求也会自动停止。
    """
    try:
        from services import stock_minute_engine as _mengine
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})
    try:
        body = request.get_json(force=True, silent=True) or {}
    except Exception:
        body = {}
    action = str(request.args.get("action") or body.get("action") or "").strip().lower()
    if action == "stop":
        return jsonify({"ok": True, "started": False,
                        "stopped": _mengine.stop_minute_engine("监控页关闭")})
    # start / beat: 刷新保活时间, 未启动则惰性启动
    _mengine.touch_minute_engine()
    return jsonify({"ok": True, "started": True, "engine": _mengine.minute_status()})


def api_scan():
    """扫描指定股票列表(单请求上限 200), 写入缓存并返回近期金叉标记。"""
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}
    symbols = data.get("symbols") or []
    if not isinstance(symbols, list):
        symbols = []
    symbols = [str(s) for s in symbols][:200]
    out = {}
    for sym in symbols:
        try:
            res = _scan_one(sym)
            if res:
                _upsert_cross(sym, res)
                w = res.get("week", {})
                m = res.get("month", {})
                out[sym] = {"recent_golden": bool(w.get("recent", 0)),
                            "last_golden_date": w.get("golden_date") or "",
                            "recent_golden_month": bool(m.get("recent", 0)),
                            "last_golden_month_date": m.get("golden_date") or ""}
            else:
                out[sym] = {"recent_golden": False, "last_golden_date": "", "no_kline": True}
        except Exception as e:
            out[sym] = {"recent_golden": False, "error": str(e)}
    return jsonify({"ok": True, "results": out})


def _scan_full_run():
    """全市场遍历。2026-09-28 迁移: 数据源=腾讯 tx, 不再依赖天勤扫描账号
    (原实现受扫描账号 A 股权限/到期制约, 实测吞吐 0.42s/只; tx 实测 ~119 请求/秒)。"""
    global _SCAN
    with _STOCK_LOCK:
        syms = [x["symbol"] for x in _STOCK_LIST]
    if not syms:
        _SCAN["running"] = False
        return
    # 2026-09-30 缺行优先: 池内还没有 cross_cache 行的票(新上市/历史漏扫)排到队首先扫,
    #   避免全市场 5500+ 只按序慢扫时, 排在后面的票一整天都没有金叉状态(605577 案例)。
    #   _CROSS_CACHE 启动时已从库全量载入, 内存判断零开销; 各周期均无数据也会落"裸行",
    #   不会反复重扫(裸行即视为已覆盖)。
    with _CROSS_LOCK:
        _known = set(_CROSS_CACHE.keys())
    _missing = [s for s in syms if s not in _known]
    if _missing:
        print(f"[股票模块] 缺行优先: {len(_missing)} 支排到队首先扫")
        syms = _missing + [s for s in syms if s in _known]
    ok_n = 0
    t0 = time.time()
    _SCAN["total"] = len(syms); _SCAN["done"] = 0; _SCAN["running"] = True
    _SCAN["started"] = t0; _SCAN["account"] = "tx腾讯源"
    print(f"[股票模块] 全市场遍历开始: {len(syms)} 支, 数据源 {_SCAN['account']}")
    for i, sym in enumerate(syms):
        try:
            res = _scan_one(sym)
            if res:
                _upsert_cross(sym, res)
                ok_n += 1
        except Exception:
            pass
        _SCAN["done"] = i + 1
        _SCAN["last"] = sym
        # 去掉原先 sleep(0.25)/只 + 每50只2秒 的人为等待(会把 39 分钟拖到 66 分钟),
        # 改为每 500 只让出一次主线程, 避免长时间独占 wait_update 影响行情响应。
        if (i + 1) % 500 == 0:
            time.sleep(0.05)
    _SCAN["running"] = False
    _SCAN["ok"] = ok_n
    _SCAN["elapsed"] = round(time.time() - t0, 1)
    print(f"[股票模块] 全市场遍历完成: {ok_n}/{len(syms)} 有数据, 耗时 {_SCAN['elapsed']}s")


# ===================== 定时遍历调度(11:31 / 15:01) =====================
_SCAN_TIMES = ["11:31", "15:01"]   # 每天触发时刻(北京时间)
_SCAN_FIRED = {}                   # "MMDD-HH:MM" -> True, 幂等防重复触发
_SCHED_STARTED = False


def _start_scan_scheduler():
    """启动后台调度线程: 每天 11:31 与 15:01 各跑一遍全市场遍历。"""
    global _SCHED_STARTED
    if _SCHED_STARTED:
        return
    _SCHED_STARTED = True

    def _loop():
        while True:
            try:
                now = datetime.now()
                key = now.strftime("%m%d-%H:%M")
                hm = now.strftime("%H:%M")
                if hm in _SCAN_TIMES and not _SCAN_FIRED.get(key):
                    _SCAN_FIRED[key] = True
                    if not _SCAN["running"]:
                        print(f"[股票模块] 定时遍历触发 {hm}")
                        threading.Thread(target=_scan_full_run, daemon=True).start()
                    else:
                        print(f"[股票模块] 定时遍历 {hm} 跳过: 上一轮仍在运行")
            except Exception as e:
                print("[股票模块] 调度异常:", e)
            time.sleep(20)
    threading.Thread(target=_loop, daemon=True).start()
    print(f"[股票模块] 全市场定时遍历已设定: 每天 {' / '.join(_SCAN_TIMES)}")


def _maybe_catchup_scan():
    """live_state 覆盖率过低(新列/新装/刚升级)时延迟 120s 补一轮全市场扫描,
    让监控「实时当前K线」判定尽快有基态可用。有 11:31/15:01 定时兜底, 不强求。"""
    try:
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*), COUNT(live_state) FROM t_stock_cross_cache")
        tot, lv = cur.fetchone(); conn.close()
        if not tot or (lv or 0) * 1.0 / tot >= 0.3:
            return
        def _run():
            time.sleep(120)
            if not _SCAN.get("running"):
                print("[股票模块] live_state 覆盖率低, 启动补扫(实时判定基态)...")
                _scan_full_run()
        threading.Thread(target=_run, daemon=True).start()
        print(f"[股票模块] live_state 覆盖 {lv or 0}/{tot}, 120s 后补扫")
    except Exception as e:
        print("[股票模块] live_state 覆盖检查失败(可忽略):", e)


def api_scan_full():
    if _SCAN["running"]:
        return jsonify({"ok": True, "running": True, "scan": _SCAN})
    threading.Thread(target=_scan_full_run, daemon=True).start()
    return jsonify({"ok": True, "started": True})


def api_scan_status():
    return jsonify({"ok": True, "scan": _SCAN,
                    "account": get_scan_account(),
                    "accounts": known_accounts(),
                    "next_times": _SCAN_TIMES})


def api_scan_account():
    """GET 查看 / POST 设置 A 股扫描账号(需有 A 股权限)。"""
    if request.method == "POST":
        data = request.get_json(force=True, silent=True) or {}
        return jsonify(set_scan_account(data.get("username", "")))
    return jsonify({"ok": True, "account": get_scan_account(),
                    "known": known_accounts(),
                    "hint": "只有具备 A 股权限且未过期的天勤账号才能取到股票数据; "
                            "默认从 tq_accounts 动态选有效账户; "
                            "也可 POST /api/scan/account 手动指定"})


# ===================== 每日筛选(MACD 5 条件, 15:05 定时) =====================
# 口径(2026-09-21 与用户确认):
#   零轴 = 固定 0 轴(直接与 DIF 的 0 比较, 不用"跟随区间")
#   c1 零轴下二次金叉: 日线最近两次金叉的 DIF 均 < 0, 两次间隔 <= SCREEN_C1_GAP 个交易日,
#                      且【第二次金叉须发生在末根(当前这根日K线)】(2026-09-30 收口: 去掉近5日窗口, 几天前金叉不标)
#   c2 零轴附近或零轴上金叉: 最近一次金叉处 DIF >= -SCREEN_C2_ATR_MULT * ATR14(零轴附近),
#                      或金叉处 DEA > SCREEN_C2_ABOVE_DEA(零轴上); 且【金叉须发生在末根(当前这根日K线)】
#                      (2026-09-30 收口: 去掉近5日窗口, 几天前金叉、当前只是维持金叉态的不标)
#       "零轴上" 子判定(2026-09-26 用户口径): 金叉处 DEA > SCREEN_C2_ABOVE_DEA 视为零轴上方
#       (默认 -0.05 = 信号线在零轴附近甚至略低于零轴 0.05 内即算"零轴上", 放宽原 DIF>0 硬杠)
#   c3 日线 MACD 底背离: 枢轴低点处价格创新低而 DIF 抬高(半径 SCREEN_C3_ORDER),
#                      且第二个低点距今 <= SCREEN_C3_FRESH 个交易日
#   c4 周线金叉: 最近一次金叉距序列末尾 <= SCREEN_C4_BARS 根周线(2026-09-29 收口=2, 即最近一两根周K内刚金叉), 且仍处金叉
#   c5 月线金叉: 最近一次金叉距序列末尾 <= SCREEN_C5_BARS 根月线(同上收口=2) 且仍处金叉;
#                另外"每次发现的新月线金叉都落库"(t_stock_month_golden), 供历史回看不重复调接口
SCREEN_C1_GAP = 60         # c1: 两次金叉最大间隔(交易日)
SCREEN_C2_ATR_MULT = 0.2   # c2: |DIF| <= 0.2*ATR14 视为"零轴附近"
SCREEN_C2_ABOVE_DEA = -0.05 # c2: 金叉处 DEA > 此值 即判定"零轴上"(替代原 DIF>0 硬判定)
SCREEN_C3_ORDER = 5        # c3: 枢轴半径
SCREEN_C3_LOOKBACK = 120   # c3: 背离扫描窗口(交易日)
SCREEN_C3_FRESH = 30       # c3: 第二个低点距今不超过 N 交易日
SCREEN_C4_BARS = 2         # c4: 周线"近期"窗口(根) —— 2026-09-29 需求人口径收口: 只算最近一两根周K内刚金叉(原12根太宽, 白云机场/宁波联合等"金叉持续数周"的也被贴标)
SCREEN_C5_BARS = 2         # c5: 月线"近期"窗口(根) —— 同上收口(原3根)
# 2026-09-30 需求人裁定(与监控「只保留一根当前这根」同口径): 日线金叉类条件(c1/c2)只标
#   【当前这根日K线(末根)刚发生金叉】的标的; 几天前金叉、当前只是维持金叉态的一律不标。
#   实现见 _screen_one: 要求金叉索引 == 末根索引(n-1)。(原 SCREEN_C1_FRESH/SCREEN_C2_FRESH=5 的"近5交易日"窗口过宽, 误标 603105 等历史金叉)
SCREEN_C1_FRESH = 0        # 已废弃: 日线 c1 现改"末根刚金叉"硬判定; 保留常量名仅为兼容 import
SCREEN_C2_FRESH = 0        # 已废弃: 日线 c2 现改"末根刚金叉"硬判定; 保留常量名仅为兼容 import
SCREEN_FETCH_BARS = 1500   # 一次取 1500 根日线(约 6 年)本地重采样出周/月线, 省掉每标的 3 次取数
                           # (月线 MACD 需 >=35 根, 1500 根日线 ~= 60 根月线, 留足余量)
SCREEN_SAVE_BATCH = 100    # 每 N 只落库一次
# ⚠ 批量并发取数: 实测【不可用】。批量 12 时前 ~500 只 0.36s/只飞快, 之后天勤进入
#   惩罚性静默(set_chart 不再回数据, 且 3 分钟以上不恢复)。保持 1 = 逐只串行(每只等
#   就绪再取下一只), 这是唯一稳定的形态(对照: 现有全市场金叉扫描串行可跑到 1318 只)。
SCREEN_PREFETCH_BATCH = 1
# ⚠ 分块重开会话: 实测【不可用】。重开确实执行了(日志可见), 但新会话约 300 轮后彻底静默
#   (got=False 距上次收包 137s), 反而更差。0 = 禁用(>0 才启用)。
SCREEN_RECONNECT_EVERY = 0
SCREEN_READY_WAIT = 900    # 跑之前等扫描账号会话就绪的最长秒数(重启后会话是异步连上的)
SCREEN_RETRY_MAX = 3       # 一轮"取数全失败"时的延迟重试次数
SCREEN_RETRY_DELAY = 300   # 重试间隔(秒)
SCREEN_TIMES = ["11:35", "15:05"]   # 触发时刻(北京时间): 11:35 午休先出当日盘中档, 15:05 收盘后定档覆盖

_SCREEN = {"running": False, "total": 0, "done": 0, "started": None, "ok": 0,
           "hits": 0, "elapsed": 0.0, "account": "", "last": "", "last_date": "",
           "source": "", "cond_cnt": {}, "error": ""}
_SCREEN_FIRED = {}
_SCREEN_SCHED_STARTED = False
_SCREEN_RUN_LOCK = threading.Lock()


def _ensure_screen_tables():
    """建两张表: 每日筛选结果 + 月线金叉历史。幂等。"""
    try:
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS t_stock_screen_result (
            scan_date DATE NOT NULL,
            symbol VARCHAR(32) NOT NULL,
            name VARCHAR(64) DEFAULT '',
            last_price DECIMAL(18,4) DEFAULT NULL,
            change_pct DECIMAL(10,4) DEFAULT NULL,
            c1 TINYINT DEFAULT 0, c2 TINYINT DEFAULT 0, c3 TINYINT DEFAULT 0,
            c4 TINYINT DEFAULT 0, c5 TINYINT DEFAULT 0,
            hit_count TINYINT DEFAULT 0,
            dif DOUBLE DEFAULT NULL, atr DOUBLE DEFAULT NULL, zero_gap DOUBLE DEFAULT NULL,
            detail VARCHAR(255) DEFAULT '',
            d_golden VARCHAR(20) DEFAULT '', w_golden VARCHAR(20) DEFAULT '',
            m_golden VARCHAR(20) DEFAULT '',
            src VARCHAR(8) DEFAULT 'auto',
            updated_at DATETIME DEFAULT NULL,
            PRIMARY KEY (scan_date, symbol),
            KEY idx_date_hit (scan_date, hit_count)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        cur.execute("""CREATE TABLE IF NOT EXISTS t_stock_month_golden (
            symbol VARCHAR(32) NOT NULL,
            cross_date VARCHAR(20) NOT NULL,
            name VARCHAR(64) DEFAULT '',
            dif DOUBLE DEFAULT NULL,
            dea DOUBLE DEFAULT NULL,
            price DECIMAL(18,4) DEFAULT NULL,
            first_seen DATETIME DEFAULT NULL,
            last_seen DATETIME DEFAULT NULL,
            PRIMARY KEY (symbol, cross_date),
            KEY idx_cross_date (cross_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        conn.commit(); conn.close()
        print("[股票筛选] 结果表 t_stock_screen_result / 月线金叉表 t_stock_month_golden 就绪")
        return True
    except Exception as e:
        print("[股票筛选] 建表失败:", e); return False


def _screen_resample(daily, freq, count):
    """本地把日线 df 重采样为周/月线, 口径与 futures_kline._resample_daily 完全一致
    (周线标签 -6 天对齐到周一)。输出带 dt 列(北京时间索引)。"""
    if daily is None or getattr(daily, "empty", True):
        return daily
    try:
        df = daily.copy()
        df["dt"] = futures_kline._dt_local(df["datetime"])
        df = df.set_index("dt")
        agg_map = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
        if "open_interest" in df.columns:
            agg_map["open_interest"] = "last"
        agg = df.resample(freq).agg(agg_map).dropna(subset=["close"])
        if freq.upper().startswith("W"):
            agg.index = agg.index - pd.Timedelta(days=6)
        agg = agg.tail(count).reset_index()
        if "dt" not in agg.columns:
            agg = agg.rename(columns={agg.columns[0]: "dt"})
        return agg
    except Exception as e:
        print("[股票筛选] 重采样失败 %s:" % freq, e)
        return daily


def _screen_cross_info(df):
    """从 df 计算 close/dif/dea/crosses/atr14/dts。不足 35 根返回 None。"""
    if df is None or getattr(df, "empty", True) or len(df) < 35:
        return None
    try:
        close = df["close"].astype(float).tolist()
        dif, dea, _bar = indicators.macd(close, 12, 26, 9)
        crosses = ta_patterns.detect_macd_cross(dif, dea, close)
        if "dt" in df.columns:
            dts = list(df["dt"])
        else:
            dts = list(futures_kline._dt_local(df["datetime"]))
        try:
            atr14 = indicators.atr(df["high"].astype(float).tolist(),
                                   df["low"].astype(float).tolist(), close, 14)
        except Exception:
            atr14 = []
        return {"n": len(close), "close": close, "dif": dif, "dea": dea,
                "crosses": crosses, "dts": dts, "atr14": atr14,
                "highs": df["high"].astype(float).tolist(),
                "lows": df["low"].astype(float).tolist()}
    except Exception:
        return None


def _screen_fmt(D, idx, fmt="%Y-%m-%d"):
    try:
        if idx is None or idx < 0 or idx >= len(D["dts"]):
            return ""
        v = D["dts"][idx]
        return v.strftime(fmt) if hasattr(v, "strftime") else str(v)[:10]
    except Exception:
        return ""


def _screen_last(D, typ):
    xs = [c for c in D["crosses"] if c.get("type") == typ]
    return xs[-1] if xs else None


def _screen_in_golden(D):
    g = _screen_last(D, "golden"); d = _screen_last(D, "death")
    if not g:
        return False
    return (not d) or g["idx"] > d["idx"]


def _screen_cache_entry(D, period, fmt):
    """按 _scan_one 的口径生成写缓存的 {golden_date, golden_idx, recent, death_date}。"""
    n = D["n"]
    rec_n = 12 if period == "week" else (6 if period == "month" else 20)
    out = {"ok": True, "golden_date": "", "golden_idx": -1, "recent": 0, "death_date": ""}
    dep = _screen_last(D, "death")
    if dep:
        out["death_date"] = _screen_fmt(D, dep["idx"], fmt)
    gol = _screen_last(D, "golden")
    if gol:
        out["golden_date"] = _screen_fmt(D, gol["idx"], fmt)
        out["golden_idx"] = gol["idx"]
        out["recent"] = 1 if (n - gol["idx"]) <= rec_n else 0
    return out


def _screen_one(symbol, day_df):
    """单只股票的 5 个条件计算(日线 df 由调用方【批量预取】后传入)。

    2026-09-21: 原先在此处 _fetch_kline_df(slot, symbol, ...) 逐只串行取数(≈3.2s/只),
    改为调用方 fetch_kline_df_batch 分批预取(往返重叠), 本函数只做纯计算。
    返回 dict(row=落库行, month=月线历史行) 或 None。
    """
    if day_df is None:
        return None
    D = _screen_cross_info(day_df)
    if not D:
        return None
    W = _screen_cross_info(_screen_resample(day_df, "W", PERIODS["week"]["count"]))
    M = _screen_cross_info(_screen_resample(day_df, "MS", PERIODS["month"]["count"]))

    c1 = c2 = c3 = c4 = c5 = 0
    details = []
    dif_at = None
    atr_at = None
    zero_gap = None
    month_new_row = None

    golds = [c for c in D["crosses"] if c.get("type") == "golden"]
    in_golden = _screen_in_golden(D)
    g_last = golds[-1] if golds else None

    # ---- c1 零轴下二次金叉 ----
    if len(golds) >= 2 and in_golden:
        g2, g1 = golds[-1], golds[-2]
        d2 = D["dif"][g2["idx"]] if g2["idx"] < len(D["dif"]) else None
        d1v = D["dif"][g1["idx"]] if g1["idx"] < len(D["dif"]) else None
        gap = g2["idx"] - g1["idx"]
        # 2026-09-30 需求人裁定(与监控「只保留一根当前这根」同口径): 日线金叉类条件只标
        #   【当前这根日K线(末根)刚发生金叉】的标的; 几天前金叉、当前只是维持金叉态的不标。
        fresh_ok = (g2["idx"] == D["n"] - 1)
        # 2026-09-23 用户要求「两次金叉须相邻(不跳叉)」: 全量交叉序列里 g2 的前一项必须是
        #   死叉、再前一项必须是 g1 —— 即两次金叉之间恰好隔一个死叉, 不跨越其它交叉。
        #   detect_macd_cross 本体严格交替, 此条为显式护栏(口径若被改动会在此拦住)。
        _cx = D["crosses"]
        _i2 = len(_cx) - 1
        while _i2 >= 0 and not (_cx[_i2].get("type") == "golden"
                                and _cx[_i2].get("idx") == g2["idx"]):
            _i2 -= 1
        adjacent = (_i2 >= 2 and _cx[_i2 - 1].get("type") == "death"
                    and _cx[_i2 - 2].get("idx") == g1["idx"])
        if (d2 is not None and d1v is not None and d2 < 0 and d1v < 0
                and gap <= SCREEN_C1_GAP and fresh_ok and adjacent):
            c1 = 1
            details.append("零轴下二次金叉 %s→%s 隔%d日" %
                           (_screen_fmt(D, g1["idx"]), _screen_fmt(D, g2["idx"]), gap))

    # ---- c2 零轴附近 / 零轴上金叉 ----
    if in_golden and g_last is not None:
        gi = g_last["idx"]
        if gi < len(D["dif"]):
            dif_at = D["dif"][gi]
        if gi < len(D["dea"]):
            dea_at = D["dea"][gi]
        if gi < len(D["atr14"]):
            atr_at = D["atr14"][gi]
        if dif_at is not None and atr_at and atr_at > 0:
            zero_gap = dif_at / atr_at
        # 2026-09-30 需求人裁定(与监控「只保留一根当前这根」同口径): 日线金叉类条件只标
        #   【当前这根日K线(末根)刚发生金叉】的标的; 几天前金叉、当前只是维持金叉态的不标。
        fresh_ok = (gi == D["n"] - 1)
        if dif_at is not None and fresh_ok:
            if dea_at is not None and dea_at > SCREEN_C2_ABOVE_DEA:
                c2 = 1
                details.append("零轴上金叉(DEA %.3f)" % dea_at)
            elif atr_at and atr_at > 0 and abs(dif_at) <= SCREEN_C2_ATR_MULT * atr_at:
                c2 = 1
                details.append("零轴附近金叉(|DIF|/ATR=%.2f)" % abs(dif_at / atr_at))

    # ---- c3 日线底背离 ----
    try:
        divs = ta_patterns.detect_divergence(D["highs"], D["lows"], D["dif"],
                                             order=SCREEN_C3_ORDER, lookback=SCREEN_C3_LOOKBACK)
        bots = [x for x in divs if x.get("type") == "bottom" and x["x2"] >= D["n"] - SCREEN_C3_FRESH]
        if bots:
            x = bots[-1]
            c3 = 1
            details.append("底背离 %s→%s" % (_screen_fmt(D, x["x1"]), _screen_fmt(D, x["x2"])))
    except Exception:
        pass

    # ---- c4 周线金叉 ----
    w_golden = w_death = ""
    if W:
        gw = _screen_last(W, "golden")
        w_golden = _screen_fmt(W, gw["idx"]) if gw else ""
        wd = _screen_last(W, "death")
        w_death = _screen_fmt(W, wd["idx"]) if wd else ""
        if gw and _screen_in_golden(W) and (W["n"] - gw["idx"]) <= SCREEN_C4_BARS:
            c4 = 1
            details.append("周线金叉 %s" % w_golden)

    # ---- c5 月线金叉 (并落月线历史表) ----
    m_golden = m_death = ""
    if M:
        gm = _screen_last(M, "golden")
        m_golden = _screen_fmt(M, gm["idx"], "%Y-%m") if gm else ""
        md = _screen_last(M, "death")
        m_death = _screen_fmt(M, md["idx"], "%Y-%m") if md else ""
        if gm and _screen_in_golden(M) and (M["n"] - gm["idx"]) <= SCREEN_C5_BARS:
            c5 = 1
            details.append("月线金叉 %s" % m_golden)
        if gm and m_golden:
            gi = gm["idx"]
            month_new_row = {"symbol": symbol, "cross_date": m_golden,
                             "dif": gm.get("dif"), "dea": gm.get("dea"),
                             "price": gm.get("price")}

    # 写回金叉缓存(与全市场遍历同一口径, 保持监控页数据同步)
    res = {"day": _screen_cache_entry(D, "day", "%Y-%m-%d"),
           "week": _screen_cache_entry(W, "week", "%Y-%m-%d") if W else {"ok": False},
           "month": _screen_cache_entry(M, "month", "%Y-%m") if M else {"ok": False}}
    try:
        _upsert_cross(symbol, res)
    except Exception:
        pass

    hit = c1 + c2 + c3 + c4 + c5
    row = {"symbol": symbol, "c1": c1, "c2": c2, "c3": c3, "c4": c4, "c5": c5,
           "hit_count": hit, "dif": _r(dif_at), "atr": _r(atr_at), "zero_gap": _r(zero_gap),
           "detail": " | ".join(details)[:255],
           "d_golden": _screen_fmt(D, g_last["idx"]) if g_last else "",
           "w_golden": w_golden, "m_golden": m_golden}
    return {"row": row, "month": month_new_row}


def _screen_save_month(rows, names):
    """月线金叉历史: 首次发现记 first_seen, 之后每次见到只刷 last_seen。"""
    if not rows:
        return 0
    try:
        conn = _cross_conn(); cur = conn.cursor()
        vals = [(r["symbol"], r["cross_date"], names.get(r["symbol"], ""),
                 r["dif"], r["dea"], r["price"]) for r in rows]
        cur.executemany("""INSERT INTO t_stock_month_golden
            (symbol,cross_date,name,dif,dea,price,first_seen,last_seen)
            VALUES (%s,%s,%s,%s,%s,%s,NOW(),NOW())
            ON DUPLICATE KEY UPDATE last_seen=NOW(), dif=VALUES(dif), dea=VALUES(dea),
              price=VALUES(price), name=VALUES(name)""", vals)
        conn.commit(); conn.close()
        return len(vals)
    except Exception as e:
        print("[股票筛选] 月线金叉历史落库失败:", e); return 0


def _screen_names():
    """symbol -> 名称: 优先 Go 内存全市场表, 缺失再查库。"""
    d = {}
    with _STOCK_LOCK:
        for s, v in _STOCK_MAP.items():
            if v.get("name"):
                d[s] = v["name"]
    return d


_SCREEN_RETRY = {"n": 0}


def _tx_day_batch(symbols, bars):
    """并发取腾讯日线(前复权, 上限 800 根/只)并归一化为天勤结构(UTC ns)。
    返回 {symbol: df}; 取数失败的标的不在字典。6 并发 + 批内不额外限速
    (实测吞吐 ~119 请求/秒, 全市场 5557 只约 1-3 分钟; 保守起见每只间隔 30ms)。"""
    from concurrent.futures import ThreadPoolExecutor
    out = {}

    def _one(sym):
        try:
            df = tx_kline.fetch_df(sym, "day", bars)
            if df is None or getattr(df, "empty", True):
                return None
            time.sleep(0.03)   # 限流护栏: 每只取完小憩, 长跑防封
            return sym, _norm_tx_dt(df)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=6) as ex:
        for r in ex.map(_one, symbols):
            if r:
                out[r[0]] = r[1]
    return out


def _screen_probe(syms):
    """探针: 腾讯源无会话概念, 抽样试取日线, 成功即认为数据源可用。"""
    n = len(syms or [])
    if not n:
        return True
    # 取样必须分散: 列表头部多为指数(SSE.000001 等), 避免只探前几只误判
    idxs = [0, n // 4, n // 2, (3 * n) // 4, n - 1] if n >= 5 else list(range(n))
    for s in [syms[i] for i in idxs]:
        try:
            df = tx_kline.fetch_df(s, "day", 60)
            if df is not None and not df.empty:
                return True
        except Exception:
            continue
    return False


def _screen_wait_ready(syms, wait_max=None):
    """等数据源就绪(腾讯源通常立即成功; 网络抖动时重试到 wait_max)。"""
    wait_max = SCREEN_READY_WAIT if wait_max is None else wait_max
    t0 = time.time()
    first = True
    while time.time() - t0 < wait_max:
        if _screen_probe(syms):
            if not first:
                print("[股票筛选] 数据源已就绪, 开始筛选")
            return True
        if first:
            print("[股票筛选] 等待腾讯数据源就绪(最多 %ds)…" % wait_max)
            first = False
        time.sleep(15)
    print("[股票筛选] 等待数据源就绪超时(%ds), 放弃本轮" % wait_max)
    return False


def _screen_retry_later(reason):
    """一轮取数全失败 → 延迟重试(最多 SCREEN_RETRY_MAX 次)。"""
    if _SCREEN_RETRY["n"] >= SCREEN_RETRY_MAX:
        print("[股票筛选] 重试次数已用尽(%s)" % reason)
        return
    _SCREEN_RETRY["n"] += 1
    print("[股票筛选] %s, %d 秒后第 %d 次重试" % (reason, SCREEN_RETRY_DELAY, _SCREEN_RETRY["n"]))

    def _f():
        time.sleep(SCREEN_RETRY_DELAY)
        if not _SCREEN["running"]:
            _screen_wait_scan_then_run()
    threading.Thread(target=_f, daemon=True).start()


def _screen_full_run(limit=None, symbols=None, source="auto"):
    """全市场筛选一轮。2026-09-28 迁移: 数据源=腾讯 tx(前复权日线, 6 并发),
    不再依赖天勤扫描账号/会话(原分块重开会话逻辑随会话一起移除)。"""
    if not _SCREEN_RUN_LOCK.acquire(blocking=False):
        print("[股票筛选] 已有任务在跑, 本次跳过")
        return
    try:
        # [2026-09-21] 启动补跑可能早于 A 股列表加载完成 -> 先等列表就绪(最多 180s),
        # 否则 len(syms)=0 会直接判"股票列表为空"中止整轮。
        _wt0 = time.time()
        while True:
            with _STOCK_LOCK:
                _n = len(_STOCK_LIST)
                _rdy = _STOCK_READY
            if _rdy or _n > 0:
                break
            if time.time() - _wt0 > 180:
                print("[股票筛选] 等待 A 股列表就绪超时(180s)")
                break
            time.sleep(3)
        with _STOCK_LOCK:
            all_syms = [x["symbol"] for x in _STOCK_LIST]
        syms = [str(s) for s in symbols] if symbols else all_syms
        if limit:
            try:
                syms = syms[:max(1, int(limit))]
            except Exception:
                pass
        if not syms:
            _SCREEN.update(running=False, error="股票列表为空")
            return
        # 状态先置 running: 等待数据源就绪期间前端也能看到进度(否则页面显示"尚未跑过")
        _SCREEN.update(running=True, total=len(syms), done=0, ok=0, hits=0,
                       started=time.time(), account="tx腾讯源",
                       last="", last_date="", source=source, error="等待腾讯数据源就绪…",
                       waiting=1, cond_cnt={"c1": 0, "c2": 0, "c3": 0, "c4": 0, "c5": 0},
                       elapsed=0.0)
        if not _screen_wait_ready(syms):
            _SCREEN.update(running=False, waiting=0, error="腾讯数据源未就绪(等待超时)")
            _screen_retry_later("数据源未就绪")
            return
        _SCREEN["waiting"] = 0
        _SCREEN["error"] = ""
        scan_date = date.today()
        t0 = time.time()
        _SCREEN["started"] = t0
        print("[股票筛选] 开始: %d 只, 数据源 %s, 来源 %s" % (len(syms), _SCREEN["account"], source))
        names = _screen_names()
        buf = []
        mbuf = []
        # 2026-09-29 提速(实测旧串行路径全市场 9h 不可接受): 逐只串行 fetch_df →
        #   分块(300/块)并发批量预取 _tx_day_batch(6 并发, 实测吞吐 ~119 req/s,
        #   单只 800 根日线仅 ~0.15s), 取数与计算解耦; 分块控制内存(~60MB/块), 批间小憩防限流。
        #   单轮全市场预期 9h → 5-10 分钟, 支撑日内加跑(11:35)当轮出当日盘中档。
        _CHUNK = 300
        _done = 0
        for _cs in range(0, len(syms), _CHUNK):
            _chunk = syms[_cs:_cs + _CHUNK]
            dfmap = _tx_day_batch(_chunk, SCREEN_FETCH_BARS)
            for i, sym in enumerate(_chunk):
                try:
                    df = dfmap.get(sym)
                    out = _screen_one(sym, df) if df is not None else None
                except Exception:
                    out = None
                if out:
                    _SCREEN["ok"] += 1
                    row = out["row"]
                    if row["hit_count"]:
                        _SCREEN["hits"] += 1
                        for k in ("c1", "c2", "c3", "c4", "c5"):
                            if row[k]:
                                _SCREEN["cond_cnt"][k] = _SCREEN["cond_cnt"].get(k, 0) + 1
                    nm = names.get(sym) or ""
                    out["row"]["_name"] = nm
                    buf.append(out["row"])
                    if out["month"]:
                        mbuf.append(out["month"])
                _done += 1
                _SCREEN["done"] = _done
                _SCREEN["last"] = sym
                if len(buf) >= SCREEN_SAVE_BATCH:
                    _screen_save_batch(buf, scan_date, source, names)
                    if mbuf:
                        _screen_save_month(mbuf, names)
                    buf = []; mbuf = []
            del dfmap   # 及时释放本块 K 线内存
            if _cs + _CHUNK < len(syms):
                time.sleep(0.3)   # 批间小憩防限流
        if buf:
            _screen_save_batch(buf, scan_date, source, names)
        if mbuf:
            _screen_save_month(mbuf, names)
        _SCREEN["elapsed"] = round(time.time() - t0, 1)
        _SCREEN["running"] = False
        if _SCREEN["ok"] == 0 and len(syms) >= 50:
            # 取数全失败(会话被限流/权限/网络) —— 不写 last_date, 延迟重试
            _SCREEN["error"] = "本轮取数全部失败(会话未就绪或被限流)"
            _screen_retry_later("本轮取数全部失败")
            return
        _SCREEN_RETRY["n"] = 0
        _SCREEN["last_date"] = str(scan_date)
        # 2026-09-30 需求人裁定: 不要历史 —— 每轮全量跑完只保留当天结果, 旧 scan_date 直接清掉。
        #   (当天 11:35/15:05 多轮同表覆盖写, 不受影响; 次日开盘后旧结果仍可看, 直到当天首轮跑完才清。)
        try:
            _pc = _cross_conn(); _pu = _pc.cursor()
            _pu.execute("DELETE FROM t_stock_screen_result WHERE scan_date < %s", (scan_date,))
            _pn = _pu.rowcount
            _pc.commit(); _pc.close()
            if _pn:
                print("[股票筛选] 已清理历史筛选结果 %d 行(只保留 %s)" % (_pn, scan_date))
        except Exception as _pe:
            print("[股票筛选] 清理历史筛选结果失败:", _pe)
        print("[股票筛选] 完成: %d/%d 有数据, 命中 %d 只, 耗时 %ss"
              % (_SCREEN["ok"], len(syms), _SCREEN["hits"], _SCREEN["elapsed"]))
    finally:
        _SCREEN["running"] = False
        _SCREEN_RUN_LOCK.release()


def _screen_save_batch(rows, scan_date, src, names):
    """带名称的批量落库(名称来自内存表, 表里没有再落空)。"""
    if not rows:
        return 0
    try:
        conn = _cross_conn(); cur = conn.cursor()
        vals = [(scan_date, r["symbol"], r.get("_name") or "", None, None,
                 r["c1"], r["c2"], r["c3"], r["c4"], r["c5"], r["hit_count"],
                 r["dif"], r["atr"], r["zero_gap"], r["detail"],
                 r["d_golden"], r["w_golden"], r["m_golden"], src) for r in rows]
        cur.executemany("""INSERT INTO t_stock_screen_result
            (scan_date,symbol,name,last_price,change_pct,c1,c2,c3,c4,c5,hit_count,
             dif,atr,zero_gap,detail,d_golden,w_golden,m_golden,src,updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE
              c1=VALUES(c1),c2=VALUES(c2),c3=VALUES(c3),c4=VALUES(c4),c5=VALUES(c5),
              hit_count=VALUES(hit_count),dif=VALUES(dif),atr=VALUES(atr),
              zero_gap=VALUES(zero_gap),detail=VALUES(detail),d_golden=VALUES(d_golden),
              w_golden=VALUES(w_golden),m_golden=VALUES(m_golden),src=VALUES(src),
              updated_at=NOW()""", vals)
        conn.commit(); conn.close()
        return len(vals)
    except Exception as e:
        print("[股票筛选] 批量落库失败:", e); return 0


def _screen_has_result(d=None):
    """当天是否【已经有】筛选结果(任意行数)。"""
    try:
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM t_stock_screen_result WHERE scan_date=%s",
                    (d or date.today(),))
        n = cur.fetchone()[0]; conn.close()
        return n > 0
    except Exception:
        return True   # 查不到就当作已有结果, 避免误触发长任务


def _screen_is_complete(d=None, ratio=0.6):
    """当天筛选是否【已完整跑过】。

    2026-09-21 修正: 原先补跑判定直接复用 _screen_has_result(有行即算跑过),
    一轮跑到一半重启时当天只落了部分行 -> 被判"已跑过" -> 启动补跑永不触发,
    当天数据永久残缺(实测当天只有 143/5557 行就被认定完成)。
    改为按行数占比判定: >= ratio × 全市场股票数 才算完整
    (留出停牌/退市/取数失败造成的自然缺口)。
    """
    try:
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM t_stock_screen_result WHERE scan_date=%s",
                    (d or date.today(),))
        n = cur.fetchone()[0]; conn.close()
    except Exception:
        return True   # 查不到就当作已完成, 避免误触发长任务
    try:
        with _STOCK_LOCK:
            total = len(_STOCK_LIST)
            ready = _STOCK_READY
    except Exception:
        total, ready = 0, False
    if (not ready) or total <= 0:
        # [2026-09-21] 启动初期 A 股列表还没加载完时 total=0, 旧逻辑 `return n > 0` 会把
        # "已有几行部分结果"误判成"已完成" -> 补跑永不触发(实测重启后 done=0/0 且无任何判定日志)。
        # 列表未就绪时一律视为【不完整】, 交给补跑线程去等(_screen_full_run 内有列表就绪等待)。
        print("[股票筛选] 完整性判定 %s: A股列表尚未就绪(ready=%s, total=%d), 视为不完整"
              % (d or date.today(), ready, total))
        return False
    need = int(total * float(ratio))
    print("[股票筛选] 完整性判定 %s: 已有 %d 行 / 股票 %d 只 (需 >= %d)" % (d or date.today(), n, total, need))
    return n >= need


def _screen_wait_scan_then_run(wait_max=5400):
    """等全市场遍历(11:31/15:01)跑完再跑筛选, 避免两个全市场任务并发压垮天勤会话。"""
    t0 = time.time()
    while _SCAN.get("running"):
        if time.time() - t0 > wait_max:
            print("[股票筛选] 等待全市场遍历超时, 仍开始筛选")
            break
        time.sleep(15)
    _screen_full_run(source="auto")


def _start_screen_scheduler():
    """每天 SCREEN_TIMES 触发一轮筛选; 启动时若工作日已过时刻且当天无结果 → 补跑。"""
    global _SCREEN_SCHED_STARTED
    if _SCREEN_SCHED_STARTED:
        return
    _SCREEN_SCHED_STARTED = True

    def _loop():
        while True:
            try:
                now = datetime.now()
                day = now.strftime("%Y%m%d")
                _nowmin = now.hour * 60 + now.minute
                # 2026-09-30 修复: 原精确分钟匹配(hm in SCREEN_TIMES + 20s 轮询)极易错过
                #   (轮询 sleep 跨过该分钟 / 服务恰在该分钟内未就绪), 错过即等下一档。
                #   改为【触发窗】判定: 进入档位后 10 分钟内都算命中, 每档每天只跑一次。
                for _t in SCREEN_TIMES:
                    _t0 = int(_t[:2]) * 60 + int(_t[3:5])
                    _key = day + "-" + _t
                    if _t0 <= _nowmin < _t0 + 10 and not _SCREEN_FIRED.get(_key):
                        _SCREEN_FIRED[_key] = True
                        if not _SCREEN["running"]:
                            print("[股票筛选] 定时触发 %s (等待全市场遍历让路)" % _t)
                            threading.Thread(target=_screen_wait_scan_then_run, daemon=True).start()
            except Exception as e:
                print("[股票筛选] 调度异常:", e)
            time.sleep(20)
    threading.Thread(target=_loop, daemon=True).start()
    print("[股票筛选] 定时已设定: 每天 %s (收盘后)" % " / ".join(SCREEN_TIMES))

    # 启动补跑: 工作日 + 已过【上午档 11:35】+ 当天库里没有结果
    #   2026-09-30 修复: 原条件 >= SCREEN_TIMES[-1](15:05) 导致 11:36~15:04 之间启动
    #   永远不补上午档(11:35 精确分钟又易错过) → 当天筛选表全空、状态列全空(实锤)。
    try:
        now = datetime.now()
        if now.weekday() < 5 and now.strftime("%H:%M") >= SCREEN_TIMES[0] \
                and not _screen_is_complete(now.date()):
            print("[股票筛选] 检测到当天(%s)筛选结果不完整, 30 秒后自动补跑一轮" % now.date())
            def _delay():
                time.sleep(30)
                _screen_wait_scan_then_run()
            threading.Thread(target=_delay, daemon=True).start()
    except Exception as e:
        print("[股票筛选] 补跑判断失败:", e)


# --------------------- 筛选 API ---------------------
def api_screen_run():
    """POST {limit?, symbols?, src?} 手动补跑(limit/symbols 供小范围试跑)。"""
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}
    if _SCREEN["running"]:
        return jsonify({"ok": True, "running": True, "screen": _SCREEN})
    limit = data.get("limit")
    symbols = data.get("symbols")
    if not isinstance(symbols, list):
        symbols = None
    threading.Thread(target=_screen_full_run,
                     kwargs={"limit": limit, "symbols": symbols, "source": "manual"},
                     daemon=True).start()
    return jsonify({"ok": True, "started": True})


def api_screen_status():
    dates = []
    try:
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute("SELECT scan_date, COUNT(*), SUM(hit_count>0) FROM t_stock_screen_result "
                    "GROUP BY scan_date ORDER BY scan_date DESC LIMIT 30")
        for d, n, hit in cur.fetchall():
            dates.append({"date": str(d), "total": int(n), "hits": int(hit or 0)})
        conn.close()
    except Exception as e:
        print("[股票筛选] 取历史日期失败:", e)
    return jsonify({"ok": True, "screen": _SCREEN, "account": get_scan_account(),
                    "next_times": SCREEN_TIMES, "dates": dates})


def api_screen_result():
    """GET ?date=&cond=c1..c5&limit=200 取某日筛选结果。date 缺省=库里最新一天。"""
    d = (request.args.get("date") or "").strip()
    cond = (request.args.get("cond") or "").strip().lower()
    try:
        limit = min(1000, max(10, int(request.args.get("limit", "200"))))
    except Exception:
        limit = 200
    try:
        conn = _cross_conn(); cur = conn.cursor()
        if not d:
            cur.execute("SELECT MAX(scan_date) FROM t_stock_screen_result")
            row = cur.fetchone()
            d = str(row[0]) if row and row[0] else ""
        if not d:
            conn.close()
            return jsonify({"ok": True, "date": "", "items": [], "total": 0,
                            "hint": "还没有筛选结果, 先点「立即筛选」跑一轮"})
        where = "scan_date=%s"
        args = [d]
        if cond in ("c1", "c2", "c3", "c4", "c5"):
            where += " AND %s=1" % cond
        else:
            where += " AND hit_count>0"
        cur.execute("SELECT COUNT(*) FROM t_stock_screen_result WHERE " + where, args)
        total = int(cur.fetchone()[0])
        cur.execute("""SELECT symbol,name,c1,c2,c3,c4,c5,hit_count,dif,atr,zero_gap,detail,
                              d_golden,w_golden,m_golden
                       FROM t_stock_screen_result WHERE """ + where +
                    " ORDER BY hit_count DESC, symbol LIMIT %s", args + [limit])
        cols = ["symbol", "name", "c1", "c2", "c3", "c4", "c5", "hit_count", "dif",
                "atr", "zero_gap", "detail", "d_golden", "w_golden", "m_golden"]
        items = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
        # 名称兜底: 库里 name 为空时用内存全市场表补
        nm = _screen_names()
        for it in items:
            if not it.get("name"):
                it["name"] = nm.get(it["symbol"], "")
            for k in ("c1", "c2", "c3", "c4", "c5", "hit_count"):
                it[k] = int(it.get(k) or 0)
            for k in ("dif", "atr", "zero_gap"):
                it[k] = _r(it.get(k))
        # 附实时价(2026-09-28 迁移: 腾讯批量报价, 不再依赖天勤扫描账号)
        try:
            if items:
                qmap = tx_kline.fetch_quotes_batch([it["symbol"] for it in items[:200]])
                for it in items:
                    qb = qmap.get(it["symbol"]) or {}
                    last = _r(qb.get("last")); pre = _r(qb.get("pre_close"))
                    it["last"] = last
                    if last is not None and pre and pre > 0:
                        it["change_pct"] = round((last - pre) / pre * 100, 2)
        except Exception:
            pass
        return jsonify({"ok": True, "date": d, "total": total, "items": items})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def api_screen_month_history():
    """GET ?symbol=&limit= 月线金叉历史(落库数据, 不再调天勤接口)。"""
    sym = (request.args.get("symbol") or "").strip()
    try:
        limit = min(500, max(10, int(request.args.get("limit", "200"))))
    except Exception:
        limit = 200
    try:
        conn = _cross_conn(); cur = conn.cursor()
        if sym:
            cur.execute("""SELECT symbol,name,cross_date,dif,dea,price,first_seen,last_seen
                           FROM t_stock_month_golden WHERE symbol=%s
                           ORDER BY cross_date DESC LIMIT %s""", (sym, limit))
        else:
            cur.execute("""SELECT symbol,name,cross_date,dif,dea,price,first_seen,last_seen
                           FROM t_stock_month_golden ORDER BY first_seen DESC, symbol LIMIT %s""",
                        (limit,))
        cols = ["symbol", "name", "cross_date", "dif", "dea", "price", "first_seen", "last_seen"]
        items = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
        nm = _screen_names()
        for it in items:
            if not it.get("name"):
                it["name"] = nm.get(it["symbol"], "")
            for k in ("first_seen", "last_seen"):
                if it.get(k) is not None:
                    it[k] = str(it[k])
            it["price"] = _r(it.get("price"))
        return jsonify({"ok": True, "items": items})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ===================== 页面 =====================
_THEME = ''':root{--bg:#0b1120;--surface:#111827;--surface2:#1b2538;--border:#1e2d4a;--text:#e6edf7;--muted:#94a3b8;--accent:#38bdf8;--accent2:#f472b6;--violet:#c084fc;--up:#ef4444;--down:#19c37d;}'''


def stock_list_page():
    try:
        from services import stock_minute_engine as _me
        _minute_status_str = _me.minute_status_string()
    except Exception:
        _minute_status_str = "分钟引擎状态未知"
    doc = '<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate"><meta http-equiv="Pragma" content="no-cache"><meta http-equiv="Expires" content="0"><title>股票 - 周/月 MACD 金叉</title>'
    doc += '<script src="static/echarts.min.js"></script>'
    doc += '<style>' + _THEME + '''
    *{margin:0;padding:0;box-sizing:border-box}
    html,body{height:100%}
    body{background:var(--bg);color:var(--text);font-family:Microsoft YaHei,-apple-system,Segoe UI,sans-serif;font-size:14px;display:flex;flex-direction:column}
    .sk-wrap{display:flex;flex-direction:column;flex:1;min-height:0;position:relative}
    .sk-left-col{display:flex;flex-direction:column;flex:1;min-height:0;background:var(--surface)}
    .sk-right-col{position:fixed;inset:0;z-index:900;background:var(--bg);display:none;flex-direction:column;min-height:0}
    .sk-right-col.show{display:flex}
    .sk-mon-col{position:relative;background:var(--surface);display:flex;flex-direction:column;flex:1;min-height:0;overflow:hidden}
    .sk-mon-col .sk-monitor-hd{cursor:default;display:flex;align-items:center;gap:8px;padding:10px 12px;background:var(--surface2);border-bottom:1px solid var(--border)}
    .sk-mon-col .sk-monitor-hd .t{font-weight:700;font-size:14px;flex:0 0 auto}
    .sk-mon-col .sk-monitor-hd .badge{font-size:11px;background:var(--up);color:#fff;border-radius:10px;padding:1px 7px}
    .sk-mon-tabs{display:flex;align-items:center;gap:4px}
    .sk-mon-tabs button{padding:5px 12px;background:var(--surface);border:1px solid var(--border);color:var(--muted);border-radius:5px;cursor:pointer;font-size:13px}
    .sk-mon-tabs button.active{background:var(--accent);color:#05101c;font-weight:700;border-color:var(--accent)}
    .sk-list-hd{padding:12px 14px;border-bottom:1px solid var(--border)}
    .sk-list-hd h2{font-size:16px;margin-bottom:8px}
    .sk-search{width:100%;padding:8px 10px;background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:6px;font-size:13px}
    .sk-stat{font-size:12px;color:var(--muted);margin-top:6px}
    .sk-rows{flex:1;overflow:auto;min-height:0}
    .sk-row{display:flex;align-items:center;gap:8px;padding:7px 14px;border-bottom:1px solid rgba(255,255,255,.03);cursor:pointer}
    .sk-row:hover{background:var(--surface2)}
    .sk-dot{width:8px;height:8px;border-radius:50%;background:var(--up);display:inline-block;flex:0 0 auto;box-shadow:0 0 6px var(--up)}
    .sk-scanbar{display:flex;align-items:center;gap:8px;margin-top:8px;flex-wrap:wrap}
    .sk-scanbar button{padding:5px 12px;background:var(--surface2);border:1px solid var(--border);color:var(--accent);border-radius:6px;cursor:pointer;font-size:12px}
    .sk-scanbar button:hover{border-color:var(--accent)}
    .sk-scanbar button:disabled{opacity:.6;cursor:default}
    .sk-cb{font-size:12px;color:var(--muted);display:flex;align-items:center;gap:4px;cursor:pointer}
    .sk-code{font-weight:700;min-width:84px}
    .sk-name{color:var(--muted);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .sk-ex{font-size:11px;color:var(--muted);background:var(--surface2);border:1px solid var(--border);border-radius:4px;padding:1px 5px}
    .sk-more{text-align:center;padding:10px;color:var(--accent);cursor:pointer;font-size:13px}
    .sk-detail{display:none;flex-direction:column;min-height:0;background:var(--bg);position:absolute;inset:0;z-index:5}
    .sk-detail.open{display:flex}
    .sk-detail-hd{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:14px;flex-wrap:wrap}
    .sk-detail-hd .t{font-size:18px;font-weight:700}
    .sk-detail-hd .s{color:var(--muted);font-size:13px}
    .sk-q{font-size:13px;color:var(--muted)}
    .sk-q b{font-size:16px;margin-left:4px}
    .sk-periods{margin-left:auto;display:flex;gap:6px}
    .sk-periods button{padding:6px 14px;background:var(--surface);border:1px solid var(--border);color:var(--muted);border-radius:6px;cursor:pointer;font-size:13px}
    .sk-periods button.active{background:var(--accent);color:#05101c;font-weight:700;border-color:var(--accent)}
    .sk-detail-bd{flex:1;display:flex;flex-direction:column;min-height:0;overflow:hidden;position:relative}
    .sk-chart{flex:1;min-height:120px;overflow:hidden;display:none}
    .sk-ind-params{position:absolute;top:8px;right:8px;z-index:6;background:rgba(15,23,42,.94);border:1px solid var(--border);border-radius:8px;padding:10px 12px;display:none;flex-direction:column;gap:7px;font-size:12px;color:var(--text);box-shadow:0 8px 24px rgba(0,0,0,.55)}
    .sk-ind-params.show{display:flex}
    .sk-ind-params .ip-t{font-weight:700;color:var(--accent);margin-bottom:2px}
    .sk-ind-params .ip-row{display:flex;align-items:center;gap:6px}
    .sk-ind-params label{color:var(--muted);min-width:34px}
    .sk-ind-params input{width:46px;background:var(--surface);border:1px solid var(--border);color:var(--text);border-radius:4px;padding:2px 5px}
    .sk-ind-params .ip-ma input{width:36px}
    .sk-ind-params button{margin-top:4px;padding:5px 10px;background:var(--surface);border:1px solid var(--border);color:var(--accent);border-radius:6px;cursor:pointer}
    .sk-ind-params button:hover{border-color:var(--accent)}
    .sk-cross{flex:1;min-height:120px;overflow:auto;border-top:1px solid var(--border);background:var(--surface)}
    .sk-cross table{width:100%;border-collapse:collapse;font-size:13px}
    .sk-cross th,.sk-cross td{padding:5px 12px;text-align:left;border-bottom:1px solid var(--border)}
    .sk-cross th{color:var(--muted);font-weight:600;position:sticky;top:0;background:var(--surface)}
    .sk-cross tbody tr.sk-cross-row{cursor:pointer;transition:background .1s}
    .sk-cross tbody tr.sk-cross-row:hover{background:rgba(56,189,248,.12)}
    .sk-cross tbody tr.sk-cross-row.active{background:rgba(56,189,248,.22)}
    .sk-gold{color:var(--up);font-weight:700}
    .sk-death{color:var(--down);font-weight:700}
    .sk-empty{padding:40px;color:var(--muted);text-align:center}
    .sk-cross-bar{display:flex;align-items:center;gap:14px;padding:8px 12px;border-bottom:1px solid var(--border);flex-wrap:wrap;background:var(--surface2)}
    .sk-cross-bar .sk-cb{font-size:13px;color:var(--muted);display:flex;align-items:center;gap:4px}
    .sk-cross-bar input[type=number]{width:54px;background:var(--surface);border:1px solid var(--border);color:var(--text);border-radius:4px;padding:2px 4px}
    .sk-cross-bar input[type=text]{width:96px;background:var(--surface);border:1px solid var(--border);color:var(--text);border-radius:4px;padding:2px 6px}
    .sk-cross-bar button{padding:5px 12px;background:var(--surface);border:1px solid var(--border);color:var(--accent);border-radius:6px;cursor:pointer;font-size:13px}
    .sk-cross-bar button:hover{border-color:var(--accent)}
    .sk-cross-stat{margin-left:auto;font-size:12px;color:var(--muted)}
    .up{color:var(--up)} .down{color:var(--down)}
    /* 股票监控浮窗 */
    .sk-monitor-fab{position:fixed;right:20px;bottom:20px;z-index:9999;padding:10px 16px;background:linear-gradient(135deg,var(--accent),var(--violet));color:#05101c;border:none;border-radius:24px;cursor:pointer;font-weight:700;box-shadow:0 4px 16px rgba(56,189,248,.35);font-size:13px}
    .sk-monitor-fab:hover{transform:translateY(-2px)}
    .sk-monitor-win{position:fixed;right:20px;bottom:64px;width:420px;max-height:70vh;background:var(--surface);border:1px solid var(--border);border-radius:10px;box-shadow:0 10px 40px rgba(0,0,0,.55);z-index:9999;display:flex;flex-direction:column;overflow:hidden;resize:both;min-width:300px;min-height:180px}
    .sk-monitor-win.collapsed{height:44px!important;min-height:44px!important;resize:none}
    .sk-monitor-hd{padding:10px 12px;background:var(--surface2);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px;cursor:move;user-select:none}
    .sk-monitor-hd .t{font-weight:700;font-size:14px;flex:1}
    .sk-monitor-hd .badge{font-size:11px;background:var(--up);color:#fff;border-radius:10px;padding:1px 7px}
    .sk-monitor-hd .btn{font-size:13px;color:var(--muted);cursor:pointer;padding:2px 6px;border-radius:4px}
    .sk-monitor-hd .btn:hover{background:var(--surface);color:var(--text)}
    .sk-monitor-body{flex:1;overflow:auto;min-height:0}
    .sk-monitor-body table{width:100%;border-collapse:collapse;font-size:12px}
    .sk-monitor-body th,.sk-monitor-body td{padding:7px 8px;border-bottom:1px solid var(--border);text-align:left;white-space:nowrap}
    .sk-monitor-body th{position:sticky;top:0;background:var(--surface);color:var(--muted);font-weight:600}
    .sk-monitor-body tr:hover{background:var(--surface2)}
    .sk-monitor-body tr.gone{opacity:.55}
    .sk-monitor-body .nm{color:var(--muted);max-width:90px;overflow:hidden;text-overflow:ellipsis}
    .sk-monitor-body .last{font-weight:700}
    .sk-monitor-body .dot{width:7px;height:7px;border-radius:50%;display:inline-block;margin-right:4px;background:var(--up);box-shadow:0 0 5px var(--up)}
    .sk-monitor-body .dot.death{background:var(--down);box-shadow:0 0 5px var(--down)}
    .sk-monitor-ft{padding:8px 10px;border-top:1px solid var(--border);background:var(--surface2);display:flex;align-items:center;flex-wrap:wrap;gap:6px;font-size:12px;color:var(--muted)}
    .sk-monitor-ft button{padding:4px 10px;background:var(--surface);border:1px solid var(--border);color:var(--accent);border-radius:5px;cursor:pointer;font-size:12px}
    .sk-monitor-ft button:hover{border-color:var(--accent)}
    .sk-monitor-ft .status{flex:1}
    .sk-row .sk-add{opacity:0;font-size:12px;color:var(--accent);margin-left:6px;cursor:pointer;padding:2px 6px;border-radius:4px}
    .sk-row:hover .sk-add{opacity:1}
    .sk-row .sk-add:hover{background:var(--surface2)}
    .sk-detail-hd .sk-addmon{padding:5px 12px;background:var(--surface2);border:1px solid var(--border);color:var(--accent);border-radius:6px;cursor:pointer;font-size:12px;margin-left:10px}
    .sk-detail-hd .sk-addmon:hover{border-color:var(--accent)}
    .sk-row .sk-add.monitored{opacity:1;color:var(--up)}
    .sk-sortbar{display:flex;align-items:center;gap:10px;padding:6px 14px;border-bottom:1px solid var(--border);flex-wrap:wrap}
    .sk-sortbar select{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:5px;padding:3px 6px;font-size:12px}
    .sk-sortbar .sk-note{font-size:11px;color:var(--muted);margin-left:auto;cursor:help}
    .sk-pct{margin-left:auto;font-size:12px;font-weight:600;min-width:54px;text-align:right}
    .sk-pct.up{color:var(--up)}
    .sk-pct.down{color:var(--down)}
    .sk-pin-sep{position:sticky;top:0;padding:4px 14px;font-size:11px;font-weight:700;color:var(--accent);background:var(--surface2);border-bottom:1px solid var(--border);border-top:1px solid var(--border);z-index:1}
    .sk-detail-hd .sk-addmon.on{border-color:var(--up);color:var(--up)}
    /* 监控预警 */
    .sk-monitor-body tr.alert{background:rgba(239,68,68,.15);animation:skAlertPulse 1.2s infinite}
    .sk-monitor-body tr.alert-gold{background:rgba(245,158,11,.15);animation:skAlertPulseGold 1.2s infinite}
    .sk-monitor-body tr.alert-death{background:rgba(25,195,125,.15);animation:skAlertPulseDeath 1.2s infinite}
    .sk-monitor-body tr.alert-mgold{background:rgba(192,132,252,.15);animation:skAlertPulseMgold 1.2s infinite}
    .sk-monitor-body tr.alert-mdeath{background:rgba(56,189,248,.15);animation:skAlertPulseMdeath 1.2s infinite}
    @keyframes skAlertPulse{0%,100%{background:rgba(239,68,68,.15)}50%{background:rgba(239,68,68,.30)}}
    @keyframes skAlertPulseGold{0%,100%{background:rgba(245,158,11,.15)}50%{background:rgba(245,158,11,.30)}}
    @keyframes skAlertPulseDeath{0%,100%{background:rgba(25,195,125,.15)}50%{background:rgba(25,195,125,.30)}}
    @keyframes skAlertPulseMgold{0%,100%{background:rgba(192,132,252,.15)}50%{background:rgba(192,132,252,.30)}}
    @keyframes skAlertPulseMdeath{0%,100%{background:rgba(56,189,248,.15)}50%{background:rgba(56,189,248,.30)}}
    .sk-monitor-body tr.alert-dgold{background:rgba(234,179,8,.15);animation:skAlertPulseDgold 1.2s infinite}
    .sk-monitor-body tr.alert-ddeath{background:rgba(13,148,136,.15);animation:skAlertPulseDdeath 1.2s infinite}
    @keyframes skAlertPulseDgold{0%,100%{background:rgba(234,179,8,.15)}50%{background:rgba(234,179,8,.30)}}
    @keyframes skAlertPulseDdeath{0%,100%{background:rgba(13,148,136,.15)}50%{background:rgba(13,148,136,.30)}}
    .sk-alert-item.dgold{border-left:3px solid #eab308}
    .sk-alert-item.ddeath{border-left:3px solid #0d9488}
    /* 分钟金叉触发时关联展示的各周期近期金叉面板 */
    .sk-min-cross-info{position:fixed;right:16px;bottom:16px;width:320px;max-height:62vh;overflow:auto;background:var(--surface);border:1px solid var(--border);border-radius:8px;box-shadow:0 8px 30px rgba(0,0,0,.5);z-index:10002;display:none}
    .sk-min-cross-info.open{display:block;animation:skToastIn .25s ease}
    .sk-min-cross-info .mi-hd{padding:8px 10px;border-bottom:1px solid var(--border);background:var(--surface2);font-weight:700;font-size:13px;display:flex;align-items:center;justify-content:space-between;gap:8px}
    .sk-min-cross-info .mi-hd .close{cursor:pointer;color:var(--muted);font-size:15px;line-height:1}
    .sk-min-cross-info .mi-bd{padding:8px}
    .sk-min-cross-info .mi-tb{width:100%;border-collapse:collapse;font-size:12px}
    .sk-min-cross-info .mi-tb th{text-align:left;color:var(--muted);font-weight:600;padding:5px 6px;border-bottom:1px solid var(--border)}
    .sk-min-cross-info .mi-tb td{padding:5px 6px;border-bottom:1px solid var(--border);white-space:nowrap}
    .sk-min-cross-info .mi-tb tr.r{background:rgba(34,197,94,.12)}
    .sk-min-cross-info .mi-tb tr.h td{color:var(--muted);font-size:11px;border-bottom:1px solid var(--border)}
    .sk-min-cross-info .mi-recent{color:#22c55e;font-weight:700}
    .sk-min-cross-info .mi-old{color:var(--muted)}
    .sk-min-cross-info .mi-none{color:var(--muted)}
    .sk-min-cross-info .mi-hint{color:var(--muted);font-size:11px}
    .sk-min-cross-info .mi-empty{color:var(--muted);padding:14px;text-align:center}
    .sk-threshold{display:flex;align-items:center;gap:6px;margin-left:auto}
    .sk-threshold input{width:50px;background:var(--surface);border:1px solid var(--border);color:var(--text);border-radius:4px;padding:3px 5px;font-size:12px}
    .sk-toast-wrap{position:fixed;right:20px;bottom:20px;z-index:10001;display:flex;flex-direction:column;gap:8px;pointer-events:none}
    .sk-toast{pointer-events:auto;background:var(--surface);border:1px solid var(--border);border-left:4px solid var(--up);border-radius:6px;padding:10px 14px;box-shadow:0 6px 24px rgba(0,0,0,.5);min-width:220px;max-width:320px;font-size:13px;animation:skToastIn .25s ease}
    .sk-toast.gold{border-left-color:var(--accent2)}
    .sk-toast .tt{font-weight:700;margin-bottom:2px}
    .sk-toast .tc{color:var(--muted);font-size:12px}
    .sk-toast .close{float:right;cursor:pointer;color:var(--muted);margin-left:8px}
    @keyframes skToastIn{from{transform:translateX(100%);opacity:0}to{transform:translateX(0);opacity:1}}
    /* 预警历史记录 */
    .sk-alert-log{position:absolute;right:0;top:44px;bottom:0;width:260px;background:var(--surface);border-left:1px solid var(--border);display:none;flex-direction:column;z-index:10}
    .sk-alert-log.open{display:flex}
    .sk-alert-log-hd{padding:8px 10px;border-bottom:1px solid var(--border);background:var(--surface2);display:flex;align-items:center;gap:8px;font-weight:700;font-size:13px}
    .sk-alert-log-hd .btn{font-size:12px;color:var(--muted);cursor:pointer}
    .sk-alert-log-bd{flex:1;overflow:auto;padding:8px;font-size:12px}
    .sk-alert-item{padding:8px;border-bottom:1px solid var(--border);margin-bottom:6px;border-radius:4px;background:var(--surface2)}
    .sk-alert-item.price{border-left:3px solid var(--up)}
    .sk-alert-item.gold{border-left:3px solid var(--accent2)}
    .sk-alert-item.death{border-left:3px solid var(--down)}
    .sk-alert-item.mgold{border-left:3px solid var(--violet)}
    .sk-alert-item.mdeath{border-left:3px solid var(--accent)}
    .sk-alert-item .t{font-weight:700;margin-bottom:2px}
    .sk-alert-item .c{color:var(--muted);font-size:11px}
    .sk-alert-item .tm{color:var(--muted);font-size:10px;margin-top:3px}
    .sk-alert-empty{color:var(--muted);padding:20px;text-align:center}
    /* 顶部条(模块主页工具条) */
    .sk-topbar{display:flex;align-items:center;gap:12px;padding:10px 16px;background:var(--surface2);border-bottom:1px solid var(--border)}
    .sk-topbar .t{font-size:16px;font-weight:700}
    .sk-topbar .sk-acc{font-size:12px;color:var(--muted);display:inline-flex;align-items:center;gap:6px}
    .sk-topbar .sk-acc b{color:var(--accent)}
    .sk-topbar .sk-acc .sk-close{background:none;border:none;color:var(--muted);font-size:14px;cursor:pointer;padding:0 2px;line-height:1}
    .sk-topbar .sk-acc .sk-close:hover{color:var(--text)}
    .sk-topbar button{padding:6px 12px;background:var(--surface);border:1px solid var(--border);color:var(--accent);border-radius:6px;cursor:pointer;font-size:13px}
    .sk-topbar button:hover{border-color:var(--accent)}
    .sk-topbar .sk-stat{font-size:12px;color:var(--muted)}
    /* 左列监控: 自选监控 / 本周金叉 切换 */
    .sk-mon-period{margin-left:auto;background:var(--surface);border:1px solid var(--border);color:var(--text);border-radius:5px;padding:3px 6px;font-size:12px}
    .sk-mon-next{font-size:11px;color:var(--muted);flex:0 0 auto;min-width:96px;text-align:right}
    .sk-monitor-body{flex:1;overflow:auto;min-height:0}
    .sk-cross-recents{flex:1;display:flex;flex-direction:column;min-height:0;background:var(--bg)}
    .sk-cross-recents.hidden{display:none}
    .sk-cr-hd{padding:8px 12px;background:var(--surface2);border-bottom:1px solid var(--border);font-weight:700;font-size:13px;display:flex;align-items:center;gap:8px;cursor:pointer}
    .sk-cr-hd .cnt{font-size:11px;color:var(--muted);font-weight:400}
    .sk-cr-hd .toggle{margin-left:auto;font-size:12px;color:var(--muted);cursor:pointer}
    .sk-cr-body{flex:1;overflow:auto;min-height:0}
    .sk-cr-row{padding:6px 12px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;font-size:12px;cursor:pointer}
    .sk-cr-row:hover{background:var(--surface2)}
    .sk-cr-row .nm{font-weight:600}
    .sk-cr-row .sym{color:var(--muted);font-size:11px}
    .sk-cr-row .pd{margin-left:auto;color:var(--muted)}
    .sk-cr-row .p{color:var(--up);font-weight:600}
    .sk-cr-empty{padding:18px;color:var(--muted);text-align:center;font-size:12px}
    .sk-chart-modal{position:fixed;inset:0;background:rgba(0,0,0,.78);z-index:10000;display:flex;align-items:center;justify-content:center;padding:30px}
    .sk-chart-modal.hidden{display:none}
    .sk-chart-modal-inner{width:92vw;height:88vh;background:var(--surface);border:1px solid var(--border);border-radius:12px;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 24px 70px rgba(0,0,0,.75)}
    .sk-chart-modal-hd{padding:12px 16px;background:var(--surface2);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
    .sk-chart-modal-hd span{font-weight:700;color:var(--text)}
    .sk-chart-modal-hd button{background:none;border:none;color:var(--muted);font-size:18px;cursor:pointer;line-height:1}
    .sk-chart-modal-hd button:hover{color:var(--accent)}
    .sk-chart-modal-bd{flex:1;min-height:0;padding:10px;position:relative}
    .sk-chart-modal-bd #skChart{width:100%;height:100%}
    /* 每日筛选(MACD 5 条件) */
    .sk-screens{border-bottom:1px solid var(--border);background:var(--surface);font-size:12px}
    .sk-sc-row{display:flex;align-items:center;gap:8px;padding:6px 10px;flex-wrap:wrap}
    .sk-sc-row select{background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:5px;padding:3px 6px;font-size:12px}
    .sk-sc-row button{padding:4px 10px;background:var(--surface2);border:1px solid var(--border);color:var(--accent);border-radius:5px;cursor:pointer;font-size:12px}
    .sk-sc-row button:hover{border-color:var(--accent)}
    .sk-sc-row button:disabled{opacity:.5;cursor:default}
    .sk-sc-row .status{color:var(--muted);font-size:11px}
    .sk-sc-chips{display:flex;align-items:center;gap:6px;padding:0 10px 7px;flex-wrap:wrap}
    .sk-sc-lab{color:var(--muted);font-size:11px}
    .sk-sc-chip{display:inline-flex;align-items:center;gap:3px;padding:1px 7px;border-radius:10px;border:1px solid var(--border);color:var(--muted);cursor:pointer;font-size:11px;user-select:none}
    .sk-sc-chip b{color:var(--text)}
    .sk-sc-chip.on{background:rgba(56,189,248,.16);border-color:var(--accent);color:var(--accent)}
    .sk-sc-hit{display:inline-block;padding:0 5px;margin:0 3px 2px 0;border-radius:3px;background:rgba(244,114,182,.16);color:var(--accent2);font-size:10px;line-height:15px;vertical-align:1px}
    .sk-sc-detail{color:var(--muted);font-size:10px;line-height:13px}
    .sk-sc-code{color:var(--muted);font-size:10px}
    .sk-sc-cell{max-width:260px;white-space:normal}
.sk-monitor-body td.sk-sc-cell{white-space:normal;max-width:520px}
.sk-monitor-body td.sk-sc-cell .sk-sc-hit{font-size:11px;line-height:17px;margin:0 4px 3px 0}
.sk-none{color:var(--muted)}
    .sk-gold{color:#fbbf24}
    </style></head><body>'''
    # 顶部条(模块主页工具条)
    doc += '<div class="sk-topbar">'
    doc += '<span class="t">📈 股票监控</span>'
    doc += '<span class="sk-acc" id="skScanAcc">扫描账号: <b id="skAccName">' + get_scan_account() + '</b> <span style="color:var(--muted);font-size:11px">(每日 11:31 / 15:01 自动扫描)</span><button class="sk-close" onclick="skHideScanAcc()" title="隐藏">×</button></span>'
    doc += '</div>'
    doc += '<div class="sk-wrap">'
    # 左列: 监控(自选/本周金叉 切换)
    doc += '<div class="sk-left-col" id="skLeftCol">'
    doc += '<div class="sk-mon-col" id="skMonCol">'
    doc += '<div class="sk-min-cross-info" id="skMinCrossInfo" style="display:none"></div>'
    doc += '<div class="sk-monitor-hd" id="skMonHd">'
    doc += '<div class="sk-mon-tabs" id="skMonTabs"><button class="active" data-mode="watch" onclick="skSetMonMode(\'watch\')">实时金叉</button><button data-mode="week" onclick="skSetMonMode(\'week\')">本周金叉</button><button data-mode="screen" onclick="skSetMonMode(\'screen\')" title="收盘后按 MACD 5 个条件全市场筛选(每日 11:35 盘中档 / 15:05 收盘定档自动)">每日筛选</button></div>'
    doc += '<span class="badge" id="skMonBadge">0</span>'
    doc += '<select class="sk-mon-period" id="skMonPeriodSel" onchange="skSetMonPeriod(this.value)" title="监控刷新周期(以 9:00 为基轴对齐)"></select>'
    doc += '<span class="sk-mon-next" id="skMonNext">下次刷新 --:--</span>'
    doc += '<span class="sk-mon-next" id="skMinuteStat" title="分钟级金叉引擎状态(隔离线程, 专用扫描账号)">' + _minute_status_str + '</span></div>'
    # 每日筛选工具条(仅「每日筛选」标签显示)
    doc += '<div class="sk-screens" id="skScreenBar" style="display:none">'
    doc += '<div class="sk-sc-row"><select id="skScreenDate" onchange="skScreenPickDate(this.value)" title="选择扫描日期(可回看历史)"></select>'
    doc += '<button onclick="skScreenRun()" id="skScreenRunBtn" title="手动补跑一轮全市场筛选(约 40 分钟)">立即筛选</button>'
    doc += '<button onclick="skScreenMonthHistory()" title="查看已落库的月线金叉历史(直接读库, 不重新取数)">月线金叉历史</button>'
    doc += '<span class="status" id="skScreenStat">未加载</span></div>'
    doc += '<div class="sk-sc-chips" id="skScreenChips"></div>'
    doc += '</div>'
    doc += '<div class="sk-monitor-body" id="skMonBody"><table><thead id="skMonHead"><tr><th>标的</th><th>最新价</th><th>涨跌</th><th title="MACD 五条件：日线零轴下双金叉 / 日线零轴附近或零轴上金叉 / 日线底背离 / 周线金叉 / 月线金叉。命中即贴上标签；已扫过但五条都没命中显示「无」，尚未扫到显示「—」（悬停可看该结果属于哪一天的筛选）">MACD 状态</th></tr></thead><tbody id="skMonRows"><tr><td colspan="4" style="color:var(--muted);padding:20px">正在获取今日实时金叉…（自动刷新，只显示当前这根K线刚金叉且仍处金叉态的股票）</td></tr></tbody></table></div>'
    doc += '<div class="sk-monitor-ft"><span class="status" id="skMonStat">就绪</span>'
    doc += '<button onclick="skToggleAlertLog()">预警记录 <span id="skAlertCount">0</span></button>'
    doc += '<label class="sk-threshold">涨跌超 <input type="number" id="skAlertPct" value="0" min="0" step="0.5" onchange="skMonSaveThreshold()" title="涨跌幅绝对值超过此值时高亮+声音提醒, 0=关闭"> % 预警</label>'
    doc += '<button onclick="skMonRefresh()">立即刷新</button>'
    doc += '<button onclick="skExportMonitorList()">导出列表</button>'
    doc += '<button onclick="skToggleMute()" id="skMuteBtn" title="开启/关闭预警声音">🔊 声音</button>'
    doc += '<button onclick="skToggleNotify()" id="skNotifyBtn" title="开启/关闭桌面通知">🔕 通知</button>'
    doc += '<button onclick="skMonTogglePause()" id="skPauseBtn" title="暂停/继续自动刷新">⏸ 暂停</button>'
    doc += '<button onclick="skMonToggleSort()" id="skSortBtn" title="按涨跌幅排序">排序</button>'
    doc += '<label class="sk-cb"><input type="checkbox" id="skOnlyAlert" onchange="skMonToggleOnlyAlert()"> 只看预警</label>'
    doc += '</div>'
    doc += '<div class="sk-alert-log" id="skAlertLog"><div class="sk-alert-log-hd"><span>预警记录</span><span class="btn" onclick="skExportAlertLog()">导出</span><span class="btn" onclick="skClearAlertLog()">清空</span><span class="btn" onclick="skToggleAlertLog()">关闭</span></div><div class="sk-alert-log-bd" id="skAlertLogBd"><div class="sk-alert-empty">暂无预警</div></div></div>'
    doc += '</div>'
    doc += '</div>'
    # 右列: K线详情 / 金叉记录 —— 全屏浮层覆盖监控(打开时显示), 不再常驻右侧
    doc += '<div class="sk-right-col" id="skRightCol">'
    doc += '<div class="sk-cross-recents hidden" id="skCrossRecents">'
    doc += '<div class="sk-cr-hd" id="skCrHd"><button onclick="skCloseRightOverlay()" style="margin-right:10px;padding:4px 12px;background:var(--surface);border:1px solid var(--border);color:var(--accent);border-radius:6px;cursor:pointer;font-size:12px">← 返回监控</button>最近一周金叉记录 <span class="cnt" id="skCrCount"></span><span style="margin-left:auto;font-size:11px;color:var(--muted)" title="双击某一行弹出 K 线图并定位到该金叉">双击行弹出 K 线</span></div>'
    doc += '<div class="sk-cr-body" id="skCrBody"><div class="sk-cr-empty">加载中…</div></div>'
    doc += '</div>'
    doc += '<div class="sk-detail" id="skDetail"></div>'
    doc += '</div>'
    doc += '</div>'

    # 监控已改为常驻列(见上方 skMonCol), 不再使用悬浮浮窗(已移除 skMonFab / skMonWin)

    # Toast 容器
    doc += '''<div class="sk-toast-wrap" id="skToastWrap"></div>'''

    # K 线弹窗(双击金叉/死叉事件时弹出并定位到该节点)
    doc += '''<div class="sk-chart-modal hidden" id="skChartModal" onclick="if(event.target===this)skCloseChartModal()">
      <div class="sk-chart-modal-inner">
        <div class="sk-chart-modal-hd">
          <span id="skChartModalTitle">K 线图</span>
          <button onclick="skCloseChartModal()" title="关闭">✕</button>
        </div>
        <div class="sk-chart-modal-bd" id="skChartModalBd"><div class="sk-empty">加载中…</div></div>
      </div>
    </div>'''

    # JS
    # 周期配置注入 JS: 页面周期按钮与"按周期时间点刷新"都由这份配置驱动
    _pd_js = [{"k": k, "label": PERIODS[k]["label"], "align": PERIODS[k]["align"]} for k in PERIOD_ORDER]
    doc += '<script>var SK_PERIODS=' + json.dumps(_pd_js, ensure_ascii=False) \
           + ';var SK_DEFAULT_PERIOD="' + DEFAULT_STOCK_PERIOD + '";</script>'
    doc += r'''
    <script>
    function skBase(){
      var p=location.pathname;
      var i=p.indexOf('/stockkline');
      if(i>=0) return p.substring(0,i)+'/stockkline/';
      return '/stockkline/';
    }
    var SK_STATE={q:'',offset:0,limit:200,loading:false,hasMore:true,symbol:null,period:(typeof SK_DEFAULT_PERIOD!=='undefined'?SK_DEFAULT_PERIOD:'day'),chart:null,cur:null,scanning:false,scan:{},recentOnly:false,listRecent:{},scanAlerted:{},monitor:[],seen:{},seenOrder:[],seenSnap:{},monTimer:null,monPaused:false,names:{},monPrev:{},alertPct:0,alertLog:[],showLog:false,muted:false,monItems:[],monSort:'none',monOnlyAlert:false,notify:false,pinMon:false,listSort:'default',monMap:{},rowSeq:0,monPeriod:'m5',monNextTick:0,detailNextTick:0,monMode:'watch',weekCrosses:[],weekFilter:'',weekNextTick:0,goldenPeriod:''};
    try{ SK_STATE.pinMon=(localStorage.getItem('sk_pin_mon')==='1'); }catch(e){ SK_STATE.pinMon=false; }
    function skApi(u){ return skBase()+'api/'+u; }

    function skLoadStocks(reset){
      if(SK_STATE.loading) return;
      if(reset){ SK_STATE.offset=0; SK_STATE.hasMore=true; document.getElementById('skRows').innerHTML=''; }
      if(!SK_STATE.hasMore) return;
      SK_STATE.loading=true;
      var qs='q='+encodeURIComponent(SK_STATE.q)+'&offset='+SK_STATE.offset+'&limit='+SK_STATE.limit;
      if(SK_STATE.recentOnly) qs+='&recent=1';
      fetch(skApi('stocks?'+qs)).then(function(r){ return r.text(); }).then(function(t){ try{return JSON.parse(t);}catch(e){ throw new Error('返回不是JSON: '+t.slice(0,200)); } }).then(function(d){
        if(!d.ok){ throw new Error(d.error||'接口返回失败'); }
        var rows=document.getElementById('skRows');
        var stat=document.getElementById('skStat');
        stat.textContent='共 '+d.total+' 支 A 股'+(d.filtered_total!==d.total?(' (匹配 '+d.filtered_total+')'):'')+(d.ready?'':' · 列表加载中…');
        var pageSyms=[];
        d.stocks.forEach(function(s){
          if(s.name) SK_STATE.names[s.symbol]=s.name;
          SK_STATE.listRecent[s.symbol]={w:!!s.recent_golden, m:!!s.recent_golden_month, wg:s.last_golden_date||'', mg:s.last_golden_month_date||''};
          var row=document.createElement('div'); row.className='sk-row'; row.setAttribute('data-sym', s.symbol); row._s=s; row.setAttribute('data-idx', SK_STATE.rowSeq++); row.ondblclick=function(){ skOpen(s.symbol); };
          var nm=s.name?(''+s.name):'—';
          var dot=s.recent_golden?'<span class="sk-dot" title="近期周线金叉: '+(s.last_golden_date||'')+'"></span>':'';
          row.innerHTML=dot+'<span class="sk-code">'+s.code+'</span><span class="sk-name">'+nm+'</span><span class="sk-ex">'+s.exchange+'</span><span class="sk-add" onclick="event.stopPropagation();skMonAdd(\''+s.symbol+'\',\''+(s.name||'').replace(/\\/g,'\\\\').replace(/\'/g,"\\'")+'\')" title="加入监控">+</span>';
          var pctEl=document.createElement('span'); pctEl.className='sk-pct'; var addEl=row.querySelector('.sk-add'); if(addEl) row.insertBefore(pctEl, addEl);
          rows.appendChild(row);
          pageSyms.push(s.symbol);
        });
        skScanSymbols(pageSyms);
        SK_STATE.hasMore=d.has_more;
        var oldMore=document.getElementById('skMore'); if(oldMore) oldMore.remove();
        if(SK_STATE.hasMore){
          var more=document.createElement('div'); more.className='sk-more'; more.textContent='加载更多 ▾';
          more.id='skMore'; more.onclick=function(){ SK_STATE.offset+=SK_STATE.limit; skLoadStocks(false); };
          rows.appendChild(more);
        }
        SK_STATE.offset=d.offset+d.stocks.length;
        SK_STATE.loading=false;
        skSyncListMonFlags();
        skArrangeList();
      }).catch(function(e){
        SK_STATE.loading=false;
        var stat=document.getElementById('skStat');
        var rows=document.getElementById('skRows');
        if(stat) stat.textContent='加载失败: '+e.message;
        if(rows && !rows.children.length) rows.innerHTML='<div class="sk-empty" style="padding:20px;font-size:13px">股票列表加载失败，请刷新重试<br><span style="color:var(--muted)">'+e.message+'</span></div>';
      });
    }
    function skSearchInput(){
      var v=document.getElementById('skSearch').value.trim();
      SK_STATE.q=v; skLoadStocks(true);
    }
    // 全部A股列表已移除, 滚动加载保留兼容(元素不存在时不绑定)
    (function(){ var el=document.getElementById('skRows'); if(!el) return; el.addEventListener('scroll', function(e){
      var t=e.target;
      if(t.scrollTop+t.clientHeight >= t.scrollHeight-40){ if(SK_STATE.hasMore) skLoadStocks(false); }
    }); })();

    function skOpen(symbol){
      SK_STATE.symbol=symbol; SK_STATE.period=(typeof SK_DEFAULT_PERIOD!=='undefined'?SK_DEFAULT_PERIOD:'day');
      // 与 skRenderDetail/skTickDetail 一致: 长周期最多 30s 兜底, 不按"明天9:00"冻结
      SK_STATE.detailNextTick=Math.min(skNextPeriodTick(SK_STATE.period).getTime(), Date.now()+30000);
      var rc=document.getElementById('skRightCol'); if(rc) rc.classList.add('show');
      var rec=document.getElementById('skCrossRecents'); if(rec) rec.classList.add('hidden');
      var det=document.getElementById('skDetail'); if(det){ det.classList.add('open'); det.innerHTML='<div class="sk-empty">加载中…</div>'; }
      skRenderDetail();
    }
    function skBackToRecents(){
      SK_STATE.symbol=null;
      var rc=document.getElementById('skRightCol'); if(rc) rc.classList.add('show');
      var det=document.getElementById('skDetail'); if(det) det.classList.remove('open');
      var rec=document.getElementById('skCrossRecents'); if(rec) rec.classList.remove('hidden');
      skLoadRecentCrosses();
    }
    function skCloseRightOverlay(){
      SK_STATE.symbol=null;
      var rc=document.getElementById('skRightCol'); if(rc) rc.classList.remove('show');
      var det=document.getElementById('skDetail'); if(det) det.classList.remove('open');
      var rec=document.getElementById('skCrossRecents'); if(rec) rec.classList.add('hidden');
      if(SK_STATE.chart){ SK_STATE.chart.dispose(); SK_STATE.chart=null; }
    }
    function skShowChartModal(symbol, date, period){
      var modal=document.getElementById('skChartModal'); if(!modal) return;
      var title=document.getElementById('skChartModalTitle');
      var bd=document.getElementById('skChartModalBd');
      modal.classList.remove('hidden');
      bd.innerHTML='<div class="sk-empty">加载中…</div>';
      if(title) title.textContent=symbol+' · K 线图';
      var p=period || (typeof SK_DEFAULT_PERIOD!=='undefined'?SK_DEFAULT_PERIOD:'day');
      fetch(skApi('kline?symbol='+encodeURIComponent(symbol)+'&period='+p)).then(function(r){return r.json();}).then(function(d){
        if(!d.ok){ bd.innerHTML='<div class="sk-empty">加载失败: '+(d.error||'')+'</div>'; return; }
        bd.innerHTML='<div id="skChart"></div>';
        if(title) title.textContent=(d.name?d.name+' ':'')+d.symbol+' · '+d.period_label;
        skRenderChart(d);
        if(date){
          var target=(d.crosses||[]).find(function(c){return c.date===date && c.type==='golden';});
          if(!target) target=(d.crosses||[]).find(function(c){return c.date===date;});
          if(target){ setTimeout(function(){ skFocusChart(SK_STATE.chart, d, target.idx); }, 200); }
        }
      }).catch(function(e){ bd.innerHTML='<div class="sk-empty">请求失败: '+e+'</div>'; });
    }
    function skCloseChartModal(){
      var modal=document.getElementById('skChartModal'); if(modal) modal.classList.add('hidden');
      if(SK_STATE.chart){ SK_STATE.chart.dispose(); SK_STATE.chart=null; }
    }
    function skSetPeriod(p){ SK_STATE.period=p; SK_STATE.detailNextTick=Math.min(skNextPeriodTick(p).getTime(), Date.now()+30000); skRenderDetail(); }
    function skPeriodBtns(){
      var html='';
      (SK_PERIODS||[]).forEach(function(p){ html+='<button data-p="'+p.k+'">'+p.label+'</button>'; });
      return html;
    }
    // ===== 按周期时间点刷新(以 9:00 为基轴) =====
    function skNextPeriodTick(periodKey){
      var cfg=null; (SK_PERIODS||[]).forEach(function(p){ if(p.k===periodKey) cfg=p; });
      if(!cfg) return new Date(Date.now()+60000);
      var align=cfg.align;
      var now=new Date();
      function inSession(d){ var hm=d.getHours()*60+d.getMinutes(); return (hm>=540&&hm<=690)||(hm>=780&&hm<=900); }
      function next900(d){ var n=new Date(d); n.setHours(9,0,0,0);
        while(true){ var day=n.getDay(); if(day!==0&&day!==6&&n>now) return n; n.setDate(n.getDate()+1); n.setHours(9,0,0,0); } }
      if(!inSession(now)) return next900(now);
      var base=new Date(now); base.setHours(9,0,0,0);
      var elapsed=Math.floor((now-base)/1000);
      var nextSec=Math.ceil(elapsed/align)*align;
      var nd=new Date(base.getTime()+nextSec*1000);
      if(!inSession(nd)) return next900(now);
      return nd;
    }
    function skUpdateMonNext(){ var el=document.getElementById('skMonNext'); if(el){ var t=SK_STATE.monNextTick||0;
      if(t){ var d=new Date(t); var hh=('0'+d.getHours()).slice(-2), mm=('0'+d.getMinutes()).slice(-2); el.textContent='下次刷新 '+hh+':'+mm; }
      else { el.textContent='下次刷新 --:--'; } } }
    function skTickMonitor(){ if(SK_STATE.monPaused) return; var now=Date.now();
      if(SK_STATE.monMode==='screen'){
        if(now>=(SK_STATE.screenNextTick||0)){ skLoadScreen(); SK_STATE.screenNextTick=now+60000; }
        return;
      }
      if(SK_STATE.monMode==='week'){
        if(now>=(SK_STATE.weekNextTick||0)){ skLoadWeeklyCrosses(); SK_STATE.weekNextTick=now+60000; }
        return;
      }
      // 自选监控现在展示全量实时金叉, 数据变化不快, 固定 60 秒兜底刷新
      if(now>=(SK_STATE.monNextTick||0)){ skMonRefresh(); SK_STATE.monNextTick=now+60000; skUpdateMonNext(); } }
    // 实时价: 每 3 秒轻量刷新(腾讯源, 只更新头部"最新价", 不重建 K 线/指标/不占天勤会话)
    function skRefreshDetailQuote(){
      var sym=SK_STATE.symbol; if(!sym) return;
      fetch(skApi('quote?symbol='+encodeURIComponent(sym))).then(function(r){return r.json();}).then(function(d){
        if(!d||!d.ok||d.last==null) return;
        var q=d; var sub=document.getElementById('skSub');
        if(sub){ var cls=q.change>=0?'up':'down'; var sign=q.change>=0?'+':'';
          var qh='最新 <b class="'+cls+'">'+q.last+'</b> <span class="'+cls+'">'+sign+q.change+' ('+sign+q.change_pct+'%)</span>';
          var golds=(SK_STATE.cur&&SK_STATE.cur.crosses||[]).filter(function(c){return c.type==='golden';}).sort(function(a,b){return b.idx-a.idx;});
          if(golds.length){ qh+=' · 最近金叉 <b class="sk-gold">'+golds[0].date+'</b>'; }
          sub.innerHTML=qh; }
        if(SK_STATE.cur&&SK_STATE.cur.quote){ SK_STATE.cur.quote.last=q.last; SK_STATE.cur.quote.pre_close=q.pre_close; SK_STATE.cur.quote.change=q.change; SK_STATE.cur.quote.change_pct=q.change_pct; }
      }).catch(function(){});
    }
    // K线/交叉/形态: 拉取最新数据刷新(缓存命中时仅补实时价), 但整屏重建受限(不重置指标输入)
    function skRefreshDetailData(){
      var sym=SK_STATE.symbol; if(!sym||SK_STATE.monPaused) return;
      fetch(skApi('kline?symbol='+encodeURIComponent(sym)+'&period='+SK_STATE.period)).then(function(r){return r.json();}).then(function(d){
        if(!d||!d.ok) return;
        SK_STATE.cur=d;
        var q=d.quote||{}; var sub=document.getElementById('skSub');
        if(sub){ var qh=''; if(q.last!=null){ var cls=q.change>=0?'up':'down'; var sign=q.change>=0?'+':'';
          qh='最新 <b class="'+cls+'">'+q.last+'</b> <span class="'+cls+'">'+sign+q.change+' ('+sign+q.change_pct+'%)</span>'; }
          var golds=(d.crosses||[]).filter(function(c){return c.type==='golden';}).sort(function(a,b){return b.idx-a.idx;});
          if(golds.length){ qh+=' · 最近金叉 <b class="sk-gold">'+golds[0].date+'</b>'; }
          sub.innerHTML=qh; }
        var t=document.getElementById('skTitle'); if(t) t.textContent=(d.name?d.name+' ':'')+d.symbol;
        document.querySelectorAll('.sk-periods button').forEach(function(b){ b.classList.toggle('active', b.getAttribute('data-p')===SK_STATE.period); });
        skRenderCross();
      }).catch(function(){});
    }
    function skTickDetail(){
      if(!SK_STATE.symbol||SK_STATE.monPaused) return;
      var now=Date.now();
      // 实时价: 每 1 秒刷新(主源天勤/兜底腾讯, 轻量)
      if(now>=(SK_STATE.detailQuoteNextTick||0)){ SK_STATE.detailQuoteNextTick=now+1000; skRefreshDetailQuote(); }
      // K线/交叉: 周期内对齐刷新, 但日线等长时间周期改为最多 30 秒兜底, 保证盘中数据不冻结(不再一天只刷一次)
      var periodTick=skNextPeriodTick(SK_STATE.period).getTime();
      var next=Math.min(periodTick, now+30000);
      if(now>=(SK_STATE.detailNextTick||0)){ SK_STATE.detailNextTick=next; skRefreshDetailData(); }
    }
    function skSetMonPeriod(p){
      if(SK_STATE.monMode==='week'){
        SK_STATE.weekFilter=p; skLoadWeeklyCrosses();
      } else {
        SK_STATE.goldenPeriod=p; skMonRefresh();
      }
      var sel=document.getElementById('skMonPeriodSel'); if(sel) sel.value=p;
    }
    function skSetMonMode(mode){
      SK_STATE.monMode=mode;
      var tabs=document.getElementById('skMonTabs'); if(tabs){ tabs.querySelectorAll('button').forEach(function(b){ b.classList.toggle('active', b.getAttribute('data-mode')===mode); }); }
      skRebuildMonPeriodOptions();
      var nextEl=document.getElementById('skMonNext');
      var bar=document.getElementById('skScreenBar');
      if(mode==='screen'){ if(bar) bar.style.display=''; if(nextEl) nextEl.textContent='筛选 · 11:35/15:05'; skLoadScreen(); return; }
      if(bar) bar.style.display='none';
      if(mode==='week'){ SK_STATE.weekNextTick=Date.now(); if(nextEl) nextEl.textContent='本周金叉 · 60秒刷新'; skLoadWeeklyCrosses(); }
      else { SK_STATE.monNextTick=Date.now(); if(nextEl) nextEl.textContent='实时金叉 · 60秒刷新'; skMonRefresh(); }
    }
    function skLoadWeeklyCrosses(){
      var tb=document.getElementById('skMonRows'); var head=document.getElementById('skMonHead'); if(!tb) return;
      document.getElementById('skMonBadge').textContent='...';
      var filter=SK_STATE.weekFilter||'';
      fetch(skApi('recent_crosses?days=7')).then(function(r){return r.json();}).then(function(d){
        if(!d.ok){ tb.innerHTML='<tr><td colspan="5" style="color:var(--muted);padding:20px">加载失败: '+(d.error||'')+'</td></tr>'; return; }
        var items=(d.items||[]).slice();
        SK_STATE.weekCrosses=items;
        if(filter){ items=items.filter(function(it){ return it.period===filter; }); }
        document.getElementById('skMonBadge').textContent=''+items.length;
        if(head){ head.innerHTML='<tr><th>名称</th><th>代码</th><th>周期</th><th>金叉日期</th><th>收盘价</th></tr>'; }
        if(!items.length){
          var emptyMsg=filter?('本周暂无 '+((SK_PERIODS||[]).filter(function(p){return p.k===filter;})[0]||{label:filter}).label+' 已确认金叉记录'):'本周暂无已确认金叉记录(周线金叉每周收盘后确认, 全市场扫描 11:31/15:01 后更新)';
          tb.innerHTML='<tr><td colspan="5" style="color:var(--muted);padding:20px">'+emptyMsg+'</td></tr>'; return; }
        var html='';
        function xd(it){ var t=(it.ts?it.ts.substr(5,11):null)||it.date||''; var tag=(it.src==='rt')?'<span style="display:inline-block;background:#fbbf24;color:#412402;font-size:10px;line-height:14px;padding:0 4px;border-radius:3px;margin-right:4px;vertical-align:1px">实时</span>':''; return tag+t; }
        items.forEach(function(it){ var pn=(SK_PERIODS||[]).filter(function(p){return p.k===it.period;})[0]; var pl=pn?pn.label:it.period;
          html+='<tr ondblclick="skShowChartModal(\''+it.symbol+'\',\''+it.date+'\',\''+it.period+'\')" title="双击弹出 K 线图并定位到该金叉">'
            +'<td>'+(it.name||it.symbol)+'</td>'
            +'<td>'+it.symbol+'</td>'
            +'<td>'+pl+'</td>'
            +'<td class="sk-gold">'+xd(it)+'</td>'
            +'<td>'+(it.price!=null?it.price:'—')+'</td>'
            +'</tr>'; });
        tb.innerHTML=html;
      }).catch(function(e){ if(tb) tb.innerHTML='<tr><td colspan="5" style="color:var(--muted);padding:20px">请求失败: '+e+'</td></tr>'; });
    }
    // ===== 每日筛选(MACD 5 条件, 收盘后 15:05 全市场跑) =====
    var SK_SCREEN_CONDS=[
      {k:'c1',label:'日线零轴下双金叉',title:'MACD日线零轴下双金叉：最近两次金叉 DIF 均<0, 两次间隔<=60交易日, 且第二次金叉发生在近5个交易日内'},
      {k:'c2',label:'日线零轴附近或零轴上金叉',title:'MACD日线零轴附近或零轴上金叉：金叉发生在零轴附近(|DIF|<=0.2×ATR14)或零轴上方, 且在近5个交易日内'},
      {k:'c3',label:'日线底背离',title:'MACD日线底背离：价格创新低而 DIF 抬高(枢轴低点), 第二个低点在近30个交易日内'},
      {k:'c4',label:'周线金叉',title:'MACD周线金叉：最近一两根周K内刚金叉且仍处金叉状态(2026-09-29 口径收口)'},
      {k:'c5',label:'月线金叉',title:'MACD月线金叉：最近一两根月K内刚金叉且仍处金叉状态; 每次发现的新金叉都会落库, 可查历史'}
    ];
    SK_STATE.screenItems=[]; SK_STATE.screenDate=''; SK_STATE.screenConds={}; SK_STATE.screenNextTick=0;

    function skScreenFiltered(){
      var ks=SK_SCREEN_CONDS.filter(function(c){ return SK_STATE.screenConds[c.k]; }).map(function(c){ return c.k; });
      var all=SK_STATE.screenItems||[];
      if(!ks.length) return all;
      return all.filter(function(it){ return ks.every(function(k){ return it[k]; }); });
    }
    function skScreenSetStat(s){ var el=document.getElementById('skScreenStat'); if(el) el.textContent=s; }
    function skScreenRenderChips(){
      var box=document.getElementById('skScreenChips'); if(!box) return;
      var all=SK_STATE.screenItems||[];
      var html='<span class="sk-sc-lab">条件过滤:</span>';
      SK_SCREEN_CONDS.forEach(function(c){
        var cnt=all.filter(function(it){ return it[c.k]; }).length;
        var on=!!SK_STATE.screenConds[c.k];
        html+='<span class="sk-sc-chip'+(on?' on':'')+'" title="'+c.title+'" onclick="skScreenToggleCond(\''+c.k+'\')">'+c.label+' <b>'+cnt+'</b></span>';
      });
      html+='<span class="sk-sc-lab" style="margin-left:auto">显示 '+skScreenFiltered().length+' / '+all.length+' 只</span>';
      box.innerHTML=html;
    }
    function skScreenToggleCond(k){ SK_STATE.screenConds[k]=!SK_STATE.screenConds[k]; skScreenRenderChips(); skScreenRenderTable(); }
    function skScreenRenderTable(){
      var tb=document.getElementById('skMonRows'); var head=document.getElementById('skMonHead');
      if(!tb) return;
      var items=skScreenFiltered();
      if(head) head.innerHTML='<tr><th>标的</th><th>最新价</th><th>涨跌</th><th>命中条件</th><th>日线金叉</th><th>周线金叉</th><th>月线金叉</th></tr>';
      var bad=document.getElementById('skMonBadge'); if(bad) bad.textContent=''+items.length;
      if(!items.length){ tb.innerHTML='<tr><td colspan="7" style="color:var(--muted);padding:20px">没有命中记录(点「立即筛选」跑一轮, 或切换扫描日期)</td></tr>'; return; }
      var html='';
      items.forEach(function(it){
        var chips='';
        SK_SCREEN_CONDS.forEach(function(c){ if(it[c.k]) chips+='<span class="sk-sc-hit" title="'+c.title+'">'+c.label+'</span>'; });
        var cp=it.change_pct;
        var cl=(''+(cp==null?'':(cp>=0?'color:var(--up)':'color:var(--down)')));
        html+='<tr ondblclick="skShowChartModal(\''+it.symbol+'\',\''+(it.d_golden||'')+'\',\'day\')" title="双击弹出 K 线图">'
          +'<td>'+(it.name||'')+'<div class="sk-sc-code">'+it.symbol+'</div></td>'
          +'<td>'+(it.last!=null?it.last:'—')+'</td>'
          +'<td style="'+cl+'">'+(cp!=null?((cp>0?'+':'')+cp+'%'):'—')+'</td>'
          +'<td class="sk-sc-cell" title="'+(it.detail||'')+'">'+chips+'</td>'
          +'<td class="sk-gold">'+(it.d_golden||'—')+'</td>'
          +'<td class="sk-gold">'+(it.w_golden||'—')+'</td>'
          +'<td class="sk-gold">'+(it.m_golden||'—')+'</td>'
          +'</tr>';
      });
      tb.innerHTML=html;
    }
    function skScreenPickDate(d){ SK_STATE.screenDate=d; skLoadScreen(); }
    function skLoadScreen(){
      skScreenSetStat('加载中…');
      fetch(skApi('screen/status')).then(function(r){ return r.json(); }).then(function(st){
        if(!st||!st.ok) return;
        var sel=document.getElementById('skScreenDate');
        if(sel&&st.dates){
          var cur=SK_STATE.screenDate||(st.dates[0]?st.dates[0].date:'');
          sel.innerHTML='';
          st.dates.forEach(function(x){ var o=document.createElement('option'); o.value=x.date; o.textContent=x.date+' ('+x.hits+'/'+x.total+')'; sel.appendChild(o); });
          if(cur&&st.dates.some(function(x){ return x.date===cur; })) sel.value=cur;
          else { SK_STATE.screenDate=st.dates[0]?st.dates[0].date:''; if(sel) sel.value=SK_STATE.screenDate; }
        }
        var sc=st.screen||{};
        if(sc.running) skScreenSetStat('扫描中 '+sc.done+'/'+sc.total+(sc.error?(' · '+sc.error):''));
        else if(sc.started) skScreenSetStat('上次完成 '+(sc.last_date||'')+' · 命中 '+sc.hits+' 只 · 耗时 '+Math.round((sc.elapsed||0)/60)+' 分');
        else skScreenSetStat('尚未跑过 (每天 15:05 自动, 也可点「立即筛选」)');
      }).catch(function(){});
      var q='screen/result?limit=300'+(SK_STATE.screenDate?('&date='+encodeURIComponent(SK_STATE.screenDate)):'');
      fetch(skApi(q)).then(function(r){ return r.json(); }).then(function(d){
        if(!d.ok){ skScreenSetStat('加载失败: '+(d.error||'')); return; }
        SK_STATE.screenItems=d.items||[]; SK_STATE.screenDate=d.date||'';
        var sel=document.getElementById('skScreenDate'); if(sel&&d.date) sel.value=d.date;
        skScreenRenderChips(); skScreenRenderTable();
        if(!SK_STATE.screenItems.length) skScreenSetStat('该日无命中记录'+(d.hint?(' · '+d.hint):''));
      }).catch(function(e){ skScreenSetStat('请求失败: '+e); });
    }
    function skScreenRun(){
      var btn=document.getElementById('skScreenRunBtn'); if(btn) btn.disabled=true;
      skScreenSetStat('已提交, 排队中(等上一个任务让路)…');
      fetch(skApi('screen/run'),{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'})
        .then(function(r){ return r.json(); }).then(function(){
          setTimeout(function(){ if(btn) btn.disabled=false; skLoadScreen(); }, 3000);
        }).catch(function(){ if(btn) btn.disabled=false; skScreenSetStat('提交失败'); });
    }
    function skScreenMonthHistory(){
      skScreenSetStat('读取月线金叉历史…');
      fetch(skApi('screen/month?limit=200')).then(function(r){ return r.json(); }).then(function(d){
        var tb=document.getElementById('skMonRows'); var head=document.getElementById('skMonHead');
        if(!d.ok){ skScreenSetStat('取月线历史失败: '+(d.error||'')); return; }
        if(head) head.innerHTML='<tr><th>标的</th><th>金叉月份</th><th>DIF</th><th>DEA</th><th>价格</th><th>首次发现</th></tr>';
        var items=d.items||[];
        var back='<tr><td colspan="6" style="color:var(--accent);padding:6px 8px;cursor:pointer" onclick="skLoadScreen()">← 返回筛选结果</td></tr>';
        if(!items.length){ tb.innerHTML=back+'<tr><td colspan="6" style="color:var(--muted);padding:20px">还没有月线金叉记录(每次筛选发现有新金叉会自动落库)</td></tr>'; skScreenSetStat('月线金叉历史 0 条'); return; }
        var html='';
        items.forEach(function(it){
          html+='<tr ondblclick="skShowChartModal(\''+it.symbol+'\',\'\',\'month\')" title="双击弹出月线 K 线图">'
            +'<td>'+(it.name||'')+'<div class="sk-sc-code">'+it.symbol+'</div></td>'
            +'<td class="sk-gold">'+it.cross_date+'</td>'
            +'<td>'+(it.dif!=null?it.dif:'—')+'</td><td>'+(it.dea!=null?it.dea:'—')+'</td>'
            +'<td>'+(it.price!=null?it.price:'—')+'</td>'
            +'<td style="color:var(--muted)">'+(it.first_seen||'').substr(0,10)+'</td></tr>';
        });
        tb.innerHTML=back+html;
        skScreenSetStat('月线金叉历史 '+items.length+' 条');
      }).catch(function(){ skScreenSetStat('取月线历史失败'); });
    }
    function skLoadRecentCrosses(){ var bd=document.getElementById('skCrBody'); if(!bd) return;
      fetch(skApi('recent_crosses?days=7')).then(function(r){return r.json();}).then(function(d){
        if(!d.ok){ bd.innerHTML='<div class="sk-cr-empty">加载失败: '+(d.error||'')+'</div>'; return; }
        var cnt=document.getElementById('skCrCount'); if(cnt) cnt.textContent='('+d.count+')';
        if(!d.items.length){ bd.innerHTML='<div class="sk-cr-empty">最近一周暂无已确认金叉记录(周线金叉每周收盘后确认, 全市场扫描 11:31/15:01 后更新)</div>'; return; }
        var html='';
        function xd(it){ var t=(it.ts?it.ts.substr(5,11):null)||it.date||''; var tag=(it.src==='rt')?'<span style="display:inline-block;background:#fbbf24;color:#412402;font-size:10px;line-height:14px;padding:0 4px;border-radius:3px;margin-right:4px;vertical-align:1px">实时</span>':''; return tag+t; }
        d.items.forEach(function(it){ var pn=(SK_PERIODS||[]).filter(function(p){return p.k===it.period;})[0]; var pl=pn?pn.label:it.period;
          html+='<div class="sk-cr-row" ondblclick="skShowChartModal(\''+it.symbol+'\',\''+it.date+'\',\''+it.period+'\')" title="双击弹出 K 线图并定位到该金叉">'
            +'<span class="nm">'+(it.name||it.symbol)+'</span><span class="sym">'+it.symbol+'</span>'
            +'<span class="pd">'+pl+' · '+xd(it)+'</span>'
            +'<span class="p">'+(it.price!=null?it.price:'')+'</span></div>'; });
        bd.innerHTML=html;
      }).catch(function(e){ bd.innerHTML='<div class="sk-cr-empty">加载失败: '+e+'</div>'; });
    }
    function skToggleRecents(){ var el=document.getElementById('skCrossRecents'); if(el) el.classList.toggle('collapsed'); }
    function skToggleStockDrawer(){ /* 抽屉已改为左下常驻列表, 此函数保留仅作兼容 */ }
    var SK_GOLDEN_PERIODS=[{k:'',label:'全部金叉'},{k:'week',label:'周线金叉'},{k:'month',label:'月线金叉'},{k:'day',label:'日线金叉'},{k:'minute',label:'分钟金叉'}];
    function skPopulateMonPeriod(){ skRebuildMonPeriodOptions(); }
    function skRebuildMonPeriodOptions(){
      var sel=document.getElementById('skMonPeriodSel'); if(!sel) return;
      sel.style.display=(SK_STATE.monMode==='screen')?'none':'';
      sel.innerHTML='';
      var opts=(SK_STATE.monMode==='week')?SK_PERIODS:SK_GOLDEN_PERIODS;
      if(SK_STATE.monMode==='week'){
        var all=document.createElement('option'); all.value=''; all.textContent='全部周期'; sel.appendChild(all);
      }
      (opts||[]).forEach(function(p){ var o=document.createElement('option'); o.value=p.k; o.textContent=p.label; sel.appendChild(o); });
      sel.value=(SK_STATE.monMode==='week'?SK_STATE.weekFilter:SK_STATE.goldenPeriod)||'';
    }

    function skRenderDetail(){
      var sym=SK_STATE.symbol; if(!sym) return;
      if(!SK_STATE.indParams) SK_STATE.indParams={ma:[5,10,20,60],bollN:20,bollM:2,kdjN:9,kdjK:3,kdjD:3,rsiN:14};
      var ip=SK_STATE.indParams;
      var det=document.getElementById('skDetail');
      det.innerHTML='<div class="sk-detail-hd" id="skHd"></div>'
        +'<div class="sk-detail-bd" id="skBd">'
        +'<div class="sk-cross" id="skCross"></div>'
        +'<div class="sk-ind-params" id="skIndParams">'
        +'<div class="ip-t">指标参数</div>'
        +'<div class="ip-row ip-ma"><label>MA</label>'
        +'<input id="ipMa1" type="number" min="1" value="'+ip.ma[0]+'"><input id="ipMa2" type="number" min="1" value="'+ip.ma[1]+'"><input id="ipMa3" type="number" min="1" value="'+ip.ma[2]+'"><input id="ipMa4" type="number" min="1" value="'+ip.ma[3]+'"></div>'
        +'<div class="ip-row"><label>BOLL</label><input id="ipBollN" type="number" min="2" value="'+ip.bollN+'"><input id="ipBollM" type="number" min="1" step="0.5" value="'+ip.bollM+'"></div>'
        +'<div class="ip-row"><label>KDJ</label><input id="ipKdjN" type="number" min="2" value="'+ip.kdjN+'"><input id="ipKdjK" type="number" min="1" value="'+ip.kdjK+'"><input id="ipKdjD" type="number" min="1" value="'+ip.kdjD+'"></div>'
        +'<div class="ip-row"><label>RSI</label><input id="ipRsiN" type="number" min="2" value="'+ip.rsiN+'"></div>'
        +'<button onclick="skApplyIndParams()">应用并重算</button>'
        +'</div>'
        +'</div>';
      // 周期按钮
      var hd=document.getElementById('skHd');
      hd.innerHTML='<button onclick="skCloseRightOverlay()" style="margin-right:6px">← 返回监控</button>'
        +'<span class="t" id="skTitle">'+sym+'</span><span class="s" id="skSub"></span>'
        +'<span class="sk-periods">'+skPeriodBtns()
        +'<button onclick="skExportCSV()">导出CSV</button></span>';
      var pbtns=hd.querySelectorAll('button[data-p]');
      for(var pi=0; pi<pbtns.length; pi++){
        (function(btn){ btn.onclick=function(){ skSetPeriod(btn.getAttribute('data-p')); }; })(pbtns[pi]);
      }
      fetch(skApi('kline?symbol='+encodeURIComponent(sym)+'&period='+SK_STATE.period)).then(function(r){return r.json();}).then(function(d){
        if(!d.ok){ document.getElementById('skCross').innerHTML='<div class="sk-empty">'+ (d.error||'无数据') +'</div>'; return; }
        SK_STATE.cur=d;
        document.getElementById('skTitle').textContent=(d.name?d.name+' ':'')+d.symbol;
        var q=d.quote||{}; var qh='';
        if(q.last!=null){ var cls=q.change>=0?'up':'down'; var sign=q.change>=0?'+':''; qh='最新 <b class="'+cls+'">'+q.last+'</b> <span class="'+cls+'">'+sign+q.change+' ('+sign+q.change_pct+'%)</span>'; }
        // 最近一次金叉日期
        var golds=(d.crosses||[]).filter(function(c){return c.type==='golden';}).sort(function(a,b){return b.idx-a.idx;});
        if(golds.length){ qh+=' · 最近金叉 <b class="sk-gold">'+golds[0].date+'</b>'; }
        document.getElementById('skSub').innerHTML=qh;
        // 周期按钮高亮
        document.querySelectorAll('.sk-periods button').forEach(function(b){ b.classList.toggle('active', b.getAttribute('data-p')===SK_STATE.period); });
        skRenderCross();
        // 首屏排程与 skTickDetail 一致: 长周期(日/周/月)最多 30s 兜底, 不再按"明天9:00"冻结一整天
        SK_STATE.detailNextTick=Math.min(skNextPeriodTick(SK_STATE.period).getTime(), Date.now()+30000);
      }).catch(function(e){ document.getElementById('skCross').innerHTML='<div class="sk-empty">请求失败: '+e+'</div>'; });
    }


    function skSMA(arr,n){
      var out=[],sum=0;
      for(var i=0;i<arr.length;i++){ sum+=arr[i]; if(i>=n) sum-=arr[i-n]; out.push(i>=n-1?sum/n:null); }
      return out;
    }
    function skBOLL(close,n,m){
      var mid=skSMA(close,n), upper=[], lower=[];
      for(var i=0;i<close.length;i++){
        if(i>=n-1){ var mean=mid[i], s=0; for(var j=i-n+1;j<=i;j++){ var dd=close[j]-mean; s+=dd*dd; } var sd=Math.sqrt(s/n); upper.push(mean+m*sd); lower.push(mean-m*sd); }
        else { upper.push(null); lower.push(null); }
      }
      return {mid:mid,upper:upper,lower:lower};
    }
    function skKDJ(high,low,close,n,kp,dp){
      var rsv=[],k=[],d=[],j=[]; var kf=1/kp, df=1/dp;
      for(var i=0;i<close.length;i++){
        if(i>=n-1){ var hh=-Infinity,ll=Infinity; for(var t=i-n+1;t<=i;t++){ if(high[t]>hh)hh=high[t]; if(low[t]<ll)ll=low[t]; } var r=(hh===ll)?50:(close[i]-ll)/(hh-ll)*100; rsv.push(r); }
        else rsv.push(null);
        if(rsv[i]==null){ k.push(null); d.push(null); j.push(null); }
        else {
          var pK=(i>0&&k[i-1]!=null)?k[i-1]:50;
          var cK=pK+kf*(rsv[i]-pK); k.push(cK);
          var pD=(i>0&&d[i-1]!=null)?d[i-1]:50;
          var cD=pD+df*(cK-pD); d.push(cD);
          j.push(3*cK-2*cD);
        }
      }
      return {k:k,d:d,j:j};
    }
    function skRSI(close,n){
      var out=[],ag=null,al=null;
      for(var i=0;i<close.length;i++){
        if(i===0){ out.push(null); continue; }
        var ch=close[i]-close[i-1], gain=ch>0?ch:0, loss=ch<0?-ch:0;
        if(ag===null){
          if(i<n){ out.push(null); continue; }
          var sg=0,sl=0; for(var t=1;t<=n;t++){ var c2=close[t]-close[t-1]; if(c2>0)sg+=c2; else sl+=-c2; }
          ag=sg/n; al=sl/n;
        } else { ag=(ag*(n-1)+gain)/n; al=(al*(n-1)+loss)/n; }
        out.push(al===0?100:(function(){var rs=ag/al;return 100-100/(1+rs);})());
      }
      return out;
    }
    function skRenderChart(d){
      var chartEl=document.getElementById('skChart');
      var existing=echarts.getInstanceByDom(chartEl);
      if(existing){ existing.dispose(); }
      var chart=echarts.init(chartEl);
      SK_STATE.chart=chart;
      var cats=d.categories, k=d.kline, vol=d.volume, macd=d.macd;
      var open=[],close=[],high=[],low=[];
      for(var bi=0;bi<k.length;bi++){ var b=k[bi]; open.push(b[0]); close.push(b[1]); low.push(b[2]); high.push(b[3]); }
      if(!SK_STATE.indParams) SK_STATE.indParams={ma:[5,10,20,60],bollN:20,bollM:2,kdjN:9,kdjK:3,kdjD:3,rsiN:14};
      var ip=SK_STATE.indParams;
      var boll=skBOLL(close,ip.bollN,ip.bollM);
      var kdj=skKDJ(high,low,close,ip.kdjN,ip.kdjK,ip.kdjD);
      var rsi=skRSI(close,ip.rsiN);
      var vma5=skSMA(vol,5), vma10=skSMA(vol,10);
      var maColors=['#facc15','#38bdf8','#fb923c','#c084fc','#f472b6','#a78bfa'];
      var maSeries=[];
      for(var mi=0; mi<ip.ma.length; mi++){ var mn=ip.ma[mi]; if(mn>0) maSeries.push({name:'MA'+mn,type:'line',data:skSMA(close,mn),showSymbol:false,lineStyle:{width:1,color:maColors[mi%maColors.length]}}); }
      var goldPts=[]; var deathPts=[]; var crossHigh=[]; var crossLow=[];
      (d.crosses||[]).forEach(function(c){
        var pt={coord:[cats[c.idx], c.dif]};
        if(c.type==='golden'){ pt.value='金叉'; goldPts.push(pt); crossHigh.push([cats[c.idx], high[c.idx]]); }
        else { pt.value='死叉'; deathPts.push(pt); crossLow.push([cats[c.idx], low[c.idx]]); }
      });
      var series=[
        {name:'K线',type:'candlestick',data:k,
          itemStyle:{color:'#ef4444',color0:'#19c37d',borderColor:'#ef4444',borderColor0:'#19c37d'}},
        {name:'金叉点',type:'scatter',data:crossHigh,symbol:'triangle',symbolSize:12,itemStyle:{color:'#ef4444'},z:10},
        {name:'死叉点',type:'scatter',data:crossLow,symbol:'triangle',symbolRotate:180,symbolSize:12,itemStyle:{color:'#19c37d'},z:10},
        {name:'BOLL中',type:'line',data:boll.mid,showSymbol:false,lineStyle:{width:1,color:'#a78bfa'}},
        {name:'BOLL上',type:'line',data:boll.upper,showSymbol:false,lineStyle:{width:1,color:'#f472b6',opacity:.7}},
        {name:'BOLL下',type:'line',data:boll.lower,showSymbol:false,lineStyle:{width:1,color:'#f472b6',opacity:.7}}
      ];
      maSeries.forEach(function(s){ series.push(s); });
      series.push(
        {name:'成交量',type:'bar',xAxisIndex:1,yAxisIndex:1,data:vol,itemStyle:{color:'#3b82f6'}},
        {name:'DIF',type:'line',xAxisIndex:1,yAxisIndex:1,data:macd.dif,showSymbol:false,lineStyle:{width:1,color:'#fbbf24'}},
        {name:'DEA',type:'line',xAxisIndex:1,yAxisIndex:1,data:macd.dea,showSymbol:false,lineStyle:{width:1,color:'#38bdf8'}},
        {name:'MACD',type:'bar',xAxisIndex:1,yAxisIndex:1,data:macd.bar.map(function(v){return v==null?0:v;}),
          itemStyle:{color:function(p){return (p.data>=0)?'#ef4444':'#19c37d';}}},
        {name:'VMA5',type:'line',xAxisIndex:1,yAxisIndex:1,data:vma5,showSymbol:false,lineStyle:{width:1,color:'#f59e0b'}},
        {name:'VMA10',type:'line',xAxisIndex:1,yAxisIndex:1,data:vma10,showSymbol:false,lineStyle:{width:1,color:'#22d3ee'}},
        {name:'K',type:'line',xAxisIndex:2,yAxisIndex:2,data:kdj.k,showSymbol:false,lineStyle:{width:1,color:'#fbbf24'}},
        {name:'D',type:'line',xAxisIndex:2,yAxisIndex:2,data:kdj.d,showSymbol:false,lineStyle:{width:1,color:'#38bdf8'}},
        {name:'J',type:'line',xAxisIndex:2,yAxisIndex:2,data:kdj.j,showSymbol:false,lineStyle:{width:1,color:'#a78bfa'}},
        {name:'RSI',type:'line',xAxisIndex:3,yAxisIndex:3,data:rsi,showSymbol:false,lineStyle:{width:1,color:'#34d399'}},
        {name:'金叉',type:'scatter',xAxisIndex:1,yAxisIndex:1,data:[],markPoint:{symbolSize:46,data:goldPts.concat(deathPts),
          itemStyle:{color:'#ef4444'},label:{formatter:'{b}',color:'#fff',fontSize:10}}}
      );
      var option={
        backgroundColor:'transparent',
        animation:false,
        tooltip:{trigger:'axis',axisPointer:{type:'cross'}},
        axisPointer:{link:[{xAxisIndex:'all'}]},
        legend:{top:0,textStyle:{color:'#94a3b8'},itemHeight:8,itemWidth:12,itemGap:8},
        grid:[
          {left:60,right:20,top:'11%',height:'31%'},
          {left:60,right:20,top:'45%',height:'13%'},
          {left:60,right:20,top:'60%',height:'11%'},
          {left:60,right:20,top:'73%',height:'10%'}
        ],
        xAxis:[
          {type:'category',data:cats,axisLabel:{color:'#94a3b8'},axisLine:{lineStyle:{color:'#27344b'}},scale:true},
          {type:'category',gridIndex:1,data:cats,axisLabel:{show:false},axisLine:{lineStyle:{color:'#27344b'}},axisTick:{show:false}},
          {type:'category',gridIndex:2,data:cats,axisLabel:{show:false},axisLine:{lineStyle:{color:'#27344b'}},axisTick:{show:false}},
          {type:'category',gridIndex:3,data:cats,axisLabel:{color:'#94a3b8'},axisLine:{lineStyle:{color:'#27344b'}},axisTick:{show:false}}
        ],
        yAxis:[
          {scale:true,axisLabel:{color:'#94a3b8'},splitLine:{lineStyle:{color:'rgba(39,52,75,.5)'}}},
          {gridIndex:1,scale:true,axisLabel:{color:'#94a3b8'},splitLine:{lineStyle:{color:'rgba(39,52,75,.5)'}}},
          {gridIndex:2,scale:true,axisLabel:{color:'#94a3b8'},splitLine:{lineStyle:{color:'rgba(39,52,75,.5)'}}},
          {gridIndex:3,scale:true,min:0,max:100,axisLabel:{color:'#94a3b8'},splitLine:{lineStyle:{color:'rgba(39,52,75,.5)'}}}
        ],
        dataZoom:[
          {type:'inside',xAxisIndex:[0,1,2,3]},
          {type:'slider',xAxisIndex:[0,1,2,3],bottom:2,height:16,textStyle:{color:'#94a3b8'}}
        ],
        series:series
      };
      chart.setOption(option);
      window.addEventListener('resize', function(){ chart.resize(); });
    }

    function skToggleIndParams(){
      var p=document.getElementById('skIndParams');
      if(p) p.classList.toggle('show');
    }
    function skApplyIndParams(){
      var ip=SK_STATE.indParams; if(!ip) ip=SK_STATE.indParams={ma:[5,10,20,60],bollN:20,bollM:2,kdjN:9,kdjK:3,kdjD:3,rsiN:14};
      var ma=[];
      ['ipMa1','ipMa2','ipMa3','ipMa4'].forEach(function(id){
        var v=parseInt((document.getElementById(id)||{}).value||'0',10); ma.push(v>0?v:5);
      });
      ip.ma=ma;
      ip.bollN=Math.max(2, parseInt((document.getElementById('ipBollN')||{}).value||'20',10)||20);
      ip.bollM=Math.max(0.5, parseFloat((document.getElementById('ipBollM')||{}).value||'2')||2);
      ip.kdjN=Math.max(2, parseInt((document.getElementById('ipKdjN')||{}).value||'9',10)||9);
      ip.kdjK=Math.max(1, parseInt((document.getElementById('ipKdjK')||{}).value||'3',10)||3);
      ip.kdjD=Math.max(1, parseInt((document.getElementById('ipKdjD')||{}).value||'3',10)||3);
      ip.rsiN=Math.max(2, parseInt((document.getElementById('ipRsiN')||{}).value||'14',10)||14);
      skToast('指标参数已保存', '当前仅用于记录, 详情页已不再显示K线图');
    }

    function skFilteredCrosses(){
      var d=SK_STATE.cur; if(!d) return [];
      var rows=(d.crosses||[]).slice();
      var goldOnly=document.getElementById('skGoldOnly') && document.getElementById('skGoldOnly').checked;
      var n=parseInt((document.getElementById('skRecentN')||{}).value||'0',10)||0;
      var q=((document.getElementById('skCrossSearch')||{}).value||'').trim();
      if(goldOnly) rows=rows.filter(function(c){return c.type==='golden';});
      if(n>0){ var total=(d.categories||[]).length; rows=rows.filter(function(c){return c.idx>=total-n;}); }
      if(q) rows=rows.filter(function(c){return (c.date||'').indexOf(q)>=0;});
      var dir=SK_STATE.crossSort==='asc'?1:-1;
      rows.sort(function(a,b){ return dir*(a.idx-b.idx); });
      return rows;
    }

    function skRenderCross(){
      var d=SK_STATE.cur; if(!d) return;
      var box=document.getElementById('skCross'); if(!box) return;
      var all=(d.crosses||[]);
      var goldN=all.filter(function(c){return c.type==='golden';}).length;
      var deathN=all.length-goldN;
      var rows=skFilteredCrosses();
      // 工具条(含输入控件)只创建一次, 之后只刷新 tbody, 避免实时刷新时重置用户输入
      var barEl=document.getElementById('skCrossBar');
      if(!barEl){
        box.innerHTML='<div class="sk-cross-bar" id="skCrossBar">'
          +'<label class="sk-cb"><input type="checkbox" id="skGoldOnly" onchange="skRenderCross()"> 只看金叉</label>'
          +'<label class="sk-cb">近 <input type="number" id="skRecentN" value="0" min="0" onchange="skRenderCross()"> 根内</label>'
          +'<label class="sk-cb">搜索 <input type="text" id="skCrossSearch" placeholder="日期" oninput="skRenderCross()"></label>'
          +'<button onclick="skExportCSV()">导出CSV</button>'
          +'<span class="sk-cross-stat" id="skCrossStat"></span>'
          +'</div><div id="skCrossBody"></div>';
      }
      var stEl=document.getElementById('skCrossStat');
      if(stEl) stEl.textContent='全部 '+all.length+' · 金叉 '+goldN+' / 死叉 '+deathN+(rows.length!==all.length?(' · 当前筛选 '+rows.length):'');
      var sortArrow=SK_STATE.crossSort==='asc'?'▲':'▼';
      var html='<table><thead><tr><th>类型</th><th style="cursor:pointer;user-select:none" onclick="skToggleCrossSort()" title="点击切换升/降序">日期 '+sortArrow+'</th><th>收盘价</th><th>DIF</th><th>DEA</th></tr></thead><tbody>';
      if(rows.length===0){ html+='<tr><td colspan="5" style="color:var(--muted)">无匹配的金叉/死叉</td></tr>'; }
      rows.forEach(function(c){
        var cls=c.type==='golden'?'sk-gold':'sk-death';
        var label=c.type==='golden'?'金叉':'死叉';
        var rtTag=c.rt?'<span style="display:inline-block;background:#fbbf24;color:#412402;font-size:10px;line-height:14px;padding:0 4px;border-radius:3px;margin-right:4px;vertical-align:1px" title="盘中实时检测的精确交叉时刻">实时</span>':'';
        var shown=(c.rt&&c.rt_ts)?c.rt_ts.substr(5,11):c.date;
        html+='<tr data-idx="'+c.idx+'" class="sk-cross-row" onclick="skFocusCross('+c.idx+')" ondblclick="skShowChartModal(\''+d.symbol+'\',\''+c.date+'\',\''+SK_STATE.period+'\')" title="单击在表中定位，双击弹出 K 线图"><td class="'+cls+'">'+label+'</td><td>'+rtTag+shown+'</td><td>'+(c.price!=null?c.price:'—')+'</td><td>'+(c.dif!=null?c.dif:'—')+'</td><td>'+(c.dea!=null?c.dea:'—')+'</td></tr>';
      });
      html+='</tbody></table>';
      var body=document.getElementById('skCrossBody'); if(body) body.innerHTML=html;
    }

    function skFocusChart(chart, d, idx){
      if(!chart || !d) return;
      var cats=d.categories; var total=cats.length;
      var half=Math.max(5, Math.floor(total*0.06));
      var start=Math.max(0, idx-half); var end=Math.min(total-1, idx+half);
      chart.dispatchAction({type:'dataZoom', dataZoomIndex:0, startValue:cats[start], endValue:cats[end]});
      chart.dispatchAction({type:'showTip', seriesIndex:0, dataIndex:idx});
    }
    function skFocusCross(idx){
      var d=SK_STATE.cur; if(!d || !SK_STATE.chart) return;
      skFocusChart(SK_STATE.chart, d, idx);
      var rows=document.querySelectorAll('#skCross tbody tr.sk-cross-row');
      rows.forEach(function(r){ r.classList.remove('active'); });
      var cur=document.querySelector('#skCross tbody tr[data-idx="'+idx+'"]');
      if(cur){ cur.classList.add('active'); cur.scrollIntoView({behavior:'smooth',block:'nearest'}); }
    }

    function skToggleCrossSort(){
      SK_STATE.crossSort = SK_STATE.crossSort==='asc'?'desc':'asc';
      skRenderCross();
    }

    function skExportCSV(){
      var d=SK_STATE.cur; if(!d){ alert('请先打开一只股票'); return; }
      var rows=skFilteredCrosses();
      var lines=[];
      lines.push('股票,'+(d.name||'')+','+d.symbol+',周期,'+d.period_label+',共'+rows.length+'条');
      lines.push('类型,日期,收盘价,DIF,DEA');
      rows.forEach(function(c){
        lines.push((c.type==='golden'?'金叉':'死叉')+','+c.date+','+(c.price!=null?c.price:'')+','+(c.dif!=null?c.dif:'')+','+(c.dea!=null?c.dea:''));
      });
      var csv='\ufeff'+lines.join('\n');
      var blob=new Blob([csv],{type:'text/csv;charset=utf-8'});
      var a=document.createElement('a'); a.href=URL.createObjectURL(blob);
      a.download=d.symbol+'_'+d.period_label+'_金叉事件.csv'; a.click();
      setTimeout(function(){ URL.revokeObjectURL(a.href); }, 1000);
    }

    function skScanSymbols(symbols){
      if(!symbols || !symbols.length || SK_STATE.scanning) return;
      var todo=symbols.filter(function(s){ return !SK_STATE.scan[s]; });
      if(!todo.length) return;
      SK_STATE.scanning=true;
      fetch(skApi('scan'), {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({symbols:todo})})
        .then(function(r){return r.json();}).then(function(d){
          if(d && d.ok){
            var res=d.results||{};
            for(var k in res){ SK_STATE.scan[k]=res[k]; }
            for(var k2 in res){
              var cur=res[k2]||{};
              var exp=SK_STATE.listRecent[k2]||{};
              var title=skNameOf(k2)||k2;
              // 列表扫描新发现周线金叉（列表API未预先标红）
              if(cur.recent_golden && !exp.w && !SK_STATE.scanAlerted[k2+'_w']){
                SK_STATE.scanAlerted[k2+'_w']=true;
                skToast(title+' 列表发现周线金叉', '周线金叉 · '+(cur.last_golden_date||''), 'gold');
                skBeep(660,150); setTimeout(function(){skBeep(880,150);},160);
                skLogAlert('gold', title, '列表扫描 周线金叉 · '+(cur.last_golden_date||''));
              }
              // 列表扫描新发现月线金叉
              if(cur.recent_golden_month && !exp.m && !SK_STATE.scanAlerted[k2+'_m']){
                SK_STATE.scanAlerted[k2+'_m']=true;
                skToast(title+' 列表发现月线金叉', '月线金叉 · '+(cur.last_golden_month_date||''), 'mgold');
                skBeep(660,150); setTimeout(function(){skBeep(1000,150);},160);
                skLogAlert('mgold', title, '列表扫描 月线金叉 · '+(cur.last_golden_month_date||''));
              }
              if(cur.recent_golden) skApplyDot(k2, cur.last_golden_date);
            }
          }
        }).catch(function(){}).then(function(){ SK_STATE.scanning=false; });
    }
    function skApplyDot(sym, date){
      var rows=document.querySelectorAll('#skRows [data-sym]');
      for(var i=0;i<rows.length;i++){
        var r=rows[i];
        if(r.getAttribute('data-sym')===sym && !r.querySelector('.sk-dot')){
          var dot=document.createElement('span'); dot.className='sk-dot'; dot.title='近期周线金叉: '+(date||'');
          r.insertBefore(dot, r.firstChild);
        }
      }
    }
    function skToggleRecent(){
      SK_STATE.recentOnly=document.getElementById('skRecentOnly').checked;
      skLoadStocks(true);
    }
    function skHideScanAcc(){
      var el=document.getElementById('skScanAcc'); if(el){ el.style.display='none'; try{ localStorage.setItem('sk_hide_scan_acc','1'); }catch(e){} }
    }
    function skRestoreScanAcc(){
      try{ if(localStorage.getItem('sk_hide_scan_acc')!=='1') return; }catch(e){ return; }
      var el=document.getElementById('skScanAcc'); if(el) el.style.display='none';
    }
    function skScanFull(){
      var btn=document.getElementById('skScanBtn'); if(!btn || btn.disabled) return;
      btn.disabled=true; btn.textContent='扫描中…';
      fetch(skApi('scan/full'), {method:'POST'}).then(function(r){return r.json();}).then(function(d){ skPollScan(); })
        .catch(function(){ btn.disabled=false; btn.textContent='全市场扫描金叉'; });
    }
    function skPollScan(){
      fetch(skApi('scan/status')).then(function(r){return r.json();}).then(function(d){
        if(!d || !d.ok) return;
        var s=d.scan||{};
        var el=document.getElementById('skScanStat');
        if(el) el.textContent = s.running ? ('扫描中 '+s.done+'/'+s.total+' ('+(s.last||'')+')') : (s.done? ('扫描完成 '+s.done+'/'+s.total) : '');
        if(s.running){ setTimeout(skPollScan, 3000); }
        else {
          var b=document.getElementById('skScanBtn');
          if(b){ b.disabled=false; b.textContent='全市场扫描金叉'; }
          if(s.done) skLoadStocks(true);
        }
      }).catch(function(){});
    }

    // ===== 全市场实时金叉监控(会话累计, 无自选概念) =====
    function skMonLoad(){
      // 会话累计列表从空开始, 不再读取本地自选缓存
      SK_STATE.monitor=[]; SK_STATE.seen={}; SK_STATE.seenOrder=[];
      try{ var raw2=localStorage.getItem('sk_alert_log'); if(raw2) SK_STATE.alertLog=JSON.parse(raw2); }catch(e){}
      if(!Array.isArray(SK_STATE.alertLog)) SK_STATE.alertLog=[];
      try{ var t=parseFloat(localStorage.getItem('sk_alert_pct')||'0'); if(!isNaN(t)) SK_STATE.alertPct=t; }catch(e){}
      document.getElementById('skAlertPct').value=SK_STATE.alertPct||0;
      try{ SK_STATE.muted=(localStorage.getItem('sk_muted')==='1'); }catch(e){}
      try{ SK_STATE.notify=(localStorage.getItem('sk_notify')==='1'); }catch(e){}
      skApplyMuteLabel(); skApplyNotifyLabel();
    }
    function skMonSave(){ localStorage.setItem('sk_monitor_symbols', JSON.stringify(SK_STATE.monitor)); }
    function skSaveAlertLog(){ localStorage.setItem('sk_alert_log', JSON.stringify(SK_STATE.alertLog.slice(0,100))); }
    function skMonSaveThreshold(){
      var v=parseFloat(document.getElementById('skAlertPct').value)||0;
      if(v<0) v=0; SK_STATE.alertPct=v; localStorage.setItem('sk_alert_pct', String(v));
    }
    function skMonSetPct(sym,val){
      var m=SK_STATE.monitor.find(function(x){return x.s===sym;});
      if(!m) return;
      var v=val.trim()===''?null:parseFloat(val);
      if(v!=null && isNaN(v)) v=null;
      if(v!=null && v<0) v=0;
      m.pct=v;
      skMonSave();
      skMonRefresh();
    }
    function skMonHas(sym){ return SK_STATE.monitor.some(function(m){return m.s===sym;}); }
    function skMonAdd(sym,name){
      if(!sym) return;
      if(!skMonHas(sym)){
        SK_STATE.monitor.push({s:sym, pct:null});
        skMonSave();
      }
      skMonShow();
      if(name) SK_STATE.names[sym]=name;
      skSyncListMonFlags();
      skRefreshDetailMonBtn(sym);
      skMonRefresh();
    }
    function skMonRemove(sym){
      SK_STATE.monitor=SK_STATE.monitor.filter(function(m){return m.s!==sym;});
      skMonSave();
      skSyncListMonFlags();
      skRefreshDetailMonBtn(sym);
      if(SK_STATE.monitor.length===0){ skMonRender([]); return; }
      skMonRefresh();
    }
    function skMonClear(){ if(SK_STATE.monMode==='week'){ skToast('本周金叉模式不可清空','请切换到「自选监控」再管理自选股'); return; } SK_STATE.monitor=[]; skMonSave(); skSyncListMonFlags(); skMonRefresh(); document.getElementById('skMonStat').textContent='已清空'; }
    function skMonShow(){ var w=document.getElementById('skMonCol'); if(w) w.style.display='flex'; }
    function skMonToggle(){ var w=document.getElementById('skMonCol'); if(w) w.classList.toggle('collapsed'); }
    function skMonClose(){ var w=document.getElementById('skMonCol'); if(w) w.style.display='none'; }
    function skMonOpen(sym){ if(sym) skOpen(sym); }
    function skNameOf(sym){
      if(SK_STATE.names[sym]) return SK_STATE.names[sym];
      var row=document.querySelector('#skRows [data-sym="'+sym+'"]');
      if(row){ var nm=row.querySelector('.sk-name'); if(nm) return nm.textContent; }
      var d=SK_STATE.cur; if(d && d.symbol===sym && d.name) return d.name;
      return '';
    }
    function skMonAddFromWeek(sym,name){ skSetMonMode('watch'); skMonAdd(sym,name); }
    function skMonAddByCode(){
      var inp=document.getElementById('skAddSym'); if(!inp) return;
      var v=inp.value.trim(); if(!v) return;
      fetch(skApi('stocks?q='+encodeURIComponent(v)+'&limit=5')).then(function(r){return r.json();}).then(function(d){
        var sym=null, name='';
        if(d && d.ok && d.stocks && d.stocks.length){
          var exact=d.stocks.filter(function(s){return s.code===v;})[0]||d.stocks[0];
          sym=exact.symbol; name=exact.name||'';
        } else if(/^\d{6}$/.test(v)){
          sym=(/^[036]/.test(v)?'SSE':'SZSE')+'.'+v;
        } else if(v.indexOf('.')>0){
          sym=v;
        }
        if(!sym){ skToast('未找到该代码','请检查代码或带交易所前缀, 如 SSE.600519'); return; }
        inp.value='';
        skSetMonMode('watch');
        skMonAdd(sym, name);
      }).catch(function(e){ skToast('添加失败', e.message||'网络错误'); });
    }
    function skMonRefresh(){
      if(SK_STATE.monMode==='week'){ skLoadWeeklyCrosses(); return; }
      fetch(skApi('monitor'), {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({all_golden:true, watch_symbols:[], golden_period:SK_STATE.goldenPeriod||''})})
        .then(function(r){return r.json();}).then(function(d){
          if(d && d.ok){
            var items=d.items||[];
            // 2026-09-30 需求人裁定: 不要历史 —— 取消"会话累计/曾金叉", 列表=当前实时有状态的股票
            SK_STATE.monItems=items; SK_STATE.monMap={};
            items.forEach(function(it){ SK_STATE.monMap[it.symbol]=it; });
            skCheckAlerts(items); skRenderMonitor();
            document.getElementById('skMonStat').textContent='已更新 '+new Date().toLocaleTimeString();
          }
          else { document.getElementById('skMonStat').textContent='更新失败: '+(d.error||''); }
        }).catch(function(e){ document.getElementById('skMonStat').textContent='请求失败: '+e.message; });
    }
    function skCheckAlerts(items){
      var gth=SK_STATE.alertPct||0; var alerts={};
      items.forEach(function(it){
        // 无自选概念: 展示列表内的金叉股票都是监控对象, 首轮不弹、状态翻转才提醒
        var isWatch=true;
        var effTh=gth;
        var prev=SK_STATE.monPrev[it.symbol];
        var hasPrev=(prev && typeof prev==='object' && Object.keys(prev).length>0);
        var pct=it.change_pct!=null?parseFloat(it.change_pct):null;
        var priceAlert=false, goldAlert=false, deathAlert=false, mgoldAlert=false, mdeathAlert=false, dGoldAlert=false, dDeathAlert=false, minGoldAlert=false, minDeathAlert=false, alertType=null;

        if(effTh>0 && pct!=null && Math.abs(pct)>effTh){
          priceAlert=true;
          if(isWatch && hasPrev && prev.priceAlert===false){
            var dir=pct>=0?'上涨':'下跌';
            skToast((it.name||it.code)+' '+dir+' '+Math.abs(pct)+'%', it.code+' 实时'+dir+'突破 '+effTh+'% 阈值', 'price');
            skBeep(880,120);
            skLogAlert('price', it.name||it.code, it.code+' 实时'+dir+'突破 '+effTh+'% 阈值', pct);
            skNotify((it.name||it.code)+' '+dir+' '+Math.abs(pct)+'%', it.code+' 实时'+dir+'突破 '+effTh+'% 阈值');
          }
        }
        if(it.w_recent && hasPrev && prev.w_recent===false){
          goldAlert=true;
          if(isWatch){
            skToast((it.name||it.code)+' 出现周线金叉', '周线金叉 · '+it.w_golden, 'gold');
            skBeep(660,150); setTimeout(function(){skBeep(880,150);},160);
            skLogAlert('gold', it.name||it.code, '周线金叉 · '+it.w_golden);
            skNotify((it.name||it.code)+' 出现周线金叉', '周线金叉 · '+it.w_golden);
          }
        }
        if(it.w_death && hasPrev && prev.w_death && prev.w_death!==it.w_death){
          deathAlert=true;
          if(isWatch){
            skToast((it.name||it.code)+' 出现周线死叉', '周线死叉 · '+it.w_death, 'death');
            skBeep(440,150);
            skLogAlert('death', it.name||it.code, '周线死叉 · '+it.w_death);
            skNotify((it.name||it.code)+' 出现周线死叉', '周线死叉 · '+it.w_death);
          }
        }
        if(it.m_recent && hasPrev && prev.m_recent===false){
          mgoldAlert=true;
          if(isWatch){
            skToast((it.name||it.code)+' 出现月线金叉', '月线金叉 · '+it.m_golden, 'mgold');
            skBeep(660,150); setTimeout(function(){skBeep(1000,150);},160);
            skLogAlert('mgold', it.name||it.code, '月线金叉 · '+it.m_golden);
            skNotify((it.name||it.code)+' 出现月线金叉', '月线金叉 · '+it.m_golden);
          }
        }
        if(it.m_death && hasPrev && prev.m_death && prev.m_death!==it.m_death){
          mdeathAlert=true;
          if(isWatch){
            skToast((it.name||it.code)+' 出现月线死叉', '月线死叉 · '+it.m_death, 'mdeath');
            skBeep(330,150);
            skLogAlert('mdeath', it.name||it.code, '月线死叉 · '+it.m_death);
            skNotify((it.name||it.code)+' 出现月线死叉', '月线死叉 · '+it.m_death);
          }
        }
        // ===== 日线级金叉/死叉（补全 周/月/日 三档一致性）=====
        if(it.d_recent && hasPrev && prev.d_recent===false){
          dGoldAlert=true;
          if(isWatch){
            skToast((it.name||it.code)+' 出现日线金叉', '日线金叉 · '+it.d_golden, 'dgold');
            skBeep(620,160); setTimeout(function(){skBeep(980,160);},170);
            skLogAlert('dgold', it.name||it.code, '日线金叉 · '+it.d_golden);
            skNotify((it.name||it.code)+' 出现日线金叉', '日线金叉 · '+it.d_golden);
          }
        }
        if(it.d_death && hasPrev && prev.d_death && prev.d_death!==it.d_death){
          dDeathAlert=true;
          if(isWatch){
            skToast((it.name||it.code)+' 出现日线死叉', '日线死叉 · '+it.d_death, 'ddeath');
            skBeep(400,160);
            skLogAlert('ddeath', it.name||it.code, '日线死叉 · '+it.d_death);
            skNotify((it.name||it.code)+' 出现日线死叉', '日线死叉 · '+it.d_death);
          }
        }
        // ===== 分钟级金叉/死叉（实时警报）=====
        if(it.min_recent && hasPrev && prev.min_recent===false){
          minGoldAlert=true;
          if(isWatch){
            skToast((it.name||it.code)+' 出现分钟金叉', '分钟金叉 · '+it.min_golden, 'mingold');
            skBeep(720,140); setTimeout(function(){skBeep(960,140);},150);
            skLogAlert('mingold', it.name||it.code, '分钟金叉 · '+it.min_golden);
            skNotify((it.name||it.code)+' 出现分钟金叉', '分钟金叉 · '+it.min_golden);
            skShowMinCrossInfo(it.symbol);
          }
        }
        if(it.min_death && hasPrev && prev.min_death && prev.min_death!==it.min_death){
          minDeathAlert=true;
          if(isWatch){
            skToast((it.name||it.code)+' 出现分钟死叉', '分钟死叉 · '+it.min_death, 'mindeath');
            skBeep(360,150);
            skLogAlert('mindeath', it.name||it.code, '分钟死叉 · '+it.min_death);
            skNotify((it.name||it.code)+' 出现分钟死叉', '分钟死叉 · '+it.min_death);
          }
        }
        if(priceAlert) alertType='price';
        else if(goldAlert) alertType='gold';
        else if(deathAlert) alertType='death';
        else if(mgoldAlert) alertType='mgold';
        else if(mdeathAlert) alertType='mdeath';
        else if(dGoldAlert) alertType='dgold';
        else if(dDeathAlert) alertType='ddeath';
        else if(minGoldAlert) alertType='mingold';
        else if(minDeathAlert) alertType='mindeath';
        alerts[it.symbol]=alertType;
        SK_STATE.monPrev[it.symbol]={priceAlert:priceAlert, w_recent:!!it.w_recent, w_death:it.w_death||'', m_recent:!!it.m_recent, m_death:it.m_death||'', d_recent:!!it.d_recent, d_death:it.d_death||'', min_recent:!!it.min_recent, min_death:it.min_death||'', change_pct:pct};
      });
      SK_STATE.monAlerts=alerts;
      skUpdateAlertCount();
    }
    function skLogAlert(type,title,content,value){
      var t=new Date().toLocaleTimeString();
      SK_STATE.alertLog.unshift({type:type, title:title, content:content, value:value, time:t});
      if(SK_STATE.alertLog.length>100) SK_STATE.alertLog=SK_STATE.alertLog.slice(0,100);
      skSaveAlertLog(); skUpdateAlertCount();
      if(SK_STATE.showLog) skRenderAlertLog();
    }
    function skUpdateAlertCount(){
      var el=document.getElementById('skAlertCount'); if(el) el.textContent=''+SK_STATE.alertLog.length;
    }
    function skToggleAlertLog(){
      var el=document.getElementById('skAlertLog');
      if(!el) return;
      SK_STATE.showLog=!SK_STATE.showLog;
      el.classList.toggle('open', SK_STATE.showLog);
      if(SK_STATE.showLog) skRenderAlertLog();
    }
    function skClearAlertLog(){
      SK_STATE.alertLog=[]; skSaveAlertLog(); skUpdateAlertCount(); skRenderAlertLog();
    }
    function skRenderAlertLog(){
      var bd=document.getElementById('skAlertLogBd'); if(!bd) return;
      if(!SK_STATE.alertLog.length){ bd.innerHTML='<div class="sk-alert-empty">暂无预警</div>'; return; }
      var html='';
      SK_STATE.alertLog.forEach(function(a){
        var v=a.value!=null?(' · '+a.value):'';
        html+='<div class="sk-alert-item '+(a.type||'price')+'"><div class="t">'+a.title+v+'</div><div class="c">'+(a.content||'')+'</div><div class="tm">'+(a.time||'')+'</div></div>';
      });
      bd.innerHTML=html;
    }
    function skExportAlertLog(){
      if(!SK_STATE.alertLog.length){ alert('暂无可导出的预警记录'); return; }
      var map={price:'涨跌预警', gold:'周线金叉', death:'周线死叉', mgold:'月线金叉', mdeath:'月线死叉', dgold:'日线金叉', ddeath:'日线死叉', mingold:'分钟金叉', mindeath:'分钟死叉'};
      var lines=[];
      lines.push('时间,类型,标的,详情');
      SK_STATE.alertLog.forEach(function(a){
        var typ=map[a.type]||a.type;
        var t=(a.time||'').replace(/,/g,'，');
        var title=(a.title||'').replace(/,/g,'，');
        var c=(a.content||'').replace(/,/g,'，');
        lines.push(t+','+typ+','+title+','+c);
      });
      var csv='\ufeff'+lines.join('\n');
      var blob=new Blob([csv],{type:'text/csv;charset=utf-8'});
      var a=document.createElement('a'); a.href=URL.createObjectURL(blob);
      a.download='股票预警记录_'+new Date().toISOString().slice(0,10)+'.csv'; a.click();
      setTimeout(function(){ URL.revokeObjectURL(a.href); }, 1000);
    }
    function skToggleMute(){
      SK_STATE.muted=!SK_STATE.muted;
      try{ localStorage.setItem('sk_muted', SK_STATE.muted?'1':'0'); }catch(e){}
      skApplyMuteLabel();
    }
    function skApplyMuteLabel(){
      var b=document.getElementById('skMuteBtn'); if(!b) return;
      b.textContent=SK_STATE.muted?'🔇 静音':'🔊 声音';
    }
    function skApplyNotifyLabel(){
      var b=document.getElementById('skNotifyBtn'); if(!b) return;
      var ok=('Notification' in window) && Notification.permission==='granted' && SK_STATE.notify;
      b.textContent=ok?'🔔 通知开':'🔕 通知';
    }
    function skToggleNotify(){
      try{
        if(!('Notification' in window)){ alert('当前浏览器不支持桌面通知'); return; }
        if(Notification.permission==='granted'){ SK_STATE.notify=!SK_STATE.notify; }
        else if(Notification.permission!=='denied'){
          Notification.requestPermission().then(function(p){ SK_STATE.notify=(p==='granted'); skApplyNotifyLabel(); try{ localStorage.setItem('sk_notify', SK_STATE.notify?'1':'0'); }catch(e){} });
          return;
        } else { alert('桌面通知已被浏览器拒绝, 请在地址栏权限设置中允许后重试'); return; }
      }catch(e){}
      try{ localStorage.setItem('sk_notify', SK_STATE.notify?'1':'0'); }catch(e){}
      skApplyNotifyLabel();
    }
    function skNotify(title, content){
      if(!SK_STATE.notify) return;
      try{ if(!('Notification' in window) || Notification.permission!=='granted') return; var n=new Notification(title, {body:content, tag:'sk-alert'}); setTimeout(function(){ if(n) n.close(); }, 8000); }catch(e){}
    }
    function skExportMonitorList(){
      var items=SK_STATE.monItems||[];
      if(!items.length){ alert('当前无实时金叉股票, 稍后再导出'); return; }
      var lines=['名称,代码,最新价,涨跌幅%,周线金叉日期,月线金叉日期,日线金叉日期,分钟金叉日期'];
      items.forEach(function(it){
        var cp=it.change_pct!=null?String(it.change_pct):'';
        var name=(it.name||'').replace(/,/g,'，'); var code=(it.code||it.symbol||'').replace(/,/g,'，');
        lines.push(name+','+code+','+(it.last!=null?it.last:'')+','+cp+','+(it.w_golden||'')+','+(it.m_golden||'')+','+(it.d_golden||'')+','+(it.min_golden||''));
      });
      var csv='\ufeff'+lines.join('\n');
      var blob=new Blob([csv],{type:'text/csv;charset=utf-8'});
      var a=document.createElement('a'); a.href=URL.createObjectURL(blob);
      a.download='股票监控列表_'+new Date().toISOString().slice(0,10)+'.csv'; a.click();
      setTimeout(function(){ URL.revokeObjectURL(a.href); }, 1000);
    }
    function skSyncListMonFlags(){
      var rows=document.querySelectorAll('#skRows .sk-row');
      rows.forEach(function(row){
        var sym=row.getAttribute('data-sym'); if(!sym) return;
        var span=row.querySelector('.sk-add'); if(!span) return;
        if(skMonHas(sym)){
          span.className='sk-add monitored'; span.title='取消监控'; span.textContent='✓';
          span.setAttribute('onclick',"event.stopPropagation();skMonRemove('"+sym+"')");
        } else {
          span.className='sk-add'; span.title='加入监控'; span.textContent='+';
          span.setAttribute('onclick',"event.stopPropagation();skMonAdd('"+sym+"','"+(SK_STATE.names[sym]||'').replace(/'/g,"\\'")+"')");
        }
        var pctEl=row.querySelector('.sk-pct');
        if(pctEl){
          var it=SK_STATE.monMap&&SK_STATE.monMap[sym];
          if(it && it.change_pct!=null && it.change_pct!==''){
            var v=parseFloat(it.change_pct); var up=v>=0; var sign=v>=0?'+':'';
            pctEl.textContent=sign+v+'%'; pctEl.className='sk-pct '+(up?'up':'down');
          } else { pctEl.textContent=''; pctEl.className='sk-pct'; }
        }
      });
      skArrangeList();
    }
    function skPctOf(sym){ var m=SK_STATE.monMap; if(m && m[sym] && m[sym].change_pct!=null && m[sym].change_pct!=='') return parseFloat(m[sym].change_pct); return null; }
    function skArrangeList(){
      var rowsEl=document.getElementById('skRows'); if(!rowsEl) return;
      var more=document.getElementById('skMore'); if(more) more.remove();
      var sep=document.getElementById('skPinSep'); if(sep) sep.remove();
      var rows=Array.prototype.slice.call(rowsEl.querySelectorAll('.sk-row'));
      if(!rows.length){ if(more) rowsEl.appendChild(more); return; }
      var mon=SK_STATE.monitor.map(function(m){return m.s;});
      var pin=SK_STATE.pinMon && mon.length>0;
      var sort=SK_STATE.listSort||'default';
      function base(r){ var s=r._s||{}; return (pin && mon.indexOf(s.symbol)>=0)?0:1; }
      rows.sort(function(a,b){
        var ba=base(a), bb=base(b); if(ba!==bb) return ba-bb;
        var A=a._s||{}, B=b._s||{};
        var c=0;
        if(sort==='code_asc'){ c=(A.code||'')<(B.code||'')?-1:(A.code||'')>(B.code||'')?1:0; }
        else if(sort==='code_desc'){ c=(A.code||'')>(B.code||'')?-1:(A.code||'')<(B.code||'')?1:0; }
        else if(sort==='name'){ var na=(A.name||''), nb=(B.name||''); c=na<nb?-1:na>nb?1:0; }
        else if(sort==='exchange'){ var ea=(A.exchange||''), eb=(B.exchange||''); c=ea<eb?-1:ea>eb?1:0; }
        else if(sort==='golden'){ c=((B.recent_golden?1:0)-(A.recent_golden?1:0)); }
        else if(sort==='pct'){ var pa=skPctOf(A.symbol), pb=skPctOf(B.symbol); if(pa==null&&pb==null) c=0; else if(pa==null) c=1; else if(pb==null) c=-1; else c=pb-pa; }
        if(c!==0) return c;
        return (parseInt(a.getAttribute('data-idx')||'0')||0)-(parseInt(b.getAttribute('data-idx')||'0')||0);
      });
      rows.forEach(function(r){ rowsEl.appendChild(r); });
      if(pin){
        var lastPin=null;
        rows.forEach(function(r){ var s=r._s||{}; if(mon.indexOf(s.symbol)>=0) lastPin=r; });
        if(lastPin){
          var cnt=rows.filter(function(r){ var s=r._s||{}; return mon.indexOf(s.symbol)>=0; }).length;
          var sep2=document.createElement('div'); sep2.id='skPinSep'; sep2.className='sk-pin-sep';
          sep2.textContent='★ 自选股 ('+cnt+')';
          rowsEl.insertBefore(sep2, lastPin.nextSibling);
        }
      }
      if(more) rowsEl.appendChild(more);
    }
    function skTogglePin(){ var b=document.getElementById('skPinBtn'); SK_STATE.pinMon=!!(b&&b.checked); try{ localStorage.setItem('sk_pin_mon', SK_STATE.pinMon?'1':'0'); }catch(e){} skArrangeList(); }
    function skOnSortChange(){ var sel=document.getElementById('skSortSel'); if(sel) SK_STATE.listSort=sel.value; skArrangeList(); }
    function skRefreshDetailMonBtn(sym){
      var btn=document.querySelector('#skHd .sk-addmon'); if(!btn || !sym) return;
      if(skMonHas(sym)){ btn.textContent='✓ 已监控'; btn.className='sk-addmon on'; btn.setAttribute('onclick',"skMonRemove('"+sym+"')"); }
      else { btn.textContent='+ 加入监控'; btn.className='sk-addmon'; btn.setAttribute('onclick',"skMonAdd('"+sym+"')"); }
    }
    function skMonTogglePause(){
      SK_STATE.monPaused=!SK_STATE.monPaused;
      var b=document.getElementById('skPauseBtn'); if(b) b.textContent=SK_STATE.monPaused?'▶ 继续':'⏸ 暂停';
      document.getElementById('skMonStat').textContent=SK_STATE.monPaused?'已暂停':'已恢复';
    }
    function skMonToggleSort(){
      SK_STATE.monSort = SK_STATE.monSort==='desc'?'asc':(SK_STATE.monSort==='asc'?'none':'desc');
      var b=document.getElementById('skSortBtn'); if(b) b.textContent = SK_STATE.monSort==='desc'?'排序↓':(SK_STATE.monSort==='asc'?'排序↑':'排序');
      skRenderMonitor();
    }
    function skMonToggleOnlyAlert(){
      var c=document.getElementById('skOnlyAlert'); SK_STATE.monOnlyAlert=!!(c&&c.checked);
      skRenderMonitor();
    }
    function skRenderMonitor(){
      if(SK_STATE.monMode==='week'){ skLoadWeeklyCrosses(); return; }
      // 2026-09-30 需求人裁定: 不要历史 —— 列表只显示本次返回的实时有状态股票, 不再保留"曾金叉"
      var seenMap={};
      var items=[];
      (SK_STATE.monItems||[]).forEach(function(it){
        if(seenMap[it.symbol]) return;
        seenMap[it.symbol]=true;
        items.push(it);
      });
      if(SK_STATE.monOnlyAlert){
        items=items.filter(function(it){ return (SK_STATE.monAlerts||{})[it.symbol]; });
      }
      items.sort(function(a,b){
        // 优先显示当前实时金叉, 曾金叉沉底
        if(!!a._gone!==!!b._gone) return a._gone?1:-1;
        // 同层级再按用户选择的涨跌排序
        if(SK_STATE.monSort!=='none'){
          var av=a.change_pct!=null?parseFloat(a.change_pct):(a.change!=null?a.change:0);
          var bv=b.change_pct!=null?parseFloat(b.change_pct):(b.change!=null?b.change:0);
          return SK_STATE.monSort==='desc'? bv-av : av-bv;
        }
        return 0;
      });
      skMonRender(items);
    }
    function skMonRender(items){
      var tb=document.getElementById('skMonRows');
      var head=document.getElementById('skMonHead');
      if(head){ head.innerHTML='<tr><th>标的</th><th>最新价</th><th>涨跌</th><th title="MACD 五条件：日线零轴下双金叉 / 日线零轴附近或零轴上金叉 / 日线底背离 / 周线金叉 / 月线金叉。命中即贴上标签并附数据抓取时间；无命中的不显示。悬停可看该结果属于哪一天的筛选">MACD 状态</th></tr>'; }
      document.getElementById('skMonBadge').textContent=''+items.length;
      if(!items.length){
        var emptyMsg=SK_STATE.goldenPeriod?('当前暂无「'+((SK_GOLDEN_PERIODS||[]).filter(function(p){return p.k===SK_STATE.goldenPeriod;})[0]||{label:SK_STATE.goldenPeriod}).label+'」有状态的股票'):'暂无有状态的股票（口径=当前这根K线内刚金叉且当前仍金叉；全市场扫描 11:35/15:05 后更新）';
        tb.innerHTML='<tr><td colspan="4" style="color:var(--muted);padding:20px">'+emptyMsg+'</td></tr>';
        return;
      }
      var html='';
      items.forEach(function(it){
        var gone=!!it._gone;
        var cls=(it.change!=null && it.change>=0)?'up':'down';
        var sign=(it.change!=null && it.change>=0)?'+':'';
        var price=it.last!=null?it.last:'—';
        var chg=it.change!=null?(sign+it.change+(it.change_pct!=null?' ('+sign+it.change_pct+'%)':'')):'—';
        // 2026-09-23(二次) 用户要求: 周线/月线/日线/分钟这 4 列"不要", 直接换成【MACD 状态】——
        //   有哪个状态就贴哪个 chip。五条件直接取每日筛选结果表(t_stock_screen_result),
        //   按标的取近 10 天内最近一次; sc_known=false 表示该股本轮全市场筛选还没扫到。
        function scChips(it){
          // 2026-09-30 需求人裁定: 状态列【全实时】—— 后端 w/m/d/min_chip 为实时递推的
          //   「当前K线内刚发生」交叉事件(金叉/死叉), 与列表准入同口径; c1-c5 仍来自当天筛选。
          //   全空才不渲染。配色遵循中国行情惯例: 金叉=红(多), 死叉=绿(空)。
          var out='';
          [{k:'w',label:'周'},{k:'m',label:'月'},{k:'d',label:'日'},{k:'min',label:'分'}].forEach(function(p){
            var st=it[p.k+'_chip'];
            if(st==='金叉'){
              out+='<span class="sk-sc-hit" style="background:rgba(255,82,82,.16);color:#ff6b6b" title="'+p.label+'线当前K线内刚金叉(实时递推 dif>dea)">'+p.label+':金叉</span> ';
            }else if(st==='死叉'){
              out+='<span style="display:inline-block;font-size:10px;padding:1px 6px;border-radius:8px;margin:0 4px 2px 0;background:rgba(76,175,80,.14);color:#4caf50" title="'+p.label+'线当前K线内刚死叉(实时递推 dif<dea)">'+p.label+':死叉</span> ';
            }
          });
          var sc='';
          if(it.sc_known){
            (SK_SCREEN_CONDS||[]).forEach(function(c){
              if(it[c.k]){ sc+='<span class="sk-sc-hit" title="'+c.title+'　命中于 '+it.sc_date+'">'+c.label+'</span>'; }
            });
          }
          if(!out && !sc){
            return '';
          }
          var tm=it.sc_time&&(it.c1||it.c2||it.c3||it.c4||it.c5)?(' <span style="font-size:10px;color:var(--muted)" title="该状态数据的抓取时间(每日筛选落库时间)">'+it.sc_time+'</span>'):'';
          return out+sc+tm;
        }
        var scHtml=scChips(it);
        var alertType=(SK_STATE.monAlerts||{})[it.symbol];
        var trCls=alertType==='price'?'alert':(alertType==='gold'?'alert-gold':(alertType==='death'?'alert-death':(alertType==='mgold'?'alert-mgold':(alertType==='mdeath'?'alert-mdeath':(alertType==='dgold'?'alert-dgold':(alertType==='ddeath'?'alert-ddeath':(alertType==='mingold'?'alert-mgold':(alertType==='mindeath'?'alert-mdeath':''))))))));
        var nameHtml=(it.name||it.code);
        html+='<tr class="'+trCls+'">'
          +'<td ondblclick="skMonOpen(\''+it.symbol+'\')" title="双击查看K线" style="cursor:pointer"><div>'+nameHtml+'</div><div style="font-size:11px;color:var(--muted)">'+it.code+'</div></td>'
          +'<td class="last '+cls+'">'+price+'</td>'
          +'<td class="'+cls+'">'+chg+'</td>'
          +'<td class="sk-sc-cell" title="'+(it.sc_detail||'')+'">'+scHtml+'</td>'
          +'</tr>';
      });
      tb.innerHTML=html;
    }
    function skShowMinCrossInfo(symbol){
      var wrap=document.getElementById('skMinCrossInfo'); if(!wrap) return;
      wrap.style.display='block'; wrap.classList.add('open');
      wrap.innerHTML='<div class="mi-hd"><span>'+symbol+' · 各周期近期金叉</span><span class="close" onclick="skHideMinCrossInfo()">×</span></div><div class="mi-bd">加载中…</div>';
      fetch(skApi('recent_crosses_for')+'?symbol='+encodeURIComponent(symbol)).then(function(r){return r.json();}).then(function(d){
        var bd=wrap.querySelector('.mi-bd');
        if(d && d.ok){
          if(!d.periods || !d.periods.length){ bd.innerHTML='<div class="mi-empty">暂无金叉记录</div>'; return; }
          var html='<table class="mi-tb"><thead><tr><th>周期</th><th>最近金叉</th><th>状态</th></tr></thead><tbody>';
          d.periods.forEach(function(p){
            var st = p.recent?'<span class="mi-recent">近期 ✓</span>':(p.has?'<span class="mi-old">较早</span>':'<span class="mi-none">—</span>');
            var dt = p.has?p.date:'';
            html+='<tr class="'+(p.recent?'r':(p.has?'':'n'))+'"><td>'+p.label+'</td><td>'+dt+'</td><td>'+st+'</td></tr>';
            if(p.has){ html+='<tr class="h"><td colspan="3">'+p.hint+(p.death_date?' · 最近死叉 '+p.death_date:'')+'</td></tr>'; }
          });
          html+='</tbody></table>';
          bd.innerHTML=html;
        } else { bd.innerHTML='<div class="mi-empty">加载失败: '+((d&&d.error)||'')+'</div>'; }
      }).catch(function(e){ wrap.querySelector('.mi-bd').innerHTML='<div class="mi-empty">请求失败: '+e.message+'</div>'; });
    }
    function skHideMinCrossInfo(){ var w=document.getElementById('skMinCrossInfo'); if(w){ w.style.display='none'; w.classList.remove('open'); } }
    function skBeep(freq,dur){
      if(SK_STATE.muted) return;
      try{
        var AudioCtx=window.AudioContext||window.webkitAudioContext; if(!AudioCtx) return;
        var ctx=new AudioCtx();
        var osc=ctx.createOscillator(); var gain=ctx.createGain();
        osc.frequency.value=freq||880; osc.type='sine';
        gain.gain.setValueAtTime(0.15,ctx.currentTime); gain.gain.exponentialRampToValueAtTime(0.001,ctx.currentTime+(dur||100)/1000);
        osc.connect(gain); gain.connect(ctx.destination); osc.start(); osc.stop(ctx.currentTime+(dur||100)/1000);
      }catch(e){}
    }
    function skToast(title, content, type){
      var wrap=document.getElementById('skToastWrap'); if(!wrap) return;
      var el=document.createElement('div'); el.className='sk-toast '+(type==='gold'?'gold':'');
      el.innerHTML='<span class="close" onclick="this.parentElement.remove()">×</span><div class="tt">'+title+'</div><div class="tc">'+(content||'')+'</div>';
      wrap.appendChild(el);
      setTimeout(function(){ if(el.parentElement) el.remove(); }, 6000);
    }
    function skMonStart(){ if(SK_STATE.monTimer) return; SK_STATE.monNextTick=skNextPeriodTick(SK_STATE.monPeriod).getTime(); skUpdateMonNext(); SK_STATE.monTimer=setInterval(function(){ skTickMonitor(); skTickDetail(); }, 3000); skEngine('start'); }
    function skMonStop(){ if(SK_STATE.monTimer){ clearInterval(SK_STATE.monTimer); SK_STATE.monTimer=null; } }
    // 分钟金叉引擎惰性启停: 打开本页才启动, 关闭页面立即停止(后端另有 300s 空闲兜底)
    function skEngine(action){
      try{ fetch(skApi('minute/engine')+'?action='+action, {cache:'no-store', keepalive:action==='stop'}).then(function(){ }).catch(function(){ }); }catch(e){}
    }
    window.addEventListener('pagehide', function(){ skMonStop(); skEngine('stop'); });

    // 监控主页初始化
    skPopulateMonPeriod();
    skMonLoad();
    skUpdateAlertCount();
    skMonStart();
    skRestoreScanAcc();
    skMonRefresh();
    skLoadRecentCrosses();

    </script></body></html>'''
    return doc


def stock_serve_static(filename):
    return send_from_directory(_STATIC_DIR, filename)


def register_routes(app, slots, default_key, get_slot=None, make_slot=None):
    global _SLOTS, _DEFAULT_KEY, _GET_SLOT, _MAKE_SLOT
    _SLOTS = slots
    _DEFAULT_KEY = default_key
    _GET_SLOT = get_slot
    _MAKE_SLOT = make_slot
    app.add_url_rule("/stockkline", view_func=stock_list_page, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/", view_func=stock_list_page, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/stocks", view_func=api_stocks, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/kline", view_func=api_stock_kline, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/quote", view_func=api_stock_quote, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/monitor", view_func=api_monitor, methods=["POST"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/scan", view_func=api_scan, methods=["POST"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/scan/full", view_func=api_scan_full, methods=["POST"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/scan/status", view_func=api_scan_status, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/scan/account", view_func=api_scan_account, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/recent_crosses", view_func=api_recent_crosses, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/recent_crosses_for", view_func=api_recent_crosses_for, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/minute/status", view_func=api_minute_status, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/minute/engine", view_func=api_minute_engine, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/screen/run", view_func=api_screen_run, methods=["POST"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/screen/status", view_func=api_screen_status, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/screen/result", view_func=api_screen_result, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/api/screen/month", view_func=api_screen_month_history, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stockkline/static/<path:filename>", view_func=stock_serve_static, endpoint="stock_serve_static", methods=["GET"], strict_slashes=False)
    _ensure_cross_table()
    _ensure_cross_table_live()
    _ensure_event_table()
    _load_cross_cache()
    _ensure_scan_cfg_table()
    _load_scan_account()
    _load_scan_credential(get_scan_account())
    _init_stock_list()
    _start_scan_scheduler()
    _maybe_catchup_scan()
    _ensure_screen_tables()
    _start_screen_scheduler()
    # 分钟级实时金叉引擎: 2026-09-08 起改为"惰性启动" —— 服务启动不再拉起,
    # 只有打开监控页(前端调 /api/minute/engine?action=start)或调用监控接口时才启动,
    # 连续 300s 无人使用自动停止。原因: 常驻订阅会把天勤会话灌到限流, 导致整条行情流静默。
    print("[股票模块] 分钟引擎已改为惰性启动(监控页打开时才启动, 空闲 %ds 自动停止)" % 300)
    print("[股票模块] 路由已注册: /stockkline, /stockkline/api/stocks, /stockkline/api/kline, /stockkline/api/scan[*], /stockkline/api/minute/status, /stockkline/static/<file>")
