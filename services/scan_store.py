# -*- coding: utf-8 -*-
"""MVP: 收盘事件驱动判定 + K线仓库读窗(2026-10-13 用户拍板"按MVP方式处理")。

核心原则(与 S0-S2 已落地的仓库口径一致):
  1. 信号唯一入口 = **新 bar 收盘事件** —— 成型中的 bar 永不参与判定
     (结构性根治 rb 60m 09:00 "幻影死叉": 09:39-09:50 成型柱反复写库 13 行);
  2. 判定输入 = **kline_store 中的定稿窗口**(DB 权威), 不再用天勤内存序列算指标;
  3. 信号归属 = 交叉所在 bar 的 bar_time(trigger_time = bar_time)
     (根治 lu 周线"日线冒充"错档: 周/月/季一律由 day bar 派生);
  4. 写库幂等: (period, symbol, rule_id, signal_type, trigger_time) 已存在则跳过
     (根治 30s 轮询重复落库: 330 行 -> 33 独立信号)。

与旧路径(scan_symbols + live_only 轮询)并存, 由 KLINE_STORE_ENABLED 开关控制:
    KLINE_STORE_ENABLED=0  -> 一键回退旧行为(排错用)。
默认开启(用户 2026-10-13 裁定"直接切, 不灰度")。

数据流(每个周期线程一拍):
    ingest: 天勤内存序列(推送模型, 零网络成本) -> 只取已收盘 bar -> upsert 进仓库
    judge : 仅当"最近一根已收盘 bar"比上次判定的新 -> 读库定稿窗口 -> 判定 -> 幂等落库

性能(事件驱动, 非轮询): 收盘事件速率 ≈ 1.3 次/秒(60标的 x 6原子周期按周期边界铺开),
每次判定只读 ~300 行(DB 已池化), 相比旧路径"每 5~300s x 60标的 x 7周期全量重算"降 2~3 个数量级。
"""
import os
import time
import threading
import datetime as _dt
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

from services import indicators
from services import kline_store as ks

TZ8 = _dt.timezone(_dt.timedelta(hours=8))


# ---------------------------------------------------------------- 开关(回退用)
def _env_flag(name, default="1"):
    v = os.environ.get(name, default)
    return str(v).strip().lower() not in ("0", "false", "no", "off")


KLINE_STORE_ENABLED = _env_flag("KLINE_STORE_ENABLED", "1")
# 主力换月检测 + 历史重刷(2026-10-17): 天勤主连K线=当前主力完整历史(无拼接),
# 换月后 ingest 增量语义导致仓库历史停留在旧合约(拼凑序列), MACD 全程 EMA 递推
# 把骑线 bar 的死叉/金叉挤错位(10-16 乙二醇 60m 实锤: 列表 22:00 vs 图表 21:00)。
# 检测 real_symbol 变化 -> 清该 (symbol, 原子周期) 全部历史 -> 全量重灌。
# 置 0 可一键回退旧行为(排错用)。
KLINE_ROLL_REFRESH_ENABLED = _env_flag("KLINE_ROLL_REFRESH_ENABLED", "1")

# 原子周期映射: 周/月/季由 day bar 派生(TqSdk 不支持 >86400, 且这是 lu 错档的根治点)
ATOMIC_OF = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "60m": "60m",
    "day": "day", "week": "day", "month": "day", "quarter": "day",
}
DUR = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "60m": 3600}
# 日线不参与"按秒对齐"分支(它是自然日 bar, 不是等长窗口), 故不进 DUR。
_DAY_DUR = 86400
DERIVED = ("week", "month", "quarter")

MIN_BARS = 45        # MACD(12,26,9) 种子纪律: <45 根必为种子垃圾(PVC 假金叉教训), 不出信号
WIN = 300            # 读窗根数(与实时扫描 count=300 同键, 覆盖 MA60+lookback30=90)
WIN_DERIVED = 1500   # 周/月/季派生时读日线的根数(月线过 45 门槛需日线 >=945 根)

_stats = {"judged": 0, "ingested": 0, "skipped_dup": 0, "skipped_short": 0,
          "errors": 0, "last_error": None}


_stats_lock = threading.Lock()


def store_stats():
    with _stats_lock:
        return dict(_stats)


def _bump(key, n=1):
    try:
        with _stats_lock:
            _stats[key] = _stats.get(key, 0) + n
    except Exception:
        pass


def _set_error(msg):
    try:
        with _stats_lock:
            _stats["last_error"] = str(msg)[:200]
            _stats["errors"] = _stats.get("errors", 0) + 1
    except Exception:
        pass


# ---------------------------------------------------------------- 收盘边界
def _bj_now():
    """当前北京时间(naive)。"""
    return _dt.datetime.now(TZ8).replace(tzinfo=None)


def _month_start(y, m):
    return _dt.datetime(y, m, 1)


def _closed_cutoff(period, now=None):
    """该周期"最后一根已收盘 bar"的 bar_time 上界(北京时间 naive, 含)。

    成型中的 bar 一律被这个上界排除 —— 幻影信号的结构性防线。
    - 分钟族: floor(now 到周期边界) 再退一个周期(该 bar 的窗口刚好走完);
    - 日线: 15:00 收盘前, 当日 bar 仍在成型 -> 上界取前一自然日;
    - 周线: bar 标签=周一 D, 该周收盘=周五 15:00;
    - 月/季: 标签=期首, 收盘=下一期首。
    """
    now = now or _bj_now()
    if period in DUR:
        secs = DUR[period]
        ep = _dt.datetime.fromtimestamp(int(now.timestamp()) // secs * secs, TZ8).replace(tzinfo=None)
        return ep - _dt.timedelta(seconds=secs)
    if period == "day":
        d = now.date() if now.time() >= _dt.time(15, 0) else (now - _dt.timedelta(days=1)).date()
        return _dt.datetime.combine(d, _dt.time(0, 0))
    if period == "week":
        mon = (now - _dt.timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0)
        close_at = mon + _dt.timedelta(days=4, hours=15)   # 周五 15:00
        return mon if now >= close_at else mon - _dt.timedelta(days=7)
    if period == "month":
        first = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        nxt = _month_start(first.year + (1 if first.month == 12 else 0),
                           1 if first.month == 12 else first.month + 1)
        if now >= nxt:
            return first
        return _month_start(first.year - 1, 12) if first.month == 1 else _month_start(first.year, first.month - 1)
    if period == "quarter":
        qm = (now.month - 1) // 3
        qs = _month_start(now.year, qm * 3 + 1)
        qe = _month_start(now.year + 1, 1) if qm == 3 else _month_start(now.year, qm * 3 + 4)
        if now >= qe:
            return qs
        return _month_start(qs.year - 1, 10) if qm == 0 else _month_start(qs.year, qs.month - 3)
    return now


# ---------------------------------------------------------------- 仓库读窗
def _bars_to_df(bars):
    """仓库 bars(df, index=bar_time 北京时间) -> 判定用 df(与 _fetch_kline_df 同形)。"""
    d = bars.reset_index()   # bar_time, open, high, low, close, volume
    bt = pd.to_datetime(d["bar_time"])
    d["datetime"] = bt.dt.tz_localize("Asia/Shanghai").dt.tz_convert("UTC").astype("int64")
    return d[["datetime", "open", "high", "low", "close", "volume"]]


# 判定侧是否优先读库中已算好的指标(用户 2026-10-14拍板: 数据已入库, 出状态应直接读库)
# 开关: 环境变量 IND_FROM_STORE=0 可一键回退到"读 bar 现算"(排查用)
def _ind_from_store_enabled():
    return os.environ.get("IND_FROM_STORE", "1").strip().lower() not in ("0", "false", "no", "off")


def _ind_from_store(symbol, period, bars):
    """从指标库读已算好的全指标, 转成 detect_signals 需要的 ind 字典。

    返回 None 表示"库里没有可用指标"(调用方回退到 compute_all 现算),
    这样即使指标表落后/缺失, 判定也不会失效 —— 正确性优先于性能。

    🔴 窗口口径纪律(2026-10-14 实测踩坑, 勿改):
      库中指标是入库时用 `compute_all(仓库全部已收盘bar)` 算的, 每根的值都"从仓库
      最早bar起算"——包括 rolling 型(wvad/vr/vroc/boll/ma/rsi/atr)与累计量(obv/pvt/ad)。
      判定窗口(load_window)只取近 N 根, 若拿现算结果与库值比:
        - 窗口头部 ma/boll/rsi 等因预热不足而偏小(实测 ma60 差 59 处);
        - 累计量整体偏移一个常数(规则只做相对比较, 不影响判定)。
      **库值本身是对的**(全窗口径 508 根对拍差0.000000)。所以直接 reindex 取用即可,
      不需要给判定窗口补预热 —— 因为库值不是"按窗口起算"的, 而是"按仓库起点起算"的,
      每根都已含其全部历史。
      实测: rb/day(窗口=仓库全量500根) 逐位零差异; 窗口300根时仅头部若干根预热差异,
      而这些差异在现算路径同样存在(现算也是从窗口头起算), 属同一口径。
      切换后判定仍用 bars 对齐的行, 不会把窗口外的行喂给 detect_signals。
    """
    atomic = ATOMIC_OF.get(period, period)
    if atomic != period:
        return None            # 派生周期: 现算(库中无派生 bar)
    try:
        from services import kline_store as ks
        n = len(bars)
        if n <= 0:
            return None
        ind_df = ks.get_inds(symbol, period, count=n + 120)
        if ind_df is None or ind_df.empty:
            return None
        ind_df = ind_df.reindex(bars.index)   # 只取与判定窗口对齐的行
        if ind_df["dif"].notna().sum() < max(10, int(min(n, ks.MACD_SEED_MIN) * 0.6)):
            return None        # 覆盖不足 -> 回退现算
        ind = {}
        for key, col in ks.IND_KEY2COL.items():
            if col in ind_df.columns:
                ser = ind_df[col]
                ind[key] = [None if pd.isna(v) else float(v) for v in ser.tolist()]
        return ind or None
    except Exception:
        return None


def _derive_from_daily(daily_bars, period, count):
    """由日线仓库派生周/月/季(口径与 futures_kline._resample_daily 完全一致)。

    聚合: open=first / high=max / low=min / close=last / volume=sum, 按北京自然日历重采样;
    周线标签 pandas 'W'=周日结束, 减 6 天对齐到中国习惯的周一。
    """
    freq = {"week": "W", "month": "MS", "quarter": "QS"}[period]
    df = daily_bars.copy()
    df["_bt"] = pd.to_datetime(df.index)
    df = df.set_index("_bt")
    agg_map = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    agg = df.resample(freq).agg(agg_map).dropna(subset=["close"])
    if period == "week":
        agg.index = agg.index - pd.Timedelta(days=6)
    agg = agg.tail(count)
    # 关键: resample 后索引无名, 而 _bars_to_df 依赖名为 bar_time 的索引
    # (取仓库窗口时索引即 bar_time)。这里显式命名, 否则周/月/季取窗直接 KeyError。
    agg.index.name = "bar_time"
    return agg


def load_window(symbol, period, cutoff=None, win=None):
    """读该周期在 cutoff(已收盘上界)之前的定稿窗口。返回 bars(df, index=bar_time) 或 None。

    窗口根数来自设置「计算根数」(services/kline_settings):
      - 原子周期: 直接取用户配置的根数;
      - 派生周期(周/月/季): 由日线重采样, 读日线的根数 = 计算根数 x 换算系数,
        再受日线深度钳制(kline_settings.get_effective_calc_bars)。
    """
    from services import kline_settings as ks_set
    cutoff = cutoff if cutoff is not None else _closed_cutoff(period)
    atomic = ATOMIC_OF.get(period, period)
    n_out = None          # 该周期最终保留多少根(= 计算根数)
    if win is None:
        eff = ks_set.get_effective_calc_bars(period)   # 该周期实际可用根数
        n_out = eff if eff > 0 else WIN
        if period in DERIVED:
            # 读日线的根数要够派生出 n_out 根该周期
            factor = {"week": 5, "month": 21, "quarter": 63}[period]
            win = max(60, min(WIN_DERIVED, n_out * factor))
        else:
            win = n_out
    try:
        bars = ks.get_bars(symbol, atomic, count=win)
    except Exception as e:
        _set_error(f"get_bars {symbol}/{period}: {e}")
        return None
    if bars is None or bars.empty:
        return None
    # 只保留已收盘 bar: 成型中 bar 绝不参与判定(仓库里可能残留建库时的成型柱)
    bars = bars[bars.index <= pd.Timestamp(cutoff)]
    if bars.empty:
        return None
    if period in DERIVED:
        # tail 的长度是"该周期"的根数(不是日线的根数)
        bars = _derive_from_daily(bars, period, n_out or WIN)
        if bars is None or bars.empty:
            return None
    return bars


# ---------------------------------------------------------------- 取数: 天勤 -> 仓库
def ingest_closed_bars(slot, symbol, period, cutoff=None, count_override=None):
    """把该标的该周期"新收盘"的 bar 写进仓库。返回写入根数。

    天勤是推送模型: _fetch_kline_df 只读内存序列, 无网络往返。

    🔴 count 必须【固定 WIN=300】, 不得跟随设置里的「计算根数」变化!
       原因: 天勤 K 线对象的缓存键含 count, 改 count = 新建订阅对象。
       2026-10-13 实锤: 补扫用 400 而实时用 300, 每标的多 2~4 个对象,
       60 只主连直接把监控槽打到 420/420 天勤静默失效阈值, 15m 起不来、会话冻死。
       正确分工: ingest 固定拉 300 根(与实时扫描同键, 零新增订阅) -> 落仓库;
                 判定时按设置「计算根数」从【仓库】读(仓库是持久层, 500/1500 根都读得到)。
       这样用户把计算根数调到 500/1000 也只是多读库, 不会多占天勤订阅。
    🔴 count_override 仅给【尾盘补齐】(backfill_intraday_tail) 用: 用不同 count 强制新建
       kline 对象(绕过日内创建的陈旧对象, 该对象对不活跃合约尾盘 bar 未及时更新), 一次性抓取
       已固化的尾盘。属"一次性 transient 对象", 用完释放, 不占持续订阅。
    """
    cutoff = cutoff if cutoff is not None else _closed_cutoff(period)
    try:
        from services import futures_kline as fk
        _count = count_override if count_override else WIN
        # 派生周期(week/month/quarter)与 day 同口径拉日线(ATOMIC_OF 归并, 见下):
        # 2026-10-17 修复: 此前写 DUR[period], 派生周期不在 DUR -> KeyError 被静默吞,
        # 每轮 ingest 白抛一次异常并污染 stats.last_error(数据靠 day 线程灌, 功能未断)。
        _dur = DUR[period] if period in DUR else _DAY_DUR
        df, real = fk._fetch_kline_df(slot, symbol, _dur, _count)
    except Exception as e:
        _set_error(f"fetch {symbol}/{period}: {e}")
        return 0
    if df is None or df.empty:
        return 0
    try:
        d = df.copy()
        d["_bt"] = pd.to_datetime(d["datetime"].astype("int64"), unit="ns") + _dt.timedelta(hours=8)
        d = d[d["_bt"] <= pd.Timestamp(cutoff)]        # 只留已收盘
        if d.empty:
            return 0
        atomic = ATOMIC_OF.get(period, period)
        # ---------------------------------------------------------------- 换月检测(2026-10-17)
        # 天勤主连 K 线 = 当前主力合约的完整历史(无拼接): 换月后实时口径整体切换为
        # 新合约价格, 而 ingest 是增量语义(只写 >coverage.max_time 的新 bar),
        # 历史段永远停留在旧合约 -> 仓库成"多合约拼凑序列"。
        # MACD(12,26,9) 全程 EMA 递推: 旧口径几百点的差异一路影响到当前 bar,
        # 恰好骑在交叉线上的 bar, 死叉/金叉落点被挤错位(10-16 乙二醇 60m 实锤:
        # 监控列表 22:00 死叉 vs K 线图 21:00, 同算法不同数据)。
        # 处置: real 与已记录值不同 -> 清该 (symbol, atomic) 全部历史
        #       (bar/ind/coverage) -> 本次序列全量重灌。新品种首次进来时库本就是
        #       空的, "清+全量灌" = 原行为, 零副作用。roll 在重灌成功后记录,
        #       中途失败则 roll 不更新, 下次 ingest 自动重来, 不会锁死半清状态。
        full_reload = False
        if KLINE_ROLL_REFRESH_ENABLED and real:
            try:
                prev_real = ks.get_roll(symbol, atomic)
            except Exception:
                prev_real = None
            if prev_real != real:
                try:
                    ks.purge_symbol_period(symbol, atomic)
                    _purge_recent_signals(symbol)
                    _bump("roll_refresh", 1)
                except Exception as _pe:
                    _set_error(f"roll refresh {symbol}/{period}: {_pe}")
                    return 0
                full_reload = True
        if not full_reload:
            # 只写比库里更新的(增量, 幂等)
            try:
                cov = ks.get_coverage(symbol, atomic) or {}
            except Exception:
                cov = {}
            last = cov.get("max_time")
            if last:
                d = d[d["_bt"] > pd.Timestamp(last)]
                if d.empty:
                    return 0
        rows = []
        for i in range(len(d)):
            c = _num(d["close"].iloc[i])
            if c is None or c <= 0:
                continue
            rows.append({
                "bar_time": d["_bt"].iloc[i].to_pydatetime(),
                "open": _num(d["open"].iloc[i]), "high": _num(d["high"].iloc[i]),
                "low": _num(d["low"].iloc[i]), "close": c,
                "volume": _num(d["volume"].iloc[i]) if "volume" in d.columns else 0,
                "oi": _num(d["open_interest"].iloc[i]) if "open_interest" in d.columns else None,
            })
        if not rows:
            return 0
        ks.upsert_bars(symbol, ATOMIC_OF.get(period, period), rows)
        # 🔴 2026-10-15 修复(性能 P0): 写完 bar 必须刷新 coverage。
        # coverage.max_time 是"增量过滤"的唯一依据(见上 cov/max_time -> d['_bt'] > last)。
        # 此前实时 ingest 链路从不调 refresh_coverage(只在 tmp/backfill_*.py 离线脚本与
        # AsyncWriter 的 coverage 队列里调), 导致 coverage.max_time 长期停在夜盘(实测
        # KQ.m@SHFE.cu 1m: bar 表已到 13:34/880根, coverage 却是 00:59/740根) ->
        # 每轮 ingest 都把全部历史 bar 判定为"新"重复重写 + recompute_forward 全量重算指标
        # (420 组合 x 每轮 -> 摄入极慢, 页面看起来"没数据")。
        # 口径说明: ATOMIC_OF 把 week/month/quarter 归并到 day, coverage 只有一个 day 行,
        # 派生周期不再单列 coverage —— get_coverage 侧按 ATOMIC_OF 归一, 保持一致。
        try:
            ks.refresh_coverage(symbol, ATOMIC_OF.get(period, period))
        except Exception as _ce:
            _set_error(f"coverage {symbol}/{period}: {_ce}")
        # 指标层补算(仓库 MACD 是审计/对账用; 判定侧自己按读窗重算, 不依赖此表)。
        # 2026-10-15: 改用 calc_incremental(幂等, 只写"表中缺失"的行)。
        # 原先调 recompute_forward —— 它每轮都对全部 ~800 根重算并 upsert 全部行,
        # 420 组合热路径上是纯浪费; 两者口径逐位一致(都取全窗 compute_all)。
        ks.calc_incremental(symbol, ATOMIC_OF.get(period, period))
        # 换月跟踪: 重灌成功才记录主力名(失败时 roll 不更新 -> 下次继续触发重刷)
        if KLINE_ROLL_REFRESH_ENABLED and real:
            try:
                ks.set_roll(symbol, atomic, real)
            except Exception:
                pass
        _bump("ingested", len(rows))
        return len(rows)
    except Exception as e:
        _set_error(f"ingest {symbol}/{period}: {e}")
        return 0


def backfill_intraday_tail(slot, symbols, periods, count_override=None):
    """收盘后尾盘补齐(2026-10-15 修复, 实锤 60/60 品种 1m 全停在 14:55):
    天勤对不活跃合约的日内 kline 对象(在盘中创建)尾盘那几根 bar(14:56~14:59)未及时固化,
    首次收盘 ingest 漏掉 -> 监控尾盘信号整体缺失。用"不同 count 强制新建 kline 对象"
    (天勤缓存键含 count, 新键=新对象, 会按当前服务端已固化的完整序列填充)重新拉取,
    把漏掉的尾盘 bar 补进仓库。幂等(只写比 coverage.max_time 更新的)。

    仅作用于日内周期(1m/5m/15m/30m/60m); day/week/month/quarter 不走此路径。
    count_override 默认 WIN+20(新键), 顺序处理 + 小憩, 避免一次性瞬时对象过多冲击订阅。
    """
    intraday = [p for p in (periods or []) if p in DUR]
    if not intraday or not symbols:
        return 0
    _count = count_override or (WIN + 20)
    _total = 0
    for period in intraday:
        cutoff = _closed_cutoff(period)
        for sym in symbols:
            try:
                _total += ingest_closed_bars(slot, sym, period, cutoff=cutoff, count_override=_count)
            except Exception:
                pass
            time.sleep(0.05)   # 批间小憩, 降瞬时订阅压力
    return _total


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f


def _purge_recent_signals(symbol, days=7):
    """换月重刷时作废该标的最近 days 天的信号历史(2026-10-17)。

    理由: 信号是"新 bar 收盘事件"驱动, 只判最近一根; 历史重刷后那些 bar 不会
    重新判定(已超出取值窗口), 不清则列表永远残留旧口径的错误信号
    (10-16 乙二醇 60m: 22:00 假死叉会一直挂在列表里)。
    只删 scan_time 在最近 days 天内落库的行, 不动更早的有效信号。
    """
    from services import futures_db
    conn = futures_db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM t_scan_signal_history WHERE symbol=%s AND scan_time >= %s",
                (symbol, datetime.now() - _dt.timedelta(days=days)))
        conn.commit()
    finally:
        conn.close()


def _ma_at(closes, i, k):
    """第 i 根(0-based)处的 k 周期简单移动均线; 不足 k 根返回 None。

    与监控列表"最新价"同口径: 同一根 bar 的收盘价序列上取 MA,
    供前端在涨跌幅旁渲染 60/20 标签(均线高于现价=绿, 低于=红)。
    """
    if i is None or k is None or k <= 0:
        return None
    if i < k - 1 or i >= len(closes):
        return None
    seg = closes[i - k + 1:i + 1]
    if len(seg) < k:
        return None
    return _num(sum(seg) / k)


# ---------------------------------------------------------------- 判定
def _resolve_rules(symbol, rules, symbol_rules):
    """逐标的规则: 命中 symbol_rules 则用该标的的"最严合并集"(同 _scan_one)。"""
    if symbol_rules:
        _sr = symbol_rules.get(symbol)
        cand = None
        if isinstance(_sr, dict):
            cand = _sr.get("rules")
        elif isinstance(_sr, list):
            cand = _sr
        if cand:
            return cand
    return rules


def _template_tags(symbol, symbol_rules, symbol_tpl):
    _sr = symbol_rules.get(symbol) if symbol_rules else None
    tps = None
    if isinstance(_sr, dict):
        tps = _sr.get("templates") or []
    if symbol_tpl is not None:
        _st = symbol_tpl.get(symbol)
        if isinstance(_st, dict):
            tps = _st.get("templates") or []
        elif isinstance(_st, list):
            tps = _st
    ids, names = [], []
    for t in (tps or []):
        if not isinstance(t, dict):
            continue
        if t.get("id") is not None:
            ids.append(t["id"])
        if t.get("name"):
            names.append(t["name"])
    return ids, names


def _rules_for_pinned_bar(rules, n_bars):
    """判定"指定的那一根 bar"时, 把 recent_bars 放宽到覆盖整窗。

    为什么必须放宽(2026-10-13 实锤坑):
      macd_cross 规则的 recent_bars 默认 3, 语义是"只看最近 3 根"(实时在线行为, 正确)。
      但本模块判定的是【已被 pin 死的某一根 bar】(定稿制/补扫逐根回放), 该 bar 可能在
      窗口中段 —— recent_bars=3 会让 detect_signals 根本扫不到它, 补扫静默漏信号。
      我们判完后已用 `s.get("idx") == pos` 精确过滤, 放宽窗口不会引入额外信号。
    """
    out = []
    for r in (rules or []):
        if not isinstance(r, dict):
            continue
        if str(r.get("type", "")).startswith("macd_cross"):
            r2 = dict(r)
            p = dict(r2.get("params") or {})
            cur = p.get("recent_bars")
            try:
                cur_i = int(cur) if cur is not None else 3
            except (TypeError, ValueError):
                cur_i = 3
            if cur_i < n_bars:
                p["recent_bars"] = int(n_bars)
            r2["params"] = p
            out.append(r2)
        else:
            out.append(r)
    return out


def _rules_for_take_window(rules, take_bars, n_bars):
    """把规则的 recent_bars 对齐到用户设置的「取值根数」(每周期一个值)。

    语义(用户 2026-10-13 确认): 最近 N 根【已收盘】bar 内出现过状态就记录。
    与【被 pin 死的那一根】判定(_rules_for_pinned_bar)是两种不同场景:
      - 取值窗口(本函数): 允许最近 take_bars 根都算, 命中就记录(各自归自己的 bar_time);
      - pin 判定: 只认指定那一根, 但 recent_bars 要放宽到能扫到它, 否则中段 bar 扫不到。
    这里取 max(用户取值根数, 已有值), 保证用户设得越大越宽松; 用户设得比 pin 需要的
    小也不影响 pin 路径(pin 路径走 _rules_for_pinned_bar)。
    """
    out = []
    for r in (rules or []):
        if not isinstance(r, dict):
            continue
        if str(r.get("type", "")).startswith("macd_cross"):
            r2 = dict(r)
            p = dict(r2.get("params") or {})
            try:
                cur = int(p.get("recent_bars") or 0)
            except (TypeError, ValueError):
                cur = 0
            want = max(cur, int(take_bars or 1))
            if want > n_bars:
                want = n_bars
            p["recent_bars"] = want
            r2["params"] = p
            out.append(r2)
        else:
            out.append(r)
    return out


def _stamp_signals(signals, df, bars, period):
    """给每个信号打上【它自己所在 bar】的时间(不是扫描时刻, 也不是末根时间)。

    这是"不报之前日期金叉"的关键: idx=i 的信号归属 bars.index[i],
    而不是"今天"或"末根"。判定输出与落库都以这个 bar_time 为准。
    """
    n = len(df)
    closes = list(df["close"])
    # [2026-10-16] MA60/MA20 盖【行级末根】值(与 item.close/落库 close_price 同根):
    #   库里 close_price 取的是 item.close(末根), 不是信号自身 bar 的收盘价,
    #   所以随 signal_json 落库的 MA 也必须同口径, 否则重启后从库回填的条目、
    #   扫描历史弹窗里"最新价 vs MA"不同根 -> 配色错。
    row_ma60 = _ma_at(closes, n - 1, 60)
    row_ma20 = _ma_at(closes, n - 1, 20)
    for s in signals:
        i = s.get("idx")
        if i is None or not (0 <= i < n):
            s["trigger_time"] = None
            continue
        t = pd.Timestamp(bars.index[i])
        s["trigger_time"] = t.strftime("%Y-%m-%d %H:%M")
        s["trigger_time_ns"] = int(t.tz_localize("Asia/Shanghai").tz_convert("UTC").value)
        s["bar_time"] = t.to_pydatetime()
        s["ma60"] = row_ma60
        s["ma20"] = row_ma20
    return signals


def judge_closed(symbol, period, slot=None, rules=None, cutoff=None,
                 only_bar_time=None, symbol_rules=None, symbol_tpl=None):
    """判定该周期【最近 N 根已收盘 bar】上的信号(N = 设置里的「取值根数」)。

    only_bar_time=None -> 取值窗口模式: 最近 take_bars 根已收盘 bar 内出现过状态就记录,
                          每个信号归属自己那一根的 bar_time;
    only_bar_time 有值 -> pin 模式: 只判定该时刻那一根(补扫逐根回放用)。

    🔴 取值窗口里的 bar 全部【已收盘】(cutoff 已排除成型 bar), 所以放宽到 N 根
       并不会让幻影信号回来 —— 幻影的根因是"判定成型中的 bar", 不是"看多根"。
    """
    from services import scan_engine as se   # 延迟导入: 避免与 scan_engine 循环依赖
    from services import kline_settings as ks_set
    try:
        cfg_rules = _resolve_rules(symbol, rules, symbol_rules)
        cutoff = cutoff if cutoff is not None else _closed_cutoff(period)
        bars = load_window(symbol, period, cutoff=cutoff)
        if bars is None or len(bars) < MIN_BARS:
            _bump("skipped_short")
            return None
        df = _bars_to_df(bars)
        n = len(df)
        # 指标来源: 优先读指标库中已算好的全指标(2026-10-14 优化);
        # 派生周期/覆盖不足/开关关闭 -> 回退现算(正确性优先)。
        ind = None
        if _ind_from_store_enabled():
            ind = _ind_from_store(symbol, period, bars)
        if ind is None:
            ind = indicators.compute_all(df)
        tids, tnames = _template_tags(symbol, symbol_rules, symbol_tpl)

        if only_bar_time is not None:
            # pin 模式: 只认指定那一根
            target = pd.Timestamp(only_bar_time)
            if target not in bars.index:
                return None
            pos = int(bars.index.get_loc(target))
            signals = se.detect_signals(df, ind, _rules_for_pinned_bar(cfg_rules, n))
            picked = [s for s in signals if s.get("idx") == pos]
        else:
            # 取值窗口模式: 最近 take_bars 根已收盘 bar 内出现过状态就记录
            take = ks_set.get_take_bars(period)
            lo = max(0, n - max(1, int(take)))
            signals = se.detect_signals(df, ind, _rules_for_take_window(cfg_rules, take, n))
            picked = [s for s in signals
                      if s.get("idx") is not None and lo <= int(s["idx"]) < n]
        if not picked:
            return None
        if (tids or tnames):
            for s in picked:
                s["template_ids"] = tids
                s["template_names"] = tnames
        picked = _stamp_signals(picked, df, bars, period)
        picked = [s for s in picked if s.get("trigger_time")]
        if not picked:
            return None
        close_pos = n - 1
        close_val = _num(df["close"].iloc[close_pos])
        pre_settlement = None
        if slot is not None:
            try:
                q = slot.client.get_quote(symbol)
                pre_settlement = _num(getattr(q, "pre_settlement", None))
            except Exception:
                pass
        if pre_settlement is None or pre_settlement <= 0:
            pre_settlement = _num(df["open"].iloc[0])
        change = change_pct = None
        if close_val is not None and pre_settlement and pre_settlement > 0:
            change = round(close_val - pre_settlement, 4)
            change_pct = round((change / pre_settlement) * 100, 2)
        # [2026-10-16] 行级 MA60/MA20: 取窗口最后一根(=行内"最新价"同一根),
        # 供监控列表在涨跌幅旁渲染均线标签; 不足周期根数时为 None(前端不渲染)。
        closes_all = list(df["close"])
        ma60 = _ma_at(closes_all, n - 1, 60)
        ma20 = _ma_at(closes_all, n - 1, 20)
        _bump("judged")
        return {
            "symbol": symbol, "real_symbol": symbol,
            "display_name": se._symbol_display_name(symbol, symbol),
            "close": close_val, "change": change, "change_pct": change_pct,
            "ma60": ma60, "ma20": ma20,
            "signals": picked,
        }
    except Exception as e:
        _set_error(f"judge {symbol}/{period}: {e}")
        return None


# ---------------------------------------------------------------- 幂等落库
def _existing_keys(period, symbols, bar_time=None, bar_from=None, bar_to=None):
    """一次查询拿到本批 (symbol, rule_id, signal_type, trigger_time) 已存在的集合。

    bar_time: 精确时刻(= 单根);
    bar_from/bar_to: 区间(取值根数放开后一批可能跨多根 -> 一次范围查询搞定, 避免 N 次往返)。
    """
    syms = [s for s in symbols if s]
    if not syms:
        return set()
    out = set()
    try:
        from services import futures_db
        conn = futures_db.get_connection()
        try:
            with conn.cursor() as cur:
                if bar_from is not None and bar_to is not None:
                    where_t = "AND trigger_time >= %s AND trigger_time <= %s"
                    targs = [bar_from, bar_to]
                else:
                    where_t = "AND trigger_time = %s"
                    targs = [bar_time]
                B = 400
                for i in range(0, len(syms), B):
                    chunk = syms[i:i + B]
                    ph = ",".join(["%s"] * len(chunk))
                    cur.execute(
                        "SELECT symbol, rule_id, signal_type, trigger_time "
                        "FROM t_scan_signal_history "
                        f"WHERE period=%s {where_t} AND symbol IN ({ph})",
                        tuple([period] + targs + chunk))
                    for r in cur.fetchall():
                        out.add((r[0], r[1] or "", r[2] or "", r[3]))
        finally:
            conn.close()
    except Exception as e:
        _set_error(f"dedup query: {e}")
    return out


def _mk_result(period, long_items, short_items, extra=None):
    r = {
        "ok": True,
        "scan_time": datetime.now().isoformat(timespec="seconds"),
        "period": period,
        "live_only": bool(extra and extra.get("live_only")),
        "live_data": True,
        "elapsed": round((extra or {}).get("elapsed", 0.0), 2),
        "stats": {
            "total": (extra or {}).get("total", 0),
            "scanned": (extra or {}).get("scanned", 0),
            "failed": (extra or {}).get("failed", 0),
            "no_live": 0,
            "long_count": len(long_items),
            "short_count": len(short_items),
        },
        "long": long_items,
        "short": short_items,
    }
    return r


def _side_split(item):
    long_s = [s for s in item["signals"] if s.get("side") == "long"]
    short_s = [s for s in item["signals"] if s.get("side") == "short"]
    out = []
    for sigs, side in ((long_s, "long"), (short_s, "short")):
        if sigs:
            it = {k: v for k, v in item.items() if k != "signals"}
            it["signals"] = sigs
            out.append((side, it))
    return out


def scan_period_from_store(symbols, slot, period="day", rules=None, max_workers=8,
                           executor=None, symbol_rules=None, symbol_tpl=None,
                           judged=None):
    """收盘事件驱动的一拍: ingest 新收盘 bar -> 只对"新的已收盘 bar"判定 -> 幂等结果。

    judged: {symbol: 最后判定的 bar_time(datetime)}, 跨拍去重(同一根 bar 只判一次)。
             传 None 则每拍都判最近一根已收盘 bar(靠落库幂等兜底)。
    """
    t0 = time.time()
    rules = rules or []
    judged = judged if judged is not None else {}
    cutoff = _closed_cutoff(period)

    # 1) ingest: 只对"还没进仓库的已收盘 bar"写库(增量, 通常 0~1 根)
    for sym in symbols:
        try:
            ingest_closed_bars(slot, sym, period, cutoff=cutoff)
        except Exception:
            pass

    # 2) judge: 仅当最近一根已收盘 bar 比上次判定的新
    todo = []
    for sym in symbols:
        bars = None
        try:
            bars = load_window(sym, period, cutoff=cutoff, win=2)
        except Exception:
            bars = None
        if bars is None or bars.empty:
            continue
        target = bars.index[-1]
        prev = judged.get(sym)
        if prev is not None and pd.Timestamp(prev) >= target:
            continue                      # 这根已判过 -> 本拍跳过(事件驱动)
        todo.append((sym, target))
    if not todo:
        return _mk_result(period, [], [], {"elapsed": time.time() - t0, "total": len(symbols)})

    own = executor is None
    exe = executor if executor is not None else ThreadPoolExecutor(max_workers=max_workers)
    items, scanned, failed = [], 0, 0
    try:
        futs = {exe.submit(judge_closed, sym, period, slot, rules, cutoff, tgt,
                           symbol_rules, symbol_tpl): (sym, tgt) for sym, tgt in todo}
        for fut in as_completed(futs):
            sym, tgt = futs[fut]
            scanned += 1
            try:
                res = fut.result(timeout=30)
            except Exception:
                failed += 1
                continue
            if not res:
                continue
            items.append((sym, tgt, res))
    finally:
        if own:
            exe.shutdown(wait=False)

    # 3) 幂等: 同一 (symbol, rule_id, type, 【该信号自己那一根的 bar_time】) 已落库则跳过。
    #    注意: 取值根数放开后, 一个标的的结果可能含【多根 bar】上的信号, 所以必须按
    #    信号自身的 bar_time 分组去重, 不能按"末根 target"去重(否则同批里较早 bar 的信号
    #    会互相误判为重复, 或跨批重复落库)。
    long_items, short_items = [], []
    by_bar = {}
    for sym, tgt, res in items:
        for s in res["signals"]:
            bt = s.get("bar_time")
            if bt is None:
                continue
            by_bar.setdefault(pd.Timestamp(bt), []).append((sym, s))
    exist_cache = {}
    if by_bar:
        all_syms = sorted({sym for grp in by_bar.values() for sym, _ in grp})
        lo = min(by_bar.keys()).to_pydatetime()
        hi = max(by_bar.keys()).to_pydatetime()
        # 一次范围查询覆盖本批所有 bar(取值根数放开后跨多根)
        all_exist = _existing_keys(period, all_syms, bar_from=lo, bar_to=hi)
        for bt in by_bar:
            btt = bt.to_pydatetime()
            exist_cache[bt] = {k for k in all_exist if k[3] == btt}
    # 同一标的的多条信号聚回一个 item, 前端仍按品种展示
    per_sym = {}
    for bt, group in by_bar.items():
        exist = exist_cache[bt]
        for sym, s in group:
            key = (sym, s.get("rule_id") or "", s.get("type") or "", bt.to_pydatetime())
            if key in exist:
                _bump("skipped_dup")
                continue
            exist.add(key)
            slot_d = per_sym.setdefault(sym, {"signals": [], "meta": None})
            slot_d["signals"].append(s)
            if slot_d["meta"] is None:
                for sym2, tgt2, res2 in items:
                    if sym2 == sym:
                        slot_d["meta"] = res2
                        break
    for sym, d in per_sym.items():
        meta = d["meta"] or {"symbol": sym, "real_symbol": sym,
                             "display_name": sym, "close": None,
                             "change": None, "change_pct": None,
                             "ma60": None, "ma20": None}
        res = dict(meta)
        res["signals"] = d["signals"]
        for side, it in _side_split(res):
            (long_items if side == "long" else short_items).append(it)
    for sym, tgt, _res in items:
        judged[sym] = tgt.to_pydatetime()

    return _mk_result(period, long_items, short_items, {
        "elapsed": time.time() - t0, "total": len(symbols),
        "scanned": scanned, "failed": failed,
    })


def backfill_from_store(symbols, slot, period="day", rules=None, start=None, end=None,
                        max_workers=8, executor=None, symbol_rules=None, symbol_tpl=None):
    """补扫: 把 [start, end] 区间内【每一根已收盘 bar】逐根判定, 信号精确归位。

    这是 lu 周线错档与"今天翻出前几天金叉"的正解 —— 不翻历史窗口找交叉,
    而是每根 bar 在它自己收盘那一刻被判定一次(此处是离线回放同一判定)。
    派生周期(周/月/季)先由日线仓库本地重采样再逐根判(同 _resample_daily 口径)。
    """
    t0 = time.time()
    rules = rules or []
    cutoff = _closed_cutoff(period)
    end = end or cutoff
    start = start or (end - _dt.timedelta(days=1))

    own = executor is None
    exe = executor if executor is not None else ThreadPoolExecutor(max_workers=max_workers)
    out = {}
    scanned = failed = 0
    try:
        # 先算每个标的在区间内的目标 bar 列表(读一次库)
        plans = []
        for sym in symbols:
            try:
                bars = load_window(sym, period, cutoff=end)
            except Exception:
                bars = None
            if bars is None or len(bars) < MIN_BARS:
                continue
            idxs = [t for t in bars.index if pd.Timestamp(start) <= t <= pd.Timestamp(end)]
            if not idxs:
                continue
            plans.append((sym, bars, idxs))

        futs = {}
        for sym, bars, idxs in plans:
            for t in idxs:
                futs[exe.submit(_judge_at, sym, period, slot, rules, bars, t,
                                symbol_rules, symbol_tpl)] = (sym, t)
        for fut in as_completed(futs):
            scanned += 1
            try:
                res = fut.result(timeout=30)
            except Exception:
                failed += 1
                continue
            if not res:
                continue
            sym, t = futs[fut]
            key = (sym, t)
            if key not in out:
                out[key] = res
    finally:
        if own:
            exe.shutdown(wait=False)

    long_items, short_items = [], []
    for (sym, t), res in sorted(out.items(), key=lambda kv: kv[0][1]):
        for side, it in _side_split(res):
            (long_items if side == "long" else short_items).append(it)
    return _mk_result(period, long_items, short_items, {
        "elapsed": time.time() - t0, "total": len(symbols),
        "scanned": scanned, "failed": failed,
    })


def _judge_at(symbol, period, slot, rules, bars, target, symbol_rules, symbol_tpl):
    """对已加载窗口中的指定 bar 判定(补扫内部用, 避免重复读库)。"""
    from services import scan_engine as se
    try:
        cfg_rules = _resolve_rules(symbol, rules, symbol_rules)
        target = pd.Timestamp(target)
        if target not in bars.index or len(bars) < MIN_BARS:
            return None
        df = _bars_to_df(bars)
        pos = int(bars.index.get_loc(target))
        ind = None
        if _ind_from_store_enabled():
            ind = _ind_from_store(symbol, period, bars)
        if ind is None:
            ind = indicators.compute_all(df)
        signals = se.detect_signals(df, ind, _rules_for_pinned_bar(cfg_rules, len(df)))
        picked = [s for s in signals if s.get("idx") == pos]
        if not picked:
            return None
        tids, tnames = _template_tags(symbol, symbol_rules, symbol_tpl)
        if (tids or tnames):
            for s in picked:
                s["template_ids"] = tids
                s["template_names"] = tnames
        tstr = target.strftime("%Y-%m-%d %H:%M")
        tns = int(target.tz_localize("Asia/Shanghai").tz_convert("UTC").value)
        scan_now = datetime.now()
        for s in picked:
            s["trigger_time"] = tstr
            s["trigger_time_ns"] = tns
            s["bar_time"] = target.to_pydatetime()
            s["scan_time"] = scan_now
        close_val = _num(df["close"].iloc[pos])
        pre_settlement = None
        if slot is not None:
            try:
                q = slot.client.get_quote(symbol)
                pre_settlement = _num(getattr(q, "pre_settlement", None))
            except Exception:
                pass
        if pre_settlement is None or pre_settlement <= 0:
            pre_settlement = _num(df["open"].iloc[0])
        change = change_pct = None
        if close_val is not None and pre_settlement and pre_settlement > 0:
            change = round(close_val - pre_settlement, 4)
            change_pct = round((change / pre_settlement) * 100, 2)
        # [2026-10-16] MA60/MA20 取该 bar 自身位置(与 close 同根), 同 judge_closed 口径
        closes_all = list(df["close"])
        ma60 = _ma_at(closes_all, pos, 60)
        ma20 = _ma_at(closes_all, pos, 20)
        for s in picked:
            s["ma60"] = ma60
            s["ma20"] = ma20
        _bump("judged")
        return {
            "symbol": symbol, "real_symbol": symbol,
            "display_name": se._symbol_display_name(symbol, symbol),
            "close": close_val, "change": change, "change_pct": change_pct,
            "ma60": ma60, "ma20": ma20,
            "signals": picked,
        }
    except Exception as e:
        _set_error(f"judge_at {symbol}/{period}@{target}: {e}")
        return None