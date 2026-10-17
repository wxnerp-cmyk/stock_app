# -*- coding: utf-8 -*-
"""
期货全市场监控扫描引擎。

用途：批量扫描指定合约列表，按用户配置的指标规则检测多头/空头信号，
      返回达标的合约及具体信号，供前端悬浮监控面板展示、弹窗与声音报警。

设计约束：
- 纯 Python/numpy/pandas，不调用 tqsdk.tafunc。
- 单个合约独立计算，线程池并发，避免阻塞 Flask 主线程。
- 默认只扫描主力连续合约，控制量级；支持用户传入自定义列表。
"""
import time
import threading
import traceback
import os
import datetime as _dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import numpy as np
import pandas as pd

from services import indicators, ta_patterns

# ===== K线仓库开关(2026-10-13 用户拍板 MVP) =====
# 默认走新路径: 收盘事件驱动 + 读 kline_store 定稿窗口(services/scan_store.py)。
# 结构性根治三事故:
#   rb 60m 09:00 幻影死叉(成型 bar 瞬时态写库 13 行) / lu 周线日线冒充错档 /
#   30s 轮询重复落库(330 行 -> 33 信号)。
# 设 KLINE_STORE_ENABLED=0 可一键回退到旧的 live_only 轮询路径(排错用)。
KLINE_STORE_ENABLED = str(os.environ.get("KLINE_STORE_ENABLED", "1")).strip().lower() \
    not in ("0", "false", "no", "off")


# 金叉/死叉 距零线过滤的可选比较算子（用户可自行切换 大于/大于等于/小于/小于等于）
_OPS = {
    "gt": lambda a, b: a > b,
    "ge": lambda a, b: a >= b,
    "lt": lambda a, b: a < b,
    "le": lambda a, b: a <= b,
}
_OP_LABEL = {
    "gt": "大于 >",
    "ge": "大于等于 ≥",
    "lt": "小于 <",
    "le": "小于等于 ≤",
}


# 默认监控规则：金叉/死叉(MACD, 距零线过滤) + 顶背离/底背离
DEFAULT_RULES = [
    {
        "id": "macd_golden",
        "name": "MACD金叉",
        "type": "macd_cross",
        "side": "long",
        "enabled": True,
        # 需求人规则: 金叉 (DIF-零线) 大于 -0.5; 算子可前端切换
        # trend_ma_n/trend_lookback: 用慢速均线整体斜率判趋势(默认 MA60/窗口30)
        # trend_lookback 原默认10根太短, 局部横盘/微弹易翻转方向(2026-09-01 修正: 升到30)
        # trend_mode: "trend"=趋势驱动(上升中任一交叉均金叉/下降中均死叉, 需求人2026-08-28);
        #             "match"=方向匹配(上穿+上升=金叉, 下穿+下降=死叉)。
        #             [2026-10-12 需求人裁定] 切回 "match": nr2612 上升趋势里的下穿被报成"金叉"
        #             与 WH6 对照冲突, 金叉死叉回归方向语义。
        "params": {"zero_line": 0.0, "threshold": -0.5, "op": "gt", "recent_bars": 3,
                   "trend_ma_n": 60, "trend_lookback": 30, "trend_mode": "match",
                   "persist": 2},
        # MACD 指标自身参数(12,26,9), 可在规则/模板中调整
        "indicator_params": {"fast": 12, "slow": 26, "signal": 9},
    },
    {
        "id": "macd_death",
        "name": "MACD死叉",
        "type": "macd_cross",
        "side": "short",
        "enabled": True,
        # 需求人规则: 死叉 (DIF-零线) 小于等于 0.5; 算子可前端切换
        "params": {"zero_line": 0.0, "threshold": 0.5, "op": "le", "recent_bars": 3,
                   "trend_ma_n": 60, "trend_lookback": 30, "trend_mode": "match",
                   "persist": 2},
        # MACD 指标自身参数(12,26,9), 可在规则/模板中调整
        "indicator_params": {"fast": 12, "slow": 26, "signal": 9},
    },
    {
        "id": "top_divergence",
        "name": "顶背离",
        "type": "divergence",
        "side": "short",
        "enabled": True,
        "params": {"recent_bars": 10},
    },
    {
        "id": "bottom_divergence",
        "name": "底背离",
        "type": "divergence",
        "side": "long",
        "enabled": True,
        "params": {"recent_bars": 10},
    },
]


# 可选扩展指标规则模板（供前端选择）
OPTIONAL_RULE_TEMPLATES = [
    {"id": "rsi_oversold", "name": "RSI超卖", "type": "rsi", "side": "long", "params": {"n": 14, "threshold": 30}},
    {"id": "rsi_overbought", "name": "RSI超买", "type": "rsi", "side": "short", "params": {"n": 14, "threshold": 70}},
    {"id": "kdj_oversold", "name": "KDJ超卖", "type": "kdj", "side": "long", "params": {"n": 9, "m1": 3, "m2": 3, "threshold": 20}},
    {"id": "kdj_overbought", "name": "KDJ超买", "type": "kdj", "side": "short", "params": {"n": 9, "m1": 3, "m2": 3, "threshold": 80}},
    {"id": "boll_lower", "name": "BOLL下轨", "type": "boll", "side": "long", "params": {"n": 20, "k": 2}},
    {"id": "boll_upper", "name": "BOLL上轨", "type": "boll", "side": "short", "params": {"n": 20, "k": 2}},
    {"id": "cci_oversold", "name": "CCI超卖", "type": "cci", "side": "long", "params": {"n": 14, "threshold": -100}},
    {"id": "cci_overbought", "name": "CCI超买", "type": "cci", "side": "short", "params": {"n": 14, "threshold": 100}},
    {"id": "vol_ratio", "name": "量比异动", "type": "vol_ratio", "side": "both", "params": {"n": 5, "threshold": 2.0}},
    {"id": "vroc", "name": "量变动速率", "type": "vroc", "side": "both", "params": {"n": 10, "threshold": 50.0}},
    {"id": "obv", "name": "OBV新高", "type": "obv", "side": "long", "params": {"lookback": 20}},
    {"id": "wvad", "name": "WVAD转正", "type": "wvad", "side": "long", "params": {"n": 30}},
]


def _safe_float(v):
    try:
        if v is None:
            return None
        f = float(v)
        return None if (np.isnan(f) or np.isinf(f)) else round(f, 6)
    except Exception:
        return None


def _ma_at(closes, i, k):
    """第 i 根(0-based)处的 k 周期简单移动均线; 不足 k 根返回 None。"""
    if i is None or k is None or k <= 0 or i < k - 1 or i >= len(closes):
        return None
    try:
        return _safe_float(sum(closes[i - k + 1:i + 1]) / k)
    except Exception:
        return None


def _ma_last(closes, k):
    """序列末尾的 k 周期简单移动均线; 不足 k 根返回 None(同 scan_store._ma_at 口径)。

    用于监控列表涨跌幅旁的 MA60/MA20 标签: 均线高于现价=绿, 低于=红。
    """
    if not closes:
        return None
    return _ma_at(closes, len(closes) - 1, k)


# 实时 bar 判定: 实时模式下, 仅当最新一根 K 线属于"当前正在交易"的 bar 才报警。
# 休市/周末取到的是历史 bar, 返回 False —— 避免把历史信号当"实时"报警。
_INTRADAY_SECS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "60m": 3600}
_LIVE_INTRADAY_TOL = 180  # 秒: 容忍网络/合成延迟, 边界在过去180s内仍算实时


def _is_live_bar(period, last_dt_ns):
    """判断最新一根 K 线 datetime(UTC 纳秒) 是否为实时/当前交易 bar。

    - 分钟周期(1m~60m): forming bar 的结束边界在未来 => bar_end >= now-TOL 即实时。
    - 日/周/月/季: 最新 bar 所属"期"等于当前期 => 实时(周末/休市自然不等)。
    无法判断时保守返回 True(避免误吞真实信号)。
    """
    try:
        now = _dt.datetime.now(_dt.timezone(_dt.timedelta(hours=8)))
        if last_dt_ns is None:
            return False
        dt_bj = pd.to_datetime(int(last_dt_ns), unit="ns", utc=True).tz_convert("Asia/Shanghai")
        if period in _INTRADAY_SECS:
            bar_end = dt_bj + _dt.timedelta(seconds=_INTRADAY_SECS[period])
            return bar_end >= (now - _dt.timedelta(seconds=_LIVE_INTRADAY_TOL))
        if period == "day":
            return dt_bj.date() == now.date()
        if period == "week":
            return dt_bj.isocalendar()[:2] == now.isocalendar()[:2]
        if period == "month":
            return (dt_bj.year, dt_bj.month) == (now.year, now.month)
        if period == "quarter":
            return (dt_bj.year, (dt_bj.month - 1) // 3) == (now.year, (now.month - 1) // 3)
    except Exception:
        return True
    return True


def _get_rule_param(rule, key, default):
    try:
        v = rule.get("params", {}).get(key)
        if v is None:
            return default
        if isinstance(default, bool):
            return bool(v)
        if isinstance(default, (int, float)):
            fv = float(v)
            if fv != fv:  # NaN(如前端传空字符串)
                return default
            # 默认值是 int 但用户传了 float(如 0.5)时, 不要被 int() 截断
            if isinstance(default, int) and fv == int(fv):
                return int(fv)
            return fv
        return v
    except Exception:
        return default


def _price_trend(close_list, n=60, lookback=10):
    """用慢速均线(MA n)在较长窗口(lookback 根)上的整体斜率判定趋势: 1=上升, -1=下降, 0=平坦/数据不足。

    需求人定义(2026-08-28 修正): 金叉=交叉点落在"整体上升的趋势线"上, 死叉=落在"下降趋势线"上;
    且强调是"整体上升"中的交叉, 而非交叉那几根 K 线短均线恰好上涨。
    故用慢速均线(默认 MA60)在较长窗口(默认 10 根)上取斜率代表"整体趋势",
    局部回调/反弹不会翻转整体方向——整体向上途中短暂走平仍判为上升。
    """
    if not close_list or len(close_list) < n + lookback - 1:
        return 0
    try:
        s = pd.Series(close_list, dtype="float64").rolling(n, min_periods=n).mean()
        vals = s.dropna()
        if len(vals) < lookback:
            return 0
        recent = vals.tail(lookback).values
        slope = float(recent[-1]) - float(recent[0])
        if slope > 1e-9:
            return 1
        if slope < -1e-9:
            return -1
        return 0
    except Exception:
        return 0


def _price_trend_series(close_list, n=60):
    """计算慢速均线序列(MA n), 长度与 close_list 一致, 前 n-1 项为 NaN。用于按交叉点 idx 取局部趋势。"""
    if not close_list or len(close_list) < n:
        return None
    try:
        return pd.Series(close_list, dtype="float64").rolling(n, min_periods=n).mean()
    except Exception:
        return None


def _trend_at_ma(ma_series, idx, lookback=30, dif_series=None, dea_series=None):
    """在交叉点 idx 处判趋势: 1=上升,-1=下降,0=平坦/不足。

    2026-09-01 修正(用户反馈"看两根线的趋势"): 以 **MACD 双线(DIF/DEA)趋势** 为主判据,
    价格 MA60 斜率作为双线平坦时的兜底。
    - 用户视觉里"金叉/死叉"看的是 DIF/DEA 两根线的整体方向, 而非价格均线;
      故 DIF/DEA 在窗口内明显下行 -> 即使价格局部反弹, 仍判下降趋势(死叉方向)。
    - 显著性阈值: 斜率需超过基准值的极小比例(默认万分之二), 过滤横盘/微动造成的伪方向。
    - lookback 默认30(原10太短, 局部横盘易翻转方向)。
    """
    # 1) MACD 双线趋势(主判据)
    macd_dir = 0
    if dif_series is not None and idx is not None:
        try:
            lo = idx - lookback + 1
            if lo >= 0 and idx < len(dif_series):
                d0, d1 = _safe_float(dif_series[lo]), _safe_float(dif_series[idx])
                if d0 is not None and d1 is not None:
                    dslope = d1 - d0
                    dsig = abs(d0) * 2e-4 + 1e-9
                    if dslope > dsig:
                        macd_dir = 1
                    elif dslope < -dsig:
                        macd_dir = -1
        except Exception:
            pass
    if macd_dir != 0:
        return macd_dir
    # 2) 价格 MA60 趋势(兜底, 双线平坦时)
    if ma_series is None or idx is None:
        return 0
    try:
        if idx < lookback:
            return 0
        if pd.isna(ma_series.iloc[idx]):
            return 0
        seg = ma_series.iloc[idx - lookback + 1: idx + 1].dropna()
        if len(seg) < 2:
            return 0
        base = float(seg.iloc[0])
        slope = float(seg.iloc[-1]) - base
        sig = abs(base) * 2e-4 + 1e-9
        if slope > sig:
            return 1
        if slope < -sig:
            return -1
        return 0
    except Exception:
        return 0


def _detect_macd_cross(df, ind, rule):
    """检测 MACD 金叉/死叉，并应用距零线(单侧)过滤 + 价格趋势(MA)方向过滤。

    zero_line: 零线位置(默认0, 高价合约可设为典型 DIF 以锚定真实波动区);
    threshold: 比较阈值; op: 比较算子(gt/ge/lt/le)。
    过滤逻辑: 仅保留 (DIF - zero_line) OP threshold 的交叉;
    threshold 为 None/空 = 不过滤(显示全部交叉)。

    trend_ma_n / trend_lookback: 用慢速均线(默认 MA60)在较长窗口(默认10根)的斜率判趋势,
        在【每个交叉点的 idx】处单独评估(即"交叉前后节点的趋势方向"), 代表"整体趋势", 局部回调不翻转方向。
    trend_mode: 交叉与趋势的判定模式
        - "trend"(默认, 需求人2026-08-28): 趋势驱动 —— 在交叉点 idx 处, 若前后节点呈上升趋势, 任一交叉(上穿或下穿)都算金叉;
          若呈下降趋势, 任一交叉都算死叉; 趋势平坦不报。
        - "match"(旧逻辑): 方向匹配 —— 仅 上穿+上升 算金叉, 下穿+下降 算死叉。

    支持规则级指标参数: rule.indicator_params = {"fast":12, "slow":26, "signal":9}
    当指标参数与默认(12,26,9)不同, 或缺省时, 使用预计算的 ind; 否则按规则重算 MACD。
    """
    side = rule.get("side", "long")
    zero_line = _get_rule_param(rule, "zero_line", 0.0)
    threshold = _get_rule_param(rule, "threshold", None)  # None = 不过滤
    op = _get_rule_param(rule, "op", "gt")
    recent_bars = _get_rule_param(rule, "recent_bars", 3)
    cmp_fn = _OPS.get(op, _OPS["gt"])
    op_lbl = _OP_LABEL.get(op, op)

    # 趋势过滤: 在【每个交叉点 idx】处, 用慢速均线(MA n, 默认60)在窗口(lookback 根)上的斜率判趋势,
    # 即"交叉前后节点的趋势方向"决定金叉/死叉。
    # (需求人2026-08-28: 长短线交叉 + 前后上升趋势=金叉 / 前后下降趋势=死叉; 慢速均线代表整体趋势, 局部回调不翻转)
    trend_ma_n = _get_rule_param(rule, "trend_ma_n", 60)
    trend_lookback = _get_rule_param(rule, "trend_lookback", 30)
    # 自适应趋势均线(2026-08-31 增强): 历史不足 trend_ma_n(默认60) 但 >= MIN_TREND_BARS(默认20) 时,
    # 退化为更短的 MA(取 max(MIN_TREND_BARS, len//2)), 让"新合约/开盘早期(<60根)"也能尽早给出趋势线,
    # 而不是等满 60 根才出信号; 不足 MIN_TREND_BARS 仍走 _price_trend_series 的 None 分支(数据不足不瞎报)。
    # 注: 正常合约滚动窗口(扫描取 300 根)已天然跨交易日衔接, 此处只补"短历史"这一种缺口; end_dt 翻页对
    # 该缺口无效(无更早历史可翻), 故不在此处加翻页。
    MIN_TREND_BARS = 20
    if len(df) >= trend_ma_n:
        eff_ma_n = trend_ma_n
    elif len(df) >= MIN_TREND_BARS:
        eff_ma_n = max(MIN_TREND_BARS, len(df) // 2)
    else:
        eff_ma_n = trend_ma_n  # 不足 MIN_TREND_BARS -> _price_trend_series 返回 None
    ma_series = _price_trend_series(list(df["close"]), n=eff_ma_n)

    # 指标参数: 默认 MACD(12,26,9)
    ip = rule.get("indicator_params") or {}
    fast = _get_rule_param({"params": ip}, "fast", 12)
    slow = _get_rule_param({"params": ip}, "slow", 26)
    signal = _get_rule_param({"params": ip}, "signal", 9)

    # 如果规则显式指定了非默认参数, 则重新计算 MACD; 否则用预计算指标提升速度
    dif_vals = ind.get("macdDif", [])
    dea_vals = ind.get("macdDea", [])
    if fast != 12 or slow != 26 or signal != 9:
        try:
            dif_vals, dea_vals, _ = indicators.macd(list(df["close"]), fast=fast, slow=slow, signal=signal)
        except Exception:
            dif_vals, dea_vals = ind.get("macdDif", []), ind.get("macdDea", [])

    dts_scan = [int(x) for x in df["datetime"]] if "datetime" in df.columns else None
    crosses = ta_patterns.detect_macd_cross(dif_vals, dea_vals, list(df["close"]), datetimes=dts_scan)
    out = []
    n = len(df)
    for c in crosses:
        idx = c.get("idx")
        if idx is None or idx < 0 or idx >= n:
            continue
        # 只取最近 N 根内发生的交叉
        if idx < n - recent_bars:
            continue
        # 距零线过滤用的取值: 后一根 bar 的 DIF(长短线交汇的后一节点),
        # 取代公式精确交叉点的 DIF≈DEA 值(需求人2026-08-31: 不需要精确交汇点)
        dif_val = _safe_float(c.get("dif"))
        dea_val = _safe_float(c.get("dea"))
        if dif_val is None:
            continue
        # 持续性过滤(2026-10-15 实锤修复): 不活跃合约(如 lu2611)天勤 1m 收盘合成错位 ->
        # DIF/DEA 在零线附近"立刻反向"抖动, 制造出文华没有的假微交叉(柱宽极小的伪金叉/死叉)。
        # persist=k 要求交叉之后 k 根内 DIF-DEA 保持同向(金叉需持续>0 / 死叉需持续<0),
        # 立刻回穿的抖动视为噪声不报。该判据与价格/柱宽绝对值无关 -> 活跃合约(如 sc2611,
        # 价格仅~700、真实小交叉柱宽常<0.15)的真实交叉全部保留, 不受影响。
        # (经验证: lu2611 14:52/14:54/14:55 三个假交叉全部被干掉, 14:57 真金叉保留;
        #  sc2611 全部 6 个真实交叉无一误杀)。persist=0 表示关闭(默认旧行为)。
        persist = _get_rule_param(rule, "persist", 0)
        if persist and persist > 0:
            _ctype = c.get("type")
            _bad = False
            for _k in range(1, int(persist) + 1):
                _j = idx + _k
                if _j >= n or _j >= len(dif_vals) or _j >= len(dea_vals):
                    break  # 末端不足未来根 -> 保守保留(下一拍复判)
                _gk = float(dif_vals[_j]) - float(dea_vals[_j])
                if _ctype == "golden" and _gk <= 1e-9:
                    _bad = True; break
                if _ctype == "death" and _gk >= -1e-9:
                    _bad = True; break
            if _bad:
                continue
        # 在交叉点 idx 处判趋势: 主判据=MACD 双线(DIF/DEA)整体方向, 兜底=价格 MA60
        # (2026-09-01 修正: 用户视觉以 MACD 两根线趋势为准, 不再只看价格均线)
        trend = _trend_at_ma(ma_series, idx, trend_lookback, dif_series=dif_vals)
        trend_lbl = {1: "上升", -1: "下降", 0: "平坦"}.get(trend, "平坦")
        # 距零线(单侧)过滤: threshold 为 None 表示不过滤
        if threshold is not None and not cmp_fn(dif_val - zero_line, threshold):
            continue
        ctype = c.get("type")
        cross_lbl = "上穿" if ctype == "golden" else "下穿"
        trend_mode = _get_rule_param(rule, "trend_mode", "trend")
        # 趋势过滤开关(2026-10-15 用户裁定"金叉就是金叉, 按标准来"): 默认关。
        # 关 -> 标准定义: DIF 上穿 DEA=金叉 / 下穿=死叉, 不看趋势。
        # 开 -> 附加趋势过滤: 上穿还须上升趋势、下穿还须下降趋势, 否则不报(属额外条件, 非定义)。
        _trend_filter = bool(_get_rule_param(rule, "trend_filter", False))
        # 整数节点(趋势判断/去重兼容)
        _precise = {"idx": int(idx), "price": c.get("price"),
                    "dif": dif_val, "dea": dea_val}

        if trend_mode == "trend":
            # 标准定义(金叉/死叉由 DIF-DEA 交叉唯一决定, 2026-10-15 用户裁定"按标准来"):
            #   金叉 = DIF 由下向上突破 DEA
            #   死叉 = DIF 由上向下突破 DEA
            # 趋势(慢速均线方向)只是【辅助过滤条件】, 属设置项, 不改变金叉死叉本身:
            #   trend_filter=on  -> 额外要求趋势方向与交叉方向一致(上穿需上升/下穿需下降),
            #                       趋势不符则不报(既不是金叉也不是死叉)
            #   trend_filter=off -> 不看趋势, 只要标准交叉成立就报(默认, 符合标准定义)
            if ctype == "golden" and side == "long":
                if _trend_filter and trend != 1:
                    continue                      # 趋势过滤: 上穿但非上升趋势 -> 不报
                out.append({**_precise,
                    "rule_id": rule.get("id"),
                    "type": "macd_golden",
                    "name": rule.get("name", "MACD金叉"),
                    "side": "long",
                    "value": dif_val,
                    "threshold": threshold,
                    "op": op,
                    "zero_line": zero_line,
                    "trend": trend_lbl,
                    "indicator_params": {"fast": fast, "slow": slow, "signal": signal},
                    "desc": f"MACD({fast},{slow},{signal}) DIF{cross_lbl}DEA(金叉), DIF={dif_val:.4f} DEA={dea_val:.4f}, (DIF-零线){dif_val - zero_line:.4f} {op_lbl} {threshold}, 趋势:{trend_lbl}(MA{eff_ma_n})",
                })
            elif ctype == "death" and side == "short":
                if _trend_filter and trend != -1:
                    continue                      # 趋势过滤: 下穿但非下降趋势 -> 不报
                out.append({**_precise,
                    "rule_id": rule.get("id"),
                    "type": "macd_death",
                    "name": rule.get("name", "MACD死叉"),
                    "side": "short",
                    "value": dif_val,
                    "threshold": threshold,
                    "op": op,
                    "zero_line": zero_line,
                    "trend": trend_lbl,
                    "indicator_params": {"fast": fast, "slow": slow, "signal": signal},
                    "desc": f"MACD({fast},{slow},{signal}) DIF{cross_lbl}DEA(死叉), DIF={dif_val:.4f} DEA={dea_val:.4f}, (DIF-零线){dif_val - zero_line:.4f} {op_lbl} {threshold}, 趋势:{trend_lbl}(MA{eff_ma_n})",
                })
            # 非本 side 的交叉(如金叉规则遇到下穿)不报
            continue
        else:
            # 方向匹配(旧逻辑): 金叉需上穿+上升, 死叉需下穿+下降
            if ctype == "golden" and side == "long":
                if trend != 1:
                    continue  # 非上升趋势中的上穿不算金叉(如下跌途中的反弹)
                out.append({**_precise,
                    "rule_id": rule.get("id"),
                    "type": "macd_golden",
                    "name": rule.get("name", "MACD金叉"),
                    "side": "long",
                    "value": dif_val,
                    "threshold": threshold,
                    "op": op,
                    "zero_line": zero_line,
                    "trend": trend_lbl,
                    "indicator_params": {"fast": fast, "slow": slow, "signal": signal},
                    "desc": f"MACD({fast},{slow},{signal}) DIF{cross_lbl}DEA, DIF={dif_val:.4f}, (DIF-零线){dif_val - zero_line:.4f} {op_lbl} {threshold}, 趋势:{trend_lbl}(MA{eff_ma_n})",
                })
            elif ctype == "death" and side == "short":
                if trend != -1:
                    continue  # 非下降趋势中的下穿不算死叉(如上涨途中的回调)
                out.append({**_precise,
                    "rule_id": rule.get("id"),
                    "type": "macd_death",
                    "name": rule.get("name", "MACD死叉"),
                    "side": "short",
                    "value": dif_val,
                    "threshold": threshold,
                    "op": op,
                    "zero_line": zero_line,
                    "trend": trend_lbl,
                    "indicator_params": {"fast": fast, "slow": slow, "signal": signal},
                    "desc": f"MACD({fast},{slow},{signal}) DIF{cross_lbl}DEA, DIF={dif_val:.4f}, (DIF-零线){dif_val - zero_line:.4f} {op_lbl} {threshold}, 趋势:{trend_lbl}(MA{eff_ma_n})",
                })
    return out


def _detect_divergence(df, ind, rule):
    """检测顶背离/底背离，取第二枢轴在最近 N 根内的。"""
    side = rule.get("side", "short")
    recent_bars = _get_rule_param(rule, "recent_bars", 10)
    try:
        analysis = ta_patterns.analyze(df, ind, order=3, lookback=150)
    except Exception:
        return []
    divs = analysis.get("divergences", [])
    out = []
    n = len(df)
    for d in divs:
        x2 = int(d.get("x2", -1))
        if x2 < 0 or x2 >= n or x2 < n - recent_bars:
            continue
        dtype = d.get("type")
        if dtype == "top" and side == "short":
            out.append({
                "rule_id": rule.get("id"),
                "type": "top_divergence",
                "name": rule.get("name", "顶背离"),
                "side": "short",
                "value": _safe_float(d.get("i2")),
                "threshold": None,
                "desc": f"价格:{d.get('p1')}→{d.get('p2')}, MACD:{d.get('i1')}→{d.get('i2')}",
                "idx": x2,
            })
        elif dtype == "bottom" and side == "long":
            out.append({
                "rule_id": rule.get("id"),
                "type": "bottom_divergence",
                "name": rule.get("name", "底背离"),
                "side": "long",
                "value": _safe_float(d.get("i2")),
                "threshold": None,
                "desc": f"价格:{d.get('p1')}→{d.get('p2')}, MACD:{d.get('i1')}→{d.get('i2')}",
                "idx": x2,
            })
    return out


def _detect_rsi(df, ind, rule):
    """RSI 超买/超卖。"""
    side = rule.get("side", "long")
    n = _get_rule_param(rule, "n", 14)
    threshold = _get_rule_param(rule, "threshold", 30 if side == "long" else 70)
    key = f"rsi{n}"
    if key not in ind:
        # fallback: 用 rsi6/rsi12 都可以
        key = "rsi6" if "rsi6" in ind else ("rsi12" if "rsi12" in ind else None)
    if not key:
        return []
    arr = ind[key]
    if len(arr) < 2:
        return []
    val = _safe_float(arr[-1])
    if val is None:
        return []
    if side == "long" and val <= threshold:
        return [{
            "rule_id": rule.get("id"),
            "type": "rsi_oversold",
            "name": rule.get("name", "RSI超卖"),
            "side": "long",
            "value": val,
            "threshold": threshold,
            "desc": f"RSI({n})={val:.2f} <= {threshold}",
            "idx": len(df) - 1,
        }]
    if side == "short" and val >= threshold:
        return [{
            "rule_id": rule.get("id"),
            "type": "rsi_overbought",
            "name": rule.get("name", "RSI超买"),
            "side": "short",
            "value": val,
            "threshold": threshold,
            "desc": f"RSI({n})={val:.2f} >= {threshold}",
            "idx": len(df) - 1,
        }]
    return []


def _detect_kdj(df, ind, rule):
    """KDJ 超买/超卖。"""
    side = rule.get("side", "long")
    threshold = _get_rule_param(rule, "threshold", 20 if side == "long" else 80)
    k = _safe_float(ind.get("kdjK", [None])[-1])
    d = _safe_float(ind.get("kdjD", [None])[-1])
    j = _safe_float(ind.get("kdjJ", [None])[-1])
    if k is None:
        return []
    if side == "long" and k <= threshold:
        return [{
            "rule_id": rule.get("id"),
            "type": "kdj_oversold",
            "name": rule.get("name", "KDJ超卖"),
            "side": "long",
            "value": k,
            "threshold": threshold,
            "desc": f"K={k:.2f}, D={_fmt(d)}, J={_fmt(j)} 均低位",
            "idx": len(df) - 1,
        }]
    if side == "short" and k >= threshold:
        return [{
            "rule_id": rule.get("id"),
            "type": "kdj_overbought",
            "name": rule.get("name", "KDJ超买"),
            "side": "short",
            "value": k,
            "threshold": threshold,
            "desc": f"K={k:.2f}, D={_fmt(d)}, J={_fmt(j)} 均高位",
            "idx": len(df) - 1,
        }]
    return []


def _fmt(v):
    return f"{v:.2f}" if v is not None else "-"


def _detect_boll(df, ind, rule):
    """价格触及 BOLL 上轨/下轨。"""
    side = rule.get("side", "long")
    if len(df) < 2:
        return []
    close = _safe_float(df["close"].iloc[-1])
    if side == "long":
        lower = _safe_float(ind.get("bollLower", [None])[-1])
        if close is not None and lower is not None and close <= lower:
            return [{
                "rule_id": rule.get("id"),
                "type": "boll_lower",
                "name": rule.get("name", "BOLL下轨"),
                "side": "long",
                "value": close,
                "threshold": lower,
                "desc": f"收盘价{close:.4f} 触及下轨 {lower:.4f}",
                "idx": len(df) - 1,
            }]
    if side == "short":
        upper = _safe_float(ind.get("bollUpper", [None])[-1])
        if close is not None and upper is not None and close >= upper:
            return [{
                "rule_id": rule.get("id"),
                "type": "boll_upper",
                "name": rule.get("name", "BOLL上轨"),
                "side": "short",
                "value": close,
                "threshold": upper,
                "desc": f"收盘价{close:.4f} 触及上轨 {upper:.4f}",
                "idx": len(df) - 1,
            }]
    return []


def _detect_cci(df, ind, rule):
    """CCI 超买/超卖。"""
    side = rule.get("side", "long")
    threshold = _get_rule_param(rule, "threshold", -100 if side == "long" else 100)
    val = _safe_float(ind.get("cci14", [None])[-1])
    if val is None:
        return []
    if side == "long" and val <= threshold:
        return [{
            "rule_id": rule.get("id"),
            "type": "cci_oversold",
            "name": rule.get("name", "CCI超卖"),
            "side": "long",
            "value": val,
            "threshold": threshold,
            "desc": f"CCI={val:.2f} <= {threshold}",
            "idx": len(df) - 1,
        }]
    if side == "short" and val >= threshold:
        return [{
            "rule_id": rule.get("id"),
            "type": "cci_overbought",
            "name": rule.get("name", "CCI超买"),
            "side": "short",
            "value": val,
            "threshold": threshold,
            "desc": f"CCI={val:.2f} >= {threshold}",
            "idx": len(df) - 1,
        }]
    return []


def _detect_vol_ratio(df, ind, rule):
    """量比超过阈值。"""
    threshold = _get_rule_param(rule, "threshold", 2.0)
    val = _safe_float(ind.get("volRatio", [None])[-1])
    if val is not None and val >= threshold:
        return [{
            "rule_id": rule.get("id"),
            "type": "vol_ratio",
            "name": rule.get("name", "量比异动"),
            "side": "both",
            "value": val,
            "threshold": threshold,
            "desc": f"量比={val:.2f} >= {threshold}",
            "idx": len(df) - 1,
        }]
    return []


def _detect_vroc(df, ind, rule):
    """量变动速率超过阈值。"""
    threshold = _get_rule_param(rule, "threshold", 50.0)
    val = _safe_float(ind.get("vroc", [None])[-1])
    if val is not None and abs(val) >= threshold:
        return [{
            "rule_id": rule.get("id"),
            "type": "vroc",
            "name": rule.get("name", "量变动速率"),
            "side": "both",
            "value": val,
            "threshold": threshold,
            "desc": f"VROC={val:.2f}% 绝对值 >= {threshold}",
            "idx": len(df) - 1,
        }]
    return []


def _detect_obv(df, ind, rule):
    """OBV 创近 N 根新高。"""
    lookback = _get_rule_param(rule, "lookback", 20)
    arr = ind.get("obv", [])
    if len(arr) < lookback + 1:
        return []
    recent = arr[-lookback:]
    cur = _safe_float(arr[-1])
    if cur is None:
        return []
    # 近 lookback 根最大值
    try:
        mx = max([x for x in recent if x is not None])
    except ValueError:
        return []
    if cur >= mx:
        return [{
            "rule_id": rule.get("id"),
            "type": "obv_high",
            "name": rule.get("name", "OBV新高"),
            "side": "long",
            "value": cur,
            "threshold": None,
            "desc": f"OBV={cur:.0f} 创近{lookback}根新高",
            "idx": len(df) - 1,
        }]
    return []


def _detect_wvad(df, ind, rule):
    """WVAD 由负转正。"""
    arr = ind.get("wvad", [])
    if len(arr) < 2:
        return []
    prev = _safe_float(arr[-2])
    cur = _safe_float(arr[-1])
    if prev is not None and cur is not None and prev <= 0 and cur > 0:
        return [{
            "rule_id": rule.get("id"),
            "type": "wvad_positive",
            "name": rule.get("name", "WVAD转正"),
            "side": "long",
            "value": cur,
            "threshold": 0,
            "desc": f"WVAD 由{prev:.2f} 转正为 {cur:.2f}",
            "idx": len(df) - 1,
        }]
    return []


_DISPATCH = {
    "macd_cross": _detect_macd_cross,
    "divergence": _detect_divergence,
    "rsi": _detect_rsi,
    "kdj": _detect_kdj,
    "boll": _detect_boll,
    "cci": _detect_cci,
    "vol_ratio": _detect_vol_ratio,
    "vroc": _detect_vroc,
    "obv": _detect_obv,
    "wvad": _detect_wvad,
}


def detect_signals(df, ind, rules):
    """对单个合约 DataFrame 应用规则，返回信号列表。"""
    signals = []
    for rule in rules:
        if not rule.get("enabled", True):
            continue
        fn = _DISPATCH.get(rule.get("type"))
        if not fn:
            continue
        try:
            sigs = fn(df, ind, rule)
            if sigs:
                signals.extend(sigs)
        except Exception:
            pass
    # [2026-10-09] 无成交过滤: 触发bar成交量为0的信号一律丢弃。
    # 根因(动力煤ZC/普麦PM等僵尸品种实锤): 无成交时天勤仍生成K线(价格=挂单价一字线),
    # BOLL 20周期 std=0 -> 上轨=下轨=中轨=收盘价, 同一根bar上/下轨同时假触发;
    # OBV恒0也"创20根新高"。按信号自带 idx 查触发bar成交量, 实时(idx=末根)与补扫/回测(历史bar)两路统一覆盖。
    try:
        if "volume" in df.columns and signals:
            _vol = df["volume"].astype(float).reset_index(drop=True)
            _n = len(_vol)
            _kept = []
            for _s in signals:
                _i = _s.get("idx")
                _v = _vol.iloc[_i] if _i is not None and 0 <= _i < _n else 0.0
                if _v == _v and _v > 0:   # NaN(_v!=_v) 视为无成交, 一并丢弃
                    _kept.append(_s)
            signals = _kept
    except Exception:
        pass
    return signals


def _fmt_cn_time(ns):
    """将天勤纳秒 UTC 时间戳格式化为北京时间字符串。"""
    try:
        return pd.to_datetime(int(ns), unit="ns", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M")
    except Exception:
        try:
            return pd.to_datetime(int(ns), unit="ns").strftime("%Y-%m-%d %H:%M")
        except Exception:
            return None


def _symbol_display_name(symbol, real_symbol):
    """合约代码 -> 中文可读名(如 SHFE.au2610 -> 沪金2610; KQ.m@CZCE.MA -> 甲醇主连610)。"""
    try:
        from services import futures_db
        # 主连合约：显示「品种主连」+当前主力映射月份。
        # 2026-09-24 用户裁定(推翻 09-22"主连不带月份"旧规): 主连要能看到当前是哪个月份的合约
        # (如「甲醇主连610」「沪金主连2610」), 否则换月时看不出基础价属于哪个月份。
        # 月份取 real_symbol(主连底层真实合约, _get_kline_obj 返回, 如 SHFE.rb2610 / CZCE.MA610)
        # 尾部月份串, 原样透传(CZCE 3 位 / 其它 4 位); 拿不到 real 时退纯「品种主连」。
        if symbol and symbol.startswith("KQ.m@") and "@" in symbol and "." in symbol:
            rest = symbol.split("@", 1)[1]  # CZCE.MA
            ex, pc = rest.split(".", 1)
            nm = futures_db._prod_name_lookup(ex, pc)
            base = (nm + "主连") if nm else (rest + "主连")
            rs = real_symbol or ""
            tail = ""
            if rs and "." in rs:
                code = rs.split(".")[-1]
                if code.startswith(pc):
                    tail = code[len(pc):]
                else:
                    tail = "".join(ch for ch in code if ch.isdigit())
            return base + (tail or "")
        name = futures_db.option_underlying_name(real_symbol) if real_symbol else None
        if name and name != real_symbol:
            return name
        name = futures_db.option_underlying_name(symbol)
        if name and name != symbol:
            return name
    except Exception:
        pass
    return real_symbol or symbol


def _scan_one(symbol, slot=None, period="day", rules=None, count=300, live_only=False,
              df=None, quote=None, real=None, symbol_rules=None, symbol_tpl=None):
    """扫描单个合约。返回 (symbol, signals) 或 None。
    live_only=True 时: 仅当最新 bar 为实时交易 bar 才产出信号; 休市/周末返回 no_live 标记。"""
    from services import futures_kline as fk
    # 2026-09-29: 技术分析页活跃时让路(与补扫同机制)。py-spy 实锤全市场扫描 detect_signals/
    # _detect_macd_cross 烧满单核 GIL -> 技术分析报价延迟 3-4s; 每个标的扫前让 10s, 慢速推进不饿死。
    try:
        fk._tech_yield()
    except Exception:
        pass
    rules = rules or DEFAULT_RULES
    # 逐标的规则: 若传入 symbol_rules 且命中该标的, 优先用该标的的"最严合并集"(见 scan_template_db)
    if symbol_rules:
        _sr = symbol_rules.get(symbol)
        _sym_rules = None
        if isinstance(_sr, dict):
            _sym_rules = _sr.get("rules")
        elif isinstance(_sr, list):
            _sym_rules = _sr
        if _sym_rules:
            rules = _sym_rules
    try:
        cfg = fk.PERIODS.get(period, fk.PERIODS["day"])
        duration = cfg["duration"]
        if df is not None:
            # 2026-09-24 补扫路径: 外部(独立会话线程)已取好 df 直接用 —— 零占用实时槽的
            # 订阅预算(缓存 key 含 count, 补扫 400 != 实时 300, 走共享槽会新建 ~540 个订阅挤爆会话)
            real = real or symbol
        elif cfg["freq"]:
            df, real = fk._resample_daily(slot, symbol, cfg["freq"], count)
        else:
            df, real = fk._fetch_kline_df(slot, symbol, duration, count)
        if df is None or df.empty or len(df) < 45:
            # 2026-10-12: 门槛 30->45。MACD(12,26,9) 需 >=34 根才出值, 34-44 根的
            # 短序列(部分填充)必为种子 EMA 垃圾数据(PVC 假金叉事故); 45 根以上才开算。
            return None
        # 实时模式: 先截断未来 bar(天勤对远月合约可能返回未来挂牌占位数据), 避免触发时间显示未来日期
        if live_only and "datetime" in df.columns:
            now_ns = int(_dt.datetime.now(_dt.timezone(_dt.timedelta(hours=8))).timestamp() * 1e9)
            df = df[df["datetime"].astype("int64") <= now_ns].reset_index(drop=True)
            if df.empty or len(df) < 30:
                return None
        # 实时模式: 判定最新 bar 是否为实时交易 bar
        if live_only:
            last_ns = int(df["datetime"].iloc[-1]) if "datetime" in df.columns else None
            if not _is_live_bar(period, last_ns):
                return {
                    "symbol": symbol, "real_symbol": real, "signals": [],
                    "live_data": False, "no_live": True,
                    "live_note": "休市/无实时行情（实时模式仅监控当前 bar，不扫描历史）",
                }
        df = df.copy()
        ind = indicators.compute_all(df)
        signals = detect_signals(df, ind, rules)
        # 给每个信号打"模板来源"标签(落在 signal_json 中, 无需改表):
        # 该标的命中的全部模板(最严合并集=这些模板规则并集)都可能是触发方, 故标全部。
        _tpl_ids, _tpl_names = [], []
        if symbol_rules:
            _sr = symbol_rules.get(symbol)
            _tps = None
            if isinstance(_sr, dict):
                _tps = _sr.get("templates") or []
            if symbol_tpl is not None:
                _st = symbol_tpl.get(symbol)
                if isinstance(_st, dict):
                    _tps = _st.get("templates") or []
                elif isinstance(_st, list):
                    _tps = _st
            for _t in (_tps or []):
                if not isinstance(_t, dict):
                    continue
                if _t.get("id") is not None:
                    _tpl_ids.append(_t["id"])
                if _t.get("name"):
                    _tpl_names.append(_t["name"])
        if (_tpl_ids or _tpl_names) and signals:
            for s in signals:
                s["template_ids"] = _tpl_ids
                s["template_names"] = _tpl_names
        # 实时模式: 只保留最新一根 bar(idx == n-1) 的信号, 这才是"当下"触发
        if live_only and signals:
            n = len(df)
            signals = [s for s in signals if s.get("idx") == n - 1]
        if not signals:
            return None
        # 给每个信号补充触发时间:
        #  - 信号若带有 cross_time_ns(如精确交叉时间)则优先使用; 2026-08-31 起 MACD 金叉/死叉已回退为
        #    标在后一根 bar(长短线交汇的后一节点), 不再产出 cross_time_ns, 故统一回退到信号所在 bar 的 datetime(idx);
        #  - 其余信号(背离等)同样取信号所在 bar 的 datetime(整数 idx)
        if "datetime" in df.columns:
            for s in signals:
                tns = s.get("cross_time_ns")
                if tns is not None:
                    s["trigger_time"] = _fmt_cn_time(tns)
                    s["trigger_time_ns"] = int(tns)
                else:
                    idx = s.get("idx")
                    if idx is not None and 0 <= idx < len(df):
                        s["trigger_time"] = _fmt_cn_time(df["datetime"].iloc[idx])
                        # 纳秒时间戳: 供本盘累积视图精确排序(同一分钟内多条也能定序)
                        s["trigger_time_ns"] = int(df["datetime"].iloc[idx])
                    else:
                        s["trigger_time"] = None
        # 携带当前扫描整体时间戳, 供历史复盘
        scan_now = datetime.now()
        for s in signals:
            s["scan_time"] = scan_now
        close_val = _safe_float(df["close"].iloc[-1])
        # 涨跌按期货惯例基于昨结(pre_settlement)而非窗口第一根 open;
        # 先尝试取实时报价的 pre_settlement, 取不到再回退到窗口第一根 open。
        pre_settlement = None
        try:
            q = slot.client.get_quote(symbol)
            pre_settlement = _safe_float(getattr(q, "pre_settlement", None))
        except Exception:
            pass
        if (pre_settlement is None or pre_settlement <= 0) and quote is not None:
            # 补扫路径(独立会话): quote 里带昨结, 回退取之 —— 保证涨跌幅口径与实时一致
            # (否则回退到"窗口第一根 open", 500 根窗口的首根 open != 昨结, 涨跌幅全错)
            pre_settlement = _safe_float(getattr(quote, "pre_settlement", None))
        if pre_settlement is None or pre_settlement <= 0:
            pre_settlement = _safe_float(df["open"].iloc[0]) if len(df) > 0 else None
        change = None
        change_pct = None
        if close_val is not None and pre_settlement and pre_settlement > 0:
            change = round(close_val - pre_settlement, 4)
            change_pct = round((change / pre_settlement) * 100, 2)
        # [2026-10-16] 行级 MA60/MA20: 与"最新价"同取窗口最后一根, 供列表涨跌幅旁渲染均线标签
        closes_all = list(df["close"])
        ma60 = _ma_last(closes_all, 60)
        ma20 = _ma_last(closes_all, 20)
        return {
            "symbol": symbol,
            "real_symbol": real,
            "display_name": _symbol_display_name(symbol, real),
            "signals": signals,
            "live_data": True,
            "close": close_val,
            "change": change,
            "change_pct": change_pct,
            "ma60": ma60,
            "ma20": ma20,
            "datetime": int(df["datetime"].iloc[-1]) if "datetime" in df.columns else None,
        }
    except Exception as e:
        print(f"[扫描] {symbol} 失败: {e}")
        return None


def scan_symbols(symbols, slot, period="day", rules=None, max_workers=8, per_timeout=15, executor=None, live_only=False, count=300, symbol_rules=None, symbol_tpl=None):
    """批量扫描合约，返回多头/空头结果。
    live_only=True: 实时模式, 仅含实时 bar 的合约产出信号, 休市合约标记 no_live。
    count: 各合约取多少根 K 线(补扫开盘以来整段时用较大值以覆盖整个盘口)。
    executor: 可传入外部 ThreadPoolExecutor(如监控器的全局池); 为 None 时自建并在结束时关闭。"""
    rules = rules or DEFAULT_RULES
    start = time.time()
    long_results = []
    short_results = []
    scanned = 0
    failed = 0
    any_live = False
    no_live_symbols = 0

    own = executor is None
    exe = executor if executor is not None else ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {exe.submit(_scan_one, s, slot, period, rules, count, live_only, None, None, symbol_rules, symbol_tpl): s for s in symbols}
        for fut in as_completed(futures):
            scanned += 1
            try:
                res = fut.result(timeout=per_timeout)
                if not res:
                    continue
                # 实时模式: 休市合约不计入信号, 仅统计
                if res.get("no_live"):
                    no_live_symbols += 1
                    continue
                if res.get("live_data"):
                    any_live = True
                # 按信号 side 归类
                long_sigs = [s for s in res["signals"] if s.get("side") == "long"]
                short_sigs = [s for s in res["signals"] if s.get("side") == "short"]
                if long_sigs:
                    long_results.append({
                        "symbol": res["symbol"],
                        "real_symbol": res["real_symbol"],
                        "display_name": res.get("display_name", res["real_symbol"] or res["symbol"]),
                        "close": res["close"],
                        "change": res.get("change"),
                        "change_pct": res.get("change_pct"),
                        "ma60": res.get("ma60"),
                        "ma20": res.get("ma20"),
                        "signals": long_sigs,
                    })
                if short_sigs:
                    short_results.append({
                        "symbol": res["symbol"],
                        "real_symbol": res["real_symbol"],
                        "display_name": res.get("display_name", res["real_symbol"] or res["symbol"]),
                        "close": res["close"],
                        "change": res.get("change"),
                        "change_pct": res.get("change_pct"),
                        "ma60": res.get("ma60"),
                        "ma20": res.get("ma20"),
                        "signals": short_sigs,
                    })
            except Exception:
                failed += 1
    finally:
        if own:
            exe.shutdown(wait=False)

    return {
        "ok": True,
        "scan_time": datetime.now().isoformat(timespec="seconds"),
        "period": period,
        "live_only": live_only,
        "live_data": any_live,
        "elapsed": round(time.time() - start, 2),
        "stats": {
            "total": len(symbols),
            "scanned": scanned,
            "failed": failed,
            "no_live": no_live_symbols,
            "long_count": len(long_results),
            "short_count": len(short_results),
        },
        "long": long_results,
        "short": short_results,
        "rules": rules,
    }


# ===== 历史回溯(模型回溯, 与实时监控区分) =====
def _date_to_ns_beijing(date_str, end_of_day=False):
    """'YYYY-MM-DD' -> 北京时间纳秒(UTC ns)。end_of_day=True 取当日 23:59:59。"""
    try:
        d = _dt.datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None
    if end_of_day:
        d = d.replace(hour=23, minute=59, second=59)
    bj = _dt.datetime(d.year, d.month, d.day, d.hour, d.minute, d.second,
                     tzinfo=_dt.timezone(_dt.timedelta(hours=8)))
    return int(bj.timestamp() * 1_000_000_000)


def backtest_symbols(symbols, slot, period="day", rules=None, start_date=None, end_date=None,
                     max_workers=8, executor=None):
    """历史回溯: 在 [start_date, end_date] 区间内全量扫描信号(放宽 recent_bars, 保留全部),
    用于模型回溯, 与实时监控严格区分 —— 不触发弹窗/声音/报警, 不写入实时信号库。
    end_dt 传入 end_date 末刻, 取该时刻之前的 count 根, 再按区间裁剪。"""
    from services import futures_kline as fk
    from collections import Counter
    rules = rules or DEFAULT_RULES
    # 回溯放宽 recent_bars, 保留区间内全部信号(而非仅最近几根)
    bt_rules = []
    for r in rules:
        r2 = dict(r)
        rp = dict(r.get("params", {}) or {})
        rp["recent_bars"] = 10 ** 7
        r2["params"] = rp
        bt_rules.append(r2)

    cfg = fk.PERIODS.get(period, fk.PERIODS["day"])
    duration = cfg["duration"]
    freq = cfg["freq"]
    start_ns = _date_to_ns_beijing(start_date) if start_date else None
    end_ns = _date_to_ns_beijing(end_date, end_of_day=True) if end_date else None

    # 估算需要的 bar 数: 区间天数 × 每交易日 bar 数
    est_days = 30
    if start_ns and end_ns:
        est_days = max(1, (end_ns - start_ns) // (86400 * 10 ** 9)) + 2
    per_day = {"1m": 240, "5m": 48, "15m": 18, "30m": 10, "60m": 6,
               "day": 1, "week": 1, "month": 1, "quarter": 1}.get(period, 5)
    count = min(4000, max(300, int(est_days * per_day) + 50))

    def _one(sym):
        try:
            if freq:
                df, real = fk._resample_daily(slot, sym, freq, count, end_dt=end_ns)
            else:
                df, real = fk._fetch_kline_df(slot, sym, duration, count, end_dt=end_ns)
            if df is None or df.empty or len(df) < 45:
                # 基座历史不足(整体数据不够)或序列部分填充, 视为真正失败
                return None
            # 先裁掉 end_ns 之后的溢出(天勤 end_dt 可能多返回几根), 避免未来数据泄漏;
            # 保留 end_ns 之前的完整历史作为指标预热, 保证 MACD 等指标数值稳定。
            if end_ns is not None:
                df = df[df["datetime"].astype("int64") <= end_ns].reset_index(drop=True)
            if df is None or df.empty or len(df) < 45:
                return None
            df = df.copy()
            ind = indicators.compute_all(df)
            # bt_rules 已放宽 recent_bars=10**7 -> 全量扫描
            sigs = detect_signals(df, ind, bt_rules)
            if not sigs:
                return {"symbol": sym, "real_symbol": real, "signals": []}
            # 按触发时间裁剪到用户窗口 [start_ns, end_ns] (而非对短窗口直接算指标)
            kept = []
            for s in sigs:
                tns = s.get("cross_time_ns")
                idx = s.get("idx")
                if tns is None:
                    if idx is not None and 0 <= idx < len(df):
                        tns = int(df["datetime"].iloc[idx])
                if tns is None:
                    continue
                if start_ns is not None and tns < start_ns:
                    continue
                if end_ns is not None and tns > end_ns:
                    continue
                s["trigger_time"] = _fmt_cn_time(tns)
                # 历史回溯: 每个信号按触发 bar 计算自己的价格与涨跌
                # 口径: 相对前一根 bar close(用户说的"上一个时间点"); 首根则用当根 open
                if idx is not None and 0 <= idx < len(df):
                    close_val = _safe_float(df["close"].iloc[idx])
                    s["close"] = close_val
                    # [2026-10-16] 该信号所在 bar 的 MA60/MA20(历史信号表同口径渲染)
                    _closes = list(df["close"])
                    s["ma60"] = _ma_at(_closes, idx, 60)
                    s["ma20"] = _ma_at(_closes, idx, 20)
                    base = None
                    if idx > 0:
                        base = _safe_float(df["close"].iloc[idx - 1])
                    if base is None or base <= 0:
                        base = _safe_float(df["open"].iloc[idx])
                    if close_val is not None and base and base > 0:
                        s["change"] = round(close_val - base, 4)
                        s["change_pct"] = round((close_val - base) / base * 100, 2)
                    else:
                        s["change"] = None
                        s["change_pct"] = None
                else:
                    s["close"] = None
                    s["change"] = None
                    s["change_pct"] = None
                    s["ma60"] = None
                    s["ma20"] = None
                kept.append(s)
            if not kept:
                return {"symbol": sym, "real_symbol": real, "signals": []}
            return {"symbol": sym, "real_symbol": real,
                    "display_name": _symbol_display_name(sym, real),
                    "signals": kept}
        except Exception as e:
            print(f"[回溯] {sym} 失败: {e}")
            return None

    start = time.time()
    all_signals = []
    scanned = 0
    failed = 0
    empty = 0
    own = executor is None
    exe = executor if executor is not None else ThreadPoolExecutor(max_workers=max_workers)
    try:
        futs = {exe.submit(_one, s): s for s in symbols}
        for fut in as_completed(futs):
            scanned += 1
            try:
                res = fut.result(timeout=30)
            except Exception:
                failed += 1
                continue
            if res is None:
                failed += 1
                continue
            sigs = res.get("signals") or []
            if not sigs:
                empty += 1
                continue
            for s in sigs:
                s["symbol"] = res["symbol"]
                s["real_symbol"] = res.get("real_symbol")
                s["display_name"] = res.get("display_name")
                s["period"] = period
                # close/change/change_pct 已在 _one 中按信号触发时刻计算
            all_signals.extend(sigs)
    finally:
        if own:
            exe.shutdown(wait=False)

    type_counter = Counter()
    side_counter = Counter()
    sym_counter = Counter()
    month_buckets = Counter()
    for s in all_signals:
        type_counter[s.get("type", "unknown")] += 1
        side_counter[s.get("side", "unknown")] += 1
        sym_counter[s.get("symbol")] += 1
        tt = s.get("trigger_time") or ""
        if tt:
            month_buckets[tt[:7]] += 1
    return {
        "ok": True,
        "mode": "history",
        "period": period,
        "start_date": start_date,
        "end_date": end_date,
        "elapsed": round(time.time() - start, 2),
        "stats": {
            "total": len(symbols),
            "scanned": scanned,
            "failed": failed,
            "empty": empty,
            "signal_count": len(all_signals),
            "long": side_counter.get("long", 0),
            "short": side_counter.get("short", 0),
        },
        "by_type": dict(type_counter),
        "by_symbol": dict(sym_counter.most_common(50)),
        "by_month": dict(sorted(month_buckets.items())),
        # 按触发时间倒序(最新在前), 与实时监控面板(session_latest desc)保持一致;
        # 先排序再截断, 保证展示窗口内总是最新一批信号, 避免最新触发落在底部或被截断。
        "signals": sorted(all_signals, key=_sig_sort_key, reverse=True)[:500],  # 截断展示, 全量可扩展入库
    }


# ===== 多周期异步并发监控 =====
# 合法周期键(与 futures_kline.PERIODS 的 key 一致; 此处独立定义避免循环 import)
VALID_PERIODS = {"1m", "5m", "15m", "30m", "60m", "day", "week", "month", "quarter"}
# 周期扫描节奏(秒): 与前端 fkScanIntervalForPeriod 对齐, 快的周期不被慢的周期拖垮
PERIOD_INTERVAL = {
    "1m": 5, "5m": 8, "15m": 12, "30m": 15, "60m": 20,
    "day": 30, "week": 60, "month": 120, "quarter": 300,
}

# 全局有界线程池: 所有周期的符号计算共享, 避免 9周期×74合约=666 条裸线程爆炸
_shared_executor = None
_shared_executor_lock = threading.Lock()

def _get_shared_executor(max_workers=24):
    global _shared_executor
    with _shared_executor_lock:
        if _shared_executor is None or getattr(_shared_executor, "_shutdown", False):
            _shared_executor = ThreadPoolExecutor(max_workers=max_workers)
        return _shared_executor


# ===== 盘次(交易时段)生命周期 =====
# 需求人 2026-08-31: 实时面板按"盘"累积 —— 展示从开盘到当前时间的全部信号并按时间排序;
# 盘结束则清空显示(数据已入库, 到历史回溯查看); 夜盘作为独立盘次同样处理。
# 统一简化模型(需求人确认): 日盘 09:00-15:00; 夜盘 21:00-次日02:30(覆盖全部品种最大区间, 归属次交易日);
# 其余时段为休市(不属于任何盘)。
SESSION_DAY_START = (9, 0)      # 日盘开盘
SESSION_DAY_END = (15, 0)       # 日盘收盘(过三点清盘)
SESSION_NIGHT_START = (21, 0)   # 夜盘开盘
SESSION_NIGHT_END = (2, 30)     # 夜盘收盘(次日, 跨午夜)


def _now_bj_naive():
    """当前北京时间(naive), 便于与盘次起止 datetime 直接比较。"""
    return _dt.datetime.now(_dt.timezone(_dt.timedelta(hours=8))).replace(tzinfo=None)


def _session_of(now=None):
    """判定当前所属盘次。返回 (session_id, label, start, end); 休市返回 (None, '休市', None, None)。

    session_id 形如 '2026-08-31|day' / '2026-09-01|night'。
    夜盘归属【次交易日】, 天然处理 21:00 -> 次日02:30 的跨午夜: 例如 08-31 21:00 与 09-01 01:00
    同属 '2026-09-01|night', 期间累积不中断, 到 02:30 才清盘。
    """
    try:
        now = now or _now_bj_naive()
        d = now.date()
        hm = now.hour * 60 + now.minute
        day_s = SESSION_DAY_START[0] * 60 + SESSION_DAY_START[1]
        day_e = SESSION_DAY_END[0] * 60 + SESSION_DAY_END[1]
        night_s = SESSION_NIGHT_START[0] * 60 + SESSION_NIGHT_START[1]
        night_e = SESSION_NIGHT_END[0] * 60 + SESSION_NIGHT_END[1]
        if day_s <= hm < day_e:
            return (f"{d}|day", f"日盘 {d} 09:00-15:00",
                    datetime.combine(d, _dt.time(*SESSION_DAY_START)),
                    datetime.combine(d, _dt.time(*SESSION_DAY_END)))
        if hm >= night_s:
            # 夜盘起于 d 21:00, 归属次交易日 d+1(凌晨段收尾)
            nd = d + _dt.timedelta(days=1)
            return (f"{nd}|night", f"夜盘 {d} 21:00-{nd} 02:30",
                    datetime.combine(d, _dt.time(*SESSION_NIGHT_START)),
                    datetime.combine(nd, _dt.time(*SESSION_NIGHT_END)))
        if hm < night_e:
            # 00:00-02:30 属"昨夜"开始的夜盘, 归属交易日即为今天 d
            yd = d - _dt.timedelta(days=1)
            return (f"{d}|night", f"夜盘 {yd} 21:00-{d} 02:30",
                    datetime.combine(yd, _dt.time(*SESSION_NIGHT_START)),
                    datetime.combine(d, _dt.time(*SESSION_NIGHT_END)))
    except Exception:
        pass
    return (None, "休市", None, None)


# ===== 交易日(当天)口径 =====
# 需求(2026-09-22): 实时面板只展示【当天】的信号, 更早的到历史回溯里看。
# 口径与盘次保持一致: 21:00 起的夜盘归【次一自然日】的交易日, 因此
# 交易日 D 覆盖的行情范围 = [D-1 21:00, D 15:00]。
def trading_day_of(now=None):
    """当前所属交易日(date): 21:00-24:00 归次一自然日, 其余归当日。"""
    now = now or _now_bj_naive()
    if now.hour >= SESSION_NIGHT_START[0]:
        return now.date() + _dt.timedelta(days=1)
    return now.date()


def trading_day_range(day=None, now=None):
    """交易日 day 的行情范围 [day-1 21:00, day 15:00]; day 缺省取当前交易日。"""
    day = day or trading_day_of(now)
    start = datetime.combine(day - _dt.timedelta(days=1), _dt.time(*SESSION_NIGHT_START))
    end = datetime.combine(day, _dt.time(*SESSION_DAY_END))
    return start, end


def is_trading_now(now=None):
    """当前是否处于交易时段(日盘 09:00-15:00 / 夜盘 21:00-02:30)。"""
    sid, _label, _s, _e = _session_of(now)
    return sid is not None


def _sig_sort_key(sig):
    """信号排序键(越大越新): 优先纳秒时间戳(同一分钟内也精确), 回退解析 trigger_time 字符串。"""
    if not isinstance(sig, dict):
        return 0
    ns = sig.get("trigger_time_ns")
    if ns is not None:
        try:
            return int(ns)
        except Exception:
            pass
    t = sig.get("trigger_time")
    if t:
        try:
            return int(datetime.strptime(str(t)[:16], "%Y-%m-%d %H:%M").timestamp() * 1e9)
        except Exception:
            pass
    return 0


class MultiPeriodMonitor:
    """一条线程监控一个周期: 每个周期一个守护线程, 按自身节奏循环扫描,
    符号计算收敛到全局有界线程池; 所有周期共享同一条持久 TqApi(只读),
    绝不调用 wait_update。命中结果后台异步入库。"""

    def __init__(self, max_workers=24):
        self._executor = _get_shared_executor(max_workers)
        self._lock = threading.Lock()
        self._periods = {}   # period -> {thread, stop, status, latest}
        # 本盘(交易时段)累积器: 盘次变化即清空, 数据已入库故只清显示
        self._acc_lock = threading.Lock()
        self._acc = {}       # period -> {side -> {symbol -> item}}
        self._sess = {"id": None, "label": None, "start": None, "end": None}
        self._seeded = {}    # period -> 已从库回填的 session_id
        # K线仓库路径: period -> {symbol: 最后判定的已收盘 bar_time}
        # 事件驱动的跨拍去重: 同一根 bar 只判一次(落库另有幂等兜底)。
        self._judged = {}

    def is_running(self):
        with self._lock:
            return bool(self._periods)

    def running_periods(self):
        with self._lock:
            return list(self._periods.keys())

    def start(self, periods, symbols, rules, mode="realtime", symbol_rules=None, symbol_tpl=None):
        """启动指定周期的监控线程(已运行的跳过)。mode: 'realtime'=实时(仅实时bar报警) / 'history'=历史回溯。

        symbol_rules/symbol_tpl: 逐标的"最严规则"映射(见 scan_template_db.monitor_rules_by_symbol)。
        为 None 时退化为旧行为(全部标的共用同一套 rules)。"""
        periods = [p for p in periods if p in VALID_PERIODS]
        started = []
        with self._lock:
            for p in periods:
                if p in self._periods:
                    continue
                stop_ev = threading.Event()
                t = threading.Thread(
                    target=self._period_loop,
                    args=(p, stop_ev, list(symbols), rules, mode, symbol_rules, symbol_tpl),
                    daemon=True,
                    name=f"scanmon-{p}",
                )
                self._periods[p] = {
                    "thread": t, "stop": stop_ev,
                    "status": {"running": True, "period": p, "mode": mode,
                               "last_scan": None, "last_count": 0,
                               "last_error": None,
                               "interval": PERIOD_INTERVAL.get(p, 15)},
                    "latest": None,
                }
                t.start()
                started.append(p)
        return self.status()

    def stop(self, periods=None):
        """停止指定周期(默认全部)。"""
        with self._lock:
            keys = list(self._periods.keys()) if not periods \
                else [p for p in periods if p in self._periods]
            for p in keys:
                self._periods[p]["stop"].set()
        for p in keys:
            try:
                self._periods[p]["thread"].join(timeout=3)
            except Exception:
                pass
            with self._lock:
                self._periods.pop(p, None)
        return self.status()

    def _period_loop(self, period, stop_ev, symbols, rules, mode="realtime", symbol_rules=None, symbol_tpl=None):
        # slot 在循环内现取, 容忍账户切换
        slot = None
        live_only = (mode == "realtime")
        try:
            from services import futures_kline as fk
            slot = fk._get_auto_slot()   # 2026-09-24: 五级回落(末级自动从 tq_accounts 挑有效账号开槽)
        except Exception:
            slot = None
        interval = PERIOD_INTERVAL.get(period, 15)
        while not stop_ev.is_set():
            if slot is None:
                try:
                    from services import futures_kline as fk
                    slot = fk._get_auto_slot()   # 2026-09-24: 五级回落(同上)
                except Exception:
                    slot = None
            t0 = time.time()
            try:
                if slot is not None:
                    if KLINE_STORE_ENABLED:
                        # 新路径(2026-10-13): 收盘事件驱动 + 读仓库定稿窗口。
                        # 只对"新的已收盘 bar"判定 -> 成型 bar 不参与 -> 幻影信号结构性绝迹。
                        from services import scan_store as sst
                        result = sst.scan_period_from_store(
                            symbols, slot, period=period, rules=rules,
                            max_workers=8, executor=self._executor,
                            symbol_rules=symbol_rules, symbol_tpl=symbol_tpl,
                            judged=self._judged.setdefault(period, {}),
                        )
                    else:
                        # 旧路径(保留, KLINE_STORE_ENABLED=0 时回退)
                        result = scan_symbols(
                            symbols, slot, period=period, rules=rules,
                            max_workers=8, executor=self._executor, live_only=live_only,
                            symbol_rules=symbol_rules, symbol_tpl=symbol_tpl,
                        )
                    if result and result.get("ok"):
                        # 盘次生命周期: 盘次变化(日盘过15:00/夜盘过02:30/新盘开始)则清空累积器,
                        # 数据已入库故只清显示; 随后首次进入本盘时从库回填已产生的信号。
                        self._sync_session()
                        if self._seeded.get(period) != self._sess.get("id"):
                            self._seed_session_from_db(period)
                            # 补扫本盘(开盘以来)整段: 兜住"中途启动/重启"漏掉的 开盘->启动前 那段。
                            # 仅当内存仍空(全新启动或本盘尚无库记录)才扫, 避免与已落库数据重复劳动。
                            if not self._acc.get(period):
                                self._backfill_session(period, symbols, rules, slot, symbol_rules, symbol_tpl)
                            self._seeded[period] = self._sess.get("id")
                        self._merge_result(period, result)
                        # 后台异步入库, 不阻塞扫描循环
                        # 仅实时模式写入历史复盘表; 历史回溯模式不入库, 避免污染实时信号库
                        if mode == "realtime":
                            try:
                                threading.Thread(
                                    target=save_to_history, args=(result,),
                                    daemon=True
                                ).start()
                            except Exception:
                                pass
                        with self._lock:
                            st = self._periods[period]["status"]
                            st["last_scan"] = result.get("scan_time")
                            st["last_count"] = (
                                result["stats"]["long_count"]
                                + result["stats"]["short_count"]
                            )
                            st["last_error"] = None
                        with self._lock:
                            self._periods[period]["latest"] = result
            except Exception as e:
                with self._lock:
                    if period in self._periods:
                        self._periods[period]["status"]["last_error"] = str(e)[:200]
            elapsed = time.time() - t0
            wait = max(0.0, interval - elapsed)
            if stop_ev.wait(wait):
                break

    def status(self):
        with self._lock:
            return {
                p: {**v["status"], "running": not v["stop"].is_set()}
                for p, v in self._periods.items()
            }

    def latest(self, period=None):
        with self._lock:
            if period:
                return self._periods.get(period, {}).get("latest")
            return {p: v["latest"] for p, v in self._periods.items()}

    # ===== 本盘(交易时段)累积视图 =====
    def session_info(self):
        """当前盘次信息: {id, label, start, end}; 休市时 id 为 None。"""
        with self._acc_lock:
            s = dict(self._sess)
        for k in ("start", "end"):
            if s.get(k):
                s[k] = str(s[k])
        return s

    def _sync_session(self):
        """检测盘次变化; 变化则清空全部累积器(数据已入库, 此处只清显示)。返回是否变化。"""
        sid, label, start, end = _session_of()
        with self._acc_lock:
            if sid == self._sess.get("id"):
                return False
            self._sess = {"id": sid, "label": label, "start": start, "end": end}
            self._acc = {}
            self._seeded = {}
            self._judged = {}
            return True

    def _seed_session_from_db(self, period):
        """从历史库回填本盘已产生的信号: 监控中途启动/重启也能看到本盘全貌(需求人确认: 从DB回填)。"""
        with self._acc_lock:
            sid = self._sess.get("id")
            start = self._sess.get("start")
        if not sid or not start:
            return
        try:
            from services import scan_history_db
            rows = scan_history_db.query_session_signals(
                start=start, end=_now_bj_naive(), period=period)
        except Exception as e:
            print(f"[监控] 回填本盘({sid}/{period})信号失败: {e}")
            return
        with self._acc_lock:
            if self._sess.get("id") != sid:      # 回填期间盘次已切换, 丢弃
                return
            bucket = self._acc.setdefault(period, {})
            for r in rows:
                side, sym = r.get("side"), r.get("symbol")
                if not sym or side not in ("long", "short"):
                    continue
                b = bucket.setdefault(side, {})
                cur = b.get(sym)
                if cur is None:
                    _sig0 = r.get("signal") or {}
                    cur = {"symbol": sym, "real_symbol": r.get("real_symbol") or "",
                           "display_name": r.get("display_name") or r.get("real_symbol") or sym,
                           "close": r.get("close_price"), "change": r.get("change"),
                           "change_pct": r.get("change_pct"), "period": period,
                           "ma60": _sig0.get("ma60"), "ma20": _sig0.get("ma20"),
                           "live_data": True, "signals": []}
                    b[sym] = cur
                sig = r.get("signal") or {}
                if not sig:
                    continue
                key = (sig.get("rule_id"), sig.get("type"), sig.get("trigger_time"))
                if key in {(s.get("rule_id"), s.get("type"), s.get("trigger_time"))
                           for s in cur["signals"]}:
                    continue
                cur["signals"].append(sig)

    def _backfill_session(self, period, symbols, rules, slot, symbol_rules=None, symbol_tpl=None):
        """补扫本盘(交易日开盘以来)整段: 用 live_only=False 扫全窗口, 过滤出
        [session_start, now] 区间内的信号并入内存累积器。

        目的(2026-09-23 用户诉求: 默认抓取今天开盘以来的数据):
          实时监控每个 tick 只扫"当前一根 bar", 靠轮询累积拼出 开盘->当前;
          但中途启动/重启会漏掉 开盘->启动前 那段 —— 这里一次性补齐, 使"今天开盘以来"
          信号完整可见(无论服务何时启动)。
        仅并入内存(不写库: 实时 tick 已负责落库, 重复写会造重复行); 不触发报警
        (报警由前端按 trigger_time 去重, 历史信号不重复弹)。
        """
        try:
            sid = self._sess.get("id")
            sstart = self._sess.get("start")
            if not sid or not sstart:
                return
            start_ns = int(sstart.replace(tzinfo=_dt.timezone(_dt.timedelta(hours=8))).timestamp() * 1e9)
            now_ns = int(_dt.datetime.now(_dt.timezone(_dt.timedelta(hours=8))).timestamp() * 1e9)
            # [2026-10-13] count 400 -> 300: 与实时扫描同键, 补扫不再新建 K 线对象。
            # 实锤根因(监控槽预算 420/420 打满 / 15m 起不来 / 00:37 会话冻死):
            #   超模的不是周/月/季 —— 它们本就共用同一个日线对象
            #   (key=(real_symbol, 86400, 2000), _resample_daily 的 daily_count 与周期无关);
            #   真正的冗余是本补扫的 count=400 键: day/60m/30m 各多 1 个对象/标的,
            #   60 只主连 x 最多 3 = 180 个冗余订阅, 直逼天勤 ~500 静默失效阈值。
            #   300 根覆盖任何盘口窗口(日盘 6h: 15m 仅 24 根), 信号按
            #   [session_start, now] 过滤的口径不变, 取数内容零变化。
            if KLINE_STORE_ENABLED:
                # 新路径(2026-10-13): 从仓库补扫 —— [session_start, now] 内【每一根
                # 已收盘 bar】逐根判定, 信号精确归位到各自 bar_time。
                # 这是 lu 周线错档的根治点: 旧路径走 live_only=False 全窗口翻历史找交叉,
                # 且周/月/季把日线 df 直接透传 _scan_one(813 行 if df is not None 跳过
                # _resample_daily) -> 日线交叉贴上周/月/季标签(DIF 与日线一字不差)。
                from services import scan_store as sst
                result = sst.backfill_from_store(
                    symbols, slot, period=period, rules=rules,
                    start=sstart, end=_now_bj_naive(),
                    max_workers=8, executor=self._executor,
                    symbol_rules=symbol_rules, symbol_tpl=symbol_tpl,
                )
            else:
                result = scan_symbols(
                    symbols, slot, period=period, rules=rules,
                    max_workers=8, executor=self._executor, live_only=False, count=300,
                    symbol_rules=symbol_rules, symbol_tpl=symbol_tpl,
                )
            if not result or not result.get("ok"):
                return
            filtered = {"ok": True, "period": period, "long": [], "short": []}
            for side in ("long", "short"):
                for item in (result.get(side) or []):
                    sigs = [s for s in (item.get("signals") or [])
                            if isinstance(s, dict)
                            and s.get("trigger_time_ns") is not None
                            and start_ns <= int(s.get("trigger_time_ns")) <= now_ns]
                    if sigs:
                        it = dict(item)
                        it["signals"] = sigs
                        filtered[side].append(it)
            if filtered["long"] or filtered["short"]:
                # 落入内存累积器(与实时 tick 同一条去重键), 前端 today/latest 视图即可见
                self._merge_result(period, filtered)
                print(f"[监控] 补扫本盘({sid}/{period}) 并入 {len(filtered['long'])+len(filtered['short'])} 条(开盘->当前)")
        except Exception as e:
            print(f"[监控] 补扫本盘({period})失败: {e}")

    def _merge_result(self, period, result):
        """把一次扫描结果并入本盘累积器: 新信号按(规则,类型,触发时间)去重追加, 价格字段以最新为准。

        同一根 bar 在成型前会被反复扫描命中, 靠 trigger_time 去重成一条; bar 走完后新 bar 的
        新 trigger_time 才会追加, 从而形成"开盘->当前时间"的完整序列。
        """
        if not result or not result.get("ok"):
            return
        with self._acc_lock:
            if not self._sess.get("id"):     # 休市: 不属于任何盘, 不累积
                return
            bucket = self._acc.setdefault(period, {})
            for side in ("long", "short"):
                b = bucket.setdefault(side, {})
                for item in (result.get(side) or []):
                    sym = item.get("symbol")
                    if not sym or not (item.get("signals") or []):
                        continue
                    cur = b.get(sym)
                    if cur is None:
                        cur = dict(item)
                        cur["period"] = period
                        cur["signals"] = []
                        b[sym] = cur
                    else:
                        for k in ("real_symbol", "display_name", "close", "change",
                                  "change_pct", "datetime", "live_data",
                                  "ma60", "ma20"):
                            if k in item:
                                cur[k] = item[k]
                    seen = {(s.get("rule_id"), s.get("type"), s.get("trigger_time"))
                            for s in cur["signals"]}
                    for sig in (item.get("signals") or []):
                        key = (sig.get("rule_id"), sig.get("type"), sig.get("trigger_time"))
                        if key in seen:
                            continue
                        seen.add(key)
                        cur["signals"].append(dict(sig))

    def session_latest(self, period=None, order="desc"):
        """返回【本盘累积】视图(开盘->当前时间); 条目与信号均按触发时间排序, 默认最新在前。"""
        desc = str(order).lower() != "asc"
        with self._acc_lock:
            keys = [period] if period else list(self._acc.keys())
            out = {}
            for p in keys:
                b = self._acc.get(p) or {}
                long_list, short_list = [], []
                for side, sink in (("long", long_list), ("short", short_list)):
                    items = []
                    for _sym, it in (b.get(side) or {}).items():
                        it = dict(it)
                        it["period"] = p
                        it["signals"] = sorted(it.get("signals") or [],
                                               key=_sig_sort_key, reverse=desc)
                        items.append(it)
                    items.sort(key=lambda x: _sig_sort_key((x.get("signals") or [{}])[0]),
                               reverse=desc)
                    sink.extend(items)
                out[p] = {
                    "ok": True, "period": p, "accumulated": True,
                    "long": long_list, "short": short_list,
                    "live_only": True, "live_data": bool(long_list or short_list),
                    "stats": {"long_count": len(long_list), "short_count": len(short_list)},
                }
        return out.get(period) if period else out


    def trading_day_view(self, period=None, order="desc"):
        """【当天(交易日)】视图: 库中当天信号 ∪ 内存实时累积, 按(规则,类型,触发时间)去重。

        与 session_latest 的区别: 盘次结束(休市/隔盘)不清空 —— 当天已收盘的日盘信号依然可见;
        跨到新交易日自动切换; 数据以库为主, 兜住重启/中途启动。前端「实时」tab 用这个。
        """
        desc = str(order).lower() != "asc"
        try:
            start, end = trading_day_range()
        except Exception:
            start, end = None, None
        buckets = {}          # period -> {"long": {sym: item}, "short": {sym: item}}

        # 1) 内存实时累积(当前盘次)
        mem = self.session_latest(order=order)
        if isinstance(mem, dict):
            for p, d in mem.items():
                if period and p != period:
                    continue
                if not d or not d.get("ok"):
                    continue
                b = buckets.setdefault(p, {"long": {}, "short": {}})
                for side in ("long", "short"):
                    for it in (d.get(side) or []):
                        sym = it.get("symbol")
                        if sym:
                            b[side][sym] = dict(it)

        # 2) 库中当天已落库信号
        if start is not None:
            try:
                from services import scan_history_db
                # 2026-09-24 17:15 需求人: 「昨天拉取到的数据不用在实时这里展示」——
                # 库合并下界从交易日起点(昨21:00)收紧为今日日历 00:00: 昨夜盘落库
                # (scan_time=昨天)剔除; 今天凌晨 0:00~2:30 夜盘延续段落库(scan_time=今天)保留。
                _day0 = _dt.datetime.combine(_dt.date.today(), _dt.time.min)
                if start < _day0:
                    start = _day0
                # 2026-10-15 修复(用户实锤: "实时tab金叉时间停在9:30, 收盘后信号看不到"):
                # 日盘 15:00 收盘后, 最后一波 ingest+scan 有 1~2 分钟延迟(15:02 才落库),
                # 而 trading_day_range 的 end=当天15:00 是硬边界 -> scan_time>15:00 的信号
                # 全被切掉, 实时 tab 永远看不到尾盘两分钟的信号。休市后把 end 放宽到
                # now+10min, 兜住收盘延迟; 交易时段内 now<=end 不受影响。
                _now_bj = _dt.datetime.now(_dt.timezone(_dt.timedelta(hours=8))).replace(tzinfo=None)
                if end is not None and _now_bj > end:
                    end = _now_bj + _dt.timedelta(minutes=10)
                # 2026-10-09 性能修复(整页卡死根因):
                # 下游是 b[side][sym] = dict(it) 按品种去重, 只需"每个品种最新一条",
                # 不关心跨品种 trigger_time 先后 -> 用 by_scan_time=True 让排序走
                # idx_scan_trigger/idx_scan_time 逆序, 消除 filesort。
                # 实测(当天 19.7 万行): 原ORDER BY trigger_time = 15.8~23.9s(整页因此卡死),
                # 改 scan_time DESC = 0.06~0.14s; limit 5000->1500 是因为去重后品种只有 ~56 个,
                # 1500 行足够覆盖且传输量小得多。
                rows = scan_history_db.query_range_signals(
                    start=start, end=end, period=period, limit=1500, by_scan_time=True)
            except Exception as e:
                print(f"[监控] 读取当天({start}~{end})信号失败: {e}")
                rows = []
            for r in rows:
                p = r.get("period") or period or "day"
                side, sym = r.get("side"), r.get("symbol")
                if not sym or side not in ("long", "short"):
                    continue
                b = buckets.setdefault(p, {"long": {}, "short": {}})
                cur = b[side].get(sym)
                if cur is None:
                    _sig0 = r.get("signal") or {}
                    cur = {"symbol": sym, "real_symbol": r.get("real_symbol") or "",
                           "display_name": r.get("display_name") or r.get("real_symbol") or sym,
                           "close": r.get("close_price"), "change": r.get("change"),
                           "change_pct": r.get("change_pct"), "period": p,
                           "ma60": _sig0.get("ma60"), "ma20": _sig0.get("ma20"),
                           "live_data": False, "signals": []}
                    b[side][sym] = cur
                sig = r.get("signal") or {}
                if not sig:
                    continue
                sig.setdefault("period", p)
                key = (sig.get("rule_id"), sig.get("type"), sig.get("trigger_time"))
                seen = {(x.get("rule_id"), x.get("type"), x.get("trigger_time"))
                        for x in cur["signals"]}
                if key in seen:
                    continue
                cur["signals"].append(sig)

        # 3) 整理输出(与 session_latest 同构, 前端无需分支)
        out = {}
        for p, b in buckets.items():
            long_list, short_list = [], []
            for side, sink in (("long", long_list), ("short", short_list)):
                for _sym, it in (b.get(side) or {}).items():
                    it = dict(it)
                    it["period"] = p
                    it["signals"] = sorted(it.get("signals") or [],
                                           key=_sig_sort_key, reverse=desc)
                    if not it["signals"]:
                        continue
                    it["live_data"] = True      # 库里有当天记录即视为有效
                    sink.append(it)
                sink.sort(key=lambda x: _sig_sort_key((x.get("signals") or [{}])[0]),
                          reverse=desc)
            out[p] = {
                "ok": True, "period": p, "accumulated": True, "trading_day": True,
                "long": long_list, "short": short_list,
                "live_only": False, "live_data": bool(long_list or short_list),
                "stats": {"long_count": len(long_list), "short_count": len(short_list)},
            }
        return out.get(period) if period else out


def save_to_history(result):
    """供监控线程调用: 把扫描结果异步写入历史库。"""
    try:
        from services import scan_history_db
        scan_history_db.save_scan_signals(result)
    except Exception as e:
        print(f"[MultiPeriodMonitor] 入库失败: {e}")


# ===== 定时爬取调度器 =====
# 每条启用的 schedule 在窗口内各自持有一个 MultiPeriodMonitor 实例; 出窗口自动 stop。
# 与手动监控(_get_multi_monitor() 全局单例)互不干扰。
_SCHED_MONITORS = {}
_SCHED_LOCK = threading.Lock()
_SCHED_THREAD = None
_SCHED_STARTED = False


def _now_beijing():
    return _dt.datetime.now(_dt.timezone(_dt.timedelta(hours=8)))


def _in_window(start_hm, end_hm, now):
    """判断 now 是否落在 [start, end) 窗口内; 支持跨午夜(如 21:00-02:30)。"""
    try:
        sh, sm = (int(x) for x in str(start_hm).split(":"))
        eh, em = (int(x) for x in str(end_hm).split(":"))
    except Exception:
        return False
    cur = now.hour * 60 + now.minute
    start = sh * 60 + sm
    end = eh * 60 + em
    if start == end:
        return True
    if end > start:
        return start <= cur < end
    return cur >= start or cur < end  # 跨午夜


def _day_match(days_bit, now):
    """weekday(): 周一=0..周日=6; 位掩码 bit=1<<weekday。"""
    return bool(int(days_bit or 127) & (1 << now.weekday()))


def _schedule_symbols(products=None):
    from services import futures_kline as fk
    try:
        symbols = [s for s, _, _, _ in fk._get_main_contracts()]
        if products:
            symbols = fk._filter_symbols_by_products(symbols, products)
        return symbols[:300]
    except Exception:
        return []


def _run_schedule_once():
    """评估所有定时任务: 窗口内且无活跃 monitor 则启动; 出窗口且活跃则停止。"""
    from services import scan_schedule_db as sdb
    try:
        scheds = sdb.list_schedules()
    except Exception as e:
        print(f"[scheduler] 读取任务失败: {e}")
        return
    now = _now_beijing()
    active_ids = set()
    for s in scheds:
        if not s.get("enabled"):
            continue
        if not _day_match(s.get("days_bit", 127), now):
            continue
        if not _in_window(s["start_hm"], s["end_hm"], now):
            continue
        sid = s["id"]
        active_ids.add(sid)
        with _SCHED_LOCK:
            mon = _SCHED_MONITORS.get(sid)
        if mon is None or not mon.is_running():
            tpl = None
            if s.get("template_id"):
                try:
                    from services import scan_template_db
                    tpl = scan_template_db.load_template(s["template_id"])
                except Exception:
                    tpl = None
            periods = (tpl or {}).get("periods") or ["day"]
            rules = (tpl or {}).get("rules")
            products = (tpl or {}).get("products")
            symbols = _schedule_symbols(products)
            mon = MultiPeriodMonitor()
            try:
                mon.start(periods, symbols, rules)
            except Exception as e:
                print(f"[scheduler] 启动任务 {sid} 失败: {e}")
                continue
            with _SCHED_LOCK:
                _SCHED_MONITORS[sid] = mon
            print(f"[scheduler] 已按模板启动任务「{s.get('name')}」 periods={periods}")
    # 停止不在窗口内的任务
    with _SCHED_LOCK:
        stop_ids = [i for i in _SCHED_MONITORS.keys() if i not in active_ids]
    for sid in stop_ids:
        with _SCHED_LOCK:
            mon = _SCHED_MONITORS.pop(sid, None)
        if mon is not None:
            try:
                mon.stop()
                print(f"[scheduler] 已停止任务 id={sid}")
            except Exception:
                pass


_LAST_TAIL_BF = [0.0]  # 尾盘补齐节流时间戳(模块级)


def _maybe_backfill_tail():
    """收盘后窗口内, 每 ~20 分钟触发一次尾盘补齐(天勤尾盘 bar 固化有延迟, 多次重试抓住)。
    仅日内周期(1m/5m/15m/30m/60m); 用"不同 count 强制新建 kline 对象"绕过盘中陈旧对象。"""
    now = _now_beijing()
    hm = now.hour * 60 + now.minute
    # 日盘收盘(15:00)~夜盘开(21:00) 与 夜盘收盘(02:30)~日盘开(09:00) 两个窗口
    in_window = (15 * 60 <= hm) or (2 * 60 + 30 <= hm < 9 * 60)
    if not in_window:
        return
    if time.time() - _LAST_TAIL_BF[0] < 20 * 60:
        return
    _LAST_TAIL_BF[0] = time.time()
    try:
        from services import futures_kline as fk, scan_store as sst
        slot = fk._get_auto_slot()
        if slot is None:
            return
        symbols = _schedule_symbols()
        if not symbols:
            return
        periods = ["1m", "5m", "15m", "30m", "60m"]
        n = sst.backfill_intraday_tail(slot, symbols, periods)
        if n:
            print(f"[tail-bf] 补尾盘 {n} 根")
    except Exception as e:
        print(f"[tail-bf] 异常: {e}")


def _scheduler_loop():
    while True:
        try:
            _run_schedule_once()
        except Exception as e:
            print(f"[scheduler] 循环异常: {e}")
        try:
            _maybe_backfill_tail()
        except Exception:
            pass
        time.sleep(30)


def start_scan_scheduler():
    """启动定时爬取调度线程(幂等)。在 register_routes 中调用一次。"""
    global _SCHED_THREAD, _SCHED_STARTED
    if _SCHED_STARTED:
        return
    _SCHED_STARTED = True
    try:
        _run_schedule_once()  # 启动即评估一次
    except Exception as e:
        print(f"[scheduler] 首次评估异常: {e}")
    _SCHED_THREAD = threading.Thread(
        target=_scheduler_loop, daemon=True, name="scan-scheduler"
    )
    _SCHED_THREAD.start()
    print("[scheduler] 定时爬取调度线程已启动")


def schedule_status():
    """返回各 schedule 当前是否在运行, 供前端展示。"""
    with _SCHED_LOCK:
        return {str(sid): mon.is_running() for sid, mon in _SCHED_MONITORS.items()}


if __name__ == "__main__":
    print("扫描引擎模块加载完成。规则数:", len(DEFAULT_RULES))
    print("可选指标模板数:", len(OPTIONAL_RULE_TEMPLATES))
