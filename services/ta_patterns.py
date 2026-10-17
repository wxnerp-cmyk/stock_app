# -*- coding: utf-8 -*-
"""
期货 K 线形态识别 (纯 numpy, 不依赖 tqsdk)。

提供三类信号, 供 K 线详情页在图上标注 + 信号清单展示:
1. 支撑位 / 压力位: 近期枢轴低点 / 高点
2. 顶背离 / 底背离: 价格与指标(MACD/RSI/KDJ...)方向相反

约定:
- 输入为 list(float/None), None 视为缺失
- 输出索引均为 K 线数组下标 (与前端 categories 对齐)
- 单次计算 O(n), 对 120~450 根 K 线毫秒级, 满足 stock_app 性能约束
"""
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta


def _clean(arr):
    """list -> np.array(float64), None/NaN 归一为 nan。"""
    out = np.empty(len(arr), dtype="float64")
    for i, v in enumerate(arr):
        if v is None:
            out[i] = np.nan
        else:
            try:
                f = float(v)
                out[i] = np.nan if (np.isnan(f) or np.isinf(f)) else f
            except Exception:
                out[i] = np.nan
    return out


def find_pivots(values, order=5):
    """返回枢轴点列表 [(idx, value, kind)]。

    - kind='high': 以 i 为中心 ±order 窗口内的局部最大值
    - kind='low' : 以 i 为中心 ±order 窗口内的局部最小值
    窗口边缘 (前/后 order 根) 不参与, 避免端点伪枢轴。
    """
    arr = _clean(values)
    n = len(arr)
    if n < 2 * order + 1:
        return []
    # 2026-09-14 性能: 原实现逐点 np.nanmin/np.nanmax 一个 (2*order+1) 窗口, 是技术分析页
    # 最大热点(实测占 compute_period 约 31%, 每秒数万次数值归约)。改用 pandas rolling(center)
    # 一次向量化算出全部窗口的 min/max —— 语义与原逐点版完全一致: 忽略 NaN、窗口边缘
    # (前/后 order 根)不参与、命中判据与 1e-9 容差不变。既提速又不改任何识别结果。
    win = 2 * order + 1
    _s = pd.Series(arr, dtype="float64")
    # min_periods=1 才能复刻 np.nanmin/np.nanmax 的"忽略缺失值、只在窗口全空时给 NaN"语义;
    # 用 min_periods=win 会因窗口里混有 None(->_clean 变 NaN)整体判 NaN, 漏掉枢轴。
    lo_arr = _s.rolling(win, center=True, min_periods=1).min().to_numpy()
    hi_arr = _s.rolling(win, center=True, min_periods=1).max().to_numpy()
    piv = []
    for i in range(order, n - order):
        v = arr[i]
        lo = lo_arr[i]
        hi = hi_arr[i]
        if np.isnan(v) or np.isnan(lo) or np.isnan(hi):
            continue
        if v >= hi - 1e-9 and v > lo:
            piv.append((i, float(v), "high"))
        elif v <= lo + 1e-9 and v < hi:
            piv.append((i, float(v), "low"))
    return piv


def detect_levels(highs, lows, order=5, top_n=3, lookback=150):
    """近期枢轴高/低作为压力/支撑。取窗口内最近 top_n 个。"""
    n = len(_clean(highs))
    ph = [(i, v) for i, v, k in find_pivots(highs, order) if k == "high"]
    pl = [(i, v) for i, v, k in find_pivots(lows, order) if k == "low"]
    ph_r = [p for p in ph if p[0] >= n - lookback]
    pl_r = [p for p in pl if p[0] >= n - lookback]
    resistance = sorted(ph_r, key=lambda x: -x[0])[:top_n]
    support = sorted(pl_r, key=lambda x: -x[0])[:top_n]
    return (
        [{"price": round(v, 4), "idx": int(i)} for i, v in resistance],
        [{"price": round(v, 4), "idx": int(i)} for i, v in support],
    )


def detect_divergence(price_highs, price_lows, indic, order=3, lookback=150,
                      piv_h=None, piv_l=None):
    """检测价格与某指标序列的背离。

    顶背离: 相邻两个价格高点, 价格更高高 (p2>p1), 但指标更低高 (i2<i1)
    底背离: 相邻两个价格低点, 价格更低低 (p2<p1), 但指标更高低 (i2>i1)
    扫描窗口内所有相邻枢轴对 (不再只看最后一对), 灵敏度更高。
    返回 [{type, indicator, x1,x2, p1,p2, i1,i2}]。
    """
    n = len(_clean(price_highs))
    # 2026-09-14 性能: 允许调用方传入同一 (highs, lows, order) 下已算好的枢轴, 避免重复
    # find_pivots(技术分析页对同一标的做 MACD/RSI 两次背离检测, 枢轴完全相同, 可直接复用)。
    if piv_h is None:
        piv_h = [(i, v) for i, v, k in find_pivots(price_highs, order) if k == "high"]
    if piv_l is None:
        piv_l = [(i, v) for i, v, k in find_pivots(price_lows, order) if k == "low"]
    ind = _clean(indic)
    res = []

    rh = [p for p in piv_h if p[0] >= n - lookback]
    for a in range(1, len(rh)):
        (x1, p1), (x2, p2) = rh[a - 1], rh[a]
        i1, i2 = ind[x1], ind[x2]
        if (not np.isnan(i1)) and (not np.isnan(i2)) and p2 > p1 and i2 < i1:
            res.append({
                "type": "top", "x1": int(x1), "x2": int(x2),
                "p1": round(p1, 4), "p2": round(p2, 4),
                "i1": round(float(i1), 4), "i2": round(float(i2), 4),
            })

    rl = [p for p in piv_l if p[0] >= n - lookback]
    for a in range(1, len(rl)):
        (x1, p1), (x2, p2) = rl[a - 1], rl[a]
        i1, i2 = ind[x1], ind[x2]
        if (not np.isnan(i1)) and (not np.isnan(i2)) and p2 < p1 and i2 > i1:
            res.append({
                "type": "bottom", "x1": int(x1), "x2": int(x2),
                "p1": round(p1, 4), "p2": round(p2, 4),
                "i1": round(float(i1), 4), "i2": round(float(i2), 4),
            })
    return res


def detect_macd_cross(dif, dea, close=None, datetimes=None):
    """检测 MACD 金叉/死叉，标记落在后一根 bar（长短线交汇的后一节点）上。

    金叉: DIF 上穿 DEA (前<=后>)；死叉: DIF 下穿 DEA (前>=后<)。
    标记位置取交叉发生处的后一根 bar 下标 idx，Y 取该 bar 的 DIF 值
    （需求人2026-08-31: 不需要公式预测的真实交汇点, 直接标在后一根 bar 即可）。

    返回字段:
      type   'golden' / 'death'
      idx    int   后一根 bar 索引(向后对齐, 供跳转/扫描兼容)
      dif    float 后一根 bar 的 DIF
      dea    float 后一根 bar 的 DEA
      price  float 后一根 bar 的收盘价(若有 close)
    """
    n = min(len(dif), len(dea))
    if n < 2:
        return []
    crosses = []
    for i in range(1, n):
        d1, a1 = dif[i - 1], dea[i - 1]
        d2, a2 = dif[i], dea[i]
        if d1 is None or a1 is None or d2 is None or a2 is None:
            continue
        try:
            d1, a1, d2, a2 = float(d1), float(a1), float(d2), float(a2)
        except Exception:
            continue
        g1 = d1 - a1   # (DIF-DEA) 在 i-1
        g2 = d2 - a2   # (DIF-DEA) 在 i
        is_golden = (g1 <= 1e-12 and g2 > 1e-12)
        is_death = (g1 >= -1e-12 and g2 < -1e-12)
        if not (is_golden or is_death):
            continue
        # 标记落在后一根 bar（长短线交汇的后一节点）: idx=i, Y=该 bar 的 DIF
        # (需求人2026-08-31: 不需要公式预测的真实交汇点, 直接标在后一根 bar 即可)
        ctype = "golden" if is_golden else "death"
        cross = {
            "type": ctype,
            "idx": int(i),
            "dif": round(d2, 4),
            "dea": round(a2, 4),
        }
        if close is not None and i < len(close):
            try:
                cross["price"] = round(float(close[i]), 4)
            except Exception:
                pass
        crosses.append(cross)
    return crosses


def _price_trend_series(close_list, n=60):
    """计算慢速均线序列(MA n), 长度与 close_list 一致, 前 n-1 项为 NaN。用于按交叉点 idx 取局部趋势。"""
    if not close_list or len(close_list) < n:
        return None
    try:
        return pd.Series(close_list, dtype="float64").rolling(n, min_periods=n).mean()
    except Exception:
        return None


def _trend_at_ma(ma_series, idx, lookback=10):
    """在交叉点 idx 处, 取慢速均线序列 [idx-lookback+1, idx] 的首尾斜率判趋势: 1=上升,-1=下降,0=平坦/不足。

    即"交叉前后节点的趋势方向": 该窗口均线向上=上升趋势(金叉), 向下=下降趋势(死叉)。
    慢速均线(默认 MA60)+ 较长窗口(默认10)代表整体趋势, 局部回调不翻转方向。
    """
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
        slope = float(seg.iloc[-1]) - float(seg.iloc[0])
        if slope > 1e-9:
            return 1
        if slope < -1e-9:
            return -1
        return 0
    except Exception:
        return 0


def detect_macd_cross_trend(dif, dea, close=None, datetimes=None,
                            trend_ma_n=60, trend_lookback=10,
                            recent_bars=None, zero_line=0,
                            trend_mode='trend', trend_filter=False):
    """检测 MACD 金叉/死叉，与 scan_engine._detect_macd_cross 口径保持一致。

    🔴 标准定义(2026-10-15 用户裁定"金叉就是金叉, 按标准来", 勿再自定义):
       金叉 = DIF 由下向上突破 DEA; 死叉 = DIF 由上向下突破 DEA。
       趋势(慢速均线方向)只是【辅助过滤条件】, 不改变金叉死叉本身的定义。

    先由 detect_macd_cross 得到原始交叉(后一根 bar 节点), 再按模式处理:
      - trend_mode='trend'(默认): 标准定义 —— type = 原始交叉类型(上穿=金叉/下穿=死叉);
        trend_filter=True 时附加趋势过滤(上穿须上升趋势、下穿须下降趋势, 否则不报)。
      - trend_mode='match'(旧逻辑): 仅 上穿+上升=金叉, 下穿+下降=死叉。

    recent_bars: 只保留最近 N 根内发生的交叉; None=保留全部。
    zero_line: 当前仅透传保留(距零线过滤由调用方 scan_engine 处理); K线图不做 threshold 过滤。

    返回字段在 detect_macd_cross 基础上增加:
      - trend: '上升'/'下降'/'平坦'
      - original_type: 原始 golden/death(上穿/下穿)
      - type: 有效类型(标准模式下 == original_type)
    """
    crosses = detect_macd_cross(dif, dea, close=close, datetimes=datetimes)
    n = min(len(dif), len(dea)) if dif and dea else 0
    if not n:
        return []
    ma_series = _price_trend_series(list(close) if close is not None else [], n=trend_ma_n)
    out = []
    for c in crosses:
        idx = c.get("idx")
        if idx is None or idx < 0 or idx >= n:
            continue
        if recent_bars is not None and idx < n - recent_bars:
            continue
        trend = _trend_at_ma(ma_series, idx, trend_lookback)
        ctype = c.get("type")
        effective_type = None
        if trend_mode == 'trend':
            # 标准定义(2026-10-15 用户裁定): 金叉=DIF 上穿 DEA, 死叉=DIF 下穿 DEA。
            # 趋势只作可选过滤(trend_filter), 默认不过滤, 不改变金叉死叉本身。
            effective_type = ctype          # standard: 直接用原始交叉类型
            if trend_filter:
                if ctype == 'golden' and trend != 1:
                    effective_type = None    # 上穿但非上升趋势 -> 不报
                elif ctype == 'death' and trend != -1:
                    effective_type = None    # 下穿但非下降趋势 -> 不报
        else:  # match(旧逻辑: 方向必须匹配)
            if ctype == 'golden' and trend == 1:
                effective_type = 'golden'
            elif ctype == 'death' and trend == -1:
                effective_type = 'death'
        if effective_type is None:
            continue
        out.append({
            **c,
            "trend": {1: "上升", -1: "下降", 0: "平坦"}.get(trend, "平坦"),
            "original_type": ctype,
            "type": effective_type,
        })
    return out


def analyze(df, ind, order=3, lookback=150):
    """整合分析。df 需含 high/low/close; ind 为 indicators.compute_all 的结果。"""
    highs = list(df["high"].astype(float))
    lows = list(df["low"].astype(float))
    closes = list(df["close"].astype(float))
    resistance, support = detect_levels(highs, lows, order=order, top_n=3, lookback=lookback)

    divs = []
    macd_divs = detect_divergence(highs, lows, ind.get("macdDif", []), order=order, lookback=lookback)
    for d in macd_divs:
        d["indicator"] = "MACD"
        divs.append(d)

    # 金叉/死叉标记: 原始 DIF/DEA 穿越, 不过滤、不限最近 N 根。
    # K 线图用于视觉标注, 应显示所有金叉/死叉; 趋势驱动的过滤/告警由 scan_engine 负责。
    macd_crosses = detect_macd_cross(
        ind.get("macdDif", []), ind.get("macdDea", []), closes
    )

    return {"support": support, "resistance": resistance, "divergences": divs, "macd_crosses": macd_crosses}


if __name__ == "__main__":
    # 自测: 构造带明确双峰的假数据验证检测
    # 价格: 峰1(idx~40, 值~120) 与 峰2(idx~100, 值~130) -> 价格更高高
    # MACD: 峰1 高(8) 峰2 低(5) -> 指标更低高 -> 顶背离
    np.random.seed(1)
    n = 120
    idx = np.arange(n)
    high = 100 + 20 * np.exp(-((idx - 40) ** 2) / 200.0) + 30 * np.exp(-((idx - 100) ** 2) / 200.0)
    low = 90 - 20 * np.exp(-((idx - 40) ** 2) / 200.0) - 30 * np.exp(-((idx - 100) ** 2) / 200.0)
    close = (high + low) / 2.0
    macd = 8 * np.exp(-((idx - 40) ** 2) / 200.0) + 5 * np.exp(-((idx - 100) ** 2) / 200.0)
    import pandas as pd
    df = pd.DataFrame({"high": high, "low": low, "close": close})
    ind = {"macdDif": list(macd)}
    out = analyze(df, ind, order=5)
    print("支撑位:", out["support"])
    print("压力位:", out["resistance"])
    print("背离:", out["divergences"])
    assert any(d["type"] == "top" for d in out["divergences"]), "应检测到顶背离"
    print("自测通过")
