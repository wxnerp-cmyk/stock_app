# -*- coding: utf-8 -*-
"""
期货/期权 K 线技术指标库 (纯 numpy/pandas, 不依赖 tqsdk.tafunc)。

设计原则:
- 输入: 含 open/high/low/close/volume 的 pandas.DataFrame (可选 open_interest)
- 输出: 各指标序列 (list, NaN/inf -> None), 由调用方拼进 K 线 payload
- 单次计算 O(n), 对 120~450 根 K 线毫秒级完成, 满足 stock_app 性能约束
- 不含 tqsdk.tafunc, 避免逐合约 pandas 调用的性能陷阱
"""
import numpy as np
import pandas as pd


def to_list(s):
    """Series -> list, NaN/inf -> None, 保留 4 位小数。"""
    if s is None:
        return []
    arr = np.asarray(s, dtype="float64")
    out = []
    for v in arr:
        if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
            out.append(None)
        else:
            out.append(round(float(v), 4))
    return out


def ma(close, n):
    s = pd.Series(close, dtype="float64").rolling(n, min_periods=n).mean()
    return to_list(s)


def ema(series, n):
    s = pd.Series(series, dtype="float64")
    return to_list(s.ewm(span=n, adjust=False, min_periods=n).mean())


def macd(close, fast=12, slow=26, signal=9):
    """返回 (dif, dea, bar)。bar = (dif-dea)*2。"""
    cl = pd.Series(close, dtype="float64")
    ema_fast = cl.ewm(span=fast, adjust=False, min_periods=fast).mean()
    ema_slow = cl.ewm(span=slow, adjust=False, min_periods=slow).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False, min_periods=signal).mean()
    bar = (dif - dea) * 2
    return to_list(dif), to_list(dea), to_list(bar)


def boll(close, n=20, k=2, p=None):
    """返回 (upper, mid, lower)。

    参数语义(对齐主流看盘软件的 BOLL(N,P,M) 写法):
      n = 统计周期 N (标准差用 N 根)
      p = 中轨均线周期 P —— 缺省 / None / 0 时表示"与 N 相同"(即标准 BOLL)
      k = 标准差倍数 M
    中轨 = MA(P), 上轨 = 中轨 + M*STD(N), 下轨 = 中轨 - M*STD(N)。
    默认 boll(c) == BOLL(20,20,2), 与改造前逐值一致。
    """
    cl = pd.Series(close, dtype="float64")
    pn = int(p) if p else int(n)
    pn = pn if pn > 0 else int(n)
    mid = cl.rolling(pn, min_periods=pn).mean()
    std = cl.rolling(n, min_periods=n).std(ddof=0)
    upper = mid + k * std
    lower = mid - k * std
    return to_list(upper), to_list(mid), to_list(lower)


def rsi(close, n=14):
    """Wilder 平滑 RSI。"""
    cl = pd.Series(close, dtype="float64")
    delta = cl.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    avg_loss = loss.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi_s = 100 - 100 / (1 + rs)
    return to_list(rsi_s)


def kdj(high, low, close, n=9, m1=3, m2=3):
    """标准 KDJ: K=SMA(RSV,m1,1) 初值50, D=SMA(K,m2,1) 初值50, J=3K-2D。

    ⚠️ 2026-09-15 修: 旧实现把递推系数写死成 2/3、1/3, 形参 m1/m2 收了但**从不使用** ——
    用户把 (9,3,3) 改成 (9,5,5) 时结果静默不变。现改为真正按 m1/m2 递推:
        K[i] = (m1-1)/m1 * K[i-1] + 1/m1 * RSV[i]
        D[i] = (m2-1)/m2 * D[i-1] + 1/m2 * K[i]
    m1=m2=3 时退化为原式, 逐值完全一致(已回归验证)。
    """
    h = pd.Series(high, dtype="float64")
    l = pd.Series(low, dtype="float64")
    c = pd.Series(close, dtype="float64")
    llv = l.rolling(n, min_periods=n).min()
    hhv = h.rolling(n, min_periods=n).max()
    rng = (hhv - llv).replace(0, np.nan)
    rsv = (c - llv) / rng * 100
    rsv = rsv.fillna(50.0)
    arr = np.asarray(rsv, dtype="float64")
    size = len(arr)
    if size == 0:
        return [], [], []
    k = np.full(size, 50.0)
    d = np.full(size, 50.0)
    a1 = 1.0 / (float(m1) if m1 else 3.0)
    a2 = 1.0 / (float(m2) if m2 else 3.0)
    for i in range(1, size):
        k[i] = (1.0 - a1) * k[i - 1] + a1 * arr[i]
        d[i] = (1.0 - a2) * d[i - 1] + a2 * k[i]
    j = 3 * k - 2 * d
    return to_list(pd.Series(k)), to_list(pd.Series(d)), to_list(pd.Series(j))


def atr(high, low, close, n=14):
    """Wilder 平滑真实波幅。"""
    h = pd.Series(high, dtype="float64")
    l = pd.Series(low, dtype="float64")
    c = pd.Series(close, dtype="float64")
    prev = c.shift(1)
    tr = pd.concat(
        [(h - l), (h - prev).abs(), (l - prev).abs()], axis=1
    ).max(axis=1)
    atr_s = tr.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return to_list(atr_s)


def cci(high, low, close, n=14):
    """顺势指标, 系数 0.015。"""
    h = pd.Series(high, dtype="float64")
    l = pd.Series(low, dtype="float64")
    c = pd.Series(close, dtype="float64")
    tp = (h + l + c) / 3
    ma_tp = tp.rolling(n, min_periods=n).mean()
    md = tp.rolling(n, min_periods=n).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    cci_s = (tp - ma_tp) / (0.015 * md.replace(0, np.nan))
    return to_list(cci_s)


def obv(close, volume):
    """能量潮: 涨跌累计成交量。"""
    c = pd.Series(close, dtype="float64")
    v = pd.Series(volume, dtype="float64")
    sign = np.sign(c.diff().fillna(0))
    return to_list((sign * v).cumsum())


def vr(close, volume, n=26):
    """成交量比率 = 上涨日量 / 下跌日量 * 100。"""
    c = pd.Series(close, dtype="float64")
    v = pd.Series(volume, dtype="float64")
    diff = c.diff().fillna(0)
    up = pd.Series(np.where(diff > 0, v.values, 0.0))
    down = pd.Series(np.where(diff < 0, v.values, 0.0))
    avs = up.rolling(n, min_periods=1).sum()
    bvs = down.rolling(n, min_periods=1).sum()
    return to_list((avs / bvs.replace(0, np.nan)) * 100)


def vroc(volume, n=10):
    """量变动速率 = (量/前 n 期量 - 1) * 100。"""
    v = pd.Series(volume, dtype="float64")
    vroc_s = v / v.shift(n).replace(0, np.nan) - 1
    return to_list(vroc_s * 100)


def wvad(open_, high, low, close, volume, n=30):
    """威廉变异离散量 = Σ((收-开)/(高-低)*量), 周期 n 求和。"""
    o = pd.Series(open_, dtype="float64")
    h = pd.Series(high, dtype="float64")
    l = pd.Series(low, dtype="float64")
    c = pd.Series(close, dtype="float64")
    v = pd.Series(volume, dtype="float64")
    wv = (c - o) / (h - l).replace(0, np.nan) * v
    return to_list(wv.rolling(n, min_periods=1).sum())


def pvt(close, volume):
    """价量趋势 = Σ(涨跌幅 * 量)。"""
    c = pd.Series(close, dtype="float64")
    v = pd.Series(volume, dtype="float64")
    chg = c.pct_change().fillna(0)
    return to_list((chg * v).cumsum())


def ad(high, low, close, volume):
    """Accumulation/Distribution Line = Σ(CLV * 量)。"""
    h = pd.Series(high, dtype="float64")
    l = pd.Series(low, dtype="float64")
    c = pd.Series(close, dtype="float64")
    v = pd.Series(volume, dtype="float64")
    clv = ((c - l) - (h - c)) / (h - l).replace(0, np.nan)
    return to_list((clv * v).cumsum())


def vol_ratio(volume, n=5):
    """量比 = 当日量 / 近 n 日均量。"""
    v = pd.Series(volume, dtype="float64")
    ma_v = v.rolling(n, min_periods=1).mean()
    return to_list(v / ma_v.replace(0, np.nan))


def oi_series(open_interest):
    if open_interest is None:
        return []
    return to_list(pd.Series(open_interest, dtype="float64"))


def oi_diff(open_interest):
    """日增仓 = 当前持仓量 - 前一日持仓量。"""
    if open_interest is None:
        return []
    return to_list(pd.Series(open_interest, dtype="float64").diff())


def compute_all(df, include_oi=True):
    """一次性计算全部指标, 返回 dict(指标名 -> list)。

    df 需含: open/high/low/close/volume (float)
    可选: open_interest
    """
    o = df["open"].astype(float)
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)
    v = df["volume"].astype(float)

    bu, bm, bl = boll(c, 20, 2)
    mdif, mdea, mbar = macd(c)
    kk, kd, kj = kdj(h, l, c)
    res = {
        # 主图用
        "ma5": ma(c, 5), "ma10": ma(c, 10), "ma20": ma(c, 20), "ma60": ma(c, 60),
        "bollUpper": bu, "bollMid": bm, "bollLower": bl,
        # 趋势/摆动副图
        "macdDif": mdif, "macdDea": mdea, "macdBar": mbar,
        "rsi6": rsi(c, 6), "rsi12": rsi(c, 12),
        "kdjK": kk, "kdjD": kd, "kdjJ": kj,
        "atr14": atr(h, l, c, 14),
        "cci14": cci(h, l, c, 14),
        # 量价副图
        "obv": obv(c, v), "vr": vr(c, v, 26), "vroc": vroc(v, 10),
        "wvad": wvad(o, h, l, c, v, 30), "pvt": pvt(c, v), "ad": ad(h, l, c, v),
        "volRatio": vol_ratio(v, 5),
    }
    if include_oi and "open_interest" in df.columns:
        oi = df["open_interest"].astype(float)
        res["oi"] = oi_series(oi)
        res["oiDiff"] = oi_diff(oi)
    return res


if __name__ == "__main__":
    # 简单自测: 用随机游走数据验证不抛异常
    np.random.seed(0)
    n = 200
    close = np.cumsum(np.random.randn(n)) + 100
    df = pd.DataFrame({
        "open": close + np.random.randn(n) * 0.5,
        "high": close + np.abs(np.random.randn(n)) * 0.8,
        "low": close - np.abs(np.random.randn(n)) * 0.8,
        "close": close,
        "volume": np.random.randint(1000, 5000, n).astype(float),
        "open_interest": np.random.randint(1000, 5000, n).astype(float),
    })
    out = compute_all(df)
    print("指标数量:", len(out))
    for k, v in out.items():
        assert isinstance(v, list) and len(v) == n, f"{k} 长度异常"
    print("自测通过:", sorted(out.keys()))
