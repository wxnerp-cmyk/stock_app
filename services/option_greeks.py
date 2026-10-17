"""期权希腊值与隐含波动率计算（纯 Python Black-Scholes Closed-Form）。

说明：
- 这些函数均为纯数学计算，输入标的价格 S、行权价 K、无风险利率 r、
  年化波动率 v、年化到期时间 t、期权方向 option_class，输出希腊字母/隐含波动率。
- 不需要实时行情连接即可在本地验证计算正确性；实时行情只是用来提供 S / 期权价。
- 为什么不用 tqsdk.tafunc：tafunc 每个合约需多次调用且内部走 pandas Series 包装，
  对一个期权链(约 56 行权价 × 双边)调用 600+ 次会把单次请求拖到 500ms+。
  改用纯数学 BS 后，单次请求降到亚毫秒级，满足毫秒级响应需求。
"""

import datetime
import math

# 默认无风险利率（年化）。实盘可改为 SHIBOR/国债收益率。
RISK_FREE_RATE = 0.025
# 当无法取得实时隐含波动率时，BS 希腊值使用的默认年化波动率。
DEFAULT_VOL = 0.25


# ===== 纯 Python Black-Scholes 实现（替代 tafunc/pandas，避免逐合约调用开销）=====
# 说明：原实现每个合约调用 5~6 次 tafunc（pandas Series 包装），56 个行权价 × 双边
# 约 600+ 次调用，单次请求耗时 500ms+。改用纯数学实现后，单次请求降到亚毫秒级。
import math

def _norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def _norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def _bs_core(S, K, r, v, t, option_class):
    """返回 (price, delta, gamma, theta, vega, rho)，纯数学 Closed-Form。参数非法返回 None。"""
    if S is None or K is None or v is None or t is None or t <= 0 or v <= 0 or S <= 0 or K <= 0:
        return None
    S = float(S); K = float(K); r = float(r); v = float(v); t = float(t)
    is_call = str(option_class).upper().startswith("C")
    sqrt_t = math.sqrt(t)
    if sqrt_t == 0:
        price = max(S - K, 0.0) if is_call else max(K - S, 0.0)
        delta = (1.0 if (is_call and S > K) or (not is_call and S < K) else 0.0)
        return {"price": price, "delta": delta, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "rho": 0.0}
    d1 = (math.log(S / K) + (r + 0.5 * v * v) * t) / (v * sqrt_t)
    d2 = d1 - v * sqrt_t
    Nd1 = _norm_cdf(d1); Nd2 = _norm_cdf(d2)
    pdf = _norm_pdf(d1)
    discount = math.exp(-r * t)
    if is_call:
        price = S * Nd1 - K * discount * Nd2
        delta = Nd1
        theta = -(S * pdf * v) / (2.0 * sqrt_t) - r * K * discount * Nd2
        rho = K * t * discount * Nd2
    else:
        price = K * discount * _norm_cdf(-d2) - S * _norm_cdf(-d1)
        delta = -_norm_cdf(-d1)
        theta = -(S * pdf * v) / (2.0 * sqrt_t) + r * K * discount * _norm_cdf(-d2)
        rho = -K * t * discount * _norm_cdf(-d2)
    gamma = pdf / (S * v * sqrt_t)
    vega = S * pdf * sqrt_t
    return {"price": price, "delta": delta, "gamma": gamma, "theta": theta, "vega": vega, "rho": rho}


def annualized_t(exercise_date, now=None):
    """把行权日转成"年化剩余时间"（单位：年）。已过期/非法返回 0.0。

    关键修复：必须用秒级差 (exercise_date - now).total_seconds()，不能用 .days。
    .days 会截断小数，把"不足 1 天"算成 0（如 8/25 到期、当前 8/24 → 0 天），
    导致到期日前一天的期权 t=0，BS 希腊值/理论价/IV 全空。改为年秒数换算。
    """
    if exercise_date is None:
        return 0.0
    if now is None:
        now = datetime.datetime.now()
    if isinstance(exercise_date, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                exercise_date = datetime.datetime.strptime(exercise_date, fmt)
                break
            except ValueError:
                continue
        else:
            return 0.0
    secs = (exercise_date - now).total_seconds()
    if secs <= 0:
        return 0.0
    return secs / (365.0 * 24.0 * 3600.0)


def compute_greeks(S, K, r, v, t, option_class, d1=None):
    """计算单个 (S,K) 点的 BS 希腊值。任一必要量为 None 或 t<=0 时返回 None。"""
    if S is None or K is None or v is None or t is None or t <= 0:
        return None
    try:
        core = _bs_core(S, K, r, v, t, option_class)
        if core is None:
            return None
        return {
            "delta": core["delta"], "gamma": core["gamma"], "theta": core["theta"],
            "vega": core["vega"], "rho": core["rho"],
        }
    except Exception:
        return None


def compute_impv(S, opt_price, K, r, t, option_class, init_v=0.3):
    """由期权市场价反解隐含波动率（纯 Python 二分，无需 tafunc/pandas）。
    需要 S 与期权价均可得；返回年化波动率或 None。"""
    if S is None or opt_price is None or t is None or t <= 0 or S <= 0 or opt_price < 0 or K is None:
        return None
    try:
        cls = "CALL" if str(option_class).upper().startswith("C") else "PUT"
        lo, hi = 1e-4, 5.0
        best = hi
        for _ in range(60):
            mid = 0.5 * (lo + hi)
            p = _bs_core(S, K, r, mid, t, cls)
            if p is None:
                return None
            if p["price"] > float(opt_price):
                hi = mid
            else:
                lo = mid
            best = mid
            if abs(p["price"] - float(opt_price)) < 1e-5:
                break
        return best
    except Exception:
        return None


def compute_chain_greeks(S, strikes, r=RISK_FREE_RATE, v=DEFAULT_VOL, t=None,
                         exercise_date=None, opt_prices=None):
    """批量计算期权链每个行权价的希腊值 + 隐含波动率。

    参数：
      S           标的最新价（None 则全部返回 None，UI 显示 "—"）
      strikes     列表，元素为 dict: {"strike", "option_class", "symbol"(可选)}
      r, v        无风险利率 / 默认波动率
      t           年化剩余时间（优先）；否则由 exercise_date 推算
      exercise_date 行权日字符串（用于推算 t）
      opt_prices  可选，dict: symbol -> 期权最新价，用于计算隐含波动率
    返回：dict: strike -> {"delta","gamma","theta","vega","rho","impv"}
    """
    if t is None:
        t = annualized_t(exercise_date)
    result = {}
    for st in strikes:
        k = st.get("strike")
        cls = st.get("option_class")
        g = compute_greeks(S, k, r, v, t, cls) if S is not None else None
        impv = None
        sym = st.get("symbol")
        if opt_prices and sym and opt_prices.get(sym) is not None:
            impv = compute_impv(S, opt_prices.get(sym), k, r, t, cls)
        entry = g if g is not None else {kk: None for kk in ("delta", "gamma", "theta", "vega", "rho")}
        entry["impv"] = impv
        result[k] = entry
    return result


def _cls(option_class):
    return "CALL" if str(option_class).upper().startswith("C") else "PUT"


def compute_bs_value(S, K, r, v, t, option_class):
    """Black-Scholes 理论价。任一必要量为 None 或 t<=0 时返回 None。"""
    if S is None or K is None or v is None or t is None or t <= 0:
        return None
    try:
        core = _bs_core(S, K, r, v, t, option_class)
        return core["price"] if core else None
    except Exception:
        return None


def compute_intrinsic_value(S, K, option_class):
    """内在价值: CALL=max(S-K,0), PUT=max(K-S,0)。"""
    if S is None or K is None:
        return None
    S = float(S); K = float(K)
    return max(S - K, 0.0) if str(option_class).upper().startswith("C") else max(K - S, 0.0)


def compute_time_value(opt_price, intrinsic):
    """时间价值 = max(期权价 - 内在价值, 0)。"""
    if opt_price is None or intrinsic is None:
        return None
    return max(float(opt_price) - float(intrinsic), 0.0)


def compute_leverage(S, opt_price):
    """杠杆率 = 标的价格 / 期权价格。"""
    if S is None or opt_price is None or opt_price == 0:
        return None
    try:
        return float(S) / float(opt_price)
    except Exception:
        return None


def compute_real_leverage(leverage, delta):
    """真实杠杆率 = 杠杆率 × Delta。"""
    if leverage is None or delta is None:
        return None
    try:
        return float(leverage) * float(delta)
    except Exception:
        return None


def compute_premium_rate(S, opt_price, K, option_class):
    """溢价率（对齐文华 WH6 / 期货终端“溢价率”列口径：权利金占标的价格的百分比）。

    溢价率 = 期权价格 / 标的现价 × 100%   （认购/认沽通用，方向无关）

    说明（根因）：旧实现用“折溢价率/打和溢价率”口径
    (CALL:(K+opt)/S-1, PUT:1-(K-opt)/S)，对实值期权会趋近于 0
    （标的已处于打和点附近），与 WH6 显示的“权利金/标的”不一致。
    实测 bu2609 P5000：旧公式≈0.01%，WH6≈7.01% = opt/S，故统一改为 opt/S。
    K 入参保留以兼容调用方，计算已不再依赖 K。
    """
    if S is None or opt_price is None or S == 0:
        return None
    try:
        S = float(S); opt_price = float(opt_price)
        return opt_price / S * 100.0
    except Exception:
        return None


def compute_trend_degree(last, open_price, high, low):
    """文华趋势度 = (最新价 - 开盘价) / (最高价 - 最低价)。
    范围 [-1, 1]：越接近 ±1 趋势越强，接近 0 越弱。缺失任一价格返回 None。
    """
    if last is None or open_price is None or high is None or low is None:
        return None
    try:
        last = float(last); open_price = float(open_price)
        high = float(high); low = float(low)
        rng = high - low
        if rng == 0:
            return 0.0
        return (last - open_price) / rng
    except Exception:
        return None


def compute_historical_volatility(closes, window=20):
    """由收盘价序列计算年化历史波动率（对数收益率标准差）。
    closes: 时间序列，从旧到新。window 默认 20 日。
    返回小数形式波动率（如 0.2588 表示 25.88%），数据不足返回 None。
    """
    if closes is None or len(closes) < window + 1:
        return None
    try:
        vals = [float(c) for c in closes[-(window + 1):]]
        if any(v <= 0 for v in vals):
            return None
        import math
        returns = [math.log(vals[i] / vals[i - 1]) for i in range(1, len(vals))]
        n = len(returns)
        if n < 2:
            return None
        mean = sum(returns) / n
        var = sum((r - mean) ** 2 for r in returns) / (n - 1)
        if var <= 0:
            return None
        return math.sqrt(var * 252.0)  # 252 个交易日年化
    except Exception:
        return None


def compute_volume_ratio(today_volume, past_volumes):
    """量比 = 今日成交量 / 过去N日平均成交量。返回倍数，无历史数据返回 None。
    文华口径：今日每分钟均量 / 前5日每分钟均量，因交易时长固定，等价于
    今日成交量 / 前5日平均日成交量。
    """
    if today_volume is None or not past_volumes:
        return None
    try:
        avg = sum(float(v) for v in past_volumes) / len(past_volumes)
        if avg == 0:
            return None
        return float(today_volume) / avg
    except Exception:
        return None


def compute_moneyness_value(S, K, option_class):
    """虚实值(带符号金额): CALL = S-K, PUT = K-S。正=实值, 负=虚值, 0=平值。"""
    if S is None or K is None:
        return None
    try:
        S = float(S); K = float(K)
        if str(option_class).upper().startswith("C"):
            return S - K
        return K - S
    except Exception:
        return None


def compute_vix_estimate(chain, live):
    """简化版 VIX 估算：取平值(最接近标价的行权价)处 CALL/PUT 的隐含波动率均值 ×100。
    天勤无官方 VIX 字段，此为估算值，仅作参考。无足够数据返回 None。"""
    if not chain or not live:
        return None
    meta = live.get("__underlying__", {})
    S = meta.get("last")
    if S is None:
        return None
    best, best_d = None, None
    for st in chain.get("strikes", []):
        k = st.get("strike")
        if k is None:
            continue
        d = abs(float(k) - float(S))
        if best_d is None or d < best_d:
            best_d, best = d, st
    if not best:
        return None
    ivs = []
    for side in ("call", "put"):
        entry = best.get(side)
        if entry and entry.get("symbol"):
            gd = live.get(entry["symbol"])
            if gd and gd.get("impv") is not None:
                ivs.append(float(gd["impv"]))
    if not ivs:
        return None
    return (sum(ivs) / len(ivs)) * 100.0
