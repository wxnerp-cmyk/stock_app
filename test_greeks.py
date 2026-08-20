import sys, datetime
sys.path.insert(0, ".")
from services import option_greeks as og

# 1) ATM 看涨 delta 应约 0.5
S, K, r, v, t = 45000.0, 45000.0, 0.025, 0.25, 30/365
g = og.compute_greeks(S, K, r, v, t, "CALL")
print("ATM CALL:", {k: round(x, 4) for k, x in g.items()})
assert 0.45 < g["delta"] < 0.55, "ATM delta 应约 0.5"
assert g["gamma"] > 0 and g["vega"] > 0 and g["theta"] < 0, "gamma/vega>0, theta<0"

# 2) 深度实值看跌 delta 接近 -1
g2 = og.compute_greeks(30000.0, 45000.0, r, v, t, "PUT")
print("deep ITM PUT:", {k: round(x, 4) for k, x in g2.items()})
assert g2["delta"] < -0.9, "深度实值看跌 delta 应近 -1"

# 3) 隐含波动率反解
impv = og.compute_impv(S, 1200.0, K, r, t, "CALL", init_v=0.3)
print("impv(期权价1200):", round(impv, 4))
assert 0.1 < impv < 0.6

# 4) 批量链
chain = [
    {"strike": 44000, "option_class": "CALL", "symbol": "X.c2508C44000"},
    {"strike": 45000, "option_class": "CALL", "symbol": "X.c2508C45000"},
    {"strike": 45000, "option_class": "PUT",  "symbol": "X.c2508P45000"},
]
res = og.compute_chain_greeks(S, chain, v=v, exercise_date="2026-09-16",
                              opt_prices={"X.c2508C44000": 1100.0})
print("chain greeks keys:", list(res.keys()))
assert set(res[44000].keys()) == {"delta", "gamma", "theta", "vega", "rho", "impv"}
assert res[44000]["impv"] is not None

# 5) 无标的价格 -> 全部 None（UI 显示 —）
res_none = og.compute_chain_greeks(None, chain, v=v, exercise_date="2026-09-16")
assert all(res_none[k]["delta"] is None for k in res_none)
print("无标的价格回退 OK")

print("ALL GREENS OK")
