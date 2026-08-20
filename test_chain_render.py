import sys, types

# 桩 center_client
fc = types.ModuleType("center_client")
class ServiceConfig:
    def __init__(self, **kw): pass
fc.ServiceConfig = ServiceConfig
fc.register_service = lambda *a, **k: None
sys.modules["center_client"] = fc

# 桩 core.tqsdk_client, 避免真实联网连接
ct = types.ModuleType("core.tqsdk_client")
class FakeApi:
    def get_quote(self, s):
        return types.SimpleNamespace(last_price=45000.0)
    def wait_update(self, deadline=None):
        pass
class TqSdkClient:
    def __init__(self, u, p):
        self.api = FakeApi()
    def connect(self):
        pass
    def get_kline(self, *a, **k):
        return None
ct.TqSdkClient = TqSdkClient
sys.modules["core.tqsdk_client"] = ct

import main  # 导入即触发底部 get_slot + start_auto_timer(用桩, 不联网)

# 1) _greek_html 格式化
print("greek(有值):", main._greek_html({"delta":0.5257,"gamma":0.0001,"theta":-8369.0,"vega":5136.0,"rho":1834.0,"impv":0.2243}))
print("greek(None):", main._greek_html(None))
assert "Δ0.53" in main._greek_html({"delta":0.5257,"gamma":0.0001,"theta":-8369.0,"vega":5136.0,"rho":1834.0,"impv":0.2243})
assert main._greek_html(None) == "—"

# 2) _get_chain_live_greeks 实时路径(用桩 api 提供 S=45000)
chain = {
    "exercise_dates": ["2026-09-16"],
    "strikes": [
        {"strike": 44000, "call": {"symbol": "X.c2508C44000"}, "put": {"symbol": "X.c2508P44000"}},
        {"strike": 45000, "call": {"symbol": "X.c2508C45000"}, "put": {"symbol": "X.c2508P45000"}},
    ],
}
res = main._get_chain_live_greeks("SHFE.cu2508", chain)
print("live greeks:", res)
assert res is not None
assert res["X.c2508C45000"]["delta"] is not None
assert res["X.c2508P45000"]["delta"] is not None
assert -1.0 < res["X.c2508C45000"]["delta"] < 1.0
assert res["X.c2508C45000"]["impv"] is not None  # 桩期权价=45000 也能反解
print("LIVE GREEKS PATH OK")

print("ALL CHAIN RENDER TESTS OK")
