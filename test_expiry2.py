"""诊断脚本：测试 fetch_expiry_data 对 2608 和 2609 的返回"""
import sys, time, datetime
sys.path.insert(0, "d:/mylog/stock_app")
from core.tqsdk_client import TqSdkClient

client = TqSdkClient("zhubajie", "jinhu303")

# 手工测试 2608 和 2609 几个关键合约
test_symbols = [
    ("SHFE.cu2608", "沪铜2608"),
    ("SHFE.cu2609", "沪铜2609"),
    ("SHFE.cu2610", "沪铜2610"),
    ("DCE.i2608", "铁矿2608"),
    ("DCE.i2609", "铁矿2609"),
    ("DCE.i2610", "铁矿2610"),
    ("CZCE.SR608", "白糖608"),
    ("CZCE.SR609", "白糖609"),
    ("CZCE.SR610", "白糖610"),
    ("CFFEX.IF2608", "IF2608"),
    ("CFFEX.IF2609", "IF2609"),
]

print("=" * 60)
print("订阅合约并等待数据...")
for sym, name in test_symbols:
    try:
        client.api.get_quote(sym)
    except Exception as e:
        print(f"  [失败] {sym}: {e}")

client.api.wait_update(deadline=time.time() + 5)

print("\n" + "=" * 60)
print("到期日结果:")
for sym, name in test_symbols:
    try:
        q = client.api.get_quote(sym)
        exp = getattr(q, 'expire_datetime', 0)
        if exp and exp > 0:
            exp_str = datetime.datetime.fromtimestamp(exp).strftime("%Y-%m-%d %H:%M")
        else:
            exp_str = "无数据 (可能是已过期合约)"
        print(f"  {sym:20s} {name:10s} 到期: {exp_str}")
    except Exception as e:
        print(f"  {sym:20s} {name:10s} 错误: {e}")

client.close()
print("\n完成!")