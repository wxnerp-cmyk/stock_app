"""快速测试天勤连接是否正常"""
from tqsdk import TqApi, TqAuth

TEST_SYMBOLS = [
    "SHFE.cu2501",        # 铜
    "SHFE.rb2501",        # 螺纹钢
    "DCE.i2501",          # 铁矿石  
    "CFFEX.IF2503",       # 沪深300
]

print("连接天勤...")
api = TqApi(auth=TqAuth("zhubajie", "jinhu303"))

print("订阅合约...")
for sym in TEST_SYMBOLS:
    try:
        api.get_quote(sym)
        print(f"  ✓ {sym}")
    except Exception as e:
        print(f"  ✗ {sym}: {e}")

print("\n等待行情...")
count = 0
while count < 5:
    api.wait_update(timeout=5)
    count += 1
    for sym in TEST_SYMBOLS:
        q = api.get_quote(sym)
        try:
            print(f"  {sym} last_price={float(q.last_price):.2f} bid={float(q.bid_price1):.2f} ask={float(q.ask_price1):.2f}")
        except:
            print(f"  {sym} 暂无数据")

api.close()
print("测试完成")