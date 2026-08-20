import sys, os, time, collections
sys.path.insert(0, r"D:/mylog/stock_app")
# 强制直连天勤: 清除本机透明代理, 否则 TqSdk 的 requests 走 127.0.0.1:10808 会超时
for _k in ("HTTP_PROXY","HTTPS_PROXY","http_proxy","https_proxy","ALL_PROXY","all_proxy"):
    os.environ.pop(_k, None)
os.environ["NO_PROXY"] = "*"
os.environ["no_proxy"] = "*"

from tqsdk import TqApi, TqAuth

api = None
for attempt in range(1, 4):
    try:
        print(f"[探测] 第{attempt}次尝试连接天勤...")
        api = TqApi(auth=TqAuth("zhubajie", "jinhu303"))
        print("[探测] 连接成功")
        break
    except Exception as e:
        print(f"[探测] 第{attempt}次失败: {type(e).__name__}: {str(e)[:120]}")
        time.sleep(5)

if api is None:
    print("[探测] 多次连接失败, 终止")
    sys.exit(1)

t0 = time.time()
print("[探测] 拉取未到期 OPTION 列表...")
dq = api.query_quotes(ins_class="OPTION", expired=False)
keys = list(dq.keys()) if hasattr(dq, "keys") else list(dq)
print(f"[探测] OPTION 未到期共 {len(keys)} 个, 用时 {time.time()-t0:.1f}s")

by_ex = collections.Counter(k.split(".")[0] for k in keys)
print("[探测] 按交易所(前缀)统计未到期 OPTION:")
for ex, n in sorted(by_ex.items()):
    print(f"    {ex:6} {n}")

print()
print("[探测] SSE/SZSE(ETF期权) 是否出现:")
sse = [k for k in keys if k.startswith("SSE.")]
szse = [k for k in keys if k.startswith("SZSE.")]
print(f"    SSE.{len(sse)} 个, 样例: {sse[:6]}")
print(f"    SZSE.{len(szse)} 个, 样例: {szse[:6]}")

print()
print("[探测] CZCE.PX (对二甲苯) 合约月份:")
px = sorted(set(k for k in keys if k.startswith("CZCE.PX")))
print(f"    {len(px)} 个: {px}")

print()
print("[探测] CZCE 未到期期权总品种(按品种字母):", len(set(k.split('.')[1][:2] for k in keys if k.startswith('CZCE.'))))

api.close()
print("[探测] 完成")
