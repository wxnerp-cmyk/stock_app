import sys, os, time
sys.path.insert(0, r"D:/mylog/stock_app")
# 强制直连天勤(清除本机透明代理)
for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_k, None)
os.environ["NO_PROXY"] = "*"; os.environ["no_proxy"] = "*"

from services import futures_db

LOG = r"D:/mylog/stock_app/_sync_options.log"

def cb(total, done, msg):
    line = f"[{time.strftime('%H:%M:%S')}] ({done}/{total}) {msg}"
    print(line, flush=True)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

print(f"=== 期权同步开始 {time.strftime('%Y-%m-%d %H:%M:%S')} (months_ahead=18, months_back=0) ===", flush=True)
t0 = time.time()
n = futures_db.sync_options_from_tqsdk(months_ahead=18, months_back=0, progress_cb=cb)
dt = time.time() - t0
print(f"=== 期权同步结束: 入库 {n} 条, 用时 {dt:.1f}s ===", flush=True)
try:
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"DONE total={n} time={dt:.1f}s\n")
except Exception:
    pass
