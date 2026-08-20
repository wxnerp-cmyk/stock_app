import sys, os, time, traceback
sys.path.insert(0, r"D:/mylog/stock_app")
# 强制直连天勤(清除本机透明代理, 否则连不上 auth.shinnytech.com)
for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_k, None)
os.environ["NO_PROXY"] = "*"; os.environ["no_proxy"] = "*"

from services import futures_db

LOG = r"D:/mylog/stock_app/_sync_incr.log"
mode = sys.argv[1] if len(sys.argv) > 1 else "all"
exchanges = ['SSE', 'SZSE'] if mode == "sse" else None

def _w(line):
    print(line, flush=True)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def progress(total, done, msg):
    _w(f"[{time.strftime('%H:%M:%S')}] ({done}/{total}) {msg}")

def logmsg(s):
    _w(f"[{time.strftime('%H:%M:%S')}] {s}")

logmsg(f"=== 同步开始 mode={mode} exchanges={exchanges} (incremental, 每批落库) ===")
t0 = time.time()
try:
    n = futures_db.sync_options_from_tqsdk(
        months_ahead=18, months_back=0,
        progress_cb=progress, incremental=True, exchanges=exchanges)
except Exception:
    logmsg("!!! 同步异常: " + traceback.format_exc())
    n = -1
logmsg(f"=== 同步结束: 入库 {n} 条, 用时 {time.time()-t0:.1f}s ===")
