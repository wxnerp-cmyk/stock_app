import subprocess, sys, os, time
sys.path.insert(0, r"D:/mylog/stock_app")
# 强制直连天勤
for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_k, None)
os.environ["NO_PROXY"] = "*"; os.environ["no_proxy"] = "*"

from services import futures_db

LOG = r"D:/mylog/stock_app/_sync_supervisor.log"
PY = r"D:/mylog/stock_app/venv/Scripts/python.exe"
RUN = r"D:/mylog/stock_app/_sync_run.py"
OUT = r"D:/mylog/stock_app/_sync_run.out"
MY_PID = os.getpid()

def slog(s):
    print(s, flush=True)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(s + "\n")
    except Exception:
        pass

def counts():
    c = futures_db.get_connection()
    try:
        with c.cursor() as cur:
            cur.execute("SELECT exchange,COUNT(*) FROM t_option_expiry WHERE exchange IN ('SSE','SZSE') GROUP BY exchange")
            return dict(cur.fetchall())
    finally:
        c.close()

def run_sync(mode, timeout=600):
    """启动一次同步子进程, 超时则只杀该子进程(连同其线程树), 不波及监督脚本/其他服务"""
    try:
        with open(OUT, "ab") as fo:
            p = subprocess.Popen([PY, RUN, mode], stdout=fo, stderr=subprocess.STDOUT)
            try:
                p.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                slog(f"[sup] {mode} 本轮超时{timeout}s, 仅终止同步子进程(pid={p.pid})")
                try:
                    subprocess.run(["taskkill", "/F", "/T", "/PID", str(p.pid)], check=False)
                except Exception:
                    pass
                try:
                    p.kill()
                except Exception:
                    pass
    except Exception as e:
        slog(f"[sup] 启动同步子进程失败: {e}")

slog(f"########## 监督脚本启动 {time.strftime('%Y-%m-%d %H:%M:%S')} (pid={MY_PID}) ##########")

# ---- 阶段1: 优先抓 SSE+SZSE(用户最缺的) ----
for r in range(1, 9):
    b = counts()
    slog(f"[sup] SSE+SZSE 第{r}轮 开始前={b}")
    run_sync("sse", timeout=600)
    a = counts()
    slog(f"[sup] SSE+SZSE 第{r}轮 结束后={a}")
    if a.get('SSE', 0) > 0 and a.get('SZSE', 0) > 0:
        slog("[sup] >>> SSE 与 SZSE 均已入库, 阶段1完成")
        break
    if a == b and r >= 3:
        slog("[sup] 连续3轮无进展, 停止阶段1(可能试用版限速卡死)")
        break
    time.sleep(3)

# ---- 阶段2: 补全其余(郑商所远月等), 时间允许就多抓 ----
for r in range(1, 5):
    slog(f"[sup] 其余数据 第{r}轮 开始")
    run_sync("all", timeout=600)
    time.sleep(3)

# ---- 收尾: 验证并决定是否关机 ----
final = counts()
slog(f"[sup] 最终 SSE/SZSE={final}")
ok = final.get('SSE', 0) > 0 and final.get('SZSE', 0) > 0
note = ("SUCCESS: SSE与SZSE ETF期权已补齐, 其余数据也已尽力同步" if ok
        else "PARTIAL: SSE/SZSE 仍未补齐(试用版限速), 未关机, 留待后续重试")
try:
    with open(r"D:/mylog/stock_app/_SYNC_RESULT.txt", "w", encoding="utf-8") as f:
        f.write(note + "\n" + str(final) + "\n")
except Exception:
    pass
slog("[sup] " + note)

if ok:
    slog("[sup] 2分钟后自动关机 (若想取消请于关机前运行: shutdown /a)")
    os.system("shutdown /s /t 120 /f")
else:
    slog("[sup] 未完全完成, 保持开机以便重试, 不执行关机")
