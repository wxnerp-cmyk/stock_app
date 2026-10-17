# -*- coding: utf-8 -*-
"""
分钟级 MACD 金叉/死叉实时引擎（隔离于主行情链路）。

设计要点（不影响全局其它功能）:
- 数据源=腾讯 tx(2026-09-28 迁移): 每轮 fetch_quotes_batch 批量拉最新价(一次50只,
  实测 200 只 ≈ 4 请求 0.1s), 历史预热用 tx 1 分钟线 —— 完全不依赖天勤账号/会话,
  不占天勤订阅, 不受扫描账号权限/到期影响(原"专用扫描账号 slot"已移除)。
- 独立 daemon 线程；全程 try/except，任何异常仅记日志并继续，绝不抛到主循环 / 不阻塞 Flask。
- 用 last_price 在引擎内聚合成 1 分钟 bar（o/h/l/c），bar 收盘时做增量 MACD 递推。
- 增量递推: 每只维护 (ema12, ema26, dea, prev_dif, prev_dea)；新 bar 关闭时 6 次浮点乘加更新，
  与 indicators.macd 全量重算数学等价（实测误差 ~5e-5）。
- 金叉/死叉检测基于 DIF/DEA 穿越；结果写 t_stock_cross_cache.min_* 列（独立 UPSERT，不触碰 w_/m_ 列）
  与内存 _MINUTE_CACHE；并沉淀到 t_stock_cross_event(period='minute')。
- 分级报警: 引擎只写缓存；前端 skCheckAlerts 对 min_recent 上升沿弹窗+响铃+通知（仅自选股）。
- 与每日 11:31/15:01 全市场批处理(_SCAN) 协作：批处理运行期间暂停聚合, 避免抢网络/GIL。

冷启动说明: EMA 初值取首根 bar 收盘价，需 ~26 根（≈26 分钟）收敛到真实值；此前不报交叉，属正常。
  自选股优先用 tx 1 分钟线预热(120 根), 从开盘即准确。
"""
import time
import threading
from datetime import datetime

# 增量 MACD 递推系数 (EMA alpha = 2/(N+1))
_A12 = 2.0 / (12 + 1)
_A26 = 2.0 / (26 + 1)
_A9 = 2.0 / (9 + 1)

# 分钟 bar 收盘后"近期"判定窗口(根): 最近 N 个分钟内发生的金叉算 recent
RECENT_BARS = 30
# 主循环扫描间隔（秒）
POLL_INTERVAL = 3
# 分批订阅每批大小（限流/减轻单次冲击）
SUB_BATCH = 200
SUB_BATCH_PAUSE = 0.2
# 冷启动跳过的 bar 数（EMA 收敛）
WARMUP_BARS = 26
# 全 A 股宇宙订阅开关(2026-09-08 实测裁定, 默认关):
# 实测把几百~几千只股票灌进会话后, 天勤会对该会话持续限流 —— 推送几分钟后整条
# 行情流静默(got=False), 连已订阅的指数/K线一起冻死("页面时间在走、数据不动"),
# 而全新会话同样负载完全正常。试用账号下全宇宙订阅本来就送不下数据, 纯赔本。
# 引擎只订自选股(报警目标本就只有自选股)。换成带大额度行情授权的正式账号后可改 True。
ENABLE_UNIVERSE_SUBSCRIBE = False

# 惰性启停(2026-09-08 需求裁定): 分钟引擎不再随服务启动常驻, 只有"打开监控页/调用监控接口"
# 才启动; 连续 IDLE_STOP_SEC 秒没有任何触碰(页面关闭或离开)则自动停止并释放订阅,
# 避免无人使用时后台长期空转订阅、把天勤会话拖到限流(2026-09-08 页面数据冻住的根因)。
IDLE_STOP_SEC = 300

_MINUTE_CACHE = {}          # symbol -> {min_golden, min_golden_idx, min_death, min_death_idx, min_recent}
_MINUTE_LOCK = threading.Lock()
_ENGINE = None
_ENGINE_STARTED = False
_LAST_TOUCH = 0.0           # 最近一次"有人在看监控"的时间戳(0=从未启动)


def _now_minute_bucket():
    n = datetime.now()
    return n.replace(second=0, microsecond=0)


def _fmt_bar_minute(dts, idx):
    """取 bar 下标 idx 对应的时间, 返回 'YYYY-MM-DD HH:MM'(北京时间)。

    2026-09-14 修复(浦发银行 min_golden='1.78893558e+18' 事故):
    天勤 K 线的 datetime 列是纳秒时间戳, 且缓冲区未填充的行是 NaN -> pandas 会把整列
    提升为 float64; 此时 str(np.float64(1.78893558e18)) 得到 '1.78893558e+18'
    (科学计数法), 一旦直接写库就是乱码。
    故必须走 futures_kline._dt_local(int64/float64 都安全), 且任何异常只返回 '',
    绝不把纳秒数字落库。
    """
    try:
        if not dts or idx is None:
            return ""
        if not (0 <= idx < len(dts)):
            return ""
        return dts[idx].strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def _safe_minute_str(v):
    """兜底过滤: min_golden/min_death 只允许 'YYYY-MM-DD HH:MM' 形态, 其余判脏丢弃。
    形如 1.78893558e+18 / 1788935580000000000 的纳秒时间戳一律返回 None。"""
    s = str(v or "").strip()
    if not s:
        return None
    low = s.lower()
    if "e+" in low or "e-" in low:
        return None
    if s.isdigit() and len(s) >= 13:      # 纯数字且 >= 13 位 = 纳秒/毫秒时间戳
        return None
    return s


def _cross_type(prev_dif, prev_dea, dif, dea):
    """基于 DIF/DEA 穿越判定: 'golden' / 'death' / None。"""
    if prev_dif is None or prev_dea is None:
        return None
    p_pos = prev_dif - prev_dea
    c_pos = dif - dea
    if p_pos <= 0 < c_pos:
        return "golden"
    if p_pos >= 0 > c_pos:
        return "death"
    return None


def _write_minute(symbol, golden_date, golden_idx, death_date, death_idx, recent):
    """UPSERT 只更新 min_* 列，绝不触碰 w_/m_ 列。"""
    # 兜底: 纳秒时间戳等脏值不进库、不进内存缓存(前端直接展示 min_golden)
    golden_date = _safe_minute_str(golden_date)
    death_date = _safe_minute_str(death_date)
    try:
        from services.stock_kline import _cross_conn
        conn = _cross_conn(); cur = conn.cursor()
        cur.execute(
            """INSERT INTO t_stock_cross_cache
               (symbol, min_golden, min_golden_idx, min_death, min_death_idx, min_recent, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s, NOW())
               ON DUPLICATE KEY UPDATE
                 min_golden=VALUES(min_golden), min_golden_idx=VALUES(min_golden_idx),
                 min_death=VALUES(min_death), min_death_idx=VALUES(min_death_idx),
                 min_recent=VALUES(min_recent), updated_at=NOW()""",
            (symbol, golden_date or None, golden_idx, death_date or None, death_idx, int(recent)))
        conn.commit(); conn.close()
        with _MINUTE_LOCK:
            _MINUTE_CACHE[symbol] = {
                "min_golden": golden_date, "min_golden_idx": golden_idx,
                "min_death": death_date, "min_death_idx": death_idx,
                "min_recent": bool(recent)}
    except Exception as e:
        print(f"[分钟引擎] 写缓存失败 {symbol}: {e}")


def _record_minute_event(symbol, ctype, date, price, dif, dea):
    """1 分钟引擎实时金叉/死叉 -> 统一事件表(ts=bar 起点, src='rt')。
    委托 stock_kline._record_events, 保证 ts/src 字段与回溯(scan)条目口径一致。"""
    try:
        from services.stock_kline import _record_events
        _record_events(symbol, "minute",
                       [{"type": ctype, "ts": date, "price": price, "dif": dif, "dea": dea}],
                       src="rt")
    except Exception:
        pass


class MinuteCrossEngine:
    def __init__(self):
        self.states = {}        # symbol -> state dict
        self._watch = set()     # 自选股(优先历史预热/报警目标)
        self._seeded = set()    # 已尝试过历史预热的 symbol(避免重复拉取)
        self.stop = False
        self.thread = None
        self.last_error = ""
        self.last_tick = 0
        self.subscribed_n = 0   # (兼容字段) 当前监控标的数
        self.updated_n = 0
        self.started_at = time.time()

    def set_watch(self, syms):
        """设置自选股(报警目标)。优先用历史 1 分钟 K 线预热 EMA。"""
        try:
            self._watch = set(str(s) for s in (syms or []))
        except Exception:
            self._watch = set()

    @staticmethod
    def _universe():
        try:
            from services.stock_kline import _STOCK_LIST
            return [x["symbol"] for x in _STOCK_LIST] if _STOCK_LIST else []
        except Exception:
            return []

    @staticmethod
    def _scan_running():
        try:
            from services.stock_kline import _SCAN
            return bool(_SCAN.get("running"))
        except Exception:
            return False

    @staticmethod
    def _idle_expired():
        """无人看监控超过 IDLE_STOP_SEC 秒 => 引擎应退出(不再空转)。"""
        return _LAST_TOUCH > 0 and (time.time() - _LAST_TOUCH) > IDLE_STOP_SEC

    def start(self):
        self.thread = threading.Thread(target=self._loop, daemon=True,
                                      name="MinuteCrossEngine")
        self.thread.start()

    # ---- 主循环 ----
    def _loop(self):
        while not self.stop:
            try:
                # 无人看监控 => 退出(惰性启停)
                if self._idle_expired():
                    print("[分钟引擎] 已 %ds 无监控请求, 自动停止(不再空转)" % IDLE_STOP_SEC, flush=True)
                    self.stop = True
                    break
                # 2026-09-28 迁移: 数据源=腾讯 tx, 无需天勤账号/订阅/扫描账号。
                # 监控宇宙 = 自选股; ENABLE_UNIVERSE_SUBSCRIBE 打开时可扩到全市场
                # (tx 批量报价下全市场 5557 只 ≈ 112 请求/轮, 容量实测充裕)。
                syms = sorted(self._watch)
                if ENABLE_UNIVERSE_SUBSCRIBE:
                    uni = self._universe()
                    if uni:
                        syms = sorted(set(uni) | set(syms))
                self.subscribed_n = len(syms)
                if not syms:
                    self.last_error = "无自选股(打开监控页并加入自选后启动聚合)"
                    time.sleep(5)
                    continue
                self.last_error = ""
                # 主扫描循环: 每 POLL_INTERVAL 秒聚合一次分钟 bar
                resync_at = time.time() + 1200
                while not self.stop:
                    if self._idle_expired():
                        self.stop = True
                        break
                    if self._scan_running():
                        time.sleep(POLL_INTERVAL)
                        continue
                    self._poll_once(syms)
                    if time.time() > resync_at:
                        break
                    time.sleep(POLL_INTERVAL)
            except Exception as e:
                self.last_error = str(e)
                print(f"[分钟引擎] 循环异常(已捕获, 不影响主程序): {e}")
                time.sleep(10)
        # 退出(被 stop 或空闲超时) -> 重置全局, 允许之后再次惰性启动
        _shutdown_engine()

    def _seed_symbol(self, sym):
        """用历史 1 分钟 K 线(tx 源, 120 根)预热 EMA, 让分钟金叉从开盘即准确(避免 26 分钟冷启动)。
        失败则标记已尝试, 下次退回实时聚合。返回是否成功。"""
        self._seeded.add(sym)
        try:
            from services import futures_kline as fk, ta_patterns
            from services.stock_kline import _tx_kline_df
            df = _tx_kline_df(sym, "m1", 120)
            if df is None or getattr(df, "empty", True) or len(df) < 35:
                return False
            close = df["close"].astype(float).tolist()
            n = len(close)
            e12 = list(close); e26 = list(close)
            for i in range(1, n):
                e12[i] = e12[i - 1] + _A12 * (close[i] - e12[i - 1])
                e26[i] = e26[i - 1] + _A26 * (close[i] - e26[i - 1])
            dif = [e12[i] - e26[i] for i in range(n)]
            dea = [0.0] * n; dea[0] = dif[0]
            for i in range(1, n):
                dea[i] = dea[i - 1] + _A9 * (dif[i] - dea[i - 1])
            golden_date = death_date = None
            golden_idx = death_idx = None
            try:
                crosses = ta_patterns.detect_macd_cross(dif, dea, close)
                golds = [c for c in crosses if c["type"] == "golden"]
                deaths = [c for c in crosses if c["type"] == "death"]
                # 时间列统一转北京时间(int64/float64 纳秒都安全), 见 _fmt_bar_minute 注释
                dts = None
                if "datetime" in getattr(df, "columns", []):
                    try:
                        dts = list(fk._dt_local(df["datetime"]))
                    except Exception:
                        dts = None
                if golds:
                    g = max(golds, key=lambda c: c["idx"])
                    golden_idx = g["idx"]
                    golden_date = _fmt_bar_minute(dts, g["idx"])
                if deaths:
                    d = max(deaths, key=lambda c: c["idx"])
                    death_idx = d["idx"]
                    death_date = _fmt_bar_minute(dts, d["idx"])
            except Exception:
                pass
            self.states[sym] = {
                "ema12": e12[-1], "ema26": e26[-1], "dea": dea[-1],
                "prev_dif": dif[-1], "prev_dea": dea[-1],
                "bar_start": _now_minute_bucket(), "o": close[-1], "h": close[-1], "l": close[-1], "c": close[-1],
                "n": n, "golden_date": golden_date, "golden_idx": golden_idx,
                "death_date": death_date, "death_idx": death_idx, "wr": None}
            # 2026-09-14: 预热结果立即落库/落缓存。原先只在"下一根 1 分钟 bar 收盘且状态变化"
            # 时才 _write_minute, 于是改好格式后旧脏值仍会挂在库里(收盘后/休市时甚至要等到
            # 下一个交易时段才被覆盖), 页面依旧显示 e+18。这里让预热即生效, 打开监控页就自愈。
            recent = 1 if (golden_idx is not None and (n - golden_idx) <= RECENT_BARS) else 0
            self.states[sym]["wr"] = recent
            _write_minute(sym, golden_date, golden_idx, death_date, death_idx, recent)
            return True
        except Exception:
            return False

    def _poll_once(self, syms):
        now_b = _now_minute_bucket()
        # 2026-09-28 迁移: 每轮批量拉腾讯实时报价(一次50只, 3s 一轮远低于上游节拍)
        try:
            from services import tx_kline
            quotes = tx_kline.fetch_quotes_batch(syms)
        except Exception:
            return
        for sym in syms:
            try:
                q = quotes.get(sym) or {}
                lp = q.get("last")
                if lp is None or lp <= 0:
                    continue
                st = self.states.get(sym)
                if st is None:
                    # 自选股优先历史预热(让分钟金叉准确); 其余/失败退回实时聚合冷启动
                    if sym in self._watch and sym not in self._seeded:
                        if self._seed_symbol(sym):
                            continue
                    # 冷启动: 初始化 EMA 状态(随 tick 逐步收敛)
                    self.states[sym] = {
                        "ema12": lp, "ema26": lp, "dea": 0.0,
                        "prev_dif": None, "prev_dea": None,
                        "bar_start": now_b, "o": lp, "h": lp, "l": lp, "c": lp,
                        "n": 0, "golden_date": None, "golden_idx": None,
                        "death_date": None, "death_idx": None, "wr": None}
                    continue
                # 更新正在形成的 bar
                if lp > st["h"]:
                    st["h"] = lp
                if lp < st["l"]:
                    st["l"] = lp
                st["c"] = lp
                # 跨入新分钟 -> 上一分钟 bar 收盘
                if now_b > st["bar_start"]:
                    close_px = st["c"]
                    # 增量 MACD 递推(每 bar 6 次浮点乘加)
                    st["ema12"] += _A12 * (close_px - st["ema12"])
                    st["ema26"] += _A26 * (close_px - st["ema26"])
                    dif = st["ema12"] - st["ema26"]
                    st["dea"] += _A9 * (dif - st["dea"])
                    dea = st["dea"]
                    st["n"] += 1
                    if st["n"] > WARMUP_BARS:
                        ctype = _cross_type(st["prev_dif"], st["prev_dea"], dif, dea)
                        if ctype == "golden":
                            st["golden_date"] = st["bar_start"].strftime("%Y-%m-%d %H:%M")
                            st["golden_idx"] = st["n"]
                        elif ctype == "death":
                            st["death_date"] = st["bar_start"].strftime("%Y-%m-%d %H:%M")
                            st["death_idx"] = st["n"]
                        recent = 1 if (st.get("golden_idx") is not None
                                       and (st["n"] - st["golden_idx"]) <= RECENT_BARS) else 0
                        changed = (ctype is not None) or (recent != st.get("wr"))
                        if changed:
                            _write_minute(sym, st.get("golden_date"), st.get("golden_idx"),
                                          st.get("death_date"), st.get("death_idx"), recent)
                            st["wr"] = recent
                            if ctype:
                                dt = st["bar_start"].strftime("%Y-%m-%d %H:%M")
                                _record_minute_event(sym, ctype, dt, close_px, dif, dea)
                                self.updated_n += 1
                    st["prev_dif"] = dif
                    st["prev_dea"] = dea
                    # 重置为新的 forming bar
                    st["bar_start"] = now_b
                    st["o"] = lp
                    st["h"] = lp
                    st["l"] = lp
                    st["c"] = lp
            except Exception:
                continue

    def status(self):
        return {
            "started": not self.stop,   # 收到 stop 立即反映为未运行(线程最多 5s 后真正退出)
            "stopping": bool(self.stop),
            "uptime_s": int(time.time() - self.started_at),
            "idle_s": int(time.time() - _LAST_TOUCH) if _LAST_TOUCH else 0,
            "auto_stop_sec": IDLE_STOP_SEC,
            "subscribed": self.subscribed_n,
            "states": len(self.states),
            "updated": self.updated_n,
            "last_error": self.last_error,
        }


def start_minute_engine():
    """幂等启动分钟引擎（惰性: 由监控页/监控接口调用, 不再随服务启动）。"""
    global _ENGINE, _ENGINE_STARTED, _LAST_TOUCH
    if _ENGINE_STARTED:
        return
    _ENGINE_STARTED = True
    _LAST_TOUCH = time.time()
    _ENGINE = MinuteCrossEngine()
    _ENGINE.start()
    print("[分钟引擎] 已启动(惰性: 监控页打开触发; %ds 无人使用自动停止)" % IDLE_STOP_SEC, flush=True)


def touch_minute_engine(syms=None):
    """监控页/监控接口心跳: 刷新保活时间 + 设置自选股; 未启动则惰性启动。

    返回引擎是否已运行。调用点: api_monitor / api_minute_status / /api/minute/engine。
    """
    global _LAST_TOUCH
    _LAST_TOUCH = time.time()
    if _ENGINE is None and not _ENGINE_STARTED:
        start_minute_engine()
    if _ENGINE is not None and syms:
        _ENGINE.set_watch(syms)
    return _ENGINE is not None


def stop_minute_engine(reason=""):
    """页面关闭时显式停止引擎(立即释放订阅), 不必等空闲超时。"""
    global _LAST_TOUCH
    _LAST_TOUCH = 0.0
    if _ENGINE is not None:
        _ENGINE.stop = True
        print("[分钟引擎] 已停止(%s)" % (reason or "页面关闭"), flush=True)
        return True
    return False


def _shutdown_engine():
    """引擎线程退出后重置全局, 允许后续再次惰性启动。"""
    global _ENGINE, _ENGINE_STARTED
    if _ENGINE is not None:
        _ENGINE = None
        _ENGINE_STARTED = False


def get_minute_cache():
    """供 api_monitor 合并分钟字段。"""
    with _MINUTE_LOCK:
        return dict(_MINUTE_CACHE)


def minute_status():
    if _ENGINE is None:
        return {"started": False, "last_error": "未启动(打开监控页后自动启动)",
                "auto_stop_sec": IDLE_STOP_SEC}
    return _ENGINE.status()


def set_watch_symbols(syms):
    """供 api_monitor 传入自选股, 引擎优先预热这些标的的分钟 MACD。"""
    global _LAST_TOUCH
    if syms:
        _LAST_TOUCH = time.time()   # 兼容旧调用点: 传自选股即视为有人在看
    if _ENGINE is not None:
        _ENGINE.set_watch(syms)


def minute_status_string():
    """给前端展示的简短状态(只读, 不影响全局)。"""
    try:
        st = minute_status()
        if not st.get("started"):
            return "分钟引擎未启动"
        if st.get("last_error"):
            return "分钟引擎: %s · 监控 %d" % (st.get("last_error"), st.get("subscribed", 0))
        return "分钟引擎: 在线(tx源) · 监控 %d · 已就绪 %d · 更新 %d" % (
            st.get("subscribed", 0), st.get("states", 0), st.get("updated", 0))
    except Exception:
        return "分钟引擎状态未知"
