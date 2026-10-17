"""
期货全市场实时监控预警系统
- 列出全部非过期期货合约, 按品种聚合并自动识别主力合约(成交量+持仓量)
- 支持用户配置指标/操作符/阈值预警规则
- MACD 金叉/死叉按趋势方向定义, 距零轴距离绝对值过滤
- 暴露 REST 接口给前端浮动面板
"""
import math
import json
import time
import threading
import re
from datetime import datetime, timedelta
from collections import defaultdict
from flask import request, Response, render_template_string
import pymysql
try:
    from services import futures_db
except Exception:
    try:
        import futures_db
    except Exception:
        futures_db = None

# 交易所中文名映射
EXCHANGE_NAMES = {
    "SHFE": "上期所", "DCE": "大商所", "CZCE": "郑商所",
    "CFFEX": "中金所", "INE": "上期能源", "GFEX": "广期所"
}

# 品种 ID -> 默认 1 手吨数/乘数(仅用于展示参考, 真正交易以交易所为准)
# 留空表示不显示
PRODUCT_MULTIPLIER = {
    "cu": 5, "al": 5, "zn": 5, "pb": 5, "ni": 1, "sn": 1, "au": 1000, "ag": 15,
    "rb": 10, "hc": 10, "bu": 10, "ru": 10, "sp": 10, "ss": 5, "wr": 10, "fo": 10,
    "m": 10, "y": 10, "a": 10, "b": 10, "p": 10, "c": 10, "cs": 10, "jd": 10,
    "lh": 16, "jm": 60, "j": 100, "i": 100, "v": 5, "pp": 5, "l": 5, "eg": 10,
    "eb": 5, "pg": 20, "rr": 10, "ap": 10, "cj": 5, "sa": 20, "ma": 10, "ta": 5,
    "rm": 10, "oi": 10, "cf": 5, "sr": 10, "fg": 20, "ur": 20, "pf": 5, "cy": 5,
    "sm": 5, "sf": 5, "px": 5, "pr": 5,
    "if": 300, "ic": 200, "ih": 300, "im": 200, "tf": 10000, "ts": 20000, "tl": 10000,
    "sc": 1000, "ec": 50, "lu": 10,
    "si": 5, "lc": 1
}


def _safe_float(x):
    try:
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return 0.0
        return f
    except Exception:
        return 0.0


def _safe_int(x):
    try:
        return int(x)
    except Exception:
        return 0


def _pct_change(q):
    """涨跌幅: 基于 pre_settlement, 与 WH6/行业惯例一致。"""
    ps = _safe_float(getattr(q, "pre_settlement", 0) or getattr(q, "pre_close", 0))
    lp = _safe_float(getattr(q, "last_price", 0))
    if ps > 0 and lp > 0:
        return round((lp - ps) / ps * 100, 2)
    return 0.0


def _product_id(symbol: str) -> str:
    """从 SHFE.cu2501 提取 cu, 从 KQ.m@SHFE.cu 提取 cu"""
    s = symbol
    if s.startswith("KQ.m@"):
        s = s.split(".")[-1] if "." in s else s
        return s.lower()
    if "." in s:
        code = s.split(".")[-1]
        # 去掉数字月份, 保留字母品种代码
        m = re.match(r"([a-zA-Z]+)", code)
        return m.group(1).lower() if m else code.lower()
    return s.lower()


def _exch_id(symbol: str) -> str:
    # 支持主连/指数等连续合约序列: KQ.m@SHFE.rb -> SHFE; 普通 SHFE.rb2601 -> SHFE
    if "@" in symbol:
        symbol = symbol.split("@")[-1]
    return symbol.split(".")[0] if "." in symbol else ""


def _contract_month(symbol: str) -> int:
    """从 SHFE.cu2501 提取月份 1; 失败返回 99"""
    try:
        code = symbol.split(".")[-1]
        m = re.search(r"(\d{2})(\d{2})$", code)
        if m:
            return int(m.group(2))
    except Exception:
        pass
    return 99


# ---- 交易时段配置(用于"按当日时间自动监控") ----
# 标准日盘时段(中国期货通用, 商品/金融近似): 午休 11:30-13:30 自然排除
DEFAULT_DAY_WINDOWS = [("09:00", "11:30"), ("13:30", "15:00")]
# 品种级夜盘兜底(仅在 TqSdk quote.trading_time 不可用时使用; 主数据源仍是 trading_time)。
# 取值 (open, close), 若 close < open 表示跨自然日(如 21:00-02:30)。
DEFAULT_NIGHT_OVERRIDE = {
    "cu": ("21:00", "01:00"), "al": ("21:00", "01:00"), "zn": ("21:00", "01:00"),
    "pb": ("21:00", "01:00"), "ni": ("21:00", "01:00"), "sn": ("21:00", "01:00"),
    "rb": ("21:00", "23:00"), "hc": ("21:00", "23:00"), "bu": ("21:00", "23:00"),
    "ru": ("21:00", "23:00"), "sp": ("21:00", "23:00"), "ss": ("21:00", "23:00"),
    "au": ("21:00", "02:30"), "ag": ("21:00", "02:30"),
    "sc": ("21:00", "02:30"), "lu": ("21:00", "02:30"), "ec": ("21:00", "01:00"),
    "si": ("21:00", "01:00"), "lc": ("21:00", "01:00"),
}


def _parse_trading_time(tt):
    """解析 TqSdk quote.trading_time -> {day:[...], night:(o,c) or None}。"""
    day = []
    night = None
    for seg in (tt or []):
        if "-" not in seg:
            continue
        s, e = seg.split("-")[:2]
        s, e = s[:5], e[:5]
        if s < "15:00":
            day.append((s, e))
        else:
            if night is None or e > night[1]:
                night = (s, e)
    if not day:
        day = list(DEFAULT_DAY_WINDOWS)
    return {"day": day, "night": night}


def _quote_bar_time(q):
    """取行情时间(本地)字符串, 用于事件 bar_time; 取不到则用当前时间。"""
    dt = getattr(q, "datetime", None)
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class FutureMonitorService:
    def __init__(self, slots: dict, default_key: str, get_slot=None):
        self.slots = slots
        self.default_key = default_key
        self._get_slot_resolver = get_slot  # 请求感知的账号解析器（来自 main.py 的 _resolve_slot）
        self._all_symbols = []          # 全部非过期期货合约
        self._selected_symbols = []     # 实际提交订阅的合约(中国交易所 主力/近月)
        self._main_symbols = set()      # 当前识别出的主力合约集合
        self._symbol_info = {}          # symbol -> {product, exchange, name}
        self._lock = threading.Lock()
        self._last_scan_ts = 0
        self._scan_interval = 30        # 秒, 两次全量扫描间隔
        self._started = False
        # 交易时段闸门: 品种 -> {day:[...], night:(o,c)|None}; 夜盘跨自然日归次交易日
        self._sessions = {}
        # 自动评估状态: (rule_name, symbol) -> 是否当前触发(用于"上升沿"写库, 避免重复落库)
        self._alert_state = {}
        self._auto_thread = None
        self._auto_interval = 15        # 秒, 自动评估轮询间隔
        self._last_slot = None          # 后台线程回落用的"最近一次请求解析到的账号 Slot"(见 _slot)

    def _slot(self):
        """解析当前请求应选用的账户 Slot。

        关键修正(2026-09-24): 后台自动评估线程没有 Flask 请求上下文,
        _resolve_slot() 会因 request 未绑定而抛/返回 None, 导致自动线程永远拿不到账号
        -> _ensure_started 里 `if not slot: return` -> _started 恒为 False
        -> 自动监控形同虚设(实时行情页正常, 但 t_future_monitor_event 一条都不写)。
        回落顺序: ① 请求上下文解析 -> ② 最近一次请求缓存的 Slot
        -> ③ 全局任意一个 ONLINE 的 Slot -> ④ default_key。
        ②/③ 保证"用户打开过页面(或任一账号在线)"后, 后台线程也能接管监控并落库。
        """
        s = None
        if self._get_slot_resolver is not None:
            try:
                s = self._get_slot_resolver()
            except Exception:
                s = None
        if s is None:
            s = self._last_slot
        if s is None:
            for _sl in self.slots.values():
                if getattr(_sl, "status", "") == "online":
                    s = _sl
                    break
        if s is None:
            s = self.slots.get(self.default_key)
        if s is not None:
            self._last_slot = s
        return s

    def _ensure_started(self):
        if self._started:
            return
        with self._lock:
            if self._started:
                return
            slot = self._slot()
            if not slot or slot.status != "online":
                print("[期货监控] 天勤未就绪, 将在首次请求时重试")
                return
            try:
                # 用独立 TqApi 会话查询合约列表(避免在请求线程调用共享 TqApi 的 query_quotes 返回空)
                from tqsdk import TqApi, TqAuth
                import os
                os.environ.setdefault("NO_PROXY", "shinnytech.com,.shinnytech.com")
                os.environ.setdefault("no_proxy", "shinnytech.com,.shinnytech.com")
                api = TqApi(auth=TqAuth(slot.tq_user, slot.tq_pass))
                syms = api.query_quotes(ins_class="FUTURE", expired=False) or []
                api.close()
                if not syms:
                    print("[期货监控] 合约列表为空, 5秒后重试")
                    return
                # 只保留中国六大期货交易所
                cn_exch = {"SHFE", "DCE", "CZCE", "CFFEX", "INE", "GFEX"}
                self._all_symbols = sorted([s for s in syms if _exch_id(s) in cn_exch])
                info = {}
                for s in self._all_symbols:
                    info[s] = {
                        "product": _product_id(s),
                        "exchange": _exch_id(s),
                        "exchange_name": EXCHANGE_NAMES.get(_exch_id(s), _exch_id(s))
                    }
                self._symbol_info = info
                # 只监控主连: 每个品种取一个连续主力合约 KQ.m@<交易所>.<品种>
                # 主连是单会话下最稳的"每品种代表合约", 数量≈品种数(~64), 远在 MAX_SLOT_SUBSCRIBE=500 内,
                # 避免因订阅过多触发天勤单会话限流冻死(整会话 got=False)。
                prod_exch = {}
                for s in self._all_symbols:
                    p = info[s]["product"]
                    if p not in prod_exch:
                        prod_exch[p] = info[s]["exchange"]
                selected = []
                for prod, exch in prod_exch.items():
                    if not exch:
                        continue
                    selected.append(f"KQ.m@{exch}.{prod}")
                self._selected_symbols = selected
                # 请求订阅(实际注册由主线程轮询完成)
                slot.ensure_subscribed(selected)
                self._started = True
                print(f"[期货监控] 已发现 {len(self._all_symbols)} 个中国期货合约, 选中 {len(selected)} 个主连合约提交订阅", flush=True)
            except Exception as e:
                print(f"[期货监控] 启动扫描失败: {e}", flush=True)
                import traceback; traceback.print_exc()

    def _quote_to_dict(self, symbol, q):
        """把天勤 quote 对象转成前端需要的 dict。"""
        lp = _safe_float(getattr(q, "last_price", 0))
        ps = _safe_float(getattr(q, "pre_settlement", 0) or getattr(q, "pre_close", 0))
        pc = _safe_float(getattr(q, "pre_close", 0))
        vol = _safe_int(getattr(q, "volume", 0))
        oi = _safe_int(getattr(q, "open_interest", 0))
        amt = _safe_float(getattr(q, "amount", 0))
        bid1 = _safe_float(getattr(q, "bid_price1", 0))
        ask1 = _safe_float(getattr(q, "ask_price1", 0))
        info = self._symbol_info.get(symbol, {})
        product = info.get("product", _product_id(symbol))
        multiplier = PRODUCT_MULTIPLIER.get(product, 0)
        return {
            "symbol": symbol,
            "name": getattr(q, "instrument_name", symbol) or symbol,
            "exchange": info.get("exchange", _exch_id(symbol)),
            "exchange_name": info.get("exchange_name", _exch_id(symbol)),
            "product": product,
            "last_price": lp,
            "open": _safe_float(getattr(q, "open", 0)),
            "high": _safe_float(getattr(q, "highest", 0)),
            "low": _safe_float(getattr(q, "lowest", 0)),
            "pre_settlement": ps,
            "pre_close": pc,
            "change_pct": _pct_change(q),
            "change": round(lp - ps, 2) if ps > 0 and lp > 0 else 0.0,
            "volume": vol,
            "amount": amt,
            "open_interest": oi,
            "bid1": bid1,
            "ask1": ask1,
            "multiplier": multiplier,
            "update_time": datetime.now().strftime("%H:%M:%S"),
            "bar_time": _quote_bar_time(q),
        }

    def _read_all_quotes(self):
        """读取当前已缓存的全部行情(来自轮询线程写入的 latest_data)。

        注意: latest_data 的 key 是 symbol 字符串, value 是天勤 quote 对象(或等价
        dict-like)。value 内**不保证**含 symbol 字段, 因此一律用 key 作为 symbol 来匹配
        订阅集合, 再用 _quote_to_dict 转成前端需要的规范 dict。
        """
        slot = self._slot()
        if not slot:
            return []
        with slot.data_lock:
            raw = dict(slot.latest_data)
        fut_set = set(self._selected_symbols)
        out = []
        for sym, q in raw.items():
            if sym not in fut_set:
                continue
            try:
                if isinstance(q, dict):
                    d = dict(q)
                    d.setdefault("symbol", sym)
                    if "change_pct" not in d and "pct_change" in d:
                        d["change_pct"] = d["pct_change"]
                    # 补齐前端表格需要的派生字段
                    if "exchange" not in d:
                        d["exchange"] = _exch_id(sym)
                    if "exchange_name" not in d:
                        d["exchange_name"] = EXCHANGE_NAMES.get(_exch_id(sym), _exch_id(sym))
                    if "change" not in d:
                        lp = _safe_float(d.get("last_price", 0))
                        ps = _safe_float(d.get("pre_settlement", 0) or d.get("pre_close", 0))
                        d["change"] = round(lp - ps, 2) if ps > 0 and lp > 0 else 0.0
                    if "name" not in d or not d.get("name"):
                        d["name"] = sym
                    out.append(d)
                else:
                    # 天勤 quote 对象 -> 规范 dict(含 symbol/exchange_name/change)
                    out.append(self._quote_to_dict(sym, q))
            except Exception:
                continue
        return out

    def _fetch_main_contracts(self):
        """按品种+交易所识别主力合约: 优先 open_interest 最大, 相同时看 volume。"""
        quotes = self._read_all_quotes()
        by_product = defaultdict(list)
        for q in quotes:
            prod = q.get("product") or _product_id(q.get("symbol", ""))
            by_product[prod].append(q)
        main_symbols = set()
        main_list = []
        for prod, items in by_product.items():
            # 过滤无价格的
            valid = [it for it in items if it.get("last_price", 0) > 0]
            if not valid:
                continue
            # 主力: 持仓最大; 持仓相同则成交量大; 再相同取近月
            best = max(valid, key=lambda x: (x.get("open_interest", 0), x.get("volume", 0), -_contract_month(x.get("symbol", ""))))
            main_symbols.add(best["symbol"])
            main_list.append(best)
        with self._lock:
            self._main_symbols = main_symbols
        return main_list

    # ------------------------------------------------------------------
    # 交易时段闸门: 按当日时间自动判断是否监控(有夜盘按夜盘, 无夜盘不监控)
    # ------------------------------------------------------------------
    def _get_session(self, product, symbol):
        """返回品种的交易时段配置。主数据源 = TqSdk quote.trading_time; 不可用时用兜底表。"""
        with self._lock:
            if product in self._sessions:
                return self._sessions[product]
        sess = None
        try:
            slot = self._slot()
            if slot and symbol:
                q = slot.client.get_quote(symbol)
                tt = getattr(q, "trading_time", None)
                if tt:
                    sess = _parse_trading_time(tt)
                    with self._lock:
                        self._sessions[product] = sess
                    return sess
        except Exception:
            sess = None
        # 没拿到 trading_time: 用兜底(不缓存, 下次主会话数据到达后再试)
        night = DEFAULT_NIGHT_OVERRIDE.get(product)
        return {"day": list(DEFAULT_DAY_WINDOWS), "night": night}

    def is_product_in_session(self, product, symbol, now=None):
        """该品种当前是否处于交易时段(日盘或夜盘)。"""
        now = now or datetime.now()
        cur = now.time().strftime("%H:%M")
        sess = self._get_session(product, symbol)
        for (s, e) in sess["day"]:
            if s <= cur <= e:
                return True
        n = sess["night"]
        if n:
            no, nc = n
            if no < nc:                      # 当日夜盘(如 21:00-23:00)
                if no <= cur <= nc:
                    return True
            else:                            # 跨自然日夜盘(如 21:00-02:30)
                if cur >= no or cur <= nc:
                    return True
        return False

    def session_phase(self, product, symbol, now=None):
        """返回 'day' / 'night' / 'closed'。"""
        if not self.is_product_in_session(product, symbol, now):
            return "closed"
        sess = self._get_session(product, symbol)
        cur = (now or datetime.now()).time().strftime("%H:%M")
        n = sess["night"]
        if n and (cur >= n[0] or (n[0] > n[1] and cur <= n[1])):
            return "night"
        return "day"

    def trading_day_at(self, product, symbol, now=None):
        """交易日归属: 夜盘跨自然日时, 凌晨段归前一夜的交易日(往前一天)。"""
        now = now or datetime.now()
        sess = self._get_session(product, symbol)
        n = sess["night"]
        if n and n[0] > n[1]:                # 跨自然日
            cur = now.time().strftime("%H:%M")
            if cur >= n[0]:
                return now.date()            # 今夜开盘 -> 归今天
            if cur <= n[1]:
                return (now - timedelta(days=1)).date()  # 凌晨 -> 归前一晚
        return now.date()

    # ------------------------------------------------------------------
    # 自动评估 + 落库(由后台线程驱动, 仅对"当前处于交易时段"的品种生效)
    # ------------------------------------------------------------------
    def start_auto_loop(self):
        if self._auto_thread and self._auto_thread.is_alive():
            return

        def _run():
            self._ensure_event_table()
            while True:
                try:
                    self._ensure_started()
                    if self._started:
                        self._auto_eval()
                except Exception as e:
                    print(f"[期货监控] 自动评估异常: {e}", flush=True)
                time.sleep(self._auto_interval)

        t = threading.Thread(target=_run, name="future-monitor-auto", daemon=True)
        t.start()
        self._auto_thread = t
        print("[期货监控] 自动评估线程已启动(按交易时段闸门 + 上升沿落库)", flush=True)

    def _auto_eval(self):
        now = datetime.now()
        quotes = self._read_all_quotes()
        if not quotes:
            return
        # 只评估当前处于交易时段的品种(无夜盘的品种在夜盘时段不评估/不落库)
        in_session = []
        for q in quotes:
            prod = q.get("product") or _product_id(q.get("symbol", ""))
            sym = q.get("symbol", "")
            try:
                if self.is_product_in_session(prod, sym, now):
                    in_session.append(q)
            except Exception:
                pass
        if not in_session:
            return
        alerts = _alert_engine.check(in_session)
        triggered = set()
        for a in alerts:
            key = (a.get("rule_name"), a.get("symbol"))
            triggered.add(key)
            q = next((x for x in in_session if x.get("symbol") == a.get("symbol")), None)
            if q is None:
                continue
            prev = self._alert_state.get(key)
            if not prev:                     # 上升沿: 刚触发 -> 落库
                self._write_event(a, q, now)
            self._alert_state[key] = True
        # 下降沿: 清空状态, 允许下次重新触发
        for key in list(self._alert_state.keys()):
            if key not in triggered:
                self._alert_state[key] = False

    def _ensure_event_table(self):
        if futures_db is None:
            return
        try:
            conn = futures_db.get_connection()
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS t_future_monitor_event ("
                " id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                " contract VARCHAR(64) NOT NULL,"
                " exchange VARCHAR(16),"
                " product VARCHAR(16),"
                " event_type VARCHAR(32) NOT NULL,"
                " rule_name VARCHAR(128),"
                " signal_detail JSON,"
                " bar_time DATETIME,"
                " last_price DOUBLE,"
                " kline_snapshot JSON,"
                " trading_day DATE,"
                " created_at DATETIME DEFAULT CURRENT_TIMESTAMP,"
                " INDEX idx_ce (contract, trading_day),"
                " INDEX idx_et (event_type),"
                " INDEX idx_ca (created_at)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"[期货监控] 建表 t_future_monitor_event 失败: {e}", flush=True)

    def _write_event(self, alert, quote, now):
        if futures_db is None:
            return
        try:
            product = quote.get("product")
            exchange = quote.get("exchange")
            contract = alert.get("symbol")
            signal_detail = json.dumps({
                "field": alert.get("field"),
                "op": alert.get("op"),
                "threshold": alert.get("threshold"),
                "value": alert.get("value"),
            }, ensure_ascii=False)
            bar_time = quote.get("bar_time") or now.strftime("%Y-%m-%d %H:%M:%S")
            kline_snapshot = json.dumps({
                "symbol": contract, "name": quote.get("name"),
                "last_price": quote.get("last_price"), "open": quote.get("open"),
                "high": quote.get("high"), "low": quote.get("low"),
                "pre_settlement": quote.get("pre_settlement"),
                "change_pct": quote.get("change_pct"), "change": quote.get("change"),
                "volume": quote.get("volume"), "open_interest": quote.get("open_interest"),
            }, ensure_ascii=False)
            trading_day = self.trading_day_at(product, contract, now)
            conn = futures_db.get_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO t_future_monitor_event"
                " (contract, exchange, product, event_type, rule_name, signal_detail, bar_time, last_price, kline_snapshot, trading_day, created_at)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (contract, exchange, product, "RULE_ALERT", alert.get("rule_name"),
                 signal_detail, bar_time, quote.get("last_price"), kline_snapshot,
                 trading_day, now.strftime("%Y-%m-%d %H:%M:%S")),
            )
            conn.commit()
            cur.close()
            conn.close()
            print(f"[期货监控] 落库事件: {contract} {alert.get('rule_name')} @ {bar_time}", flush=True)
        except Exception as e:
            print(f"[期货监控] 写事件失败: {e}", flush=True)

    def get_futures(self, only_main: bool = False):
        """返回期货列表(默认全部, only_main=True 只返回主力)。"""
        self._ensure_started()
        slot = self._slot()
        if not slot or slot.status != "online":
            return {"ok": False, "msg": "天勤账户未就绪"}

        # 若距离上次扫描超过间隔, 重新识别主力
        now = time.time()
        with self._lock:
            need_scan = (now - self._last_scan_ts) > self._scan_interval
        if need_scan:
            self._fetch_main_contracts()
            with self._lock:
                self._last_scan_ts = now

        quotes = self._read_all_quotes()
        main_set = set()
        with self._lock:
            main_set = set(self._main_symbols)

        result = []
        mon_count = 0
        for q in quotes:
            if only_main and q.get("symbol") not in main_set:
                continue
            q = dict(q)
            q["is_main"] = q.get("symbol") in main_set
            prod = q.get("product") or _product_id(q.get("symbol", ""))
            sym = q.get("symbol", "")
            try:
                q["in_session"] = self.is_product_in_session(prod, sym)
                q["session_phase"] = self.session_phase(prod, sym)
                if q["in_session"]:
                    mon_count += 1
            except Exception:
                q["in_session"] = False
                q["session_phase"] = "closed"
            result.append(q)

        # 排序: 主力在前, 然后按涨跌幅绝对值降序(波动大的显眼)
        result.sort(key=lambda x: (-int(x.get("is_main", False)), -abs(x.get("change_pct", 0) or 0)))
        return {"ok": True, "data": result, "count": len(result), "main_count": len(main_set),
                "total_subscribed": len(self._selected_symbols), "monitoring": mon_count}


# 内置默认监控规则: 让"自动监控"开箱即用, 无需在页面手动添加规则即可开始落库。
# 主连涨跌幅 >= 2% 记为一次异动信号(上升沿落库, 回落清状态允许再次触发)。
# 用户可在页面"告警规则"里随时修改/删除; 一旦页面保存过规则, 以页面保存的为准。
DEFAULT_MONITOR_RULES = [
    {"field": "change_pct", "op": ">=", "threshold": 2, "enabled": True, "name": "主连涨跌幅≥2%"},
]


class AlertEngine:
    """预警规则引擎。规则存内存(进程级), 前端可保存/读取。"""
    def __init__(self, default_rules=None):
        self._rules = list(default_rules) if default_rules else []
        self._lock = threading.Lock()
        self._last_triggered = {}  # rule_id -> {symbol -> last_value}

    def get_rules(self):
        with self._lock:
            return list(self._rules)

    def set_rules(self, rules):
        with self._lock:
            self._rules = rules or []

    def _value_of(self, quote, field):
        if field == "change_pct":
            return quote.get("change_pct", 0)
        if field == "last_price":
            return quote.get("last_price", 0)
        if field == "volume":
            return quote.get("volume", 0)
        if field == "open_interest":
            return quote.get("open_interest", 0)
        if field == "amount":
            return quote.get("amount", 0)
        if field == "bid_ask_spread":
            return round(quote.get("ask1", 0) - quote.get("bid1", 0), 4)
        return quote.get(field, 0)

    def _op_check(self, op, val, thresh):
        try:
            if op == ">": return val > thresh
            if op == ">=": return val >= thresh
            if op == "<": return val < thresh
            if op == "<=": return val <= thresh
            if op == "=": return abs(val - thresh) < 1e-9
            if op == "!=": return abs(val - thresh) >= 1e-9
        except Exception:
            return False
        return False

    def check(self, quotes):
        """对 quotes 检查规则, 返回触发列表。"""
        alerts = []
        with self._lock:
            rules = list(self._rules)
        for rule in rules:
            if not rule.get("enabled", True):
                continue
            fid = rule.get("field", "change_pct")
            op = rule.get("op", ">=")
            thresh = _safe_float(rule.get("threshold", 0))
            name = rule.get("name", f"{fid}{op}{thresh}")
            for q in quotes:
                val = self._value_of(q, fid)
                if self._op_check(op, val, thresh):
                    alerts.append({
                        "rule_name": name,
                        "field": fid,
                        "op": op,
                        "threshold": thresh,
                        "value": val,
                        "symbol": q.get("symbol"),
                        "name": q.get("name"),
                        "time": datetime.now().strftime("%H:%M:%S")
                    })
        return alerts


HTML_PAGE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>期货全市场监控</title>
<style>
:root{--bg:#0b1220;--surface:#111827;--surface2:#1f2937;--border:#27344f;--text:#e2e8f0;--muted:#94a3b8;--accent:#38bdf8;--up:#ef4444;--down:#22c55e;--warn:#f59e0b;}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;font-size:13px;overflow:hidden}
.fm-wrap{display:flex;flex-direction:column;height:100vh}
.fm-hd{padding:10px 16px;background:var(--surface);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px;flex-shrink:0}
.fm-hd h1{margin:0;font-size:16px;color:var(--accent)}
.fm-hd .stat{color:var(--muted);font-size:12px}
.fm-hd button{padding:5px 12px;background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:4px;cursor:pointer;font-size:12px}
.fm-hd button:hover{border-color:var(--accent)}
.fm-body{flex:1;display:flex;overflow:hidden}
.fm-list{flex:1;overflow:auto;padding:0 12px}
.fm-list table{width:100%;border-collapse:collapse;font-size:12px;margin-top:8px}
.fm-list th{position:sticky;top:0;background:var(--surface);padding:8px 6px;text-align:left;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border);white-space:nowrap}
.fm-list td{padding:6px;border-bottom:1px solid var(--border);white-space:nowrap}
.fm-list tr:hover{background:rgba(56,189,248,.08)}
.fm-list .up{color:var(--up);font-weight:700}
.fm-list .down{color:var(--down);font-weight:700}
.fm-list .main td{background:rgba(245,158,11,.08)}
.fm-list .main .tag{font-size:10px;color:var(--warn);border:1px solid var(--warn);padding:0 4px;border-radius:3px;margin-left:4px}
.fm-side{width:320px;flex-shrink:0;background:var(--surface);border-left:1px solid var(--border);display:flex;flex-direction:column}
.fm-side-hd{padding:10px 12px;border-bottom:1px solid var(--border);font-weight:600}
.fm-rules{flex:1;overflow:auto;padding:12px}
.fm-rule{display:flex;gap:6px;align-items:center;margin-bottom:8px}
.fm-rule input,.fm-rule select{padding:4px 6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:4px;font-size:12px}
.fm-rule input[type=number]{width:70px}
.fm-rule select{width:90px}
.fm-alert{position:fixed;right:16px;bottom:16px;width:280px;background:var(--surface);border:1px solid var(--warn);border-radius:8px;padding:12px;box-shadow:0 8px 24px rgba(0,0,0,.45);animation:slideIn .25s ease-out;z-index:1000}
.fm-alert .title{color:var(--warn);font-weight:700;margin-bottom:4px}
@keyframes slideIn{from{transform:translateX(120%);opacity:0}to{transform:translateX(0);opacity:1}}
.fm-empty{text-align:center;color:var(--muted);padding:40px 0}
/* ===== K线设置(计算根数/取值根数) ===== */
.fm-mask{position:fixed;inset:0;background:rgba(2,6,18,.72);display:none;align-items:center;justify-content:center;z-index:1100}
.fm-mask.on{display:flex}
.fm-dlg{width:820px;max-width:94vw;max-height:88vh;overflow:auto;background:var(--surface);border:1px solid var(--border);border-radius:8px;box-shadow:0 12px 40px rgba(0,0,0,.55)}
.fm-dlg-hd{padding:12px 16px;border-bottom:1px solid var(--border);font-weight:700;font-size:14px;display:flex;align-items:center;gap:8px}
.fm-dlg-bd{padding:14px 16px}
.fm-dlg-ft{padding:12px 16px;border-top:1px solid var(--border);display:flex;gap:8px;align-items:center}
.fm-dlg-ft button{padding:6px 14px;background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:4px;cursor:pointer;font-size:12px}
.fm-dlg-ft button:hover{border-color:var(--accent)}
.fm-dlg-ft .primary{background:var(--accent);border-color:var(--accent);color:#04202e;font-weight:700}
.fm-note{color:var(--muted);font-size:12px;line-height:1.7;margin:0 0 12px}
.fm-note b{color:var(--text)}
table.fm-set{width:100%;border-collapse:collapse;font-size:12px}
table.fm-set th{padding:8px 6px;text-align:left;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border)}
table.fm-set td{padding:6px;border-bottom:1px solid var(--border);vertical-align:middle}
table.fm-set input[type=number]{width:84px;padding:5px 6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:4px;font-size:12px}
/* 低于阈值: 数字标黄(用户 2026-10-13 裁定"只警告不限制") */
.fm-set input.warn{color:var(--warn);border-color:var(--warn)}
.fm-set .eff{color:var(--muted);font-size:11px}
.fm-set .hint{color:var(--muted);font-size:11px;line-height:1.6}
/* 小问号 */
.fm-q{position:relative;display:inline-block;width:15px;height:15px;line-height:15px;text-align:center;border:1px solid var(--muted);border-radius:50%;color:var(--muted);font-size:10px;cursor:help;margin-left:4px;user-select:none}
.fm-q:hover{color:var(--accent);border-color:var(--accent)}
.fm-q .fm-tip{display:none;position:absolute;left:18px;top:-4px;width:300px;background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:9px 10px;color:var(--text);font-size:11px;line-height:1.7;z-index:1200;box-shadow:0 8px 22px rgba(0,0,0,.5);font-weight:400;text-align:left;white-space:normal}
.fm-q:hover .fm-tip{display:block}
.fm-warnline{color:var(--warn);font-size:11px;margin-top:3px}
.fm-badgenote{color:var(--warn);font-size:11px;margin-top:10px;line-height:1.7}
</style>
</head>
<body>
<div class="fm-wrap">
  <div class="fm-hd">
    <h1>期货全市场监控</h1>
    <span class="stat" id="fmStat">加载中…</span>
    <button onclick="fmToggleMain()" id="fmMainBtn">只看主力</button>
    <button onclick="fmRefresh()">立即刷新</button>
    <button onclick="location.href='review'">历史回顾</button>
    <button onclick="fmOpenSet()">K线设置</button>
    <button onclick="location.href='../'" style="margin-left:auto">返回股票K线</button>
  </div>
  <div class="fm-body">
    <div class="fm-list" id="fmList"><div class="fm-empty">正在初始化行情…</div></div>
    <div class="fm-side">
      <div class="fm-side-hd">预警规则</div>
      <div class="fm-rules" id="fmRules"></div>
      <div style="padding:12px;border-top:1px solid var(--border)">
        <button onclick="fmAddRule()">+ 添加规则</button>
        <button onclick="fmSaveRules()" style="margin-left:8px">保存规则</button>
        <button onclick="fmCheckAlerts()" style="margin-left:8px">测试预警</button>
      </div>
    </div>
  </div>
</div>

<!-- ===== K线设置弹窗: 计算根数 / 取值根数 ===== -->
<div class="fm-mask" id="fmSetMask" onclick="if(event.target===this)fmCloseSet()">
  <div class="fm-dlg">
    <div class="fm-dlg-hd">K线设置 · 期货监控</div>
    <div class="fm-dlg-bd">
      <p class="fm-note" id="fmSetNote"></p>
      <table class="fm-set">
        <thead><tr>
          <th style="width:88px">周期</th>
          <th>计算根数<span class="fm-q" id="fmQCalc">i<span class="fm-tip" id="fmTipCalc"></span></span></th>
          <th>取值根数<span class="fm-q" id="fmQTake">i<span class="fm-tip" id="fmTipTake"></span></span></th>
          <th style="width:230px">说明</th>
        </tr></thead>
        <tbody id="fmSetBody"></tbody>
      </table>
      <div class="fm-badgenote" id="fmSetWarn"></div>
    </div>
    <div class="fm-dlg-ft">
      <button class="primary" onclick="fmSaveSet()">保存</button>
      <button onclick="fmResetSet()">恢复默认</button>
      <button onclick="fmCloseSet()">关闭</button>
      <span style="margin-left:auto;color:var(--muted);font-size:11px">保存后下一轮扫描生效</span>
    </div>
  </div>
</div>

<script>
var FM_STATE={onlyMain:false,rules:[]};
function fmBase(){
  var p = location.pathname;
  var i = p.lastIndexOf('futuremonitor');
  return p.substring(0, i + 'futuremonitor'.length);
}
function fmApi(path){ return fmBase() + '/api' + path; }
function fmFetch(url,opts){ return fetch(url, Object.assign({headers:{'Content-Type':'application/json'}}, opts||{})).then(function(r){return r.json();}); }

function fmRenderList(d){
  var items=d.data||[];
  document.getElementById('fmStat').textContent='共 '+d.total_subscribed+' 个合约 · 主力 '+d.main_count+' 个 · 监控中 '+((d.monitoring!=null)?d.monitoring:0)+' 个 · 当前显示 '+items.length+' 个';
  if(!items.length){ document.getElementById('fmList').innerHTML='<div class="fm-empty">暂无行情数据(可能天勤账户未就绪)</div>'; return; }
  var html='<table><thead><tr><th>合约</th><th>交易所</th><th>最新价</th><th>涨跌%</th><th>涨跌额</th><th>成交量</th><th>持仓量</th><th>更新时间</th></tr></thead><tbody>';
  items.forEach(function(it){
    var cls=it.change_pct>=0?'up':'down';
    var tag=it.is_main?'<span class="tag">主力</span>':'';
    html+='<tr class="'+(it.is_main?'main':'')+'"><td>'+(it.name||it.symbol)+tag+'</td><td>'+it.exchange_name+'</td><td class="'+cls+'">'+it.last_price+'</td><td class="'+cls+'">'+(it.change_pct>=0?'+':'')+it.change_pct+'%</td><td class="'+cls+'">'+(it.change>=0?'+':'')+it.change+'</td><td>'+it.volume+'</td><td>'+it.open_interest+'</td><td>'+it.update_time+'</td></tr>';
  });
  html+='</tbody></table>';
  document.getElementById('fmList').innerHTML=html;
}
function fmRefresh(){
  fetch(fmApi('/futures?only_main='+FM_STATE.onlyMain)).then(function(r){return r.json();}).then(function(d){ if(d.ok) fmRenderList(d); else document.getElementById('fmList').innerHTML='<div class="fm-empty">'+d.msg+'</div>'; });
}
function fmToggleMain(){ FM_STATE.onlyMain=!FM_STATE.onlyMain; document.getElementById('fmMainBtn').textContent=FM_STATE.onlyMain?'显示全部':'只看主力'; fmRefresh(); }

function fmRuleHTML(r,i){
  r=r||{field:'change_pct',op:'>=',threshold:2,enabled:true,name:'涨跌幅超2%'};
  return '<div class="fm-rule" data-i="'+i+'">'
    +'<input type="checkbox" '+(r.enabled?'checked':'')+' onchange="fmRuleChg('+i+')">'
    +'<select onchange="fmRuleChg('+i+')">'
      +'<option value="change_pct" '+(r.field=='change_pct'?'selected':'')+'>涨跌幅%</option>'
      +'<option value="last_price" '+(r.field=='last_price'?'selected':'')+'>最新价</option>'
      +'<option value="volume" '+(r.field=='volume'?'selected':'')+'>成交量</option>'
      +'<option value="open_interest" '+(r.field=='open_interest'?'selected':'')+'>持仓量</option>'
    +'</select>'
    +'<select onchange="fmRuleChg('+i+')">'
      +'<option value=">" '+(r.op=='>'?'selected':'')+'>></option>'
      +'<option value=">=" '+(r.op=='>='?'selected':'')+'>≥</option>'
      +'<option value="<" '+(r.op=='<'?'selected':'')+'><</option>'
      +'<option value="<=" '+(r.op=='<='?'selected':'')+'>≤</option>'
    +'</select>'
    +'<input type="number" step="any" value="'+r.threshold+'" onchange="fmRuleChg('+i+')">'
    +'<input type="text" placeholder="规则名" value="'+(r.name||'')+'" onchange="fmRuleChg('+i+')">'
    +'<button onclick="fmDelRule('+i+')">×</button>'
    +'</div>';
}
function fmRenderRules(){
  var el=document.getElementById('fmRules');
  if(!FM_STATE.rules.length){ el.innerHTML='<div class="fm-empty">暂无规则, 点击添加</div>'; return; }
  el.innerHTML=FM_STATE.rules.map(function(r,i){return fmRuleHTML(r,i);}).join('');
}
function fmAddRule(){ FM_STATE.rules.push({field:'change_pct',op:'>=',threshold:2,enabled:true,name:'涨跌幅超2%'}); fmRenderRules(); }
function fmDelRule(i){ FM_STATE.rules.splice(i,1); fmRenderRules(); }
function fmRuleChg(i){ /* 占位, 实际保存时读 DOM */ }
function fmCollectRules(){
  var out=[];
  document.querySelectorAll('.fm-rule').forEach(function(row){
    var cb=row.querySelector('input[type=checkbox]');
    var sel=row.querySelectorAll('select');
    var num=row.querySelector('input[type=number]');
    var txt=row.querySelector('input[type=text]');
    out.push({enabled:cb.checked, field:sel[0].value, op:sel[1].value, threshold:parseFloat(num.value)||0, name:txt.value||''});
  });
  return out;
}
function fmSaveRules(){
  FM_STATE.rules=fmCollectRules();
  // 网关对 application/json 的 body 转发会丢/乱码; 改为 text/plain + base64(纯 ASCII) 规避中文乱码
  var jsonStr=JSON.stringify({rules:FM_STATE.rules});
  var b64=btoa(unescape(encodeURIComponent(jsonStr)));
  fmFetch(fmApi('/alerts/config'),{method:'POST', headers:{'Content-Type':'text/plain'}, body:b64}).then(function(d){ alert(d.ok?'已保存':'保存失败: '+d.msg); });
}
function fmLoadRules(){
  fmFetch(fmApi('/alerts/config')).then(function(d){ if(d.ok){ FM_STATE.rules=d.rules||[]; fmRenderRules(); } });
}
function fmCheckAlerts(){
  fmFetch(fmApi('/alerts/check'),{method:'POST'}).then(function(d){
    if(!d.ok) return;
    (d.alerts||[]).forEach(function(a){ fmAlert(a); });
    if(!d.alerts.length) alert('当前无触发');
  });
}
function fmAlert(a){
  var el=document.createElement('div'); el.className='fm-alert';
  el.innerHTML='<div class="title">'+a.rule_name+'</div><div>'+a.name+' '+a.symbol+' 当前 '+a.value+'</div>';
  document.body.appendChild(el); setTimeout(function(){el.remove();},6000);
}

// ===== K线设置(计算根数 / 取值根数) =====
var FM_SET=null;
function fmOpenSet(){
  document.getElementById('fmSetMask').classList.add('on');
  fmLoadSet();
}
function fmCloseSet(){ document.getElementById('fmSetMask').classList.remove('on'); }

function fmEsc(s){
  return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function fmRenderSet(d){
  FM_SET=d;
  var minc=d.min_calc_bars||45;
  document.getElementById('fmSetNote').innerHTML=
    '<b>计算根数</b>：从K线仓库取多少根 bar 去算指标。低于 <b>'+minc+'</b> 根时 MACD 处于种子期，'
    +'可能产生假金叉/假死叉 —— 数字会<span style="color:var(--warn)">标黄</span>，仍可保存。'
    +'<br><b>取值根数</b>：算完之后，最近 N 根内出现状态就记录该信号。';
  document.getElementById('fmTipCalc').textContent=(d.hints&&d.hints.calc)||'';
  document.getElementById('fmTipTake').textContent=(d.hints&&d.hints.take)||'';
  var body=document.getElementById('fmSetBody');
  body.innerHTML=(d.items||[]).map(function(it){
    var cw=(it.calc_check&&it.calc_check.warnings)||[];
    var tw=(it.take_check&&it.take_check.warnings)||[];
    var ccls=cw.length?' warn':'';
    var note='';
    if(it.preview){ note=fmEsc(it.preview.hint); }
    if(it.calc_check && it.calc_check.usable===false && it.eff_calc_bars<minc){
      note='<span style="color:var(--warn)">实际仅 '+it.eff_calc_bars+' 根，不足 '+minc+' 根，该周期不出信号</span>';
    }
    var w=cw.concat(tw);
    return '<tr>'
      +'<td>'+fmEsc(it.label)+(it.derived?'<span style="color:var(--muted)">(派生)</span>':'')+'</td>'
      +'<td><input type="number" min="1" step="1" class="fm-calc'+ccls+'" data-p="'+it.period+'" value="'+it.calc_bars+'" oninput="fmMarkWarn(this)"></td>'
      +'<td><input type="number" min="1" step="1" class="fm-take" data-p="'+it.period+'" value="'+it.take_bars+'" oninput="fmMarkWarn(this)"></td>'
      +'<td class="hint">'+note+(w.length?'<div class="fm-warnline">'+fmEsc(w[0])+'</div>':'')+'</td>'
      +'</tr>';
  }).join('');
  document.getElementById('fmSetWarn').textContent=
    '周/月/季由日线重采样而来：周≈日线÷5，月≈日线÷21，季≈日线÷63。月线需日线≥945 根才够 '+minc+' 根门槛。';
}
function fmMarkWarn(el){
  var minc=(FM_SET&&FM_SET.min_calc_bars)||45;
  var v=parseInt(el.value,10);
  var bad=(isNaN(v)||v<1);
  if(el.classList.contains('fm-calc')) bad=bad||(v<minc);
  el.classList.toggle('warn',bad);
}
function fmCollectSet(){
  var calc={},take={};
  document.querySelectorAll('.fm-calc').forEach(function(el){
    var v=parseInt(el.value,10); if(!isNaN(v)&&v>0) calc[el.dataset.p]=v;
  });
  document.querySelectorAll('.fm-take').forEach(function(el){
    var v=parseInt(el.value,10); if(!isNaN(v)&&v>0) take[el.dataset.p]=v;
  });
  return {calc_bars:calc,take_bars:take};
}
function fmPostSet(url,payload){
  var jsonStr=JSON.stringify(payload);
  var b64=btoa(unescape(encodeURIComponent(jsonStr)));
  return fmFetch(fmApi(url),{method:'POST',headers:{'Content-Type':'text/plain'},body:b64});
}
function fmSaveSet(){
  fmPostSet('/klinesettings', fmCollectSet()).then(function(d){
    if(d&&d.ok){ fmRenderSet(d); }
    else alert('保存失败: '+(d&&d.msg||''));
  });
}
function fmResetSet(){
  if(!confirm('恢复标准默认值？')) return;
  fmPostSet('/klinesettings/reset',{}).then(function(d){
    if(d&&d.ok){ fmRenderSet(d); }
    else alert('恢复失败: '+(d&&d.msg||''));
  });
}
function fmLoadSet(){
  fmFetch(fmApi('/klinesettings')).then(function(d){ if(d&&d.ok) fmRenderSet(d); });
}

setInterval(fmRefresh, 5000);
fmRefresh(); fmLoadRules();
</script>
</body>
</html>
"""


REVIEW_PAGE = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>期货监控回顾</title>
<style>
:root{--bg:#0b1220;--surface:#111827;--surface2:#1f2937;--border:#27344f;--text:#e2e8f0;--muted:#94a3b8;--accent:#38bdf8;--up:#ef4444;--down:#22c55e;--warn:#f59e0b;}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;font-size:13px}
.rv-wrap{padding:16px}
.rv-hd{display:flex;align-items:center;gap:12px;margin-bottom:12px}
.rv-hd h1{margin:0;font-size:18px;color:var(--accent)}
.rv-hd .stat{color:var(--muted)}
.rv-filt{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:12px;background:var(--surface);padding:12px;border:1px solid var(--border);border-radius:6px}
.rv-filt input,.rv-filt select{padding:5px 8px;background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:4px;font-size:12px}
.rv-filt button{padding:5px 12px;background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:4px;cursor:pointer;font-size:12px}
.rv-filt button:hover{border-color:var(--accent)}
.rv-table{width:100%;border-collapse:collapse;font-size:12px}
.rv-table th{position:sticky;top:0;background:var(--surface);padding:8px 6px;text-align:left;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border);white-space:nowrap}
.rv-table td{padding:6px;border-bottom:1px solid var(--border);white-space:nowrap}
.rv-table tr:hover{background:rgba(56,189,248,.08)}
.rv-table a{color:var(--accent);text-decoration:none}
.rv-table a:hover{text-decoration:underline}
.rv-empty{text-align:center;color:var(--muted);padding:40px 0}
.rv-up{color:var(--up);font-weight:700}
.rv-down{color:var(--down);font-weight:700}
.pill{font-size:10px;border:1px solid var(--warn);color:var(--warn);padding:0 5px;border-radius:3px}
</style>
</head>
<body>
<div class="rv-wrap">
  <div class="rv-hd">
    <h1>期货监控回顾</h1>
    <span class="stat" id="rvStat">加载中…</span>
    <button onclick="location.href='./'" style="margin-left:auto">返回监控</button>
  </div>
  <div class="rv-filt">
    <label>交易日<input type="date" id="rvDate"></label>
    <label>品种<input type="text" id="rvProduct" placeholder="如 rb" style="width:80px"></label>
    <label>类型
      <select id="rvType">
        <option value="">全部</option>
        <option value="RULE_ALERT">规则预警</option>
      </select>
    </label>
    <button onclick="rvLoad()">查询</button>
    <button onclick="rvReset()">重置</button>
  </div>
  <div id="rvBody"><div class="rv-empty">加载中…</div></div>
</div>
<script>
function rvBase(){ var p=location.pathname; var i=p.lastIndexOf('futuremonitor'); return p.substring(0,i+'futuremonitor'.length); }
function rvApi(path){ return rvBase()+'/api'+path; }
function rvFmt(v){ if(v==null) return ''; if(typeof v==='object') return JSON.stringify(v); return v; }
function rvCls(pct){ return (pct>=0)?'rv-up':'rv-down'; }
function rvLoad(){
  var q=[];
  var d=document.getElementById('rvDate').value; if(d) q.push('date='+encodeURIComponent(d));
  var pr=document.getElementById('rvProduct').value.trim(); if(pr) q.push('product='+encodeURIComponent(pr));
  var t=document.getElementById('rvType').value; if(t) q.push('event_type='+encodeURIComponent(t));
  fetch(rvApi('/events?'+q.join('&'))).then(function(r){return r.json();}).then(function(d){
    if(!d.ok){ document.getElementById('rvBody').innerHTML='<div class="rv-empty">'+d.msg+'</div>'; return; }
    var rows=d.data||[];
    document.getElementById('rvStat').textContent='共 '+d.count+' 条事件';
    if(!rows.length){ document.getElementById('rvBody').innerHTML='<div class="rv-empty">暂无监控事件(夜盘/日盘触发后自动落库, 先配置预警规则)</div>'; return; }
    var html='<table class="rv-table"><thead><tr><th>时间</th><th>交易日</th><th>合约</th><th>品种</th><th>类型</th><th>规则</th><th>字段</th><th>触发值</th><th>阈值</th><th>最新价</th><th>涨跌%</th><th>看K线</th></tr></thead><tbody>';
    rows.forEach(function(r){
      var sd=r.signal_detail||{};
      var ks=r.kline_snapshot||{};
      var cls=rvCls(ks.change_pct);
      var sign=ks.change_pct>=0?'+':'';
      html+='<tr>'
        +'<td>'+rvFmt(r.bar_time)+'</td>'
        +'<td>'+rvFmt(r.trading_day)+'</td>'
        +'<td>'+rvFmt(r.contract)+'</td>'
        +'<td>'+rvFmt(r.product)+'</td>'
        +'<td><span class="pill">'+(r.event_type||'')+'</span></td>'
        +'<td>'+rvFmt(r.rule_name)+'</td>'
        +'<td>'+rvFmt(sd.field)+'</td>'
        +'<td>'+rvFmt(sd.value)+'</td>'
        +'<td>'+rvFmt(sd.threshold)+'</td>'
        +'<td>'+rvFmt(r.last_price)+'</td>'
        +'<td class="'+cls+'">'+(sign+(ks.change_pct||0))+'%</td>'
        +'<td><a href="'+rvBase()+'/../futureskline?symbol='+encodeURIComponent(r.contract)+'&mark_ts='+encodeURIComponent(r.bar_time||'')+'" target="_blank">看K线</a></td>'
        +'</tr>';
    });
    html+='</tbody></table>';
    document.getElementById('rvBody').innerHTML=html;
  });
}
function rvReset(){ document.getElementById('rvDate').value=''; document.getElementById('rvProduct').value=''; document.getElementById('rvType').value=''; rvLoad(); }
(function(){ var n=new Date(); var m=('0'+(n.getMonth()+1)).slice(-2); var d=('0'+n.getDate()).slice(-2); document.getElementById('rvDate').value=n.getFullYear()+'-'+m+'-'+d; })();
rvLoad();
</script>
</body>
</html>
"""


def safe_json(data, status=200):
    def sanitize(obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return 0.0
            return obj
        if isinstance(obj, dict):
            return {k: sanitize(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [sanitize(v) for v in obj]
        return obj
    return Response(json.dumps(sanitize(data), ensure_ascii=False),
                    status=status, mimetype='application/json')


_monitor_service = None
_alert_engine = None


def register_routes(app, slots: dict, default_key: str, get_slot=None):
    global _monitor_service, _alert_engine
    _monitor_service = FutureMonitorService(slots, default_key, get_slot)
    _alert_engine = AlertEngine(default_rules=DEFAULT_MONITOR_RULES)
    # 启动"按交易时段自动监控 + 上升沿落库"后台线程
    _monitor_service.start_auto_loop()

    @app.route("/stockkline/futuremonitor")
    @app.route("/stockkline/futuremonitor/")
    def future_monitor_page():
        return render_template_string(HTML_PAGE)

    @app.route("/stockkline/futuremonitor/review")
    @app.route("/stockkline/futuremonitor/review/")
    def future_monitor_review_page():
        return render_template_string(REVIEW_PAGE)

    @app.route("/stockkline/futuremonitor/api/events")
    def api_monitor_events():
        """回顾事件列表(按交易时段自动落库的历史信号)。"""
        try:
            date = request.args.get("date", "").strip()
            product = request.args.get("product", "").strip()
            event_type = request.args.get("event_type", "").strip()
            raw_limit = request.args.get("limit", "").strip()
            limit = int(raw_limit) if raw_limit.isdigit() else 200
            limit = min(limit, 1000)
            if futures_db is None:
                return safe_json({"ok": False, "msg": "数据库模块不可用"})
            conn = futures_db.get_connection()
            cur = conn.cursor(pymysql.cursors.DictCursor)
            where = []
            params = []
            if date:
                where.append("DATE(trading_day)=%s")
                params.append(date)
            if product:
                where.append("product=%s")
                params.append(product)
            if event_type:
                where.append("event_type=%s")
                params.append(event_type)
            sql = ("SELECT id,contract,exchange,product,event_type,rule_name,signal_detail,"
                   "bar_time,last_price,kline_snapshot,trading_day,created_at "
                   "FROM t_future_monitor_event")
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
            conn.close()
            for r in rows:
                for k in ("signal_detail", "kline_snapshot"):
                    v = r.get(k)
                    if isinstance(v, str):
                        try:
                            r[k] = json.loads(v)
                        except Exception:
                            pass
                for k in ("bar_time", "created_at"):
                    v = r.get(k)
                    if hasattr(v, "strftime"):
                        r[k] = v.strftime("%Y-%m-%d %H:%M:%S")
                v = r.get("trading_day")
                if hasattr(v, "strftime"):
                    r["trading_day"] = v.strftime("%Y-%m-%d")
            return safe_json({"ok": True, "data": rows, "count": len(rows)})
        except Exception as e:
            return safe_json({"ok": False, "msg": str(e)}, 500)

    @app.route("/stockkline/futuremonitor/api/futures")
    def api_futures():
        only_main = request.args.get("only_main", "false").lower() == "true"
        return safe_json(_monitor_service.get_futures(only_main=only_main))

    @app.route("/stockkline/futuremonitor/api/alerts/config", methods=["GET"])
    def api_alerts_config_get():
        return safe_json({"ok": True, "rules": _alert_engine.get_rules()})

    @app.route("/stockkline/futuremonitor/api/alerts/config", methods=["POST"])
    def api_alerts_config_post():
        try:
            # 网关代理对 application/json 的 body 转发有 bug(会丢失/乱码), 前端改走 text/plain 并提交 base64(纯 ASCII),
            # 这里 base64 解码后再 json.loads, 彻底规避中文乱码。
            import base64 as _b64
            raw = request.get_data()
            if isinstance(raw, (bytes, bytearray)):
                raw = bytes(raw).decode("ascii", "replace")
            raw = (raw or "").strip()
            payload = {}
            if raw:
                try:
                    payload = json.loads(_b64.b64decode(raw).decode("utf-8"))
                except Exception:
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        payload = {}
            # 兼容两种提交: {"rules":[...]} 或 直接 [...]
            rules = payload.get("rules", []) if isinstance(payload, dict) else payload
            _alert_engine.set_rules(rules or [])
            return safe_json({"ok": True})
        except Exception as e:
            return safe_json({"ok": False, "msg": str(e)}, 400)

    @app.route("/stockkline/futuremonitor/api/alerts/check", methods=["POST"])
    def api_alerts_check():
        try:
            # 获取当前全部行情(或主力)
            res = _monitor_service.get_futures(only_main=False)
            if not res.get("ok"):
                return safe_json({"ok": False, "msg": res.get("msg")})
            alerts = _alert_engine.check(res.get("data", []))
            return safe_json({"ok": True, "alerts": alerts})
        except Exception as e:
            return safe_json({"ok": False, "msg": str(e)}, 500)

    # ===== K线设置: 计算根数 / 取值根数(2026-10-13 用户拍板, 仅期货监控线) =====
    @app.route("/stockkline/futuremonitor/api/klinesettings", methods=["GET"])
    def api_klinesettings_get():
        try:
            from services import kline_settings as _ks
            return safe_json(_ks.describe())
        except Exception as e:
            return safe_json({"ok": False, "msg": str(e)}, 500)

    @app.route("/stockkline/futuremonitor/api/klinesettings", methods=["POST"])
    def api_klinesettings_post():
        """保存计算根数/取值根数。

        与告警规则同款: 前端提交 base64(text/plain) —— 网关代理对 application/json
        的 body 转发有 bug(丢/乱码), base64 纯 ASCII 彻底规避中文乱码。
        """
        try:
            import base64 as _b64
            from services import kline_settings as _ks
            raw = request.get_data()
            if isinstance(raw, (bytes, bytearray)):
                raw = bytes(raw).decode("ascii", "replace")
            raw = (raw or "").strip()
            payload = {}
            if raw:
                try:
                    payload = json.loads(_b64.b64decode(raw).decode("utf-8"))
                except Exception:
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        payload = {}
            calc = payload.get("calc_bars") if isinstance(payload, dict) else None
            take = payload.get("take_bars") if isinstance(payload, dict) else None
            out = _ks.set_bars(calc_bars=calc, take_bars=take)
            out["saved"] = True
            return safe_json(out)
        except Exception as e:
            return safe_json({"ok": False, "msg": str(e)}, 400)

    @app.route("/stockkline/futuremonitor/api/klinesettings/reset", methods=["POST"])
    def api_klinesettings_reset():
        """恢复标准默认值。"""
        try:
            from services import kline_settings as _ks
            out = _ks.set_bars(
                calc_bars={p: _ks.DEFAULT_CALC_BARS[p] for p in _ks.PERIODS},
                take_bars={p: _ks.DEFAULT_TAKE_BARS[p] for p in _ks.PERIODS})
            out["saved"] = True
            return safe_json(out)
        except Exception as e:
            return safe_json({"ok": False, "msg": str(e)}, 400)

    @app.route("/stockkline/futuremonitor/api/debug")
    def api_debug():
        """调试用: 返回原始 latest_data 中期货合约的样本。"""
        try:
            slot = _monitor_service._slot()
            if not slot:
                return safe_json({"ok": False, "msg": "no slot"})
            fut_set = set(_monitor_service._selected_symbols)
            with slot.data_lock:
                cached = {k: v for k, v in slot.latest_data.items() if k in fut_set}
            # 直接读几个选中合约的 quote
            direct = []
            samples = sorted([s for s in _monitor_service._selected_symbols if s.startswith("SHFE.cu") or s.startswith("DCE.m")])[:3]
            for s in samples:
                try:
                    q = slot.client.get_quote(s)
                    direct.append({"symbol": s, "last_price": _safe_float(getattr(q, "last_price", 0)), "volume": _safe_int(getattr(q, "volume", 0)), "oi": _safe_int(getattr(q, "open_interest", 0))})
                except Exception as e:
                    direct.append({"symbol": s, "err": str(e)})
            return safe_json({"ok": True, "cached_count": len(cached), "cached_samples": list(cached.keys())[:10], "direct_samples": direct, "subscribed": len(slot.subscribed)})
        except Exception as e:
            return safe_json({"ok": False, "msg": str(e)}, 500)

    print("[期货监控] 路由已注册: /stockkline/futuremonitor, /stockkline/futuremonitor/review, /stockkline/futuremonitor/api/futures, /stockkline/futuremonitor/api/events, /stockkline/futuremonitor/api/alerts/*")
