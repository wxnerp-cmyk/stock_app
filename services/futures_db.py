# -*- coding: utf-8 -*-
"""
期货/期权到期日 MySQL 数据库操作
数据库: d_lnx_tqhq (MySQL 8.0, 端口 3304)
表: t_futures_expiry (期货到期), t_option_expiry (期权行权), t_product_info (品种名称)
"""
import pymysql
import datetime
import os
import queue

DB_CONFIG = {
    "host": "192.168.0.106",
    "port": 3304,
    "user": "zhq",
    "password": "zhq1",
    "database": "d_lnx_tqhq",
    "charset": "utf8mb4"
}

# ===== 连接池：避免每次查询都新建 TCP 连接(远程 MySQL 握手约 60ms/次) =====
# 实测未池化时，单次 chain_panel 请求约 4 次 get_connection()，浪费 ~240ms。
# 池化后连接复用，ping 探活，close() 不再真正断开而是归还池中。
_POOL = queue.Queue(maxsize=16)


class _PooledConnection:
    """连接池包装：除 close()/ping()/open 外，所有属性/方法均透传给底层真实连接。"""
    def __init__(self, conn):
        self._c = conn

    def __getattr__(self, name):
        return getattr(self._c, name)

    def close(self):
        # 归还池中而非真正关闭
        _return_connection(self._c)

    def ping(self, reconnect=True):
        return self._c.ping(reconnect=reconnect)

    @property
    def open(self):
        return getattr(self._c, "open", False)


def _return_connection(conn):
    try:
        if getattr(conn, "open", False):
            _POOL.put_nowait(conn)
            return
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass


def get_connection(cfg=None):
    """从连接池取一个可用连接(自动探活)，池空则新建。调用方照常 conn.close() 即可归还。"""
    cfg = cfg or DB_CONFIG
    try:
        conn = _POOL.get_nowait()
        if conn is not None and getattr(conn, "open", False):
            try:
                conn.ping(reconnect=True)
                return _PooledConnection(conn)
            except Exception:
                pass
    except queue.Empty:
        pass
    return _PooledConnection(pymysql.connect(**cfg))


def batch_upsert_expiry(records):
    """批量插入或更新到期日数据"""
    if not records:
        return 0
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO t_futures_expiry
                    (symbol, exchange, exchange_name, product_code, product_name, contract_month, expire_date, data_source, create_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    expire_date = VALUES(expire_date),
                    data_source = VALUES(data_source),
                    create_time = NOW()
            """
            cur.executemany(sql, records)
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def query_by_month(year, month):
    """查询指定月份期货到期日，JOIN 品种名称表获取中文名"""
    contract_month = f"{str(year)[-2:]}{month:02d}"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT e.symbol, COALESCE(p.exchange_name, e.exchange_name),
                       COALESCE(p.product_name, e.product_name), e.expire_date
                FROM t_futures_expiry e
                LEFT JOIN t_product_info p ON e.exchange=p.exchange AND e.product_code=p.product_code
                WHERE e.contract_month = %s
                ORDER BY e.expire_date, e.exchange
            """, (contract_month,))
            rows = cur.fetchall()
    finally:
        conn.close()

    from collections import defaultdict
    result = defaultdict(lambda: {"exchange": "", "products": []})
    for symbol, ex_name, prod_name, expire_date in rows:
        d_str = expire_date.strftime("%Y-%m-%d") if isinstance(expire_date, datetime.date) else str(expire_date)[:10]
        existing = [p[1] for p in result[d_str].get("products", [])]
        if prod_name not in existing:
            result[d_str]["exchange"] = ex_name
            result[d_str]["products"].append((symbol, prod_name))
    return dict(result)


def query_option_by_month(year, month):
    """查询指定月份(按行权日 exercise_date)的期权行权日, 按标的合约分组.
    注意: 商品期权行权日通常早于其合约月(如 b2610 期权 2026-09-16 行权, 合约月为 2610),
    故按行权日所在月份过滤, 保证日历上落在正确的行权日."""
    ym = f"{year}{month:02d}"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, exchange_name, underlying, option_class,
                       strike_price, exercise_date
                FROM t_option_expiry
                WHERE DATE_FORMAT(exercise_date, %s) = %s
                ORDER BY underlying, strike_price, option_class
            """, ("%Y%m", ym))
            rows = cur.fetchall()
    finally:
        conn.close()

    from collections import defaultdict
    result = defaultdict(lambda: {"exchange": "", "underlyings": defaultdict(lambda: {"calls": [], "puts": [], "name": ""})})
    for symbol, ex_name, underlying, opt_class, strike, ex_date in rows:
        d_str = ex_date.strftime("%Y-%m-%d") if isinstance(ex_date, datetime.date) else str(ex_date)[:10]
        result[d_str]["exchange"] = ex_name
        ud = result[d_str]["underlyings"][underlying]
        if not ud["name"]:
            ud["name"] = option_underlying_name(underlying)
        entry = {"symbol": symbol, "strike": float(strike), "exercise_date": d_str}
        if opt_class in ("CALL", "看涨"):
            ud["calls"].append(entry)
        else:
            ud["puts"].append(entry)
    return dict(result)


def query_option_chain(underlying, exercise_date=None):
    """按标的(underlying, 即对应期货合约代码如 SHFE.cu2608)查询其完整期权链
    返回结构: {underlying, exchange, exercise_dates, strikes:[{strike,call,put}], count}
    参数 exercise_date: 可选, 形如 '2026-09-23'。若指定, 仅返回该行权日的合约;
                     若为 None, 返回所有行权日的合约(按 strike 聚合, 同一 strike 可能
                     来自不同行权日, 此时 strikes 列表中 call/put 仅取最早行权日)。
    无数据返回 None
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, exchange_name, option_class, strike_price, contract_month, exercise_date
                FROM t_option_expiry
                WHERE underlying = %s
                ORDER BY strike_price, option_class, exercise_date
            """, (underlying,))
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    from collections import defaultdict
    exchange = rows[0][1]
    strikes = defaultdict(lambda: {"call": None, "put": None})
    ex_dates = set()
    for symbol, ex_name, opt_class, strike, contract_month, ex_date in rows:
        d_str = ex_date.strftime("%Y-%m-%d") if hasattr(ex_date, "strftime") else str(ex_date)[:10]
        # 若指定 exercise_date, 跳过非匹配行
        if exercise_date and d_str != exercise_date:
            continue
        s = float(strike)
        entry = {"symbol": symbol, "exercise": d_str, "contract_month": contract_month}
        if opt_class in ("CALL", "看涨"):
            # 同 strike + 同方向只取第一行（按 ORDER BY exercise_date ASC）
            if strikes[s]["call"] is None:
                strikes[s]["call"] = entry
        else:
            if strikes[s]["put"] is None:
                strikes[s]["put"] = entry
        ex_dates.add(d_str)
    ordered = sorted(strikes.keys())
    return {
        "underlying": underlying,
        "exchange": exchange,
        "exercise_dates": sorted(ex_dates),
        "exercise_date_filter": exercise_date,
        "strikes": [{"strike": k, "call": strikes[k]["call"], "put": strikes[k]["put"]} for k in ordered],
        "count": len(rows),
    }


def resolve_underlying_matches(underlying):
    """将输入的期货关联值解析为库中实际存在的 option underlying 列表.
    精确匹配优先; 否则按品种前缀(如 SHFE.cu -> SHFE.cu2508, DCE.b -> DCE.b2610)匹配.
    品种匹配要求前缀之后紧跟数字, 避免误匹配(如 DCE.b 不匹配 DCE.bz2610).
    返回 [] 表示库中无对应期权数据."""
    if not underlying:
        return []
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # 1) 精确匹配(点击期货日历传来的具体合约)，优先未到期
            cur.execute("SELECT DISTINCT underlying FROM t_option_expiry WHERE underlying=%s AND exercise_date >= CURDATE()", (underlying,))
            exact = [r[0] for r in cur.fetchall()]
            if exact:
                return exact
            # 2) 品种前缀: 去掉尾随数字得到品种前缀(含交易所), 匹配 "前缀+数字" 的标的；只取未到期并按行权日升序
            pre = "".join(c for c in underlying if not c.isdigit())
            if not pre:
                return []
            like = pre + "%"
            cur.execute("SELECT DISTINCT underlying, exercise_date FROM t_option_expiry WHERE underlying LIKE %s AND exercise_date >= CURDATE() ORDER BY exercise_date", (like,))
            cands = [r[0] for r in cur.fetchall()]
            out = []
            for c in cands:
                rest = c[len(pre):] if c.startswith(pre) else c
                if rest and rest[0].isdigit():
                    out.append(c)
            return out
    finally:
        conn.close()


def get_underlying_months_with_days(underlying):
    """返回同品种所有期权标的 [(underlying, exercise_date_str, remaining_days), ...]，按行权日升序。
    用于期权链页面的"期权标的"下拉框：选择某品种后列出其全部可交易月份 + 剩余天数。
    例如 underlying='SHFE.cu2609' -> [('SHFE.cu2508','2026-07-27',-21),('SHFE.cu2609','2026-08-25',8),...]"""
    if not underlying:
        return []
    pre = "".join(c for c in underlying if not c.isdigit())
    if not pre:
        return []
    like = pre + "%"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT underlying, exercise_date FROM t_option_expiry WHERE underlying LIKE %s AND exercise_date >= CURDATE() ORDER BY exercise_date",
                (like,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    out = []
    today = datetime.date.today()
    for u, ex in rows:
        ds = ex.strftime("%Y-%m-%d") if hasattr(ex, "strftime") else str(ex)[:10]
        try:
            ed = datetime.date.fromisoformat(ds)
            rem = (ed - today).days
        except Exception:
            rem = None
        # 仅保留"品种前缀后紧跟数字"的标的，避免 DCE.b 误匹配 DCE.bz
        rest = u[len(pre):] if u.startswith(pre) else u
        if rest and rest[0].isdigit():
            out.append((u, ds, rem))
    # 去重（同一标的可能因多行权日重复），保留较早行权日
    seen = {}
    for u, ds, rem in out:
        if u not in seen:
            seen[u] = (u, ds, rem)
    return list(seen.values())


def get_nearest_underlying(prefix):
    """按品种前缀(如 SHFE.cu, DCE.b)返回最近的期权标的 underlying。
    规则：优先选行权日≥今天且最近的；没有未到期则选已到期中最晚的。
    找不到返回 None。"""
    if not prefix:
        return None
    pre = "".join(c for c in prefix if not c.isdigit())
    if not pre:
        return None
    like = pre + "%"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # 优先只取未到期（含当日）
            cur.execute(
                "SELECT DISTINCT underlying, exercise_date FROM t_option_expiry WHERE underlying LIKE %s AND exercise_date >= CURDATE() ORDER BY exercise_date",
                (like,),
            )
            rows = cur.fetchall()
            # 没有未到期则回退到全部（取最晚到期）
            if not rows:
                cur.execute(
                    "SELECT DISTINCT underlying, exercise_date FROM t_option_expiry WHERE underlying LIKE %s ORDER BY exercise_date DESC",
                    (like,),
                )
                rows = cur.fetchall()
    finally:
        conn.close()
    today = datetime.date.today()
    cands = []
    for u, ex in rows:
        rest = u[len(pre):] if u.startswith(pre) else u
        if not (rest and rest[0].isdigit()):
            continue
        ds = ex.strftime("%Y-%m-%d") if hasattr(ex, "strftime") else str(ex)[:10]
        try:
            ed = datetime.date.fromisoformat(ds)
            rem = (ed - today).days
        except Exception:
            continue
        cands.append((rem, ed, u))
    if not cands:
        return None
    # 优先未到期（rem>=0）中 rem 最小；否则已到期中 rem 最大（绝对值最小）
    future = [c for c in cands if c[0] >= 0]
    if future:
        future.sort(key=lambda x: x[0])
        return future[0][2]
    cands.sort(key=lambda x: -x[0])
    return cands[0][2]


def get_exchange_default_underlying(exchange):
    """按交易所代码返回该交易所下最近行权日的期权标的 underlying。
    优先选行权日≥今天且最近的；没有未到期则选已到期中最晚的。"""
    if not exchange:
        return None
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT underlying, exercise_date FROM t_option_expiry "
                "WHERE exchange=%s AND exercise_date >= CURDATE() ORDER BY exercise_date",
                (exchange,),
            )
            rows = cur.fetchall()
            if not rows:
                cur.execute(
                    "SELECT DISTINCT underlying, exercise_date FROM t_option_expiry "
                    "WHERE exchange=%s ORDER BY exercise_date DESC",
                    (exchange,),
                )
                rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    today = datetime.date.today()
    cands = []
    for u, ex in rows:
        ds = ex.strftime("%Y-%m-%d") if hasattr(ex, "strftime") else str(ex)[:10]
        try:
            ed = datetime.date.fromisoformat(ds)
            rem = (ed - today).days
        except Exception:
            continue
        cands.append((rem, u))
    if not cands:
        return None
    future = [c for c in cands if c[0] >= 0]
    if future:
        future.sort(key=lambda x: x[0])
        return future[0][1]
    cands.sort(key=lambda x: -x[0])
    return cands[0][1]


def list_option_underlyings_by_exchange():
    """返回全部期权标的按交易所分组，用于期权目录页的品种树。
    结构: [(exchange_code, exchange_name, [(underlying, nearest_exercise_date_str), ...]), ...]
    按交易所代码排序，每个交易所内标的按行权日升序。只返回未到期(含当日)标的。"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT underlying, exchange, exchange_name, MIN(exercise_date) "
                "FROM t_option_expiry WHERE exercise_date >= CURDATE() "
                "GROUP BY underlying, exchange, exchange_name "
                "ORDER BY exchange, MIN(exercise_date)"
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return []
    # 交易所展示顺序(商品/股指/ETF)
    order = ["SHFE", "INE", "DCE", "CZCE", "GFEX", "CFFEX", "SSE", "SZSE"]
    grouped = {}
    for underlying, exchange, ex_name, ex_date in rows:
        ds = ex_date.strftime("%Y-%m-%d") if hasattr(ex_date, "strftime") else str(ex_date)[:10]
        grouped.setdefault(exchange, {"name": ex_name or exchange, "items": []})
        grouped[exchange]["items"].append((underlying, ds))
    out = []
    for ex in order:
        if ex in grouped:
            out.append((ex, grouped[ex]["name"], grouped[ex]["items"]))
    # 补充未在预定顺序中的交易所
    for ex in grouped:
        if ex not in order:
            out.append((ex, grouped[ex]["name"], grouped[ex]["items"]))
    return out


# 期权品种前缀 -> 中文名(覆盖常见商品/股指/ETF期权, 命中不到时回退显示前缀)
PROD_NAME = {
    # 上期所
    "SHFE.cu": "沪铜", "SHFE.al": "沪铝", "SHFE.zn": "沪锌", "SHFE.pb": "沪铅",
    "SHFE.ni": "沪镍", "SHFE.sn": "沪锡", "SHFE.ao": "氧化铝", "SHFE.alu": "铝合金",
    "SHFE.au": "沪金", "SHFE.ag": "沪银", "SHFE.rb": "螺纹钢", "SHFE.fu": "燃料油",
    "SHFE.bu": "沥青", "SHFE.br": "BR橡胶", "SHFE.ru": "橡胶", "SHFE.sp": "纸浆",
    # 大商所
    "DCE.a": "豆一", "DCE.b": "豆二", "DCE.c": "玉米", "DCE.cs": "玉米淀粉",
    "DCE.eb": "苯乙烯", "DCE.eg": "乙二醇", "DCE.i": "铁矿石", "DCE.jd": "鸡蛋",
    "DCE.l": "塑料", "DCE.lg": "原木", "DCE.lh": "生猪", "DCE.m": "豆粕",
    "DCE.p": "棕榈油", "DCE.pg": "液化石油气", "DCE.pp": "聚丙烯", "DCE.v": "PVC", "DCE.y": "豆油",
    # 郑商所(库内暂无, 预留)
    "CZCE.TA": "PTA", "CZCE.MA": "甲醇", "CZCE.SR": "白糖", "CZCE.CF": "棉花",
    "CZCE.OI": "菜油", "CZCE.RM": "菜粕", "CZCE.CJ": "红枣", "CZCE.AP": "苹果",
    "CZCE.SA": "纯碱", "CZCE.UR": "尿素", "CZCE.PF": "短纤", "CZCE.SF": "硅铁",
    "CZCE.SM": "锰硅", "CZCE.PK": "花生", "CZCE.FG": "玻璃", "CZCE.ZC": "动力煤",
    "CZCE.PR": "瓶片", "CZCE.PX": "对二甲苯", "CZCE.SH": "烧碱",
    # 广期所
    "GFEX.si": "工业硅", "GFEX.lc": "碳酸锂", "GFEX.ps": "多晶硅",
    "GFEX.pd": "钯", "GFEX.pt": "铂",
    # 上期能源
    "INE.sc": "原油", "INE.lu": "低硫燃油", "INE.nr": "20号胶",
    # 中金所股指(标的形如 SSE.000300)
    "SSE.000016": "上证50股指", "SSE.000300": "沪深300股指", "SSE.000852": "中证1000股指",
    # ETF(免费版暂无, 预留)
    "SSE.50ETF": "50ETF", "SSE.300ETF": "300ETF", "SZSE.500ETF": "500ETF",
    "DCE.bz": "苯乙烯",
}
# 补充库内偶发缺漏/历史并存代码(优先于 t_product_info 自动加载)
PROD_NAME.update({
    "SHFE.op": "胶版纸", "SHFE.ad": "铝合金",
    "DCE.jm": "焦煤", "DCE.j": "焦炭",
    "INE.bc": "国际铜",
    "CZCE.PL": "瓶片",
})

# 品种中文名 DB 映射缓存(来自 t_product_info, 覆盖全市场期货/期权品种, 含 PROD_NAME 未列者)
_PROD_NAME_DB = None


def _load_prod_name_db():
    """从 t_product_info 加载 交易所.品种字母 -> 中文名, 进程内缓存一次。DB 不可用时回退空字典。"""
    global _PROD_NAME_DB
    if _PROD_NAME_DB is not None:
        return _PROD_NAME_DB
    d = {}
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT exchange, product_code, product_name FROM t_product_info")
                for e, p, n in cur.fetchall():
                    if e and p and n:
                        d[f"{e}.{p}"] = n
        finally:
            conn.close()
    except Exception as _e:
        print(f"[品种名] t_product_info 加载失败, 仅用内置映射: {_e}")
    _PROD_NAME_DB = d
    return d


def _prod_name_lookup(exchange, prod):
    """品种键 交易所.字母 -> 中文名: 内置 PROD_NAME 优先, 回退 t_product_info。"""
    key = f"{exchange}.{prod}"
    if key in PROD_NAME:
        return PROD_NAME[key]
    return _load_prod_name_db().get(key)

# 交易所代码 -> 中文名(用于目录页分区标题)
EXCHANGE_CN = {
    "SHFE": "上期所", "INE": "上期能源", "DCE": "大商所", "CZCE": "郑商所",
    "GFEX": "广期所", "CFFEX": "中金所", "SSE": "上交所", "SZSE": "深交所",
}

# ETF 期权标的代码 -> 中文名(标的形如 SSE.510050 / SZSE.159915)
ETF_NAME = {
    "510050": "50ETF", "510300": "300ETF", "510500": "500ETF",
    "588000": "科创50ETF", "588080": "科创50ETF",
    "159901": "深100ETF", "159915": "创业板ETF", "159919": "300ETF", "159922": "500ETF",
}

# 中金所指数期权产品字母 -> 中文名(用于合约代码转换, symbol 形如 CFFEX.IO2608C4000)
INDEX_OPT_PROD = {"IO": "沪深300股指", "HO": "上证50股指", "MO": "中证1000股指"}


def _prod_letters(code):
    """提取合约代码中的产品字母前缀(如 cu2608C80000 -> cu, IO2608C4000 -> IO, RM608 -> RM)。"""
    import re
    m = re.match(r"([A-Za-z]+)", code or "")
    return m.group(1) if m else ""


def option_underlying_name(underlying):
    """期权标的代码 -> 中文可读名(让人好阅读).
    SHFE.cu2608 -> 沪铜2608; CZCE.AP510 -> 苹果510; SSE.000300 -> 沪深300股指; SSE.510050 -> 50ETF。
    命中不到时回退原代码。"""
    if not underlying:
        return underlying
    if underlying in PROD_NAME:                 # 股指标的(键为完整代码如 SSE.000300)
        return PROD_NAME[underlying]
    if "." not in underlying:
        return underlying
    ex, code = underlying.split(".", 1)
    if ex in ("SSE", "SZSE") and code.isdigit():
        return ETF_NAME.get(code, code)         # ETF 标的
    letters = _prod_letters(code)
    if letters:
        nm = _prod_name_lookup(ex, letters)
        if nm:
            return nm + code[len(letters):]   # 产品中文 + 合约月(如 沪铜2608)
    return underlying


def option_symbol_display(symbol, und_name=None):
    """期权合约代码 -> 中文可读名, 原始代码保留在 data-sym/title 供复制。
    SHFE.cu2608C80000 -> 沪铜2608C80000; CFFEX.IO2608C4000 -> 沪深300股指2608C4000;
    ETF(SSE.10011257) -> 用标的名 und_name(行内已有行权价/类型列), 无则回退原代码。"""
    if not symbol or "." not in symbol:
        return symbol
    ex, code = symbol.split(".", 1)
    if ex in ("SSE", "SZSE") and code.isdigit():
        return und_name or code                  # ETF 期权代码不直观, 用标的名
    letters = _prod_letters(code)
    if ex == "CFFEX" and letters in INDEX_OPT_PROD:
        return INDEX_OPT_PROD[letters] + code[len(letters):]
    if letters:
        nm = _prod_name_lookup(ex, letters)
        if nm:
            return nm + code[len(letters):]
    return symbol


def _prod_key(exchange, underlying):
    """从 (exchange, underlying) 提取品种键: 商品取 交易所.品种字母, 股指直接用标的代码。"""
    import re
    if exchange == "CFFEX":
        return underlying  # 股指期权标的形如 SSE.000300
    parts = underlying.split(".")
    if len(parts) >= 2:
        prod = re.sub(r"\d+$", "", parts[1])
        if prod:
            return parts[0] + "." + prod
    return underlying


def list_option_product_tree():
    """返回 (exchange, ex_name, [(prod_key, prod_name, [(underlying, date), ...]), ...]) 三层结构。
    用于期权目录页的 交易所 -> 品种 -> 标的 分层渲染。只返回未到期(含当日)标的。"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT underlying, exchange, exchange_name, MIN(exercise_date) "
                "FROM t_option_expiry WHERE exercise_date >= CURDATE() "
                "GROUP BY underlying, exchange, exchange_name "
                "ORDER BY exchange, MIN(exercise_date)"
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return []
    order = ["SHFE", "INE", "DCE", "CZCE", "GFEX", "CFFEX", "SSE", "SZSE"]
    tree = {}
    for underlying, exchange, ex_name, ex_date in rows:
        ds = ex_date.strftime("%Y-%m-%d") if hasattr(ex_date, "strftime") else str(ex_date)[:10]
        pk = _prod_key(exchange, underlying)
        pname = PROD_NAME.get(pk, pk.split(".")[-1] if "." in pk else pk)
        tree.setdefault(exchange, {"name": ex_name or exchange, "prods": {}})
        tree[exchange]["prods"].setdefault(pk, {"name": pname, "items": []})
        tree[exchange]["prods"][pk]["items"].append((underlying, ds))
    out = []
    for ex in order:
        if ex in tree:
            prods = sorted(tree[ex]["prods"].items(), key=lambda kv: kv[1]["name"])
            prod_list = [(pk, pd["name"], pd["items"]) for pk, pd in prods]
            out.append((ex, EXCHANGE_CN.get(ex, tree[ex]["name"]), prod_list))
    for ex in tree:
        if ex not in order:
            prods = sorted(tree[ex]["prods"].items(), key=lambda kv: kv[1]["name"])
            prod_list = [(pk, pd["name"], pd["items"]) for pk, pd in prods]
            out.append((ex, EXCHANGE_CN.get(ex, tree[ex]["name"]), prod_list))
    return out


def list_option_products_by_exchange(exchange):
    """返回某交易所下当前有未到期期权的品种列表 [(prod_key, prod_name), ...]，按品种名排序。
    商品期权取 交易所.品种字母；股指/ETF 直接取 underlying。"""
    if not exchange or exchange.startswith("__"):
        return []
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT underlying FROM t_option_expiry "
                "WHERE exchange=%s AND exercise_date >= CURDATE()",
                (exchange,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    prod_map = {}
    for (u,) in rows:
        pk = _prod_key(exchange, u)
        # 中文名：精确匹配、忽略大小写；都失败则回退显示品种字母
        pname = PROD_NAME.get(pk)
        if not pname:
            pname = PROD_NAME.get(pk.upper()) or PROD_NAME.get(pk.lower())
        if not pname:
            pname = pk.split(".")[-1] if "." in pk else pk
        prod_map[pk] = pname
    return sorted(prod_map.items(), key=lambda x: x[1])


def get_underlying_exercise_months(underlying):
    """返回该 underlying(精确) 所有期权行权月份 [(y,m), ...], 用于点击跳转定位."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT exercise_date FROM t_option_expiry WHERE underlying=%s", (underlying,))
            dates = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    months = set()
    for d in dates:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
        try:
            months.add((int(ds[:4]), int(ds[5:7])))
        except Exception:
            pass
    return sorted(months)


def get_underlyings_exercise_months(underlyings):
    """返回多个 underlying 的全部期权行权月份 [(y,m), ...] (去重排序)."""
    if not underlyings:
        return []
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(underlyings))
            cur.execute(f"SELECT DISTINCT exercise_date FROM t_option_expiry WHERE underlying IN ({ph})", tuple(underlyings))
            dates = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    months = set()
    for d in dates:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
        try:
            months.add((int(ds[:4]), int(ds[5:7])))
        except Exception:
            pass
    return sorted(months)


def build_option_chain_summary(underlyings):
    """品种(多合约)汇总: {underlying: {exchange, exercise_date, calls, puts, count}}"""
    result = {}
    if not underlyings:
        return result
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for u in underlyings:
                cur.execute(
                    "SELECT symbol, option_class, exercise_date FROM t_option_expiry WHERE underlying=%s",
                    (u,),
                )
                rows = cur.fetchall()
                if not rows:
                    continue
                calls = sum(1 for r in rows if r[1] in ("CALL", "看涨"))
                puts = len(rows) - calls
                ex = rows[0][2]
                ex_s = ex.strftime("%Y-%m-%d") if hasattr(ex, "strftime") else str(ex)[:10]
                sym0 = rows[0][0] or ""
                exg = sym0.split(".")[0] if "." in sym0 else ""
                result[u] = {"exchange": exg, "exercise_date": ex_s, "calls": calls, "puts": puts, "count": len(rows)}
    finally:
        conn.close()
    return result


def batch_upsert_options(records):
    """批量插入期权行权日"""
    if not records:
        return 0
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            sql = """INSERT INTO t_option_expiry
                (symbol, exchange, exchange_name, underlying, option_class,
                 strike_price, contract_month, exercise_date, data_source, create_time)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON DUPLICATE KEY UPDATE exercise_date=VALUES(exercise_date)"""
            cur.executemany(sql, records)
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def _noop_cb(total, done, msg):
    pass


def count_rows(table):
    """返回表行数, 用于判断是否需要首次全量初始化."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            return cur.fetchone()[0]
    finally:
        conn.close()


def get_existing_symbols(table):
    """返回表中已有 symbol 集合, 用于增量同步时跳过已入库合约."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT symbol FROM {table}")
            return set(r[0] for r in cur.fetchall())
    finally:
        conn.close()


def _symbol_to_yyyymm(symbol):
    """从合约代码中提取合约月份并归一化为 6 位完整年月(YYYYMM).

    规则: 去掉交易所前缀后, 取品种字母后的第一个连续数字串作为合约月份。
    支持两种格式:
      - 4 位 YYMM: SHFE.cu2508 -> 202508
      - 3 位 YMM : CZCE.AP610  -> 202610 (郑商所/部分期权代码)
    3 位年份按当前十年 + 最近原则推算, 避免跨年代歧义.
    无法解析返回 None.
    """
    import re
    if not symbol:
        return None
    part = symbol.split(".")[-1] if "." in symbol else symbol
    # 品种字母后的第一个数字串才是合约月份(期权尾部还有行权价数字,不能取最后一个)
    m = re.search(r"[A-Za-z]+(\d+)", part)
    if not m:
        return None
    ms = m.group(1)
    if len(ms) == 4:
        return f"20{ms[:2]}{ms[2:]}"
    if len(ms) == 3:
        y = int(ms[0])
        mm = int(ms[1:3])
        cur_year = datetime.datetime.now().year
        decade = (cur_year // 10) * 10
        yy = decade + y
        if yy - cur_year > 5:
            yy -= 10
        elif cur_year - yy > 5:
            yy += 10
        return f"{yy}{mm:02d}"
    return None


def _build_target_yyyymms(months_ahead, months_back):
    """生成目标合约月份集合 6 位年月."""
    now = datetime.datetime.now()
    targets = set()
    for i in range(-months_back, months_ahead + 1):
        m = now.month + i
        y = now.year
        while m <= 0:
            m += 12
            y -= 1
        while m > 12:
            m -= 12
            y += 1
        targets.add(f"{y}{m:02d}")
    return targets


def _bypass_proxy():
    """强制直连天勤: 本机可能存在透明代理(HTTPS_PROXY)会拦死 auth.shinnytech.com,
    需清除代理变量并放行所有域, 否则 TqSdk 登录会超时。"""
    for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
               "ALL_PROXY", "all_proxy", "NO_PROXY", "no_proxy"):
        os.environ.pop(_k, None)
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"


def sync_from_tqsdk(months_ahead=12, months_back=12, progress_cb=None, incremental=False):
    """从天勤拉取期货到期日写入 t_futures_expiry, progress_cb(total, done, msg)
    默认覆盖过去 12 个月 + 未来 12 个月, 包含已到期合约(过期日在日历上灰色显示)"""
    if progress_cb is None:
        progress_cb = _noop_cb

    import time, math, os, re, calendar as cal
    _bypass_proxy()
    try:
        from tqsdk import TqApi, TqAuth
    except ImportError:
        print("[同步-期货] tqsdk 未安装")
        return 0

    EX = {"SHFE":"上期所","DCE":"大商所","CZCE":"郑商所","CFFEX":"中金所","INE":"上期能源","GFEX":"广期所"}
    print("[同步-期货] 连接天勤...")
    api = TqApi(auth=TqAuth("zhubajie","jinhu303"))
    all_syms = api.query_quotes(ins_class="FUTURE", expired=False)
    if incremental:
        # 增量: 仅保留库里还没有的合约, 跳过已入库的(到期日静态不会变)
        existing = get_existing_symbols("t_futures_expiry")
        all_syms = [s for s in all_syms if s not in existing]
        print(f"[同步-期货] 增量模式: 库中已 {len(existing)} 个, 待新增 {len(all_syms)} 个")
    total_count = len(all_syms)
    print(f"[同步-期货] {total_count} 个期货(待处理)")

    target_yyyymms = _build_target_yyyymms(months_ahead, months_back)
    by_month = {}
    skipped_no_month = 0
    for s in all_syms:
        ym = _symbol_to_yyyymm(s)
        if ym is None:
            skipped_no_month += 1
            continue
        by_month.setdefault(ym, []).append(s)

    months = sorted(target_yyyymms)
    print(f"[同步-期货] 目标月份 {months[0]} ~ {months[-1]}, 按月份分组 {len(by_month)} 个, 无法解析月份 {skipped_no_month} 个")

    total = 0; conn = get_connection()
    try:
        with conn.cursor() as cur:
            for idx, ms in enumerate(months):
                if ms not in by_month: continue
                syms = by_month[ms]
                progress_cb(total_count, total, f"正在处理 {ms} ({len(syms)} 合约)")
                print(f"[同步-期货] {ms}: {len(syms)} 个")

                quotes, valid = [], []
                for s in syms:
                    try: quotes.append(api.get_quote(s)); valid.append(s)
                    except: pass
                api.wait_update(deadline=time.time()+20)

                recs = []
                for s, q in zip(valid, quotes):
                    # 已到期合约也入库, 日历上以灰色显示; 不再因 expired 跳过
                    exp = getattr(q,'expire_datetime',float('nan'))
                    if exp and not math.isnan(exp) and exp > 0:
                        ds = datetime.datetime.fromtimestamp(exp).strftime("%Y-%m-%d"); src = "expire_datetime"
                    else:
                        dy = getattr(q,'delivery_year',0); dm = getattr(q,'delivery_month',0)
                        if dy>0 and dm>0: ds = f"{dy}-{dm:02d}-{cal.monthrange(dy,dm)[1]:02d}"; src = "delivery_month"
                        else: continue
                    ex = s.split(".")[0] if "." in s else ""
                    pc = ''.join(c for c in (s.split(".")[1] if "." in s else s) if c.isalpha())
                    pn = getattr(q,'instrument_name',pc)
                    recs.append((s, ex, EX.get(ex,ex), pc, pn, ms[-4:], ds, src))

                if recs:
                    cur.executemany("""INSERT INTO t_futures_expiry
                        (symbol,exchange,exchange_name,product_code,product_name,contract_month,expire_date,data_source,create_time)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON DUPLICATE KEY UPDATE
                            exchange_name=VALUES(exchange_name),
                            product_code=VALUES(product_code),
                            product_name=VALUES(product_name),
                            contract_month=VALUES(contract_month),
                            expire_date=VALUES(expire_date)""", recs)
                    conn.commit(); total += len(recs)
                    progress_cb(total_count, total, f"{ms}: +{len(recs)} 条, 累计 {total}")
                    print(f"  +{len(recs)} 条")

        progress_cb(total_count, total, f"完成: 共入库 {total} 条")
        print(f"[同步-期货] 完成, {total} 条")
    finally:
        conn.close(); api.close()
    return total


def sync_options_from_tqsdk(months_ahead=12, months_back=12, progress_cb=None, incremental=False, exchanges=None):
    """从天勤拉取期权行权日写入 t_option_expiry。

    默认仅拉未到期期权(未来 months_ahead 个月)。把 months_back 设 >0 时,
    会一并回溯拉取已到期(近月)的商品期货期权, 用于补全历史到期日。
    progress_cb(total, done, msg)
    """
    if progress_cb is None:
        progress_cb = _noop_cb

    import time, math, os, re
    _bypass_proxy()
    try:
        from tqsdk import TqApi, TqAuth
    except ImportError:
        print("[同步-期权] tqsdk 未安装")
        return 0

    EX = {"SHFE":"上期所","DCE":"大商所","CZCE":"郑商所","CFFEX":"中金所","INE":"上期能源","GFEX":"广期所","SSE":"上交所","SZSE":"深交所"}
    print("[同步-期权] 连接天勤...")
    api = TqApi(auth=TqAuth("zhubajie","jinhu303"))

    # 收集候选 symbol: 未到期 + 已到期(回溯)。
    # 合约月份无法从代码可靠解析(尤其上交所/深交所 ETF 期权代码是8位期权号),
    # 故先全部收为候选, 真正到期日/合约月份在抓取 quote 后用 last_exercise_datetime 反推。
    seen = set(); candidates = []; cand_by_ex = {}
    for exp in (False, True):
        try:
            d = api.query_quotes(ins_class="OPTION", expired=exp)
            keys = list(d.keys()) if hasattr(d, "keys") else list(d)
            added = 0
            for s in keys:
                if s in seen:
                    continue
                seen.add(s); candidates.append(s); added += 1
                ex = s.split(".")[0] if "." in s else "OTHER"
                cand_by_ex[ex] = cand_by_ex.get(ex, 0) + 1
            print(f"[同步-期权] query_quotes(expired={exp}): 候选新增 {added} 个")
        except Exception as e:
            print(f"[同步-期权] query_quotes(expired={exp}) 失败: {e}")

    print(f"[同步-期权] 候选按交易所统计: {cand_by_ex}")

    if exchanges:
        exset = set(exchanges)
        before_ex = len(candidates)
        candidates = [s for s in candidates if s.split(".")[0] in exset]
        print(f"[同步-期权] 限定交易所 {exchanges}: {before_ex} -> {len(candidates)}")

    if incremental:
        # 增量: 仅保留库里还没有的合约, 跳过已入库的(行权日静态不会变)
        existing = get_existing_symbols("t_option_expiry")
        before = len(candidates)
        candidates = [s for s in candidates if s not in existing]
        print(f"[同步-期权] 增量模式: 库中已 {len(existing)} 个, 待新增 {before}->{len(candidates)} 个")
    # ---- 合约月份窗口过滤(核心优化) ----
    # 天勤 query_quotes 不支持按到期日/product 过滤期权, 未到期+已到期候选合计 20 万+,
    # 若全部 get_quote 订阅根本跑不完(这正是历史月份大量缺失的根因)。
    # 本地解析合约月份(各交易所代码规则, _symbol_to_yyyymm 已支持 SHFE/DCE/CZCE/CFFEX/GFEX/INE,
    # ETF期权 8位数字代码无法解析->跳过), 只保留行权日窗口附近的合约月, 候选从 20万 降到 1~2 万。
    tgt = _build_target_yyyymms(months_ahead + 1, months_back + 1)  # 合约月窗口比行权日窗口各放宽1个月
    kept, skipped = [], 0
    for s in candidates:
        ym = _symbol_to_yyyymm(s)
        if ym and ym in tgt:
            kept.append(s)
        else:
            skipped += 1
    if skipped:
        print(f"[同步-期权] 合约月窗口过滤: {len(candidates)} -> {len(kept)} (跳过 {skipped} 个窗口外/ETF等无法解析)")
    candidates = kept
    total_count = len(candidates)
    print(f"[同步-期权] 合计候选 {total_count} 个期权")

    # 到期日窗口(过滤过远/过旧): [今天-months_back月, 今天+months_ahead月]
    today = datetime.date.today()
    min_ex = today - datetime.timedelta(days=int(months_back * 31))
    max_ex = today + datetime.timedelta(days=int(months_ahead * 31))
    min_ex_str = min_ex.strftime("%Y-%m-%d")
    max_ex_str = max_ex.strftime("%Y-%m-%d")
    print(f"[同步-期权] 到期日窗口: {min_ex_str} ~ {max_ex_str}")

    total = 0; inserted_by_ex = {}; permission_skipped = 0; conn = get_connection()
    try:
        with conn.cursor() as cur:
            BS = 800
            for b in range(0, len(candidates), BS):
                batch = candidates[b:b+BS]
                quotes, valid = [], []
                for s in batch:
                    try:
                        quotes.append(api.get_quote(s)); valid.append(s)
                    except Exception as e:
                        err = str(e)
                        if "不支持查看" in err or "需要购买" in err:
                            permission_skipped += 1
                api.wait_update(deadline=time.time()+15)
                progress_cb(total_count, total, f"批{b//BS+1}: 已订阅 {len(valid)}/{len(candidates)}")

                recs = []
                for s, q in zip(valid, quotes):
                    # 天勤官方字段语义(api.py 文档):
                    #   expire_datetime      = 到期具体日(=最后交易日), 即交易软件"到期日"
                    #   last_exercise_datetime= 期权最后行权日
                    # 期权日历应展示"到期日", 故用 expire_datetime(而非 last_exercise_datetime)。
                    # 实测当前活跃合约两字段同天, 但语义上 expire_datetime 才是正确"到期日"。
                    edt = getattr(q, 'expire_datetime', 0)
                    if not edt or edt <= 0:
                        continue
                    dd = datetime.datetime.fromtimestamp(edt)
                    exercise_date = dd.strftime("%Y-%m-%d")
                    if exercise_date < min_ex_str or exercise_date > max_ex_str:
                        continue
                    ex = s.split(".")[0] if "." in s else ""
                    ms = dd.strftime("%Y%m")  # 完整 YYYYMM (到期月)
                    recs.append((s, ex, EX.get(ex, ex),
                        getattr(q, 'underlying_symbol', ''),
                        getattr(q, 'option_class', ''),
                        float(getattr(q, 'strike_price', 0) or 0),
                        ms[-4:], exercise_date, "expire_datetime"))  # contract_month 存 YYMM(到期月)
                    inserted_by_ex[ex] = inserted_by_ex.get(ex, 0) + 1
                if recs:
                    cur.executemany("""INSERT INTO t_option_expiry
                        (symbol,exchange,exchange_name,underlying,option_class,
                         strike_price,contract_month,exercise_date,data_source,create_time)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON DUPLICATE KEY UPDATE
                            exchange_name=VALUES(exchange_name),
                            underlying=VALUES(underlying),
                            option_class=VALUES(option_class),
                            strike_price=VALUES(strike_price),
                            contract_month=VALUES(contract_month),
                            exercise_date=VALUES(exercise_date)""", recs)
                    conn.commit(); total += len(recs)
                    progress_cb(total_count, total, f"批{b//BS+1}: +{len(recs)} 条, 累计 {total}")
                    print(f"  +{len(recs)} 条")

        progress_cb(total_count, total, f"完成: 共入库 {total} 条")
        print(f"[同步-期权] 完成, {total} 条")
        print(f"[同步-期权] 按交易所入库统计: {inserted_by_ex}")
        if permission_skipped:
            print(f"[同步-期权] 因账号权限跳过(SSE/SZSE ETF期权等): {permission_skipped} 个")
    finally:
        conn.close(); api.close()
    return total