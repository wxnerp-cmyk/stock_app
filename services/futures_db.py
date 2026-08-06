# -*- coding: utf-8 -*-
"""
期货/期权到期日 MySQL 数据库操作
数据库: d_lnx_tqhq (MySQL 8.0, 端口 3304)
表: t_futures_expiry (期货到期), t_option_expiry (期权行权), t_product_info (品种名称)
"""
import pymysql
import datetime

DB_CONFIG = {
    "host": "192.168.0.108",
    "port": 3304,
    "user": "zhq",
    "password": "zhq",
    "database": "d_lnx_tqhq",
    "charset": "utf8mb4"
}


def get_connection(cfg=None):
    """获取 MySQL 数据库连接"""
    return pymysql.connect(**(cfg or DB_CONFIG))


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
    """查询指定月份期权行权日（按标的合约分组）"""
    contract_month = f"{str(year)[-2:]}{month:02d}"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, exchange_name, underlying, option_class,
                       strike_price, exercise_date
                FROM t_option_expiry
                WHERE contract_month = %s
                ORDER BY underlying, strike_price, option_class
            """, (contract_month,))
            rows = cur.fetchall()
    finally:
        conn.close()

    from collections import defaultdict
    result = defaultdict(lambda: {"exchange": "", "underlyings": defaultdict(lambda: {"calls": [], "puts": []})})
    for symbol, ex_name, underlying, opt_class, strike, ex_date in rows:
        d_str = ex_date.strftime("%Y-%m-%d") if isinstance(ex_date, datetime.date) else str(ex_date)[:10]
        result[d_str]["exchange"] = ex_name
        entry = {"symbol": symbol, "strike": float(strike), "exercise_date": d_str}
        if opt_class in ("CALL", "看涨"):
            result[d_str]["underlyings"][underlying]["calls"].append(entry)
        else:
            result[d_str]["underlyings"][underlying]["puts"].append(entry)
    return dict(result)


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


def sync_from_tqsdk(months_ahead=6, progress_cb=None):
    """从天勤拉取期货到期日写入 t_futures_expiry, progress_cb(total, done, msg)"""
    if progress_cb is None:
        progress_cb = _noop_cb

    import time, math, os, re, calendar as cal
    os.environ.setdefault("NO_PROXY", "shinnytech.com,.shinnytech.com")
    try:
        from tqsdk import TqApi, TqAuth
    except ImportError:
        print("[同步-期货] tqsdk 未安装")
        return 0

    EX = {"SHFE":"上期所","DCE":"大商所","CZCE":"郑商所","CFFEX":"中金所","INE":"上期能源","GFEX":"广期所"}
    print("[同步-期货] 连接天勤...")
    api = TqApi(auth=TqAuth("zhubajie","jinhu303"))
    all_syms = api.query_quotes(ins_class="FUTURE", expired=False)
    total_count = len(all_syms)
    print(f"[同步-期货] {total_count} 个期货")

    by_month = {}
    for s in all_syms:
        parts = s.split(".")
        if len(parts) < 2: continue
        nums = re.findall(r'\d+', parts[1])
        if not nums: continue
        ms = nums[-1]
        if len(ms) in (3,4): by_month.setdefault(ms, []).append(s)

    now = datetime.datetime.now()
    months = []
    for i in range(months_ahead):
        m = now.month + i; y = now.year
        if m > 12: m -= 12; y += 1
        months.append(f"{str(y)[-2:]}{m:02d}")

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
                    if getattr(q,'expired',False): continue
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
                    recs.append((s, ex, EX.get(ex,ex), pc, pn, ms, ds, src))

                if recs:
                    cur.executemany("""INSERT INTO t_futures_expiry
                        (symbol,exchange,exchange_name,product_code,product_name,contract_month,expire_date,data_source,create_time)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON DUPLICATE KEY UPDATE expire_date=VALUES(expire_date)""", recs)
                    conn.commit(); total += len(recs)
                    progress_cb(total_count, total, f"{ms}: +{len(recs)} 条, 累计 {total}")
                    print(f"  +{len(recs)} 条")

        progress_cb(total_count, total, f"完成: 共入库 {total} 条")
        print(f"[同步-期货] 完成, {total} 条")
    finally:
        conn.close(); api.close()
    return total


def sync_options_from_tqsdk(months_ahead=3, progress_cb=None):
    """从天勤拉取期权行权日写入 t_option_expiry, progress_cb(total, done, msg)"""
    if progress_cb is None:
        progress_cb = _noop_cb

    import time, math, os, re
    os.environ.setdefault("NO_PROXY", "shinnytech.com,.shinnytech.com")
    try:
        from tqsdk import TqApi, TqAuth
    except ImportError:
        print("[同步-期权] tqsdk 未安装")
        return 0

    EX = {"SHFE":"上期所","DCE":"大商所","CZCE":"郑商所","CFFEX":"中金所","INE":"上期能源"}
    print("[同步-期权] 连接天勤...")
    api = TqApi(auth=TqAuth("zhubajie","jinhu303"))
    all_opts = api.query_quotes(ins_class="OPTION", expired=False)
    total_count = len(all_opts)
    print(f"[同步-期权] {total_count} 个期权")

    now = datetime.datetime.now()
    months = []
    for i in range(months_ahead):
        m = now.month + i; y = now.year
        if m > 12: m -= 12; y += 1
        months.append(f"{str(y)[-2:]}{m:02d}")

    total = 0; conn = get_connection()
    try:
        with conn.cursor() as cur:
            for ms in months:
                opts = [s for s in all_opts if ms in s]
                if not opts: continue
                print(f"[同步-期权] {ms}: {len(opts)} 个")
                progress_cb(total_count, total, f"正在处理 {ms} ({len(opts)} 期权)")

                BS = 200
                quotes, valid = [], []
                for b in range(0, len(opts), BS):
                    batch = opts[b:b+BS]
                    for s in batch:
                        try: quotes.append(api.get_quote(s)); valid.append(s)
                        except: pass
                    api.wait_update(deadline=time.time()+15)
                    progress_cb(total_count, total, f"{ms}: {len(valid)}/{len(opts)} 已注册")

                recs = []
                for s, q in zip(valid, quotes):
                    if getattr(q,'expired',False): continue
                    lexp = getattr(q,'last_exercise_datetime',0)
                    if not lexp or lexp <= 0: continue
                    d = datetime.datetime.fromtimestamp(lexp).strftime("%Y-%m-%d")
                    ex = s.split(".")[0] if "." in s else ""
                    recs.append((s, ex, EX.get(ex,ex),
                        getattr(q,'underlying_symbol',''),
                        getattr(q,'option_class',''),
                        float(getattr(q,'strike_price',0) or 0),
                        ms, d, "last_exercise"))

                if recs:
                    cur.executemany("""INSERT INTO t_option_expiry
                        (symbol,exchange,exchange_name,underlying,option_class,
                         strike_price,contract_month,exercise_date,data_source,create_time)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON DUPLICATE KEY UPDATE exercise_date=VALUES(exercise_date)""", recs)
                    conn.commit(); total += len(recs)
                    progress_cb(total_count, total, f"{ms}: +{len(recs)} 条, 累计 {total}")
                    print(f"  +{len(recs)} 条")

        progress_cb(total_count, total, f"完成: 共入库 {total} 条")
        print(f"[同步-期权] 完成, {total} 条")
    finally:
        conn.close(); api.close()
    return total