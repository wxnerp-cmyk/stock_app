# -*- coding: utf-8 -*-
"""
监控扫描信号历史入库/查询模块。

用途：
1. 每次全市场扫描完成后，把命中信号异步写入 MySQL (d_lnx_tqhq.t_scan_signal_history)。
2. 提供历史复盘查询接口：按时间范围、周期、合约、方向、信号类型筛选。
"""
import json
import time
import uuid
import traceback
from datetime import datetime, timedelta

from services import futures_db


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS t_scan_signal_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(32) NOT NULL,
    scan_time DATETIME NOT NULL,
    period VARCHAR(16) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    real_symbol VARCHAR(64),
    side VARCHAR(16) NOT NULL,
    signal_type VARCHAR(32) NOT NULL,
    rule_id VARCHAR(64),
    rule_name VARCHAR(128),
    trigger_time DATETIME,
    close_price DECIMAL(18, 6),
    `change` DECIMAL(18, 6),
    change_pct DECIMAL(10, 4),
    zero_line DECIMAL(18, 6),
    zero_distance DECIMAL(18, 6),
    description TEXT,
    signal_json JSON,
    create_time DATETIME DEFAULT NOW(),
    INDEX idx_scan_time (scan_time),
    INDEX idx_period_trigger (period, trigger_time),
    INDEX idx_symbol (symbol),
    INDEX idx_side_type (side, signal_type),
    INDEX idx_batch (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _get_conn():
    return futures_db.get_connection()


def ensure_table():
    """幂等建表 + 追加阈值/算子列(旧表兼容)。

    关键: 必须先设较短的 lock_wait_timeout —— MySQL 元数据锁等待默认长达 1 年,
    若此处直接 ALTER 遇上锁会【永久卡死后台写线程】(2026-08-31 事故: 线程泄漏 +
    列始终补不上, INSERT 持续报 Unknown column 'threshold')。
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION lock_wait_timeout=5")
            cur.execute(_CREATE_TABLE_SQL)
            # 先查再 ALTER(避免不兼容的 IF NOT EXISTS 语法, 也避免无谓抢锁);
            # 并发下若别的连接已加上, 1060 视为成功。
            for col, ddl in (
                ("threshold", "ALTER TABLE t_scan_signal_history ADD COLUMN threshold DECIMAL(18,6)"),
                ("op", "ALTER TABLE t_scan_signal_history ADD COLUMN op VARCHAR(8)"),
            ):
                try:
                    cur.execute("SHOW COLUMNS FROM t_scan_signal_history LIKE %s", (col,))
                    if cur.fetchone():
                        continue
                    for _i in range(3):          # 短暂锁竞争: 最多重试 3 次
                        try:
                            cur.execute(ddl)
                            break
                        except Exception as ae:
                            msg = str(ae)
                            if "Duplicate column" in msg or "1060" in msg:
                                break            # 别的连接已加上
                            if "Lock wait timeout" in msg or "1205" in msg:
                                time.sleep(1)
                                continue
                            raise
                except Exception as e:
                    print(f"[scan_history] 补列 {col} 失败: {e}")
                    raise
        conn.commit()
    except Exception as e:
        # 不要静默吞掉: 建表/补列失败后续 INSERT 还会报 Unknown column, 打印以便定位。
        print(f"[scan_history] ensure_table 失败: {e}")
        raise
    finally:
        conn.close()


def _parse_dt(s):
    """把 ISO / 常用字符串转成 datetime。"""
    if not s:
        return None
    s = str(s).strip().replace('T', ' ')
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def save_scan_signals(scan_result):
    """把一次扫描结果中的全部信号写入历史表（在后台线程调用，不阻塞 HTTP 返回）。"""
    if not scan_result or not scan_result.get("ok"):
        return 0
    try:
        ensure_table()
    except Exception as e:
        # 建表/补列失败不应静默: 否则后续 INSERT 报 Unknown column 时难以定位。
        print(f"[scan_history] ensure_table 调用失败, 后续写入可能异常: {e}")

    batch_id = uuid.uuid4().hex[:16]
    scan_time = _parse_dt(scan_result.get("scan_time")) or datetime.now()
    period = scan_result.get("period", "day")

    rows = []
    for side, key in (("long", "long"), ("short", "short")):
        for item in scan_result.get(key, []):
            symbol = item.get("symbol", "")
            real_symbol = item.get("real_symbol", "")
            close_price = item.get("close")
            change = item.get("change")
            change_pct = item.get("change_pct")
            for sig in item.get("signals", []):
                trigger_time = _parse_dt(sig.get("trigger_time"))
                zero_line = sig.get("zero_line")
                zero_distance = sig.get("zero_distance")
                threshold = sig.get("threshold")
                op = sig.get("op")
                description = sig.get("desc") or ""
                try:
                    signal_json = json.dumps(sig, ensure_ascii=False, default=str)
                except Exception:
                    signal_json = "{}"
                rows.append((
                    batch_id, scan_time, period, symbol, real_symbol, side,
                    sig.get("type", ""), sig.get("rule_id", ""), sig.get("name", ""),
                    trigger_time,
                    close_price if close_price is not None else None,
                    change if change is not None else None,
                    change_pct if change_pct is not None else None,
                    zero_line if zero_line is not None else None,
                    zero_distance if zero_distance is not None else None,
                    threshold if threshold is not None else None,
                    op if op is not None else None,
                    description, signal_json
                ))
    if not rows:
        return 0

    sql = """
        INSERT INTO t_scan_signal_history
            (batch_id, scan_time, period, symbol, real_symbol, side, signal_type,
             rule_id, rule_name, trigger_time, close_price, `change`, change_pct,
             zero_line, zero_distance, threshold, op, description, signal_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
        return cur.rowcount
    except Exception as e:
        print(f"[scan_history] 写入失败: {e}\n{traceback.format_exc()}")
        return 0
    finally:
        conn.close()


def query_history(start=None, end=None, period=None, symbol=None, side=None,
                  signal_type=None, limit=200, offset=0):
    """查询历史信号列表。"""
    ensure_table()
    where = ["1=1"]
    params = []
    if start:
        dt = _parse_dt(start)
        if dt:
            where.append("scan_time >= %s")
            params.append(dt)
    if end:
        dt = _parse_dt(end)
        if dt:
            where.append("scan_time <= %s")
            params.append(dt)
    if period:
        where.append("period = %s")
        params.append(period)
    if symbol:
        where.append("(symbol LIKE %s OR real_symbol LIKE %s)")
        like = f"%{symbol}%"
        params.extend([like, like])
    if side:
        where.append("side = %s")
        params.append(side)
    if signal_type:
        where.append("signal_type = %s")
        params.append(signal_type)

    w = " AND ".join(where)
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT batch_id, scan_time, period, symbol, real_symbol, side,
                           signal_type, rule_id, rule_name, trigger_time, close_price,
                           `change`, change_pct, zero_line, zero_distance, threshold, op, description
                    FROM t_scan_signal_history
                    WHERE {w}
                    ORDER BY scan_time DESC, id DESC
                    LIMIT %s OFFSET %s""",
                tuple(params + [int(limit), int(offset)])
            )
            rows = cur.fetchall()
            cur.execute(
                f"SELECT COUNT(*) FROM t_scan_signal_history WHERE {w}",
                tuple(params)
            )
            total = cur.fetchone()[0]
    finally:
        conn.close()

    cols = [
        "batch_id", "scan_time", "period", "symbol", "real_symbol", "side",
        "signal_type", "rule_id", "rule_name", "trigger_time", "close_price",
        "change", "change_pct", "zero_line", "zero_distance", "threshold", "op", "description"
    ]
    # 兼容前端渲染: 实时/回溯统一用 close 字段名
    cols.append("close")
    rows = [list(row) + [row[10]] for row in rows]  # close_price 复制为 close
    out = []
    for row in rows:
        d = {cols[i]: row[i] for i in range(len(cols))}
        for k in ("scan_time", "trigger_time"):
            if d.get(k):
                try:
                    d[k] = d[k].strftime("%Y-%m-%d %H:%M")
                except Exception:
                    pass
        d["display_name"] = futures_db.option_underlying_name(d.get("real_symbol")) or futures_db.option_underlying_name(d.get("symbol")) or d.get("real_symbol") or d.get("symbol") or "-"
        out.append(d)
    return {"total": total, "rows": out}


def query_session_signals(start=None, end=None, period=None, limit=3000):
    """按盘次时间范围取信号(含 signal_json), 供监控启动/盘次切换时回填本盘已产生的信号。

    与 query_history 的区别: 这里按 scan_time 范围取、带完整 signal_json 以便原样还原信号对象,
    并按 id 升序(回填后由累积器统一按 trigger_time 排序)。
    """
    ensure_table()
    where = ["1=1"]
    params = []
    if start:
        dt = _parse_dt(start)
        if dt:
            where.append("scan_time >= %s")
            params.append(dt)
    if end:
        dt = _parse_dt(end)
        if dt:
            where.append("scan_time <= %s")
            params.append(dt)
    if period:
        where.append("period = %s")
        params.append(period)

    w = " AND ".join(where)
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT symbol, real_symbol, side, trigger_time, close_price, `change`,
                           change_pct, signal_json
                    FROM t_scan_signal_history
                    WHERE {w}
                    ORDER BY id ASC
                    LIMIT %s""",
                tuple(params + [int(limit)])
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    out = []
    for r in rows:
        sig = {}
        try:
            raw = r[7]
            sig = json.loads(raw) if raw else {}
        except Exception:
            sig = {}
        if not isinstance(sig, dict):
            sig = {}
        out.append({
            "symbol": r[0], "real_symbol": r[1], "side": r[2], "trigger_time": r[3],
            "close_price": r[4], "change": r[5], "change_pct": r[6], "signal": sig,
        })
    return out


def query_range_signals(start=None, end=None, period=None, side=None, limit=5000,
                        need_order=True, by_scan_time=False):
    """按时间范围取信号(含 period / display_name / signal 对象), 供「当天」视图与历史列表合并展示。

    与 query_history 的差别: 返回还原后的 signal 对象(前端实时表格需要 signals数组);
    与 query_session_signals 的差别: 额外返回 period 与 display_name, 支持 side 过滤。

    need_order=False(2026-10-09 新增): 只需"有没有/取少量任意行"时关掉排序。
    表已78 万行、当天可达 20 万, 而 `ORDER BY trigger_time` 用不上 idx_period_trigger
    (首列是 period, 而按时间范围查的查询不带 period) -> filesort 排序全量。
    只判存在性的场景(如补扫判定)传 False 可直接走 idx_scan_time 范围扫, 毫秒级返回。
    注意: 关闭排序后返回的行顺序不确定, 仅供"只判断有无"的用途。

    by_scan_time=True(2026-10-09 新增, 性能关键): 排序键换成**已有索引的 scan_time**,
    走 idx_scan_time 的 Backward index scan, 消除 filesort。实测同一"当天"查询:
      ORDER BY trigger_time(原)   -> 15.8~23.9s / LIMIT 1200~5000
      ORDER BY scan_time DESC(新) -> 0.06~0.14s / 同样行数, 去重后品种数 56(反而更全)
    适用前提: 调用方下游按 symbol 去重(b[side][sym] = dict(it)), 只关心"每个品种最新一条",
    不关心跨品种的 trigger_time 先后 —— 满足时用本模式, 不满足则保持默认。
    """
    ensure_table()
    where = ["1=1"]
    params = []
    if start:
        dt = _parse_dt(start)
        if dt:
            where.append("scan_time >= %s")
            params.append(dt)
    if end:
        dt = _parse_dt(end)
        if dt:
            where.append("scan_time <= %s")
            params.append(dt)
    if period:
        where.append("period = %s")
        params.append(period)
    if side:
        where.append("side = %s")
        params.append(side)

    w = " AND ".join(where)
    # 排序键优先级: by_scan_time(走 idx_scan_time 逆序, 无filesort) > need_order(原 trigger_time)
    if not need_order:
        order_sql = ""
    elif by_scan_time:
        order_sql = "ORDER BY scan_time DESC, id DESC"
    else:
        order_sql = "ORDER BY trigger_time ASC, id ASC"
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT symbol, real_symbol, side, period, trigger_time, close_price,
                           `change`, change_pct, signal_type, rule_name, signal_json
                    FROM t_scan_signal_history
                    WHERE {w}
                    {order_sql}
                    LIMIT %s""",
                tuple(params + [int(limit)])
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    out = []
    for r in rows:
        sig = {}
        try:
            sig = json.loads(r[10]) if r[10] else {}
        except Exception:
            sig = {}
        if not isinstance(sig, dict):
            sig = {}
        tt = r[4]
        try:
            tt = tt.strftime("%Y-%m-%d %H:%M") if tt else None
        except Exception:
            tt = str(tt) if tt else None
        sym, real = r[0], r[1]
        out.append({
            "symbol": sym, "real_symbol": real, "side": r[2], "period": r[3],
            "trigger_time": tt,
            "close_price": r[5], "change": r[6], "change_pct": r[7],
            "signal_type": r[8], "rule_name": r[9],
            "display_name": (futures_db.option_underlying_name(real)
                             or futures_db.option_underlying_name(sym)
                             or real or sym or "-"),
            "signal": sig,
        })
    return out


def query_stats(start=None, end=None, period=None, symbol=None, side=None, signal_type=None):
    """统计聚合：在 query_history 同样的筛选条件下, 返回汇总与分布。"""
    ensure_table()
    where = ["1=1"]
    params = []
    if start:
        dt = _parse_dt(start)
        if dt:
            where.append("scan_time >= %s"); params.append(dt)
    if end:
        dt = _parse_dt(end)
        if dt:
            where.append("scan_time <= %s"); params.append(dt)
    if period:
        where.append("period = %s"); params.append(period)
    if symbol:
        where.append("(symbol LIKE %s OR real_symbol LIKE %s)")
        like = f"%{symbol}%"; params.extend([like, like])
    if side:
        where.append("side = %s"); params.append(side)
    if signal_type:
        where.append("signal_type = %s"); params.append(signal_type)

    w = " AND ".join(where)
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT COUNT(*),
                           SUM(CASE WHEN side='long' THEN 1 ELSE 0 END),
                           SUM(CASE WHEN side='short' THEN 1 ELSE 0 END),
                           COUNT(DISTINCT symbol)
                    FROM t_scan_signal_history WHERE {w}""",
                tuple(params)
            )
            total, long_c, short_c, distinct_sym = cur.fetchone()

            cur.execute(
                f"SELECT period, COUNT(*) FROM t_scan_signal_history WHERE {w} GROUP BY period ORDER BY COUNT(*) DESC",
                tuple(params)
            )
            by_period = [{"period": r[0], "cnt": r[1]} for r in cur.fetchall()]

            cur.execute(
                f"""SELECT COALESCE(NULLIF(rule_name,''), signal_type), COUNT(*)
                    FROM t_scan_signal_history WHERE {w}
                    GROUP BY COALESCE(NULLIF(rule_name,''), signal_type) ORDER BY COUNT(*) DESC LIMIT 12""",
                tuple(params)
            )
            by_type = [{"name": r[0], "cnt": r[1]} for r in cur.fetchall()]

            cur.execute(
                f"""SELECT DATE(scan_time), COUNT(*) FROM t_scan_signal_history WHERE {w}
                    GROUP BY DATE(scan_time) ORDER BY DATE(scan_time)""",
                tuple(params)
            )
            by_day = [{"day": str(r[0]), "cnt": r[1]} for r in cur.fetchall()]

            cur.execute(
                f"""SELECT real_symbol, symbol, COUNT(*) c FROM t_scan_signal_history WHERE {w}
                    GROUP BY real_symbol, symbol ORDER BY c DESC LIMIT 10""",
                tuple(params)
            )
            top_symbols = [{"real_symbol": r[0], "symbol": r[1], "cnt": r[2]} for r in cur.fetchall()]
    finally:
        conn.close()

    return {
        "total": total or 0,
        "long_count": long_c or 0,
        "short_count": short_c or 0,
        "distinct_symbols": distinct_sym or 0,
        "by_period": by_period,
        "by_type": by_type,
        "by_day": by_day,
        "top_symbols": top_symbols,
    }
