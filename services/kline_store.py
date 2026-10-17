# -*- coding: utf-8 -*-
"""K线仓库: bar/指标/coverage 三层存储 + 异步写 + 查缺 + 级联重算 + 对账。

设计原则(2026-10-13 Spec, 用户拍板):
  1. 信号唯一入口 = 新 bar 收盘事件 —— 本模块只管存取, 判定在调用方(scan_engine);
  2. 判定输入 = 定稿窗口 —— bar 只存已收盘根, 成型中 bar 由调用方在内存工作集维护, 不落库;
  3. 周/月/季不落本库, 由 day bar 经 _resample_daily 派生(根治 lu 周线错档);
  4. 排序安全: bar 主键 (symbol, period, bar_time), 读取恒 ORDER BY bar_time,
     物理插入顺序无关; 指标层补缺后从 gap_time 级联重算(见 recompute_forward);
  5. 热路径永不阻塞等 DB: AsyncWriter 单写线程批量落库, 失败进内存缓冲恢复追写。

用法(调用方):
    from services import kline_store as ks
    ks.record_closed_bar(symbol, period, bar_dict)     # 收盘事件: 异步写 bar
    ks.record_indicators(symbol, period, ind_dict)     # 异步写指标
    df = ks.get_bars(symbol, period, count=300)        # 取定稿窗口(DataFrame)
"""
import json
import queue
import threading
import time
import traceback
from datetime import datetime, timedelta

import pandas as pd

from services import futures_db
from services import indicators

# 原子周期(落库); 周/月/季由 day 派生, 不在此列
ATOMIC_PERIODS = ("1m", "5m", "15m", "30m", "60m", "day")
IND_CALC_VER = "v1"          # 指标口径版本(改口径时+1, 旧值自动视为脏)
MAX_BUFFERED = 20000         # 异步写内存缓冲上限(超过告警丢弃最旧)

# ---------------------------------------------------------------- 指标列清单(2026-10-14 扩列)
# 设计(用户拍板): 首次按拉取到的范围根数一次性算全指标入库, 之后每新增一根 bar
#   只算这根; 出状态直接读库, 不再为判定单独整窗重算。
#
# 🔴 计算窗口口径(2026-10-14 实测校正, 勿改):
#   EMA 家族(dif/dea/macd_bar/ma*): 带记忆递推, 局部窗口与全窗逐位一致(实测 diff=0);
#   累计量家族(obv/pvt/ad): cumsum, 第N根依赖第1根, **局部窗口算必错**
#      (实测第250根 obv 全窗=-423098 vs 局部70根=-107436), 必须全窗;
#   Wilder 平滑(rsi*/atr14): 跨窗权重, 局部窗口差 ~1e-4~0.03, 可接受;
#   定长窗口(kdj/cci/wvad/量比/vr/boll): 局部窗口即可(实测 diff=0.0000)。
#   => 实现统一取全窗(仓库仅 500 根滑窗, 成本可接受)以保证与判定侧**逐位一致**;
#      省掉的是"判定时再算一遍", 不是常数级的"只算1根"。
#
# 列名 = indicators.compute_all() 的 key 全小写
IND_COLS = (
    # MACD
    "dif", "dea", "macd_bar",
    # 均线
    "ma5", "ma10", "ma20", "ma60",
    # 布林
    "boll_upper", "boll_mid", "boll_lower",
    # 摆动
    "rsi6", "rsi12", "kdj_k", "kdj_d", "kdj_j", "atr14", "cci14",
    # 量价
    "obv", "vr", "vroc", "wvad", "pvt", "ad", "vol_ratio",
    # 持仓
    "oi", "oi_diff",
)
# compute_all 的 key -> 库列名
IND_KEY2COL = {
    "macdDif": "dif", "macdDea": "dea", "macdBar": "macd_bar",
    "ma5": "ma5", "ma10": "ma10", "ma20": "ma20", "ma60": "ma60",
    "bollUpper": "boll_upper", "bollMid": "boll_mid", "bollLower": "boll_lower",
    "rsi6": "rsi6", "rsi12": "rsi12",
    "kdjK": "kdj_k", "kdjD": "kdj_d", "kdjJ": "kdj_j",
    "atr14": "atr14", "cci14": "cci14",
    "obv": "obv", "vr": "vr", "vroc": "vroc", "wvad": "wvad",
    "pvt": "pvt", "ad": "ad", "volRatio": "vol_ratio",
    "oi": "oi", "oiDiff": "oi_diff",
}
# 递推型列(有跨窗状态: EMA/Wilder/累计量)。注意"可增量"分两种:
#   a) EMA 家族(dif/dea/macd_bar/ma*): 带记忆的递推, 读上一行前值即可算这根;
#   b) 累计量家族(obv/pvt/ad): cumsum, 第N根值依赖第1根, 局部窗口算必错(实测),
#      必须读全窗(我们本来就只留500根滑窗, 全窗成本可接受)。
# c) Wilder 平滑(rsi*/atr14): 带跨窗权重, 实测局部窗口(70根)与全窗差 ~0.0001~0.03,
#    精度可接受; 若要严格逐位一致请用 calc_incremental 全窗路径。
IND_RECURSIVE_COLS = ("dif", "dea", "macd_bar", "ma5", "ma10", "ma20", "ma60",
                      "obv", "pvt", "ad", "oi", "oi_diff")
# 窗口型列(定长窗口, 取该bar往前N根已存值即可判定)
IND_WINDOW_COLS = ("rsi6", "rsi12", "kdj_k", "kdj_d", "kdj_j", "atr14", "cci14",
                   "vr", "vroc", "wvad", "vol_ratio", "boll_upper", "boll_mid", "boll_lower")
# 🔴 累计量列: cumsum 语义, 绝不能用局部窗口增量(会归零重算), 必须全窗
IND_CUMULATIVE_COLS = ("obv", "pvt", "ad")
# 各窗口型指标最小历史根数(实测: tmp/verify_incremental_feasibility.py)
IND_MIN_HISTORY = {"rsi6": 7, "rsi12": 13, "kdj_k": 5, "kdj_d": 5, "kdj_j": 5,
                   "atr14": 14, "cci14": 14, "wvad": 5, "vol_ratio": 5, "vr": 5,
                   "vroc": 11, "boll_upper": 20, "boll_mid": 20, "boll_lower": 20,
                   "ma60": 60, "dif": 45, "dea": 45}
# MACD 种子纪律: 低于此根数不产出 dif/dea(与 MIN_BARS=45 同源)
MACD_SEED_MIN = 45
# compute_all key -> 在 get_inds 返回列序中的位置(供 _row_to_ind_dict 用)
_IND_KEY2IDX = {k: i + 1 for i, k in enumerate(IND_COLS)}


def ensure_ind_columns():
    """幂等扩展 t_kline_bar_ind 为全指标列。可重复调用, 已存在则跳过。

    用单条 ALTER 批量加列(逐列 ALTER 在大表上极慢, 会被中间件超时杀掉)。
    """
    conn = _get_conn()
    added = []
    try:
        cur = conn.cursor()
        cur.execute("SHOW COLUMNS FROM t_kline_bar_ind")
        have = {r[0].lower() for r in cur.fetchall()}
        need = [c for c in IND_COLS if c not in have]
        if need:
            alter = "ALTER TABLE t_kline_bar_ind " + ", ".join(
                "ADD COLUMN `%s` double NULL" % c for c in need)
            cur.execute(alter)
            conn.commit()
            added = need
        cur.close()
    finally:
        conn.close()
    return added


# ---------------------------------------------------------------- 连接
def _get_conn():
    return futures_db.get_connection()


# ---------------------------------------------------------------- 建表(幂等)
# 🔴 打包/装机必须: 随包 db_dump 是旧档(2026-09-22), 不含 K线仓库三张表。
#    所以本模块自己 CREATE TABLE IF NOT EXISTS, 装机首次启动即自动建好,
#    并顺带把 t_kline_bar_ind 补齐 26 指标列(老库升级路径)。
_TABLES_SQL = (
    # 1) 数据表: 原始 OHLCV(周/月/季由此派生)
    """CREATE TABLE IF NOT EXISTS t_kline_bar (
         symbol   VARCHAR(32)  NOT NULL COMMENT '如KQ.m@DCE.jd',
         period   VARCHAR(8)   NOT NULL COMMENT '原子周期 1m/5m/15m/30m/60m/day',
         bar_time DATETIME     NOT NULL COMMENT 'bar开始时间',
         open DOUBLE NULL, high DOUBLE NULL, low DOUBLE NULL, close DOUBLE NULL,
         volume BIGINT NULL, amount DOUBLE NULL, oi DOUBLE NULL COMMENT '持仓量',
         source VARCHAR(16) NOT NULL DEFAULT 'tqsdk',
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         PRIMARY KEY (symbol, period, bar_time)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='K线bar仓库(原子周期;周/月/季由day派生)'""",
    # 2) 简介表: 覆盖范围 + 指标断点 + 查缺
    """CREATE TABLE IF NOT EXISTS t_kline_coverage (
         symbol VARCHAR(32) NOT NULL,
         period VARCHAR(8)  NOT NULL,
         bar_count INT NOT NULL DEFAULT 0 COMMENT 'bar根数',
         min_time DATETIME NULL COMMENT '覆盖起始',
         max_time DATETIME NULL COMMENT '覆盖最新',
         last_closed_time DATETIME NULL COMMENT '最近一根已收盘bar',
         last_ind_time DATETIME NULL COMMENT '指标算到哪根(级联重算断点)',
         gap_flag TINYINT NOT NULL DEFAULT 0 COMMENT '1=有空洞待补',
         gap_detail VARCHAR(255) NULL COMMENT '空洞摘要',
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         PRIMARY KEY (symbol, period),
         KEY idx_gap (gap_flag),
         KEY idx_gap_upd (updated_at)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='覆盖简化表:滑窗清理+查缺+重算断点'""",
    # 3) 主力合约跟踪: 换月检测 -> 历史重刷(2026-10-17, 乙二醇60m死叉错位根因修复)
    """CREATE TABLE IF NOT EXISTS t_kline_roll (
         symbol VARCHAR(32) NOT NULL COMMENT '如KQ.m@DCE.eg',
         period VARCHAR(8)  NOT NULL COMMENT '原子周期(与coverage同口径)',
         real_symbol VARCHAR(32) NOT NULL COMMENT '上次ingest时天勤返回的主力合约名',
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         PRIMARY KEY (symbol, period)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='主力换月跟踪: real变化即清历史重灌'""",
)

# 4) 计算表: 全指标(先建基础3列, 再由 ensure_ind_columns 补齐其余23列)
_TABLES_SQL += (
    """CREATE TABLE IF NOT EXISTS t_kline_bar_ind (
         symbol VARCHAR(32) NOT NULL,
         period VARCHAR(8)  NOT NULL,
         bar_time DATETIME NOT NULL,
         dif DOUBLE NULL, dea DOUBLE NULL, macd_bar DOUBLE NULL,
         calc_ver VARCHAR(16) NOT NULL DEFAULT 'v1' COMMENT '计算口径版本',
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         PRIMARY KEY (symbol, period, bar_time)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='K线指标层(可随时全量重建)'""",
)


def ensure_tables():
    """幂等建三张表 + 补齐指标列。启动时调用一次(轻量, IF NOT EXISTS)。"""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        for sql in _TABLES_SQL:
            cur.execute(sql)
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return ensure_ind_columns()


# ---------------------------------------------------------------- bar 层
def _f(v):
    """数值清洗: NaN/inf/非数值 -> None(MySQL 不认 NaN; 薄品种日线会有缺量)。"""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if v != v or v in (float("inf"), float("-inf")):
        return None
    return v


def upsert_bars(symbol, period, rows):
    """批量写 bar(INSERT ... ON DUPLICATE KEY UPDATE)。rows: [{bar_time, open, high, low, close, volume, amount, oi}]"""
    if not rows:
        return 0
    conn = _get_conn()
    try:
        cur = conn.cursor()
        sql = ("INSERT INTO t_kline_bar "
               "(symbol, period, bar_time, open, high, low, close, volume, amount, oi, source) "
               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
               "ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), "
               "close=VALUES(close), volume=VALUES(volume), amount=VALUES(amount), oi=VALUES(oi)")
        payload = [(symbol, period, r["bar_time"], _f(r["open"]), _f(r["high"]), _f(r["low"]),
                    _f(r["close"]), _f(r.get("volume", 0)), _f(r.get("amount")), _f(r.get("oi")),
                    r.get("source", "tq"))
                   for r in rows]
        cur.executemany(sql, payload)
        conn.commit()
        n = cur.rowcount
        cur.close()
        return n
    finally:
        conn.close()


def get_bars(symbol, period, count=300, end_time=None):
    """取最近 count 根定稿 bar(按 bar_time 升序)。end_time: 取该时刻之前(不含)。"""
    _ensure_schema_once()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        if end_time is None:
            cur.execute(
                "SELECT bar_time, open, high, low, close, volume FROM t_kline_bar "
                "WHERE symbol=%s AND period=%s ORDER BY bar_time DESC LIMIT %s",
                (symbol, period, int(count)))
        else:
            cur.execute(
                "SELECT bar_time, open, high, low, close, volume FROM t_kline_bar "
                "WHERE symbol=%s AND period=%s AND bar_time < %s ORDER BY bar_time DESC LIMIT %s",
                (symbol, period, end_time, int(count)))
        rows = list(reversed(cur.fetchall()))
        cur.close()
    finally:
        conn.close()
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["bar_time", "open", "high", "low", "close", "volume"])
    df = df.set_index("bar_time")
    return df


# ---------------------------------------------------------------- 指标层
_IND_COLUMN_CACHE = {"cols": None, "ts": 0.0}
_SCHEMA_READY = {"ok": False}


def _ensure_schema_once():
    """首次访问本模块时自动建表+补列(打包装机后无需手工执行 DDL)。

    失败不抛(可能是 DB 暂时不可达), 只标记未就绪, 由各写入路径自己报错更清晰。
    """
    if _SCHEMA_READY["ok"]:
        return
    try:
        ensure_tables()
        _SCHEMA_READY["ok"] = True
        _IND_COLUMN_CACHE["cols"] = None   # 让列缓存失效, 重新读
    except Exception:
        pass


def existing_ind_columns():
    """返回 t_kline_bar_ind 当前真实存在的指标列(带 60s 缓存, 避免热路径反复 SHOW COLUMNS)。"""
    _ensure_schema_once()
    now = time.time()
    if _IND_COLUMN_CACHE["cols"] is not None and now - _IND_COLUMN_CACHE["ts"] < 60:
        return _IND_COLUMN_CACHE["cols"]
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SHOW COLUMNS FROM t_kline_bar_ind")
        have = {r[0].lower() for r in cur.fetchall()}
        cur.close()
    finally:
        conn.close()
    cols = [c for c in IND_COLS if c in have]
    _IND_COLUMN_CACHE["cols"] = cols
    _IND_COLUMN_CACHE["ts"] = now
    return cols


def upsert_inds(symbol, period, rows):
    """批量写指标(全指标列)。rows: [{bar_time, <各指标列>}] 或 [{bar_time, _ind: {key: val}}]

    行可以给库列名(dif/dea/kdj_k...)或 compute_all 的 key(_ind 嵌套字典)。
    未提供的列写 NULL —— 窗口型指标在历史根数不足时本就没有值, 这是正确行为。
    🔴 降级保护: 若 DDL 尚未加列, 只写库里真实存在的列, 避免整条 ingest 因 Unknown column 崩。
       (加列后自动写全列; 缓存 60s 生效)
    """
    if not rows:
        return 0
    use_cols = existing_ind_columns()
    if not use_cols:
        use_cols = ["dif", "dea", "macd_bar"]
    cols = ["symbol", "period", "bar_time"] + use_cols + ["calc_ver"]
    placeholders = ",".join(["%s"] * len(cols))
    upd = ",".join("`%s`=VALUES(`%s`)" % (c, c) for c in use_cols)
    sql = ("INSERT INTO t_kline_bar_ind (%s) VALUES (%s) "
           "ON DUPLICATE KEY UPDATE %s, calc_ver=VALUES(calc_ver)"
           % (",".join("`%s`" % c for c in cols), placeholders, upd))
    payload = []
    for r in rows:
        src = dict(r.get("_ind") or {})
        src.update({k: v for k, v in r.items() if k != "_ind"})
        vals = [symbol, period, r["bar_time"]]
        for col in use_cols:
            v = src.get(col)
            if v is None:
                v = src.get(_col2key(col))
            vals.append(_f(v))
        vals.append(IND_CALC_VER)
        payload.append(tuple(vals))
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.executemany(sql, payload)
        conn.commit()
        n = cur.rowcount
        cur.close()
        return n
    finally:
        conn.close()


def _col2key(col):
    """库列名 -> compute_all key(反向映射, 供 get_inds 返回可复用命名)。"""
    for k, c in IND_KEY2COL.items():
        if c == col:
            return k
    return None


def _row_to_ind_dict(row):
    """DB 行(bar_time + IND_COLS) -> {'_ind': {key: val}} 供 indicators 复用命名。"""
    return {"_ind": {k: row[i] for k, i in _IND_KEY2IDX.items()}}


def get_inds(symbol, period, count=300, end_time=None, min_calc_ver=None, cols=None):
    """取最近 count 根指标(升序)。min_calc_ver: 只取不低于该口径版本的行(防脏值)。

    cols: 只取指定列(默认全指标列)。返回 DataFrame(index=bar_time)。
    """
    want = list(cols) if cols else list(IND_COLS)
    have = set(existing_ind_columns())
    want = [c for c in want if c in have] or ["dif", "dea", "macd_bar"]
    sel = ", ".join("`%s`" % c for c in want)
    conn = _get_conn()
    try:
        cur = conn.cursor()
        # calc_ver 存的是字符串(如 'v1'), 用字符串比较避免 MySQL 隐式数值转换
        ver_sql = "" if min_calc_ver is None else " AND calc_ver >= %s"
        args = [symbol, period] + ([] if min_calc_ver is None else [str(min_calc_ver)])
        base = ("SELECT bar_time, " + sel + " FROM t_kline_bar_ind "
                "WHERE symbol=%s AND period=%s" + ver_sql)
        if end_time is None:
            sql = base + " ORDER BY bar_time DESC LIMIT %s"
            cur.execute(sql, tuple(args + [int(count)]))
        else:
            sql = base + " AND bar_time < %s ORDER BY bar_time DESC LIMIT %s"
            cur.execute(sql, tuple(args + [end_time, int(count)]))
        rows = list(reversed(cur.fetchall()))
        cur.close()
    finally:
        conn.close()
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["bar_time"] + want).set_index("bar_time")
    return df


def _compute_ind_frame(df):
    """对 DataFrame(bar_time索引 + OHLCV[+oi]) 一次性算全指标, 返回 DataFrame(同索引)。

    所有指标在此统一由 indicators.compute_all 计算, 口径与判定侧完全一致。
    """
    d = df.copy()
    if "oi" in d.columns and "open_interest" not in d.columns:
        d["open_interest"] = d["oi"]
    res = indicators.compute_all(d, include_oi=True)
    out = {}
    for key, col in IND_KEY2COL.items():
        vals = res.get(key)
        if vals is None:
            continue
        out[col] = pd.Series(list(vals), index=df.index, dtype="float64")
    return pd.DataFrame(out, index=df.index)


def _advance_ind_breakpoint(symbol, period, last_time):
    """推进 coverage.last_ind_time(级联重算断点)。"""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO t_kline_coverage (symbol, period, last_ind_time) VALUES (%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE last_ind_time=VALUES(last_ind_time)",
            (symbol, period, pd.Timestamp(last_time).to_pydatetime()))
        conn.commit()
        cur.close()
    finally:
        conn.close()


def recompute_forward(symbol, period, from_time=None):
    """从 from_time(含)起重算该 symbol 全部指标并落库(级联重算/对账入口)。

    from_time=None = 全量重算(注意: 递推型指标必须从最早bar起算, 否则种子值不对;
                              "断点续算"仅适用于口径变更后重算新段, 见 calc_incremental)。
    返回 (重算根数, 漂移超阈值根数)。漂移容忍: 绝对差 > 1e-6 视为不一致(仅告警)。
    """
    df = get_bars(symbol, period, count=100000)
    if df is None or len(df) < MACD_SEED_MIN:
        return 0, 0  # 低于 MACD 种子门槛, 不算(45 根纪律)
    if from_time is not None:
        df = df[df.index >= pd.Timestamp(from_time)]
        if df.empty:
            return 0, 0
    ind_df = _compute_ind_frame(df)
    old = get_inds(symbol, period, count=100000, cols=["dif", "dea"])
    drift = 0
    if old is not None and not old.empty:
        merged = ind_df[["dif", "dea"]].join(
            old.rename(columns={"dif": "old_dif", "dea": "old_dea"}), how="inner")
        if not merged.empty:
            d = (merged["dif"] - merged["old_dif"]).abs() + (merged["dea"] - merged["old_dea"]).abs()
            drift = int((d > 1e-6).sum())
    rows = []
    for t in df.index:
        r = {"bar_time": t}
        for col in IND_COLS:
            v = ind_df[col].loc[t] if col in ind_df.columns else None
            r[col] = None if v is None or v != v else float(v)
        rows.append(r)
    n = upsert_inds(symbol, period, rows)
    _advance_ind_breakpoint(symbol, period, df.index[-1])
    return n, drift


def calc_incremental(symbol, period):
    """增量补算: 只为"表中尚无指标行"的已收盘 bar 计算指标并入库。

    首次全量回填 / 补齐缺口用本函数(幂等: 已有行不重复算)。
    因为指标窗口需前序历史, 实现上取该 pair 的全部已收盘 bar 统一 compute_all 后
    只写"缺失的那些行" —— 计算是全窗的, 但写入是增量的, 且与整窗口径逐位一致。
    返回 (新增行数, 计算的bar 根数)。
    """
    df = get_bars(symbol, period, count=100000)
    if df is None or df.empty:
        return 0, 0
    have = get_inds(symbol, period, count=100000, cols=["dif"])
    have_idx = set(have.index) if have is not None else set()
    missing = [t for t in df.index if t not in have_idx]
    if not missing:
        return 0, len(df)
    ind_df = _compute_ind_frame(df)
    rows = []
    for t in missing:
        r = {"bar_time": t}
        for col in IND_COLS:
            v = ind_df[col].loc[t] if col in ind_df.columns else None
            r[col] = None if v is None or v != v else float(v)
        rows.append(r)
    n = upsert_inds(symbol, period, rows)
    _advance_ind_breakpoint(symbol, period, df.index[-1])
    return n, len(df)


def calc_one_new_bar(symbol, period, bar_time):
    """新增单根 bar 的实时增量算(热路径)。

    🔴 口径纪律(实测得出, 别改):
      - 累计量列(obv/pvt/ad) 是 cumsum 语义, 第N根依赖第1根 -> **必须全窗算**,
        用局部窗口会让累计值归零重算(实测第250根 obv 全窗=-423098 vs 局部=-107436, 完全错);
      - EMA 家族(dif/dea/macd_bar/ma*) 带记忆递推, 全窗也正确(全窗仅500根, 成本可接受);
      - Wilder 平滑(rsi*/atr14) 有跨窗权重, 局部窗口差 ~1e-4 量级, 可接受;
      - 定长窗口(kdj/cci/wvad/量比/vr/boll) 局部窗口即可, 实测 diff=0.0000。
      => 当前实现统一取全窗(滑窗上限 500 根)一次算, 保证与判定侧口径**逐位一致**;
         真正省的是"不必再为判定单独重算", 而不是"只算 1 根的常数"。
    幂等: 该 bar_time 已有指标行则直接跳过。返回 (是否写入, 状态说明)。
    """
    have = get_inds(symbol, period, count=1)
    if have is not None and not have.empty and pd.Timestamp(bar_time) in have.index:
        return False, "already"
    # 取全窗(仓库本身只保留 N 根滑窗, 这里就是仓库里的全部已收盘 bar)
    bars = get_bars(symbol, period, count=100000)
    if bars is None or bars.empty:
        return False, "no_bars"
    bt = pd.Timestamp(bar_time)
    if bt not in bars.index:
        return False, "bar_missing"
    d = _compute_ind_frame(bars)
    if bt not in d.index:
        return False, "no_ind"
    r = {"bar_time": bt}
    for col in IND_COLS:
        v = d[col].loc[bt] if col in d.columns else None
        r[col] = None if v is None or v != v else float(v)
    upsert_inds(symbol, period, [r])
    _advance_ind_breakpoint(symbol, period, bt)
    return True, "ok"


# ---------------------------------------------------------------- coverage 简化表
def refresh_coverage(symbol, period):
    """重算并更新 coverage(bar_count/min/max/last_closed)。"""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*), MIN(bar_time), MAX(bar_time) FROM t_kline_bar "
            "WHERE symbol=%s AND period=%s", (symbol, period))
        cnt, mn, mx = cur.fetchone()
        cur.execute(
            "INSERT INTO t_kline_coverage (symbol, period, bar_count, min_time, max_time, last_closed_time) "
            "VALUES (%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE bar_count=VALUES(bar_count), min_time=VALUES(min_time), "
            "max_time=VALUES(max_time), last_closed_time=VALUES(last_closed_time)",
            (symbol, period, cnt or 0, mn, mx, mx))
        conn.commit()
        cur.close()
        return cnt or 0
    finally:
        conn.close()


def list_coverage(period=None, gap_only=False):
    conn = _get_conn()
    try:
        cur = conn.cursor()
        sql = "SELECT symbol, period, bar_count, min_time, max_time, last_closed_time, last_ind_time, gap_flag, gap_detail FROM t_kline_coverage"
        conds, args = [], []
        if period:
            conds.append("period=%s"); args.append(period)
        if gap_only:
            conds.append("gap_flag=1")
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY symbol, period"
        cur.execute(sql, tuple(args))
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def get_coverage(symbol, period):
    """取单个 (symbol, period) 的 coverage 行(返回 dict 或 None)。

    调用方(热路径 ingest)用它判断"库里的最后一根 bar 到哪了", 只写更新的增量。
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT symbol, period, bar_count, min_time, max_time, last_closed_time, "
            "last_ind_time, gap_flag, gap_detail FROM t_kline_coverage "
            "WHERE symbol=%s AND period=%s", (symbol, period))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return {
            "symbol": row[0], "period": row[1], "bar_count": row[2],
            "min_time": row[3], "max_time": row[4], "last_closed_time": row[5],
            "last_ind_time": row[6], "gap_flag": row[7], "gap_detail": row[8],
        }
    finally:
        conn.close()


# ---------------------------------------------------------------- 主力换月跟踪(roll)
# 🔴 三个函数都必须先 _ensure_schema_once(): 打包装机升级时 t_kline_roll 尚不存在,
# 直接查会抛 "Table doesn't exist" —— ingest 侧 except 兜底会把它误判成"换月",
# 每轮全量重灌(性能灾难)。自确保一次后幂等, 热路径成本仅一次标志位判断。
def get_roll(symbol, period):
    """取上次 ingest 时记录的主力合约名(无记录返回 None)。

    2026-10-17: 天勤主连 KQ.m@xxx 的 kline_serial = 当前主力合约的完整历史(无拼接),
    主力换月后实时口径整体切换为新合约价格。ingest 是增量语义(只写 >max_time 的
    新 bar), 历史段停留在旧合约 -> 仓库成拼凑序列。调用方(ingest)用它检测换月。
    """
    _ensure_schema_once()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT real_symbol FROM t_kline_roll WHERE symbol=%s AND period=%s",
                    (symbol, period))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    finally:
        conn.close()


def set_roll(symbol, period, real_symbol):
    """记录本次 ingest 的主力合约名(upsert)。real_symbol 为空则跳过。"""
    if not real_symbol:
        return
    _ensure_schema_once()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO t_kline_roll (symbol, period, real_symbol) VALUES (%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE real_symbol=VALUES(real_symbol)",
                    (symbol, period, real_symbol))
        conn.commit()
        cur.close()
    finally:
        conn.close()


def purge_symbol_period(symbol, period):
    """删除该 (symbol, period) 的全部历史: bar + 指标 + coverage。返回删除的 bar 行数。

    2026-10-17 换月重刷专用: 主力切换后旧口径历史必须整段作废(含指标层——
    calc_incremental 只补缺失行, 旧口径 dif/dea 不会被覆盖, 必须显式删)。
    """
    _ensure_schema_once()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM t_kline_bar WHERE symbol=%s AND period=%s", (symbol, period))
        n = cur.rowcount
        cur.execute("DELETE FROM t_kline_bar_ind WHERE symbol=%s AND period=%s", (symbol, period))
        cur.execute("DELETE FROM t_kline_coverage WHERE symbol=%s AND period=%s", (symbol, period))
        conn.commit()
        cur.close()
        return n
    finally:
        conn.close()


def mark_gap(symbol, period, detail):
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO t_kline_coverage (symbol, period, gap_flag, gap_detail) VALUES (%s,%s,1,%s) "
            "ON DUPLICATE KEY UPDATE gap_flag=1, gap_detail=VALUES(gap_detail)",
            (symbol, period, detail[:250]))
        conn.commit()
        cur.close()
    finally:
        conn.close()


def clear_gap(symbol, period):
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE t_kline_coverage SET gap_flag=0, gap_detail=NULL WHERE symbol=%s AND period=%s",
            (symbol, period))
        conn.commit()
        cur.close()
    finally:
        conn.close()


def cleanup(symbol, period, keep):
    """滑窗清理: 只保留最近 keep 根(按 bar_time), 删更早的 bar+ind。返回删除根数。"""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT bar_time FROM t_kline_bar WHERE symbol=%s AND period=%s "
            "ORDER BY bar_time DESC LIMIT 1 OFFSET %s", (symbol, period, int(keep) - 1))
        r = cur.fetchone()
        if not r:
            cur.close()
            return 0
        cutoff = r[0]
        cur.execute(
            "DELETE FROM t_kline_bar WHERE symbol=%s AND period=%s AND bar_time < %s",
            (symbol, period, cutoff))
        n_bar = cur.rowcount
        cur.execute(
            "DELETE FROM t_kline_bar_ind WHERE symbol=%s AND period=%s AND bar_time < %s",
            (symbol, period, cutoff))
        conn.commit()
        cur.close()
        return n_bar
    finally:
        conn.close()


# ---------------------------------------------------------------- 异步写
class AsyncWriter(object):
    """单写线程: bar/ind/coverage 写操作入队, 批量落库。
    - 热路径只入队(微秒级), 永不阻塞等 DB;
    - 写失败: 该项进 _failed 缓冲(上限 MAX_BUFFERED), 后台每 30s 重试追写;
    - stop(): 优雅退出(flush 残留)。
    """
    _inst = None
    _lock = threading.Lock()

    @classmethod
    def instance(cls):
        with cls._lock:
            if cls._inst is None:
                cls._inst = cls()
            return cls._inst

    def __init__(self):
        self.q = queue.Queue()
        self._failed = []
        self._stop = False
        self.t = threading.Thread(target=self._run, name="kline-store-writer", daemon=True)
        self.t.start()

    def submit(self, kind, *args, **kwargs):
        """kind: 'bars'/'inds'/'coverage'"""
        self.q.put((kind, args, kwargs))

    def _run(self):
        batch = []
        last_flush = time.time()
        while not self._stop:
            try:
                item = self.q.get(timeout=0.5)
                batch.append(item)
            except queue.Empty:
                pass
            # 批量条件: 满 200 项 或 距上次 flush > 1s
            if batch and (len(batch) >= 200 or time.time() - last_flush >= 1.0):
                self._flush(batch)
                batch = []
                last_flush = time.time()
            # 失败追写
            if self._failed and time.time() - last_flush >= 30:
                retry, self._failed = self._failed, []
                self._flush(retry, is_retry=True)
                last_flush = time.time()
        if batch:
            self._flush(batch)

    def _flush(self, batch, is_retry=False):
        # 同类合并减少 RTT: bars/inds 按 (symbol,period) 聚拢
        merged = {}
        for kind, args, kwargs in batch:
            if kind in ("bars", "inds"):
                key = (kind, args[0], args[1])
                merged.setdefault(key, []).extend(args[2] if len(args) > 2 else [])
            else:
                key = ("other", id(args), id(kwargs))
                merged[key] = (kind, args, kwargs)
        for key, val in merged.items():
            try:
                if key[0] == "bars":
                    upsert_bars(key[1], key[2], val)
                elif key[0] == "inds":
                    upsert_inds(key[1], key[2], val)
                else:
                    kind, args, kwargs = val
                    if kind == "coverage":
                        refresh_coverage(args[0], args[1])
            except Exception:
                if not is_retry and len(self._failed) < MAX_BUFFERED:
                    self._failed.append((key[0] if isinstance(key, tuple) else "other", key[1:] if isinstance(key, tuple) else (), {}))
                    traceback.print_exc()

    def stop(self):
        self._stop = True
        self.t.join(timeout=5)


def record_closed_bar(symbol, period, bar_dict):
    """热路径入口: 收盘 bar 异步入库(bar_dict 同 upsert_bars 的 row)。"""
    AsyncWriter.instance().submit("bars", symbol, period, [bar_dict])


def record_indicators(symbol, period, ind_dict):
    AsyncWriter.instance().submit("inds", symbol, period, [ind_dict])
