# -*- coding: utf-8 -*-
"""期货监控「计算根数 / 取值根数」设置(2026-10-13 用户拍板)。

两个设置作用在两个不同环节, 不可混为一谈:

  计算根数(calc_bars) —— 【算指标】环节: 从K线仓库取多少根 bar 去算指标。
      下限 45 根 = MACD(12,26,9) 的种子门槛(slow26 + signal9 = 35 根才出值,
      45 根是已验证的安全线)。低于此值算出的 DIF/DEA 是种子垃圾 = PVC 假金叉事故根因。
      → 低于阈值【数字标黄提示, 不阻止保存、不阻止运行】(用户 2026-10-13 裁定)。

  取值根数(take_bars) —— 【记录信号】环节: 算完之后, 最近 N 根内出现过状态就记录。
      语义 = "最近 N 根有交叉就记录"(对应检测器原有的 recent_bars 参数)。
      每周期一个值, 不按规则类型细分(用户裁定: 方便用户使用)。

存储: t_kline_setting (k/v, 期货监控线独占, 不与股票线共用)。

派生周期(周/月/季)换算(用户 2026-10-13 拍板双向换算提示):
    派生周期由日线本地重采样, 故"日线计算根数"决定派生周期的实际可用根数:
        日线 500 根 -> 周 ~100 / 月 ~23 / 季 ~10
    月线要过 45 根门槛, 日线需 >= 945 根(~3.8 年)。
    填月 -> 提示日线需 >= 945; 填日线 -> 提示月线实际可得根数。
"""
import json
import threading

from services import futures_db

# ---------------------------------------------------------------- 常量
PERIODS = ("1m", "5m", "15m", "30m", "60m", "day", "week", "month", "quarter")
ATOMIC_PERIODS = ("1m", "5m", "15m", "30m", "60m", "day")
DERIVED_PERIODS = ("week", "month", "quarter")

PERIOD_LABEL = {
    "1m": "1分钟", "5m": "5分钟", "15m": "15分钟", "30m": "30分钟",
    "60m": "60分钟", "day": "日线", "week": "周线", "month": "月线", "quarter": "季线",
}

MIN_CALC_BARS = 45          # MACD 种子门槛(硬底线, 低于此值不出信号)
MIN_TAKE_BARS = 1           # 取值窗口至少 1 根
MAX_CALC_BARS = 10000       # 天勤序列长度上限
MAX_TAKE_BARS = 500

# 标准默认值(用户 2026-10-13: "默认按照标准来")
#   计算根数: 原子周期 300(与现行 WIN=300 同口径, 切换不回退) / 日线 500 / 派生周期 300
#   取值根数: 3(与现行 DEFAULT_RULES 的 recent_bars=3 同口径)
DEFAULT_CALC_BARS = {
    "1m": 300, "5m": 300, "15m": 300, "30m": 300, "60m": 300,
    "day": 500, "week": 300, "month": 300, "quarter": 300,
}
DEFAULT_TAKE_BARS = {p: 3 for p in PERIODS}

# 派生周期换算系数: N 根派生周期约需多少根日线
_DERIVED_DAY_FACTOR = {"week": 5, "month": 21, "quarter": 63}

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS t_kline_setting (
    setting_key   VARCHAR(64) NOT NULL COMMENT 'k=v, 如 calc_bars.60m',
    setting_value TEXT        NOT NULL,
    update_time   DATETIME    DEFAULT NOW() ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (setting_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='期货监控K线仓库设置(计算根数/取值根数)';
"""

_lock = threading.Lock()
_cache = {}          # {'calc_bars': {period: int}, 'take_bars': {period: int}}
_cache_loaded = False


def _get_conn():
    return futures_db.get_connection()


def ensure_table():
    """幂等建表(锁等待设短, 避免元数据锁永久阻塞 —— 与本项目其它表同款防护)。"""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION lock_wait_timeout=5")
            cur.execute(_CREATE_TABLE_SQL)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------- 读写
def _load(force=False):
    """读全表到内存缓存(设置项极少, 每次判定路径都读会浪费连接)。"""
    global _cache, _cache_loaded
    with _lock:
        if _cache_loaded and not force:
            return _cache
    vals = {}
    try:
        ensure_table()
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT setting_key, setting_value FROM t_kline_setting")
                for k, v in cur.fetchall():
                    vals[k] = v
        finally:
            conn.close()
    except Exception as e:
        print(f"[kline_settings] 读取失败, 使用默认值: {e}")
    parsed = {"calc_bars": {}, "take_bars": {}}
    for p in PERIODS:
        for kind in parsed:
            try:
                parsed[kind][p] = int(vals.get(f"{kind}.{p}", ""))
            except (TypeError, ValueError):
                parsed[kind][p] = (DEFAULT_CALC_BARS if kind == "calc_bars"
                                   else DEFAULT_TAKE_BARS)[p]
    with _lock:
        _cache = parsed
        _cache_loaded = True
    return parsed


def _save_one(key, value):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO t_kline_setting (setting_key, setting_value) VALUES (%s,%s) "
                "ON DUPLICATE KEY UPDATE setting_value=VALUES(setting_value)",
                (key, str(value)))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------- 对外 API
def get_calc_bars(period):
    """该周期的计算根数(取多少根 bar 算指标)。"""
    return int(_load()["calc_bars"].get(period) or DEFAULT_CALC_BARS.get(period, 300))


def get_take_bars(period):
    """该周期的取值根数(最近 N 根内有状态就记录)。"""
    return int(_load()["take_bars"].get(period) or DEFAULT_TAKE_BARS.get(period, 3))


def get_effective_calc_bars(period):
    """该周期实际可用的计算根数 —— 受日线深度钳制(派生周期场景)。

    派生周期由日线重采样得到, 所以周/月/季的真实可用根数 = 日线深度 / 换算系数,
    小于用户填的值时以真实值为准(并由校验层标黄提示)。
    """
    n = get_calc_bars(period)
    if period in _DERIVED_DAY_FACTOR and period not in ATOMIC_PERIODS:
        day_n = get_effective_calc_bars("day")
        capped = day_n // _DERIVED_DAY_FACTOR[period]
        n = min(n, capped)
    return max(0, n)


def set_bars(calc_bars=None, take_bars=None):
    """保存设置(只写入传入的项)。返回实际保存后的全量设置 + 校验结果。"""
    ensure_table()
    written = []
    for kind, src in (("calc_bars", calc_bars), ("take_bars", take_bars)):
        if not src:
            continue
        for p in PERIODS:
            if p not in src:
                continue
            try:
                v = int(src[p])
            except (TypeError, ValueError):
                continue
            _save_one(f"{kind}.{p}", v)
            written.append(f"{kind}.{p}")
    if written:
        _load(force=True)
    return describe()


# ---------------------------------------------------------------- 校验/元数据
def validate_calc_bars(period, n):
    """计算根数校验。低于 45 根只标黄, 不阻止(用户 2026-10-13 裁定: 只警告不限制)。"""
    warnings = []
    if n < MIN_CALC_BARS:
        warnings.append(
            f"低于 MACD(12,26,9) 种子门槛 {MIN_CALC_BARS} 根: 指标处于种子期, "
            f"可能产生假金叉/假死叉(PVC 事故教训), 该周期实际不出信号")
    if n > MAX_CALC_BARS:
        warnings.append(f"超过天勤序列上限 {MAX_CALC_BARS} 根, 将被截断")
    return {
        "level": "warn" if warnings else "ok",
        "warnings": warnings,
        "usable": n >= MIN_CALC_BARS,
    }


def validate_take_bars(period, n, calc_bars=None):
    """取值根数校验: 必须 >=1; 且计算根数要留得下"种子期 + 取值窗口"。"""
    warnings = []
    if n < MIN_TAKE_BARS:
        warnings.append(f"取值根数至少 {MIN_TAKE_BARS} 根")
    cb = calc_bars if calc_bars is not None else get_calc_bars(period)
    # 取值窗口不能伸进种子期: 至少要有 45 根种子 + N 根取值
    if cb < MIN_CALC_BARS + n - 1:
        warnings.append(
            f"计算根数 {cb} 偏小: 建议 >= {MIN_CALC_BARS + n - 1} "
            f"(种子期 {MIN_CALC_BARS} + 取值窗口 {n})")
    return {
        "level": "warn" if warnings else "ok",
        "warnings": warnings,
        "usable": n >= MIN_TAKE_BARS,
    }


def _derived_preview(period, day_calc=None):
    """派生周期换算预览: 该周期实际能得到多少根。"""
    if period not in _DERIVED_DAY_FACTOR:
        return None
    day_n = day_calc if day_calc is not None else get_effective_calc_bars("day")
    avail = day_n // _DERIVED_DAY_FACTOR[period]
    need = MIN_CALC_BARS
    return {
        "factor": _DERIVED_DAY_FACTOR[period],
        "day_bars": day_n,
        "available": avail,
        "need": need,
        "enough": avail >= need,
        "hint": (f"日线 {day_n} 根 → {PERIOD_LABEL[period]} 约 {avail} 根"
                 + ("" if avail >= need else f", 少于门槛 {need} 根, 该周期不出信号")),
    }


def describe():
    """全量设置 + 校验 + 换算预览(前端设置面板直接渲染这份数据)。"""
    cfg = _load(force=True)
    items = []
    day_calc = cfg["calc_bars"].get("day", DEFAULT_CALC_BARS["day"])
    for p in PERIODS:
        cb = int(cfg["calc_bars"].get(p) or DEFAULT_CALC_BARS[p])
        tb = int(cfg["take_bars"].get(p) or DEFAULT_TAKE_BARS[p])
        eff = _effective_with(cfg, p)
        item = {
            "period": p,
            "label": PERIOD_LABEL[p],
            "derived": p in _DERIVED_DAY_FACTOR,
            "calc_bars": cb,
            "take_bars": tb,
            "eff_calc_bars": eff,
            "calc_check": validate_calc_bars(p, cb),
            "take_check": validate_take_bars(p, tb, cb),
            "preview": _derived_preview(p, day_calc),
        }
        items.append(item)
    return {
        "ok": True,
        "min_calc_bars": MIN_CALC_BARS,
        "items": items,
        "hints": {
            "calc": "计算根数: 从K线仓库取多少根 bar 去算指标。低于 45 根时 MACD 处于种子期, "
                    "可能产生假信号(数字标黄, 仍可保存)。",
            "take": "取值根数: 算完之后, 最近 N 根内出现状态就记录该信号。值越大越容易记录到"
                    "稍早的交叉, 但也可能收录已过去的信号。",
            "derived": "周/月/季由日线重采样而来, 实际可用根数受日线深度限制(周=日线/5, 月=日线/21, "
                       "季=日线/63); 月线需日线>=945 根才够 45 根门槛。",
        },
    }


def _effective_with(cfg, period):
    n = int(cfg["calc_bars"].get(period) or DEFAULT_CALC_BARS[period])
    if period in _DERIVED_DAY_FACTOR and period not in ATOMIC_PERIODS:
        day_n = int(cfg["calc_bars"].get("day") or DEFAULT_CALC_BARS["day"])
        n = min(n, day_n // _DERIVED_DAY_FACTOR[period])
    return max(0, n)