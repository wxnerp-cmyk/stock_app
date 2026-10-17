# -*- coding: utf-8 -*-
"""
期货K线模块

功能:
- 期货品种列表(复用短线量化的品种目录)
- K线详情页: ECharts 蜡烛图 + MA线 + 成交量副图 + 周期切换(1分~月)
- K线 JSON 接口: 返回 OHLCV + MA5/10/20/60 + 实时报价
- 自定义算法下拉(预留位), 后续接入 底背离/顶背离/支撑压力位

数据: 全部来自天勤 TqSdk (经 stock_app 的 AccountSlot 单一连接, 不新建连接)
"""
import os
import time
import html
import json
import traceback
import threading
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from flask import request, jsonify, send_from_directory
from services import futures_db
from services import short_term_quant  # 复用期货品种列表
from services import indicators  # 技术指标库 (MA/MACD/BOLL/RSI/KDJ/ATR/CCI/OBV/VR/VROC/WVAD/PVT/AD/量比/持仓量)
from services import ta_patterns  # 形态识别 (支撑/压力位, 顶/底背离)
from services import scan_engine  # 全市场扫描引擎
from services import scan_history_db  # 扫描信号历史入库/查询
from services import scan_template_db  # 用户保存的扫描模板
from services import scan_schedule_db  # 定时爬取任务

try:
    from services.opt_special_pages import _OPT_SPECIAL_STYLE
except Exception:
    _OPT_SPECIAL_STYLE = ''

_SLOTS = None
_DEFAULT_KEY = None
_GET_SLOT = None  # 请求感知的账号解析器（来自 main.py 的 _resolve_slot）


_LAST_SLOT = None  # 后台线程回落用的"最近一次请求解析到的账号 Slot"(见 _get_slot)
_MONITOR_SLOT = None  # 2026-09-28 隔离: 监控/补扫专用账户(role='monitor')的 Slot, 与技术分析账户物理隔离

# 2026-09-24 问题④: 自动监控的账号要与"当前对接账号"解耦 —— 库里(tq_accounts)只要还有
# 未到期的有效账号, 后台就自动挑一个开槽取数入库(数据入全局库, 所有用户可见);
# 系统里一个有效账号都没有 -> no_valid_account 闸门, 不给看监控数据。
_MAKE_SLOT = None  # main.get_slot(user, pwd) 注入, 第⑤级自动挑号开槽用
_AUTO_SLOT = {     # 自动挑号的节流/冷却状态
    "tried_at": 0.0,        # 全局节流: 距上次尝试挑号的时刻(60s 内不重试)
    "failed": {},           # tq_user -> 冷却截止时刻(单账号失败冷却 10min)
    "last_ok_user": None,
}
_BACKFILL_LOCK = threading.Lock()
_BACKFILL = {      # 今日补扫的幂等状态(每交易日最多主动补扫一次)
    "checked_at": 0.0,      # 库查询节流(60s)
    "date": None,           # 正在补扫的交易日
    "running": False,
    "started_at": 0.0,
    "done_date": None,      # 已完成补扫(或确认无需补)的交易日
    "error": "",
    "progress": "",
}
_ACCT_CHECK_LOCK = threading.Lock()
_ACCT_CHECK = {"at": 0.0, "val": False}   # "库中是否存在有效账号"的 60s 缓存
# 2026-09-24 16:55 用户需求: 实时优先, 补扫用独立会话慢慢拉 ——
# 周期顺序日线/小时线在前(进页面先看到今日最关键的信号), 周/月/季垫后;
# 周期之间歇 _BACKFILL_PERIOD_GAP 秒, 不抢账号的行情推送带宽。
_BACKFILL_PERIOD_GAP = 10
_BACKFILL_PERIOD_ORDER = ["day", "60m", "30m", "15m", "5m", "1m", "week", "month", "quarter"]

# 2026-09-24 17:15 需求人: 「今天中午如果开启的服务, 早上的数据就不用管了, 只要下午的」
# —— 补扫窗口下界 = 本进程(服务)启动时刻, 不再回溯交易日起点(昨夜盘/今晨)。
_BOOT_AT = time.time()   # 模块加载时刻 = 服务启动时刻(epoch 秒)
# 2026-09-24 19:1x 实锤: 安装启动后立刻开技术分析页, startup 预热(30 对象) + 补扫独立会话
# 同时抢天勤免费账号(限流) -> 页面 api/data 30s 超时, 整页"no quote data"(用户 19:11 报障)。
# 补扫延迟 _BACKFILL_DELAY_SEC 秒再启动, 让预热先独占天勤把页面数据建起来。
_BACKFILL_DELAY_SEC = 180
# 2026-10-15 修复: 实时监控摄入活跃(覆盖率表最近刷新)的判定窗口(秒)。交易时段内若
# 覆盖率表在此时长内被刷新过, 认为实时链路活着 -> 不触发补扫(避免误判"日盘无落库")。
_BACKFILL_INGEST_ALIVE_SEC = 900

# ---- 技术分析页优先(2026-09-24 19:1x 用户需求: 「访问技术分析的时候, 优先技术分析调用天勤」) ----
# 页面 api/data 每拍调 mark_tech_active() 打活跃标; 后台补扫(每标的间)与监控新起(每拍)看到
# 活跃标就让路: 补扫拉长间隔、监控暂不新建周期订阅 —— 页面预热/取数独占天勤会话带宽。
_TECH_ACTIVE_UNTIL = 0.0    # 活跃截止(epoch 秒)
_TECH_ACTIVE_WINDOW = 12.0  # 活跃窗宽(页面最快 0.5s 一拍, 12s 覆盖漏拍)
_TECH_YIELD_SLEEP = 10.0    # 让路间隔: 页面活跃期间补扫每标的间多等 10s(慢速不饿死)


def mark_tech_active():
    """技术分析页请求入口打活跃标(每次调用续 12s 窗口)。"""
    global _TECH_ACTIVE_UNTIL
    _TECH_ACTIVE_UNTIL = time.time() + _TECH_ACTIVE_WINDOW


def tech_active():
    """技术分析页当前是否活跃(近 _TECH_ACTIVE_WINDOW 秒内有请求)。"""
    return time.time() < _TECH_ACTIVE_UNTIL


def _tech_yield():
    """补扫标的级让路点: 页面活跃时等待 _TECH_YIELD_SLEEP 秒再继续(慢速推进, 不饿死)。"""
    if tech_active():
        time.sleep(_TECH_YIELD_SLEEP)


def _get_slot():
    """解析当前应选用的账户 Slot。

    关键修正(2026-09-24, 同 future_monitor._slot 修法): _auto_tick 是后台线程,
    没有 Flask 请求上下文 -> _GET_SLOT()(_resolve_slot 请求感知)返回 None;
    而旧回落只认 _DEFAULT_KEY(2026-09-08 改版后"不再回落默认账户"基本为 None)
    -> 自动监控永远停在"等待有效天勤账户", 行情订阅 0、信号 0。
    回落顺序: ① 请求上下文解析 -> ② 最近一次请求缓存的 Slot
    -> ③ 全局任意一个 ONLINE 的 Slot -> ④ 默认键。
    ②/③ 保证"用户打开过页面(任一接口被网关注入账号调过)"后, 后台线程也能接管监控。
    """
    global _LAST_SLOT
    s = None
    if _GET_SLOT is not None:
        try:
            s = _GET_SLOT()
        except Exception:
            s = None
    if s is None:
        s = _LAST_SLOT
    if s is None and _SLOTS:
        for _sl in _SLOTS.values():
            if getattr(_sl, "status", "") == "online":
                s = _sl
                break
    if s is None and _SLOTS and _DEFAULT_KEY:
        s = _SLOTS.get(_DEFAULT_KEY)
    if s is not None:
        _LAST_SLOT = s
    return s


def _pick_valid_tq_accounts(role=None):
    """从 tq_accounts 挑出【有效】账号(启用且未到期), 解密后返回 [(user, pwd_plain), ...]。

    口径与用户需求一致: 「监控是直接取的有权限的账号去取...如果用户有未到期的天勤账号,
    无论是否是当前对接的账号, 都可以看到这些数据」。expiry 为 NULL 视为有效(尚未探测, 不误伤)。
    2026-09-28 隔离: role='monitor' 时只挑监控专用账户(与技术分析账户分两条独立连接);
    role=None 挑全部有效账户(兜底用)。
    """
    try:
        from services import futures_db, stock_kline
        conn = futures_db.get_connection()
        try:
            cur = conn.cursor()
            if role:
                cur.execute(
                    "SELECT tq_username, tq_password FROM tq_accounts "
                    "WHERE status=1 AND (expiry IS NULL OR expiry > NOW()) "
                    "AND role=%s ORDER BY id", (role,))
            else:
                cur.execute(
                    "SELECT tq_username, tq_password FROM tq_accounts "
                    "WHERE status=1 AND (expiry IS NULL OR expiry > NOW()) ORDER BY id")
            rows = cur.fetchall() or []
        finally:
            conn.close()
    except Exception as e:
        print(f"[自动挑号] 读 tq_accounts 失败: {e}")
        return []
    out = []
    for row in rows:
        user, cipher = row[0], (row[1] if len(row) > 1 else None)
        if not user:
            continue
        try:
            pwd = stock_kline._decrypt_tq(cipher) or ""
        except Exception:
            pwd = ""
        if pwd:
            out.append((str(user), str(pwd)))
    return out


def _pick_role_tq_account(role, exclude_user=None):
    """返回某个角色下第一个能解密的【有效】账号 (user, pwd_plain); 无则 None。"""
    try:
        from services import futures_db, stock_kline
        conn = futures_db.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT tq_username, tq_password FROM tq_accounts "
                "WHERE status=1 AND (expiry IS NULL OR expiry > NOW()) "
                "AND role=%s ORDER BY id", (role,))
            rows = cur.fetchall() or []
        finally:
            conn.close()
        for user, cipher in rows:
            if not user or (exclude_user and user == exclude_user):
                continue
            try:
                pwd = stock_kline._decrypt_tq(cipher) or ""
            except Exception:
                pwd = ""
            if pwd:
                return (str(user), str(pwd))
    except Exception as e:
        print(f"[角色选账号] 读 tq_accounts 失败: {e}")
    return None


def _ensure_account_roles():
    """2026-09-28 隔离(幂等, 启动期跑一次): 自动给账户指派 role。

    - 幂等加列: tq_accounts 增加 role 列(默认 'shared');
    - 自动指派: 若库里还没有任何 'monitor' 账户, 把【首个免费账户】设为 'monitor',
      其余有效账户设为 'primary'(技术分析/主连接用)。
      这样监控走免费账户独立连接, 技术分析走专业版账户, 物理隔离、互不抢占。
    - 可用环境变量 MONITOR_ACCT_USER 强制指定监控账户(覆盖自动指派)。
    """
    import os as _os
    env_mon = (_os.environ.get("MONITOR_ACCT_USER") or "").strip()
    try:
        from services import futures_db
        conn = futures_db.get_connection()
        try:
            cur = conn.cursor()
            # 1) 幂等加列
            try:
                cur.execute(
                    "ALTER TABLE tq_accounts ADD COLUMN role VARCHAR(16) "
                    "NOT NULL DEFAULT 'shared'")
                conn.commit()
                print("[角色] 已为 tq_accounts 增加 role 列")
            except Exception as _e:
                # 1060=列已存在, 其它也忽略(列可能已存在)
                conn.rollback()
            # 2) 若已存在 monitor 指派, 直接用(除非 env 强制覆盖)
            cur.execute("SELECT COUNT(*) FROM tq_accounts WHERE role='monitor'")
            _r = cur.fetchone()
            has_mon = bool(_r and _r[0])
            if env_mon:
                cur.execute("UPDATE tq_accounts SET role='monitor' WHERE tq_username=%s", (env_mon,))
                cur.execute(
                    "UPDATE tq_accounts SET role='primary' "
                    "WHERE role='shared' AND tq_username<>%s AND status=1", (env_mon,))
                conn.commit()
                print(f"[角色] 监控账户已指定为环境变量 MONITOR_ACCT_USER={env_mon}")
                return
            if has_mon:
                return
            # 3) 自动指派: 首个免费账户 -> monitor, 其余 -> primary
            cur.execute(
                "SELECT tq_username FROM tq_accounts "
                "WHERE status=1 AND (expiry IS NULL OR expiry > NOW()) "
                "AND product_type='free' AND role='shared' ORDER BY id")
            free = cur.fetchone()
            if free:
                mon = free[0]
                cur.execute("UPDATE tq_accounts SET role='monitor' WHERE tq_username=%s", (mon,))
                cur.execute(
                    "UPDATE tq_accounts SET role='primary' "
                    "WHERE role='shared' AND tq_username<>%s AND status=1", (mon,))
                conn.commit()
                print(f"[角色] 自动指派: 监控={mon}(免费账户, 独立连接); 其余有效账户=primary(技术分析)")
            else:
                # 没有免费账户时, 把首个有效账户(非 trial 优先)设为 monitor, 其余 primary
                cur.execute(
                    "SELECT tq_username FROM tq_accounts "
                    "WHERE status=1 AND (expiry IS NULL OR expiry > NOW()) "
                    "AND role='shared' ORDER BY id")
                any_ = cur.fetchone()
                if any_:
                    mon = any_[0]
                    cur.execute("UPDATE tq_accounts SET role='monitor' WHERE tq_username=%s", (mon,))
                    cur.execute(
                        "UPDATE tq_accounts SET role='primary' "
                        "WHERE role='shared' AND tq_username<>%s AND status=1", (mon,))
                    conn.commit()
                    print(f"[角色] 无免费账户, 默认把 {mon} 设为监控专用(monitor)")
        finally:
            conn.close()
    except Exception as e:
        print(f"[角色] 自动指派失败(可忽略, 监控将回退任意有效账户): {e}")


def _any_valid_account_in_db():
    """库里是否存在【未到期】的有效账号(只看资格, 不看密码能否解开)。"""
    try:
        from services import futures_db
        conn = futures_db.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM tq_accounts "
                "WHERE status=1 AND (expiry IS NULL OR expiry > NOW())")
            row = cur.fetchone()
            return bool(row and row[0])
        finally:
            conn.close()
    except Exception:
        return False


def _any_valid_account_cached():
    """_any_valid_account_in_db 的 60s 缓存版(auto_monitor_state 被前端 2s 轮询, 不打库)。"""
    now_t = time.time()
    with _ACCT_CHECK_LOCK:
        if now_t - float(_ACCT_CHECK.get("at") or 0) < 60:
            return bool(_ACCT_CHECK.get("val"))
    val = _any_valid_account_in_db()
    with _ACCT_CHECK_LOCK:
        _ACCT_CHECK["at"] = now_t
        _ACCT_CHECK["val"] = val
    return val


def _get_auto_slot():
    """2026-09-28 隔离版: 监控/补扫专用槽, 只解析 role='monitor' 的账户,
    与技术分析(网关注入的 primary 账户)走【两条独立连接】, 物理隔离、互不抢占。

    只有后台线程调用(auto_tick / 周期线程 / 补扫线程)。绝不使用 _get_slot()(那会返回
    技术分析的账户槽, 导致监控与分析抢同一条连接)。账号选取顺序:
      ① 已有的 monitor 角色 Slot 且 online -> 直接用;
      ② 否则用 monitor 角色账户开新槽(节流 60s / 单账号冷却 10min);
      ③ 若库里没有任何 monitor 账户(极端): 回退任意有效账户(此时靠 tech_active 让路保护分析)。
    """
    global _MONITOR_SLOT
    if _MAKE_SLOT is None:
        return None
    # ① 已有 monitor slot(按用户名匹配, 与 _SLOTS 的 key 格式解耦)
    # 2026-10-15 修复: 属性名是 tq_user(AccountSlot 定义), 原写 sl.user 永远匹配不上 ->
    # 路径①是死代码, 每拍都走②重建会话(还被 60s 节流白白吞掉整拍)。
    for u, pw in _pick_valid_tq_accounts("monitor"):
        for sl in (_SLOTS or {}).values():
            if getattr(sl, "tq_user", None) == u and getattr(sl, "status", "") == "online":
                _MONITOR_SLOT = sl
                return sl
    # ② 开新 monitor slot(节流 + 冷却)
    now_t = time.time()
    if now_t - float(_AUTO_SLOT.get("tried_at") or 0) < 60:
        return None
    cands = _pick_valid_tq_accounts("monitor")
    if not cands:
        cands = _pick_valid_tq_accounts()   # 兜底: 库里无 monitor 账户
        if not cands:
            return None
    with _AUTO_LOCK:
        _AUTO_SLOT["tried_at"] = now_t
    cooldown = _AUTO_SLOT.get("failed") or {}
    for u, pw in cands:
        if now_t < float(cooldown.get(u) or 0):
            continue
        try:
            slot = _MAKE_SLOT(u, pw)
        except Exception as e:
            print(f"[监控隔离] {u} 开槽失败: {e}")
            with _AUTO_LOCK:
                _AUTO_SLOT["failed"][u] = now_t + 600
            continue
        if slot is None:
            with _AUTO_LOCK:
                _AUTO_SLOT["failed"][u] = now_t + 600
            continue
        ok = False
        try:
            for _ in range(16):                    # 最多等 8s: TqApi 连接由槽线程异步进行
                st = getattr(slot, "status", "")
                if st == "online":
                    ok = True
                    break
                if st in ("expired", "error"):
                    break
                time.sleep(0.5)
        except Exception:
            pass
        if ok:
            _MONITOR_SLOT = slot
            with _AUTO_LOCK:
                _AUTO_SLOT["last_ok_user"] = u
            print(f"[监控隔离] 已用 monitor 账户 {u} 建立监控会话(独立于技术分析连接)")
            return slot
        with _AUTO_LOCK:
            _AUTO_SLOT["failed"][u] = now_t + 600
        print(f"[监控隔离] 账号 {u} 8s 内未在线, 冷却 10min")
    return None


def _ensure_today_backfill():
    """今天(交易日)库里还没有任何落库信号 && 有可用账号 -> 后台补扫一遍"开盘->当前"并落库。

    2026-09-24 用户需求: 「如果数据库没有, 你需要去获取今天的数据」——自动监控靠交易时段
    轮询落库, 但服务重启/新装机/账号中途生效会错过前面时段, 库里当天整段空白。
    幂等: 每交易日最多主动补扫一次(done_date); 运行中不重入(running); 库里当天已有信号
    直接跳过(实时轮询已在负责, save_scan_signals 无去重, 重复补扫会写重复行)。
    轻量: 状态检查走内存, 库查询 60s 节流; 由 auto_monitor_state(前端轮询)驱动。
    """
    now_t = time.time()
    with _BACKFILL_LOCK:
        if _BACKFILL.get("running"):
            return
        if now_t - float(_BACKFILL.get("checked_at") or 0) < 60:
            return
        _BACKFILL["checked_at"] = now_t
        if _BACKFILL.get("done_date"):
            return
    # 启动窗口让路: 服务刚起的头几分钟, startup 预热对象还在天勤会话上排队就绪,
    # 此时并行开补扫独立会话会触发免费账号限流, 把预热一起拖死(页面 30s 超时全空)。
    if now_t - _BOOT_AT < _BACKFILL_DELAY_SEC:
        return
    import datetime as _dt
    try:
        today = _dt.date.today()
        if today.weekday() >= 5:      # 周末不补(非交易日, 避免无谓重复落库)
            return
        tday = scan_engine.trading_day_of()
    except Exception:
        return
    tday_s = str(tday)
    # 判断口径: 今天日盘(09:00)以来库中是否有过落库(按 scan_time)。昨夜盘的旧落库
    # scan_time 落在 [昨21:00, 昨24:00], 用整个交易日范围判断会误判成"今天已有数据"。
    try:
        from services import scan_history_db as _shdb
        # 2026-10-09 性能: 这里只需判"今天有没有落库", 不需要排序取回整行。
        # 原 query_range_signals(limit=1) 仍带 `ORDER BY trigger_time,id` -> filesort
        # 排序当天近 20 万行, 补扫判定被拖到十几秒。这里改成 EXISTS 语义走 idx_scan_time。
        rows = _shdb.query_range_signals(
            start=today.strftime("%Y-%m-%d") + " 09:00",
            end=_dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), limit=1, need_order=False) or []
    except Exception as e:
        print(f"[补扫] 查当天库信号失败: {e}")
        return
    if rows:
        with _BACKFILL_LOCK:
            _BACKFILL["done_date"] = tday_s   # 今天已有落库, 无需补
        return
    # 2026-10-15 修复: 若实时监控链路此刻仍在活跃摄入(bar 仓库表最近被刷新), 则日内信号本应由
    # 实时路径产出, 不必补扫 —— 避免开盘实时信号滞后落库时, 被误判"日盘无落库"而触发补扫
    # (09:39 T2706 假金叉事故同源)。仅在交易时段启用该抑制, 收盘后实时不产信号仍照补。
    # 注: 不能用 t_kline_coverage.updated_at(实时 ingest 不刷新它), 必须用实时 ingest 真正写入的
    # t_kline_bar.bar_time 作为"实时活跃"判定信号。
    try:
        if getattr(scan_engine, "is_trading_now", None) and scan_engine.is_trading_now():
            from services import futures_db as _fdb
            _conn = _fdb.get_connection()
            try:
                with _conn.cursor() as _cur:
                    _cur.execute("SELECT MAX(bar_time) FROM t_kline_bar")
                    _row = _cur.fetchone()
                _max_upd = _row[0] if _row else None
                if _max_upd is not None:
                    if isinstance(_max_upd, str):
                        _max_upd = _dt.datetime.strptime(_max_upd, "%Y-%m-%d %H:%M:%S")
                    _idle = (_dt.datetime.now() - _max_upd).total_seconds()
                    if _idle <= _BACKFILL_INGEST_ALIVE_SEC:
                        print(f"[补扫] 实时摄入活跃(bar仓库 {_idle:.0f}s 前刷新), 跳过本次补扫")
                        return
            finally:
                try: _conn.close()
                except Exception: pass
    except Exception as _e:
        print(f"[补扫] 查实时摄入状态失败(放行补扫): {_e}")
    # 2026-09-24 16:55 改版: 补扫走【独立会话】, 不再要求实时槽就位 ——
    # 服务启动/页面打开即补, 与实时监控(共享槽)零竞争
    if not _pick_valid_tq_accounts():
        return                                # 无可解密的有效账号, 等下个检查窗口
    with _BACKFILL_LOCK:
        if _BACKFILL.get("running"):
            return
        _BACKFILL.update({"date": tday_s, "running": True, "started_at": now_t,
                          "error": "", "progress": "准备补扫"})
    threading.Thread(target=_today_backfill_work, args=(tday,),
                     daemon=True, name="fk-today-backfill").start()
    print(f"[补扫] 交易日 {tday_s} 日盘无落库, 已启动后台补扫(独立会话)")


def _today_backfill_work(tday):
    """补扫线程体(2026-09-24 16:55 改版): 用【独立的一次性 TqApi 会话】拉今日历史并落库。

    独立会话的理由: 实时监控的槽是共享会话, K 线缓存 key 含 count(补扫 400/500 != 实时 300),
    补扫 9 周期 x 60 标的走共享槽会新建 ~540 个订阅对象挤爆实时会话 —— 用户明确要求
    「不要影响到实时抓取效率」。独立会话在本线程创建(get_kline_serial/get_quote 均在
    TqApi 创建线程调用, 合规), 用完 api.close() 全部释放, 零残留。
    慢速: 周期顺序 _BACKFILL_PERIOD_ORDER(日线/小时线先落库先可见), 周期间歇
    _BACKFILL_PERIOD_GAP 秒, 整体十几分钟拉完 —— 「今日的历史数据可以慢慢拉取」。
    tday: trading_day_of() 的 date 对象(trading_day_range 内部要做日期减法)。
    """
    tday_s = str(tday)
    client = None
    user = None
    try:
        import datetime as _dt
        from core.tqsdk_client import TqSdkClient
        start, end = scan_engine.trading_day_range(tday)
        now_ns = int(_dt.datetime.now(_dt.timezone(_dt.timedelta(hours=8))).timestamp() * 1e9)
        # 2026-09-24 17:15 需求人: 补扫只补「服务启动之后」的盘中信号(中午开服务就只补
        # 下午的, 早上的盘中历史不管); 日线及以上周期没有"盘中历史"概念 —— 今天的信号
        # trigger=今日00:00/本周一/本月1号/季初, 用户已裁定"今天扫到的就算今天的", 仍照补。
        boot_ns = int(_BOOT_AT * 1e9)
        tday00 = _dt.datetime.combine(tday, _dt.time.min)
        _LO_DT = {
            "day": tday00,
            "week": tday00 - _dt.timedelta(days=tday.weekday()),
            "month": tday00.replace(day=1),
            "quarter": tday00.replace(month=((tday.month - 1) // 3) * 3 + 1, day=1),
        }
        symbols = _auto_symbols()
        # [2026-10-09 用户裁定] 补扫也守"模板周期并集"(见下方 for 循环注释)
        _backfill_allowed = None
        try:
            _backfill_allowed = scan_template_db.monitor_periods()
        except Exception as _e:
            print("[补扫] 读取模板周期并集失败, 按全集补扫:", _e)
        rules = _monitor_rules()
        symbol_rules = _monitor_rules_by_symbol(symbols)
        if not symbols:
            with _BACKFILL_LOCK:
                _BACKFILL.update({"running": False, "error": "监控池为空", "progress": ""})
            return
        from services import scan_history_db as _shdb
        # 挑号(冷却中的跳过), 在本线程内逐个尝试建立独立会话
        now_t = time.time()
        cooldown = _AUTO_SLOT.get("failed") or {}
        cands = [(u, pw) for u, pw in _pick_valid_tq_accounts("monitor")
                 if now_t >= float(cooldown.get(u) or 0)]
        last_err = None
        for u, pw in cands:
            try:
                client = TqSdkClient(u, pw)
                user = u
                break
            except Exception as e:
                last_err = e
                with _AUTO_LOCK:
                    _AUTO_SLOT["failed"][u] = time.time() + 600
                client = None
        if client is None:
            with _BACKFILL_LOCK:
                _BACKFILL.update({"running": False,
                                  "error": "无可用有效账号: %s" % str(last_err)[:120], "progress": ""})
            return
        print(f"[补扫] 独立会话已建立(账号 {user}), 与实时监控互不占用")
        api = client.api
        total = 0
        # [2026-10-09 用户裁定] 补扫周期也守"模板并集": 所有模板都没勾的周期(如周)
        # 不补扫, 避免模板已取消却还在后台白跑并落库。并集为空(无模板/全空)时保持全集兜底。
        _bf_periods = [p for p in _BACKFILL_PERIOD_ORDER
                       if p in set(_backfill_allowed or _BACKFILL_PERIOD_ORDER)]
        if not _bf_periods:
            _bf_periods = list(_BACKFILL_PERIOD_ORDER)
        # [2026-10-09] 补扫顺序同样按"重→轻"排: _BACKFILL_PERIOD_ORDER 把 month/quarter
        # 放在最后两位, 而补扫每周期之间要歇 _BACKFILL_PERIOD_GAP 秒, 等到月/季往往已十几分钟,
        # 甚至中途跨交易日被放弃(:522 日期翻页直接 return) -> 月/季线永远补不上。
        # 月/季复用日线对象零额外成本, 提到最前先落库, 用户能尽快看到。
        _bf_periods = _auto_period_priority(_bf_periods)
        print(f"[补扫] 本次补扫周期 = {','.join(_bf_periods)}"
              + ("" if not _backfill_allowed else " (模板并集口径)"))
        for p in _bf_periods:
            with _BACKFILL_LOCK:
                if _BACKFILL.get("date") != tday_s:
                    return                    # 交易日已翻页, 放弃剩余
                _BACKFILL["progress"] = "扫描 %s 周期" % p
            cfg = PERIODS.get(p, PERIODS["day"])
            count = int(cfg.get("count") or 500)
            # 2026-10-15 修复: 已收盘闸门用的周期时长(传给 _intraday_bar_closed)。
            _period_dur = int(cfg.get("duration") or 86400)
            # 本周期补扫下界: 分钟/小时=服务启动时刻; day/week/month/quarter=当前 bar 起点
            _lo_dt = _LO_DT.get(p)
            lo_ns = boot_ns if _lo_dt is None else int(
                _lo_dt.replace(tzinfo=_dt.timezone(_dt.timedelta(hours=8))).timestamp() * 1e9)
            long_items, short_items = [], []
            try:
                for sym in symbols:
                    try:
                        if p not in _periods_for_symbol(sym):
                            continue          # 对齐混合档: 主连/子合约都只扫【设置里勾选的周期】
                        _tech_yield()          # 技术分析页活跃 -> 本标的延后(页面优先用天勤)
                        q = api.get_quote(sym)     # 同步等首包; 同 symbol 跨周期复用缓存
                        df = api.get_kline_serial(sym, int(cfg["duration"]), data_length=count)
                    except Exception:
                        continue
                    real = (getattr(q, "underlying_symbol", None) or sym)
                    r = scan_engine._scan_one(sym, None, period=p, rules=rules, count=count,
                                              live_only=False, df=df, quote=q, real=real,
                                              symbol_rules=symbol_rules, symbol_tpl=symbol_rules)
                    if not r or not (r.get("signals") or []):
                        continue
                    for side, sink in (("long", long_items), ("short", short_items)):
                        ss = [x for x in r["signals"] if x.get("side") == side]
                        if ss:
                            sink.append({"symbol": r["symbol"], "real_symbol": r["real_symbol"],
                                         "display_name": r.get("display_name", ""),
                                         "close": r.get("close"), "change": r.get("change"),
                                         "change_pct": r.get("change_pct"),
                                         "ma60": r.get("ma60"), "ma20": r.get("ma20"),
                                         "signals": ss})
            except Exception as e:
                print(f"[补扫] {p} 周期异常: {e}")
                continue
            filtered = {"ok": True, "period": p,
                        "scan_time": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "long": [], "short": []}
            for side in ("long", "short"):
                for item in (long_items if side == "long" else short_items):
                    sigs = [sg for sg in (item.get("signals") or [])
                            if isinstance(sg, dict) and sg.get("trigger_time_ns") is not None
                            and lo_ns <= int(sg.get("trigger_time_ns")) <= now_ns
                            # 2026-10-15 修复: 日内周期要求触发 bar 已收盘(bar_time+duration<=now),
                            # 否则会拿"正在形成 bar"的半截数据判出次日消失的假信号(TL2612 30m 09:30 事故)。
                            and _intraday_bar_closed(int(sg.get("trigger_time_ns")), _period_dur, now_ns)]
                    if sigs:
                        it = dict(item)
                        it["signals"] = sigs
                        filtered[side].append(it)
            if filtered["long"] or filtered["short"]:
                try:
                    total += _shdb.save_scan_signals(filtered) or 0
                except Exception as e:
                    print(f"[补扫] {p} 落库失败: {e}")
            with _BACKFILL_LOCK:
                _BACKFILL["progress"] = "%s 完成, 累计落库 %d 条" % (p, total)
            time.sleep(_BACKFILL_PERIOD_GAP)      # 慢速: 周期间歇, 不抢账号推送带宽
        with _BACKFILL_LOCK:
            _BACKFILL["done_date"] = tday_s
            _BACKFILL["progress"] = "补扫完成, 落库 %d 条" % total
            _BACKFILL["running"] = False
        print(f"[补扫] 交易日 {tday_s} 补扫完成(独立会话), 落库 {total} 条信号")
    except Exception as e:
        with _BACKFILL_LOCK:
            _BACKFILL["running"] = False
            _BACKFILL["error"] = str(e)[:200]
            _BACKFILL["progress"] = ""
        print(f"[补扫] 异常: {e}")
    finally:
        try:
            if client is not None:
                client.close()                    # 用完即弃, 全部订阅随会话释放
                print(f"[补扫] 独立会话已关闭(账号 {user})")
        except Exception:
            pass

_STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")

# 周期配置: duration(秒) / count(根数) / freq(重采样, None=直接取) / fmt(横轴格式)
PERIODS = {
    # 2026-10-16 升级: 调大取数根数, 让天勤返回完整(含夜盘)历史, MACD 才不因历史短/缺bar而偏移。
    # 5m 2200根≈26交易日(含夜盘)连续序列, 覆盖 rr2611 全段(09-23起)且留足 EMA 预热; 其余按"≈30天"等比。
    "1m":   {"label": "1分",  "duration": 60,    "count": 2000, "freq": None, "fmt": "%m-%d %H:%M"},
    "5m":   {"label": "5分",  "duration": 300,   "count": 2200, "freq": None, "fmt": "%m-%d %H:%M"},
    "15m":  {"label": "15分", "duration": 900,   "count": 1500, "freq": None, "fmt": "%m-%d %H:%M"},
    "30m":  {"label": "30分", "duration": 1800,  "count": 1000, "freq": None, "fmt": "%m-%d %H:%M"},
    "60m":  {"label": "60分", "duration": 3600,  "count": 800,  "freq": None, "fmt": "%m-%d %H:%M"},
    "day":  {"label": "日",   "duration": 86400, "count": 500, "freq": None, "fmt": "%Y-%m-%d"},
    "week": {"label": "周",   "duration": 86400, "count": 300, "freq": "W",  "fmt": "%Y-%m-%d"},
    "month":{"label": "月",   "duration": 86400, "count": 500, "freq": "MS", "fmt": "%Y-%m"},
    "quarter":{"label": "季", "duration": 86400, "count": 300, "freq": "QS", "fmt": "%Y-Q"},
}
DEFAULT_PERIOD = "5m"
MAX_KLINE_COUNT = 1500  # "加载更早" 单次最大历史根数(免费账户数据范围上限保护)

# 主连合约缓存: {symbol: {"exchange":..., "code":..., "name":...}}
_MAIN_SYMBOLS_CACHE = None
_MAIN_SYMBOLS_LOCK = threading.Lock()

# 品种-月份合约缓存: {(exchange, product_code): [{symbol, name, contract_month, expire_date}, ...]}
_PRODUCT_CONTRACTS_CACHE = {}
_PRODUCT_CONTRACTS_LOCK = threading.Lock()


def _get_main_contracts():
    """从 t_futures_expiry 读取 distinct 品种, 生成连续主力合约 KQ.m@EX.PC 列表。
    返回 [(symbol, exchange, code, name), ...], 按交易所固定顺序 + 品种名排序。"""
    global _MAIN_SYMBOLS_CACHE
    if _MAIN_SYMBOLS_CACHE is not None:
        return _MAIN_SYMBOLS_CACHE
    try:
        conn = futures_db.get_connection()
    except Exception as e:
        print(f"[期货列表] 数据库连接失败: {e}")
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT exchange, exchange_name, product_code, product_name "
                "FROM t_futures_expiry "
                "GROUP BY exchange, exchange_name, product_code, product_name "
                "ORDER BY exchange, product_code"
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    order = ["SHFE", "INE", "DCE", "CZCE", "GFEX", "CFFEX"]
    grouped = {}
    for exchange, ex_name, pc, pname in rows:
        # 过滤异常品种代码: 只保留 1~3 位纯字母, 允许全大写(CFFEX: IF/IC/IH/IM/T/TF/TL/TS)
        # 或全小写(其他交易所); 排除 DCE 月均连续合约(lF/vF/ppF 等混合大小写)以及名称含"月均"
        if not pc or not pc.isalpha() or len(pc) > 3 or len(pc) < 1:
            continue
        if not (pc.islower() or pc.isupper()):
            continue
        if "月均" in (pname or ""):
            continue
        g = grouped.setdefault(exchange, {"ex_name": ex_name or exchange, "items": {}})
        if pc not in g["items"]:
            g["items"][pc] = short_term_quant._clean_prod_name(exchange, pc, pname)

    out = []
    for ex in order:
        if ex in grouped:
            items = sorted(grouped[ex]["items"].items(), key=lambda kv: kv[1])
            for pc, name in items:
                out.append((f"KQ.m@{ex}.{pc}", ex, pc, name))
    for ex in grouped:
        if ex not in order:
            items = sorted(grouped[ex]["items"].items(), key=lambda kv: kv[1])
            for pc, name in items:
                out.append((f"KQ.m@{ex}.{pc}", ex, pc, name))
    with _MAIN_SYMBOLS_LOCK:
        _MAIN_SYMBOLS_CACHE = out
    return out


def _filter_symbols_by_products(symbols, products):
    """按品种白名单过滤主力连续合约列表。

    symbols: KQ.m@EX.PC 格式（如 KQ.m@CZCE.MA）
    products: EX.PC 格式（如 CZCE.MA），空列表表示不过滤。
    """
    if not products:
        return symbols
    prod_set = set(products)
    out = []
    for sym in symbols:
        if sym.startswith("KQ.m@") and sym[5:] in prod_set:
            out.append(sym)
        else:
            parts = sym.split(".")
            if len(parts) >= 2:
                code = parts[-2] + "." + parts[-1]
                if code in prod_set:
                    out.append(sym)
    return out


def _get_product_contracts(exchange, product_code, from_today=True):
    """从 t_futures_expiry 读取某品种下所有月份合约, 返回 [{symbol, name, contract_month, expire_date}, ...]。
    symbol 直接是 `EXCHANGE.PCYYMM` 格式, 可用于天勤订阅和 K线详情跳转。"""
    key = (exchange, product_code, from_today)
    with _PRODUCT_CONTRACTS_LOCK:
        if key in _PRODUCT_CONTRACTS_CACHE:
            return _PRODUCT_CONTRACTS_CACHE[key]
    try:
        conn = futures_db.get_connection()
    except Exception as e:
        print(f"[期货列表] 读取月份合约失败: {e}")
        return []
    today_str = datetime.now().strftime("%Y-%m-%d")
    try:
        with conn.cursor() as cur:
            if from_today:
                cur.execute(
                    "SELECT symbol, product_name, contract_month, expire_date "
                    "FROM t_futures_expiry "
                    "WHERE exchange=%s AND product_code=%s AND expire_date >= %s "
                    "ORDER BY expire_date",
                    (exchange, product_code, today_str),
                )
            else:
                cur.execute(
                    "SELECT symbol, product_name, contract_month, expire_date "
                    "FROM t_futures_expiry "
                    "WHERE exchange=%s AND product_code=%s "
                    "ORDER BY expire_date",
                    (exchange, product_code),
                )
            rows = cur.fetchall()
    finally:
        conn.close()
    out = []
    for sym, pname, cm, ed in rows:
        out.append({
            "symbol": sym,
            "name": (pname or sym).replace(str(cm) or "", "").strip() or sym.split(".")[-1],
            "contract_month": cm,
            "expire_date": ed.strftime("%Y-%m-%d") if ed else "",
        })
    with _PRODUCT_CONTRACTS_LOCK:
        _PRODUCT_CONTRACTS_CACHE[key] = out
    return out


# ==================== K 线数据获取 ====================

def _dt_local(ns_series):
    """把天勤返回的 UTC 纳秒时间戳转成北京时间(用于前端显示)。

    天勤所有 datetime 字段都是 UTC 时间(纳秒)。直接 pd.to_datetime(unit='ns')
    会被解析为 naive-local 导致显示慢 8 小时, 必须显式 utc=True 再转 Asia/Shanghai。
    """
    return pd.to_datetime(ns_series, unit="ns", utc=True).dt.tz_convert("Asia/Shanghai")


def _kcol(k, name):
    """从天勤 KlineData 安全取列: 先字典式再属性式(持仓量等字段常仅支持属性访问)。"""
    try:
        return list(k[name])
    except Exception:
        try:
            return list(getattr(k, name))
        except Exception:
            return None


# ===== A股/指数 60 分钟「标准口径」声明 (2026-09-15) =====
# 背景: 天勤 60m 柱按【自然小时】切 —— 早盘被拆成 09:30-10:00 / 10:00-11:00 / 11:00-11:30,
#   一天 5 根; 而国内看盘软件(东财/同花顺/通达信/文华 WH6)按【交易时段】切 ——
#   10:30 / 11:30 / 14:00 / 15:00, 一天 4 根。
#   KDJ(9)、MACD 等带回看窗口的指标因此会给出不同结果: 天勤口径的 9 根窗口只回溯约 1.8 天,
#   主流口径回溯约 2.25 天, 后者才装得下更早的低点/高点。实测同一根物理柱 KDJ 可一边
#   死叉、一边不死叉 (2026-09-15 中证1000/上证 60m 死叉争议, WH6 对照复现误差仅 0.5)。
# 方案(用户裁定 A): A股/指数 的 60m 不再直接取天勤 3600s, 改用天勤 30m(其柱边界与主流一致)
#   按交易时段【合成】标准柱, 标签取【结束边界】。实测与腾讯 m60 的 高/低/收 逐值一致(+0.00)。
# 期货口径: 股指期货已纳入(见下 _CFFEX_INDEX_FUT); 商品期货/国债期货时段含 10:15 / 15:15 等
#   非整点边界, 口径另议, 保持天勤原生 3600s 不变。
_STOCK_EXCH_PREFIX = ("SSE.", "SZSE.")

# 2026-09-15 扩展: 股指期货(CFFEX IF/IC/IH/IM) 的【日盘时段与 A 股完全相同】——
#   09:30-11:30 + 13:00-15:00, 主流看盘(新浪期货/文华/同花顺)的 60m 同样是 4 根/天。
#   用户实测 CFFEX.IM2610: 天勤原生 60m(5 根/天) 报 "09-15 14:00 KDJ 死叉(K-D=-5.72)",
#   而新浪期货 60m 与天勤 30m 标准合成【均无死叉】(末根 K-D = +5.59 / +5.60),
#   逐柱 H/L/C 差 ≤5 点 -> 分歧纯属切柱口径, 非数据源差异。故一并纳入标准口径。
# 仍【不】纳入: CFFEX 国债 T/TF/TS/TL(15:15 收盘) 与商品期货(日盘含 10:15/11:30 等非整点边界)。
_CFFEX_INDEX_FUT = ("IF", "IC", "IH", "IM")

# 标准 A 股 60 分钟分段: (段结束边界, 段起始) —— 均为当日本地时间的"分钟数"
_STD60_SEGMENTS = (
    (10 * 60 + 30,  9 * 60 + 30),   # 09:30-10:30 -> 标签 10:30
    (11 * 60 + 30, 10 * 60 + 30),   # 10:30-11:30 -> 标签 11:30
    (14 * 60,      13 * 60),        # 13:00-14:00 -> 标签 14:00
    (15 * 60,      14 * 60),        # 14:00-15:00 -> 标签 15:00
)

STD60_SRC_DURATION = 1800   # 合成源周期: 30 分钟(其 :00/:30 对齐与主流一致)
STD60_SRC_MULT = 2          # 每根标准 60m = 2 根 30m
STD60_SRC_MIN = 220         # 源最少根数(合成 90 根 60m 需 180 根, 留余量)


def is_stock_symbol(symbol):
    """A股/指数/ETF 的交易所前缀(SSE./SZSE.) —— 与期货(CFFEX./SHFE./DCE./CZCE./INE./GFEX.)区分。"""
    return str(symbol or "").upper().startswith(_STOCK_EXCH_PREFIX)


def uses_ashare_60m_sessions(symbol):
    """该标的的 60 分钟柱是否应按【A股交易时段】切(一天 4 根: 10:30/11:30/14:00/15:00)。

    SSE./SZSE.(A股/指数/ETF) 与 CFFEX 股指期货(IF/IC/IH/IM, 含主连 KQ.m@CFFEX.IM)
    同为 09:30-11:30 + 13:00-15:00。
    """
    s = str(symbol or "").upper()
    if s.startswith(_STOCK_EXCH_PREFIX):
        return True
    # 主连/指数合约 KQ.m@CFFEX.IM / KQ.i@CFFEX.IF: 产品码在最后一个 '.' 之后
    if "@CFFEX." in s:
        return s.rsplit(".", 1)[-1][:2] in _CFFEX_INDEX_FUT
    if s.startswith("CFFEX."):
        return s.split(".", 1)[-1][:2] in _CFFEX_INDEX_FUT
    return False


def kline_source_spec(symbol, duration, count):
    """(标的, 周期) -> 实际应订阅的 (源 duration, 源 count)。

    标准口径重采样(A股/指数/股指期货 60m) 在此声明其细周期来源, 供【预热】与【取数】共用
    同一个 key —— 天勤 K 线对象按 (symbol, duration, count) 缓存, 两边写法不一致会建出两份源序列。
    """
    if duration == 3600 and uses_ashare_60m_sessions(symbol):
        return STD60_SRC_DURATION, max(STD60_SRC_MIN, int(count) * STD60_SRC_MULT + 8)
    return duration, count


def _fetch_kline_df(slot, symbol, duration, count, end_dt=None):
    """取 K 线 DataFrame (最多 count 行, 不被 market_data 的 50 截断)。返回 (df, real_symbol)。
    end_dt: 翻页用, 传入时取该时间之前 count 根。"""
    if slot is None or slot.client is None or slot.client.api is None:
        return None, symbol
    k, real = slot._get_kline_obj(symbol, duration, count, end_dt=end_dt)
    if k is None:
        return None, real
    deadline = time.time() + 6
    # 2026-10-12 就绪判定加固(根因: PVC 主连 10-12 09:38 冷启动假金叉事故):
    # 旧逻辑只看"最后一根 bar 有效"(_kline_ready) —— 部分填充的序列末根总是有效,
    # 于是带病继续算, MACD 被种子 EMA 主导(DIF=42.23 vs 真实 27.64)产出假信号并落库。
    # 新判定: 末根有效 且 (首根也有效 且 (根数达到 count 或 根数连续 0.4s 不再增长))。
    #   - 首根有效: 防天勤 NaN 头部填充(未下载的历史行)被当成已就绪;
    #   - 根数达标/稳定: 防只下了最近几十根(短序列种子误差)被当成完整历史。
    # 未就绪 = 返回 None(宁缺勿假), 由调用方下轮重试 —— 冷启动前几轮跳过, 焐热后自然恢复。
    prev_len = -1
    stable_at = None
    ready = False
    while time.time() < deadline:
        if slot._kline_ready(k):
            cur = len(k)
            # 首根有效性: datetime 必须有; close 有则必须 >0, 取不到该列则不强求
            # (个别周期无 close 列, 不能因取不到就永久判"未就绪"把该标的饿死)
            try:
                head_ok = float(k["datetime"].iloc[0]) > 0
                try:
                    head_ok = head_ok and float(k["close"].iloc[0]) > 0
                except Exception:
                    pass
            except Exception:
                head_ok = False
            if cur >= count and head_ok:
                ready = True
                break
            if head_ok and cur == prev_len:
                if stable_at is None:
                    stable_at = time.time()
                if time.time() - stable_at >= 0.4:
                    ready = True
                    break
            else:
                stable_at = None
            prev_len = cur
        time.sleep(0.1)
    if not ready:
        return None, real
    df = _df_from_kline(k, count)
    if df is None:
        return None, real
    return df, real


def _df_from_kline(k, count):
    """天勤 Kline 对象 -> DataFrame(最多 count 行, 丢弃 close<=0 的行)。失败返回 None。

    抽自 _fetch_kline_df 原逻辑, 供「单发」与「批量」两条取数路径共用, 避免口径漂移。
    """
    try:
        # 天勤 K 线持仓量列名为 close_oi / open_oi(非 open_interest)
        oi = _kcol(k, "open_interest") or _kcol(k, "close_oi") or _kcol(k, "open_oi")
        data = {
            "datetime": _kcol(k, "datetime"),
            "open": _kcol(k, "open"),
            "high": _kcol(k, "high"),
            "low": _kcol(k, "low"),
            "close": _kcol(k, "close"),
            "volume": _kcol(k, "volume"),
        }
        if any(v is None for v in data.values()):
            raise ValueError("K线基础字段缺失")
        if oi is not None:
            data["open_interest"] = oi
        df = pd.DataFrame(data)
    except Exception:
        # 某些合约/周期不含 open_interest 列, 退化为无持仓量
        try:
            df = pd.DataFrame({
                "datetime": _kcol(k, "datetime"),
                "open": _kcol(k, "open"),
                "high": _kcol(k, "high"),
                "low": _kcol(k, "low"),
                "close": _kcol(k, "close"),
                "volume": _kcol(k, "volume"),
            })
        except Exception:
            return None
    try:
        df = df[df["close"] > 0].copy()
    except Exception:
        return None
    return df.tail(count).reset_index(drop=True)


def fetch_kline_df_batch(slot, symbols, duration, count, wait=25.0):
    """[2026-09-21 筛选提速] 批量取多标的 K 线 df(同一 duration/count)。

    返回 {symbol: df or None}。与 _fetch_kline_df 的差别只在"取":
    一次性批量登记 -> 主线程同一轮全部创建 -> 统一等就绪, 使天勤往返重叠。
    """
    syms = [str(s) for s in (symbols or [])]
    if not syms:
        return {}
    if slot is None or getattr(slot, "client", None) is None or slot.client.api is None:
        return {s: None for s in syms}
    specs = [(s, duration, count, None) for s in syms]
    try:
        objs = slot.request_kline_objs_batch(specs, wait=wait)
    except Exception as e:
        print("[批量K线] 批量登记失败:", str(e)[:120])
        return {s: None for s in syms}
    out = {}
    pending = []
    for sp in specs:
        obj = objs.get(sp)
        if obj is None:
            out[sp[0]] = None
        else:
            pending.append((sp, obj))
    deadline = time.time() + max(2.0, float(wait))
    while pending and time.time() < deadline:
        left = []
        for sp, obj in pending:
            try:
                if slot._kline_ready(obj):
                    out[sp[0]] = _df_from_kline(obj, count)
                else:
                    left.append((sp, obj))
            except Exception:
                out[sp[0]] = None
        pending = left
        if pending:
            time.sleep(0.08)
    for sp, obj in pending:      # 超时未就绪: 不阻塞整批, 该只记 None(下一轮/明天会补)
        out[sp[0]] = None
    return out


def _resample_daily(slot, symbol, freq, count, end_dt=None):
    """周/月线: 拉日线后重采样, 规避天勤 >86400 周期的限制。end_dt 用于历史翻页。"""
    _freq_mult = {"W": 7, "MS": 30, "QS": 90}
    daily_count = min(2000, count * _freq_mult.get(freq, 30))  # 提高到 2000 以支持周/月/季线"加载更早"
    df, real = _fetch_kline_df(slot, symbol, 86400, daily_count, end_dt=end_dt)
    if df is None or df.empty:
        return df, real
    try:
        # UTC -> 北京时间索引, 重采样按北京时间自然日历
        df["dt"] = _dt_local(df["datetime"])
        df = df.set_index("dt")
        agg_map = {
            "open": "first", "high": "max", "low": "min",
            "close": "last", "volume": "sum",
        }
        if "open_interest" in df.columns:
            agg_map["open_interest"] = "last"
        agg = df.resample(freq).agg(agg_map).dropna(subset=["close"])
        if freq.upper().startswith("W"):
            # pandas 'W' 默认按"周日=该周结束日"标记(美式): 本周(周一~周五)会被标成下一个周日,
            # 如 8/24~8/28 那周标成 2026-08-30(周日)。中国习惯以周一为一周起点:
            # 标签 -6 天对齐到该周周一 (2026-08-30周日 -> 2026-08-24周一)。
            agg.index = agg.index - pd.Timedelta(days=6)
        agg = agg.tail(count).reset_index()
        # 翻页需要 UTC 纳秒, 转回 UTC 后取 int64
        agg["datetime"] = agg["dt"].dt.tz_convert("UTC").astype("int64")
        return agg, real
    except Exception:
        # 2026-10-12: 重采样失败 = 返回 None, 绝不回退原始日线 df!
        # 事故实锤: 冷启动部分填充序列让 resample 抛异常, 旧兜底把"日线短序列"当月/季线用,
        # month/quarter 扫描 MACD 全错且与 day 扫描 DIF 逐位相同(同源垃圾) -> 25 条假信号落库。
        return None, real


def _resample_60m_std(df):
    """把 30 分钟 df 合成为标准 A 股 60 分钟柱(一天 4 根)。

    归并规则(与主流看盘一致): 细 bar 的【起始时刻】落在某段 [start, end) 内 → 归入该 end 柱。
        09:30 / 10:00 -> 10:30      10:30 / 11:00 -> 11:30
        13:00 / 13:30 -> 14:00      14:00 / 14:30 -> 15:00
    边界必须用严格小于(<): 写成 <= 会把下一段的头一根并进来(实测差 2.5 点)。

    输出列与 _fetch_kline_df 完全一致(datetime = UTC 纳秒 int64), 上层可无差别使用。
    """
    if df is None or df.empty:
        return df
    d = df.copy()
    try:
        loc = _dt_local(d["datetime"])
        mins = loc.dt.hour * 60 + loc.dt.minute
        day = loc.dt.strftime("%Y-%m-%d")
        seg = pd.Series(-1, index=d.index, dtype="int64")
        for end_min, start_min in _STD60_SEGMENTS:
            seg[(mins >= start_min) & (mins < end_min)] = end_min
        d = d.assign(_seg=seg.values, _day=day.values)
        d = d[d["_seg"] > 0]
        if d.empty:
            return d
        agg_map = {"open": "first", "high": "max", "low": "min",
                   "close": "last", "volume": "sum"}
        if "open_interest" in d.columns:
            agg_map["open_interest"] = "last"
        g = d.groupby(["_day", "_seg"], sort=True).agg(agg_map).reset_index()
        # 柱标签 = 该段结束边界(北京时间) -> 转回 UTC 纳秒
        labstr = (g["_day"] + " " + (g["_seg"] // 60).astype(int).astype(str).str.zfill(2)
                  + ":" + (g["_seg"] % 60).astype(int).astype(str).str.zfill(2))
        lab = pd.to_datetime(labstr, format="%Y-%m-%d %H:%M").dt.tz_localize("Asia/Shanghai")
        g["datetime"] = lab.dt.tz_convert("UTC").astype("int64")
        cols = ["datetime", "open", "high", "low", "close", "volume"]
        if "open_interest" in g.columns:
            cols.append("open_interest")
        return g[cols]
    except Exception:
        return df


def _fetch_kline_60m_std(slot, symbol, count, end_dt=None):
    """A股/指数/股指期货 60 分钟【标准交易时段口径】(4 根/天): 取 30 分钟源后重采样。

    与 _fetch_kline_df 同签名、同返回结构, 供 stock_tech / stock_kline 直接替换调用。
    end_dt(历史翻页) 暂不支持合成 —— 传了就退回天勤原生 3600s, 保持旧行为不变。
    """
    if end_dt is not None:
        return _fetch_kline_df(slot, symbol, 3600, count, end_dt=end_dt)
    src_dur, src_cnt = kline_source_spec(symbol, 3600, count)
    sdf, real = _fetch_kline_df(slot, symbol, src_dur, src_cnt)
    if sdf is None or sdf.empty:
        return sdf, real
    agg = _resample_60m_std(sdf)
    if agg is None or getattr(agg, "empty", True):
        return sdf, real
    return agg.tail(count).reset_index(drop=True), real


def _ma(series, n):
    if len(series) < n:
        return [None] * len(series)
    vals = series.rolling(n, min_periods=n).mean().round(4)
    return [None if pd.isna(v) else float(v) for v in vals]


def _build_kline_payload(slot, symbol, period, req_count=None, end_dt=None):
    cfg = PERIODS.get(period, PERIODS[DEFAULT_PERIOD])
    count = int(req_count) if req_count else cfg["count"]
    if cfg["freq"]:
        df, real = _resample_daily(slot, symbol, cfg["freq"], count, end_dt=end_dt)
    elif cfg["duration"] == 3600 and uses_ashare_60m_sessions(symbol):
        # 60m 标准交易时段口径(4 根/天): A股/指数/股指期货统一走 30m 合成,
        # 与 stock_tech / stock_kline 同一口径(2026-09-15 用户裁定)。
        df, real = _fetch_kline_60m_std(slot, symbol, count, end_dt=end_dt)
    else:
        df, real = _fetch_kline_df(slot, symbol, cfg["duration"], count, end_dt=end_dt)
    if df is None or df.empty:
        return {"ok": False, "error": "暂无K线数据(可能该周期无行情或未订阅)", "symbol": symbol, "period": period}

    # 实时报价(先取最新价, 用于拼接最后一根 K 线)
    quote = {"last": None, "pre_close": None, "change": None, "change_pct": None}
    try:
        q = slot.client.get_quote(symbol)
        last = _r(getattr(q, "last_price", None))
        pre = _r(getattr(q, "pre_close", None))
        if last is not None:
            quote["last"] = last
            if pre and pre > 0:
                quote["pre_close"] = pre
                quote["change"] = round(last - pre, 4)
                quote["change_pct"] = round((last - pre) / pre * 100, 2)
    except Exception:
        last = None
        pass

    # 把最新 tick 价拼到最后一根 forming bar, 实现"图表最右侧随行情跳动"
    if last is not None and len(df) > 0:
        try:
            last_close = float(df["close"].iloc[-1])
            if last_close > 0:
                df.at[df.index[-1], "close"] = last
                df.at[df.index[-1], "high"] = max(float(df["high"].iloc[-1]), last)
                df.at[df.index[-1], "low"] = min(float(df["low"].iloc[-1]), last)
        except Exception as e:
            print(f"[K线实时拼接] {symbol} 更新尾柱失败: {e}")

    # 时区转换: UTC -> 北京时间显示
    df["dt"] = _dt_local(df["datetime"])
    categories = [d.strftime(cfg["fmt"]) for d in df["dt"]]
    kline = [[
        _r(o), _r(c), _r(l), _r(h)
    ] for o, c, l, h in zip(df["open"], df["close"], df["low"], df["high"])]
    volume = [_r(v) for v in df["volume"]]
    close = df["close"].astype(float)
    ma5 = _ma(close, 5)
    ma10 = _ma(close, 10)
    ma20 = _ma(close, 20)
    ma60 = _ma(close, 60) if len(close) >= 60 else [None] * len(close)

    # 全部技术指标 (纯 numpy/pandas, 毫秒级)
    ind = indicators.compute_all(df)

    # 形态识别: 支撑/压力位 + 顶/底背离 (基于 K 线 + 指标序列)
    try:
        analysis = ta_patterns.analyze(df, ind, order=3, lookback=150)
    except Exception as e:
        analysis = {"support": [], "resistance": [], "divergences": [], "macd_crosses": [], "error": str(e)}

    return {
        "ok": True,
        "symbol": symbol,
        "real_symbol": real,
        "period": period,
        "period_label": cfg["label"],
        "loaded_count": len(df),  # 实际加载的历史根数(供前端"加载更早"判断)
        "categories": categories,
        "kline": kline,
        "volume": volume,
        "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
        **ind,   # 注入 Bollinger/MACD/RSI/KDJ/ATR/CCI/OBV/VR/VROC/WVAD/PVT/AD/量比/持仓量/日增仓
        "quote": quote,
        "analysis": analysis,   # 支撑/压力位 + 顶/底背离
        # 翻页拼接用: 原始 datetime(纳秒) + 当前窗口最早一根时间 + 是否还有更早数据
        "datetimes": [int(x) for x in df["datetime"].tolist()],
        "earliest_dt": int(df["datetime"].iloc[0]) if len(df) else None,
        "has_more": len(df) >= count,   # 返回满 count 根 => 大概率还有更早(可继续翻)
        "note": "",
    }


def _r(v):
    try:
        if v is None:
            return None
        v = float(v)
        if np.isnan(v) or np.isinf(v):
            return None
        return round(v, 4)
    except Exception:
        return None


# ==================== 实时行情列表 ====================

def _quote_payload(symbols, slot, wait_ready=True):
    """批量获取主连合约实时行情。先登记订阅, 等待轮询线程 push 数据后从缓存读取。
    返回 {ok, updated_at, count, quotes:[...]}。"""
    if slot is None or slot.client is None or slot.client.api is None:
        return {"ok": False, "error": "未分配有效账户", "quotes": []}
    if not symbols:
        return {"ok": True, "updated_at": datetime.now().strftime("%H:%M:%S"), "count": 0, "quotes": []}

    # 登记订阅(真正 get_quote 由主线程订阅注册循环执行)
    slot.ensure_subscribed(symbols)

    quotes = []
    # warm=1 首次加载: 耐心等数据到达(HTTP 30s 超时内); warm=0 定时刷新: 直接读缓存
    deadline = time.time() + (8.0 if wait_ready else 0.3)
    min_ready = max(10, len(symbols) * 2 // 3) if wait_ready else 1
    got_set = set()
    _ready_since = None   # 达到 min_ready 的时刻(用于"达标后再等一个短收敛窗口即返回")
    while time.time() < deadline:
        try:
            # 请求线程不能自己调 wait_update, 直接读轮询线程已写入的缓存
            cached = slot.get_quotes()
            cached_map = {q.get("symbol"): q for q in cached if q.get("symbol")}
            for sym in symbols:
                if sym in cached_map and sym not in got_set:
                    q = cached_map[sym]
                    if q.get("last_price", 0) > 0:
                        got_set.add(sym)
                        quotes.append(q)
            # 非 warm 模式直接返回; warm 模式拿到全部或足够数量再提前退出
            if not wait_ready:
                break
            if len(got_set) >= len(symbols):
                break
            # 2026-09-20 修复(首屏卡顿实锤): 旧逻辑只在【全部就绪】或【达标且已熬到
            # deadline-1s】才退出, 而主连里总有"无权限/休市无快照"的合约永远拿不到
            # last_price>0, 于是 len(got_set) 永远 < len(symbols) -> 首屏恒定干等约 7s。
            # 实测: 缓存已有 82 条时 quotes?warm=1 耗时 7.18s, 同一时刻 warm=0 仅 0.05s。
            # 现改为: 达到 min_ready(2/3) 后再等 0.4s 收敛窗口即返回, 剩余由前端 300ms 轮询补齐。
            if len(got_set) >= min_ready:
                if _ready_since is None:
                    _ready_since = time.time()
                elif time.time() - _ready_since >= 0.4:
                    break
        except Exception:
            pass
        time.sleep(0.2)

    # 补全未取到的合约(占位, 前端显示 "—")
    missing = [sym for sym in symbols if sym not in got_set]
    return {
        "ok": True,
        "updated_at": datetime.now().strftime("%H:%M:%S"),
        "count": len(quotes),
        "total": len(symbols),
        "missing": missing,
        "quotes": quotes,
    }


def _enrich_change(q):
    """给 quote 补上 change / change_pct / amplitude, 统一字段名方便前端渲染。"""
    last = float(q.get("last_price") or 0)
    pre_settlement = float(q.get("pre_settlement") or 0)
    if last > 0 and pre_settlement > 0:
        q["change"] = round(last - pre_settlement, 4)
        q["change_pct"] = q.get("pct_change") if q.get("pct_change") is not None else round((last - pre_settlement) / pre_settlement * 100, 2)
    else:
        q["change"] = None
        q["change_pct"] = None
    high = float(q.get("high") or 0)
    low = float(q.get("low") or 0)
    if high > 0 and low > 0 and pre_settlement > 0:
        q["amplitude"] = round((high - low) / pre_settlement * 100, 2)
    else:
        q["amplitude"] = None
    return q


# ==================== HTML / CSS ====================

_FK_STYLE = '''
<style>
.period-bar{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin:10px 0}
.period-bar button{padding:6px 14px;background:var(--surface);border:1px solid var(--border);color:var(--muted);border-radius:6px;cursor:pointer;font-size:13px}
.period-bar button.active{background:var(--accent);color:#05101c;border-color:var(--accent);font-weight:700}
.algo-sel{margin-left:auto;padding:6px 10px;background:#0d1424;border:1px solid var(--border);color:var(--text);border-radius:6px;font-size:13px}
.quote-bar{display:flex;gap:18px;flex-wrap:wrap;align-items:center;padding:10px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:8px;margin-bottom:12px}
.quote-bar .q-item{font-size:13px;color:var(--muted)}
.quote-bar .q-val{font-size:18px;font-weight:700;margin-left:6px}
.up{color:var(--up)} .down{color:var(--down)}
#fkChartWrap{width:100%;background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden;position:relative}
#fkChartWrap > #fkChart{width:100%;border:none;border-radius:0}
#fkDrawCanvas{position:absolute;left:0;top:0;width:100%;height:100%;pointer-events:none;z-index:6}
.placeholder-note{margin-top:12px;padding:10px 14px;background:rgba(45,212,255,.08);border:1px solid rgba(45,212,255,.3);border-radius:8px;color:var(--muted);font-size:13px}
.fk-draw-btn{margin-left:10px;padding:6px 14px;background:#13202f;border:1px solid #2a3a4d;color:#9fb3c8;border-radius:6px;cursor:pointer;font-size:13px}
.fk-draw-btn:hover{border-color:var(--accent)}
.fk-tipchk{display:inline-flex;align-items:center;gap:6px;margin-left:10px;padding:5px 10px;background:#13202f;border:1px solid #2a3a4d;border-radius:6px;color:#9fb3c8;font-size:12px;cursor:pointer;user-select:none;white-space:nowrap}
.fk-tipchk:hover{border-color:var(--accent)}
.fk-tipchk input{margin:0;accent-color:var(--accent);cursor:pointer}
.fk-tipchk input:checked + span{color:var(--accent)}
.fk-draw-panel{position:absolute;z-index:30;right:14px;top:54px;width:232px;background:rgba(13,22,38,.97);border:1px solid var(--border);border-radius:10px;box-shadow:0 10px 30px rgba(0,0,0,.5);padding:10px;display:none}
.fk-draw-panel.show{display:block}
.fk-draw-panel h4{margin:0 0 6px;font-size:13px;color:var(--accent)}
.fk-draw-tools{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:8px}
.fk-draw-tool{background:#13202f;border:1px solid #2a3a4d;color:#cfe3f5;border-radius:5px;padding:6px 0;text-align:center;font-size:12px;cursor:pointer}
.fk-draw-tool:hover{border-color:var(--accent)}
.fk-draw-tool.active{background:var(--accent);color:#06121f;border-color:var(--accent)}
.fk-draw-colors{display:flex;gap:6px;margin-bottom:8px}
.fk-draw-color{width:18px;height:18px;border-radius:50%;cursor:pointer;border:2px solid transparent}
.fk-draw-color.active{border-color:#fff}
.fk-draw-actions{display:flex;gap:6px}
.fk-draw-actions button{flex:1;padding:5px 0;background:#13202f;border:1px solid #2a3a4d;color:#cfe3f5;border-radius:5px;cursor:pointer;font-size:12px}
.fk-draw-actions button:hover{border-color:var(--accent)}
/* ===== 全市场监控悬浮面板（底部抽屉） ===== */
/* ===== 全市场监控底部抽屉 ===== */
#fkScanFloat{position:fixed;right:16px;bottom:16px;z-index:40;max-width:520px;min-width:64px;transition:all .25s ease}
#fkScanFloat .fk-scan-toggle{padding:10px 18px;background:var(--accent);color:#05101c;border-radius:50px;font-weight:700;font-size:13px;cursor:pointer;box-shadow:0 4px 14px rgba(0,0,0,.4);border:none}
#fkScanFloat .fk-scan-panel{position:fixed;left:0;right:0;bottom:0;height:0;background:#0b1220;border-top:1px solid var(--border);box-shadow:0 -8px 30px rgba(0,0,0,.55);display:flex;flex-direction:column;overflow:hidden;transition:height .28s cubic-bezier(.25,.8,.25,1);z-index:60}
#fkScanFloat.expanded .fk-scan-panel{height:70vh}
#fkScanFloat.expanded .fk-scan-toggle{display:none}
#fkScanFloat .fk-scan-resizer{height:8px;background:#13202f;border-top:1px solid var(--border);border-bottom:1px solid #0b1220;cursor:row-resize;display:flex;align-items:center;justify-content:center;flex-shrink:0;transition:background .15s}
#fkScanFloat .fk-scan-resizer:hover{background:#1a2a3b}
#fkScanFloat .fk-scan-resizer::before{content:"";width:40px;height:3px;background:#2a3a4d;border-radius:2px}
#fkScanFloat .fk-scan-body{display:flex;flex:1;overflow:hidden}
#fkScanFloat .fk-scan-half{flex:1;min-width:0;display:flex;flex-direction:column;border-right:1px solid var(--border)}
#fkScanFloat .fk-scan-half:last-child{border-right:none}
#fkScanFloat .fk-scan-half .fk-scan-section-title{flex-shrink:0}
#fkScanFloat .fk-scan-half .fk-scan-result{flex:1;overflow:auto}
.fk-scan-head{display:flex;justify-content:space-between;align-items:center;padding:10px 18px;background:#0d1524;border-bottom:1px solid var(--border)}
.fk-scan-head h4{margin:0;font-size:15px;color:var(--text);display:flex;align-items:center;gap:8px}
.fk-scan-head .fk-scan-head-right{display:flex;align-items:center;gap:12px}
.fk-scan-head .fk-scan-close{padding:5px 14px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:5px;font-size:12px;cursor:pointer;white-space:nowrap}
.fk-scan-head .fk-scan-close:hover{background:#1a2a3b;border-color:var(--accent);color:var(--accent)}
.fk-scan-head .fk-scan-count{font-size:12px;color:var(--muted)}
.fk-scan-head .fk-scan-count b{color:var(--text);font-weight:600;margin:0 4px}
.fk-scan-mode{display:inline-flex;gap:0;border:1px solid #2a3a4d;border-radius:6px;overflow:hidden}
.fk-scan-mode .fk-mode-btn{padding:4px 14px;font-size:12px;background:#0d1524;color:var(--muted);border:none;cursor:pointer;font-weight:600}
.fk-scan-mode .fk-mode-btn.active{background:var(--accent);color:#05101c}
/* [2026-10-15] 工具栏周期多选(下拉折叠): 勾选即过滤下方信号表(只影响显示, 后台仍全量抓取) */
.fk-toolbar-periods{position:relative;display:inline-block;margin:0 8px;vertical-align:middle}
.fk-toolbar-periods .fk-tp-trigger{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;background:#0d1524;border:1px solid #2a3a4d;border-radius:6px;color:var(--text);cursor:pointer;font-size:12px;line-height:1.6;white-space:nowrap}
.fk-toolbar-periods .fk-tp-trigger:hover{border-color:var(--accent)}
.fk-toolbar-periods.open .fk-tp-trigger{border-color:var(--accent);color:var(--accent)}
.fk-toolbar-periods .fk-tp-label{color:var(--muted);font-size:11px}
.fk-toolbar-periods .fk-tp-summary{max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600}
.fk-toolbar-periods .fk-tp-arrow{font-size:10px;color:var(--muted)}
.fk-toolbar-periods .fk-tp-drop{position:absolute;z-index:60;top:calc(100% + 4px);left:0;min-width:230px;max-width:320px;padding:7px;background:#101a2b;border:1px solid #2a3a4d;border-radius:6px;box-shadow:0 8px 22px rgba(0,0,0,.5)}
.fk-toolbar-periods .fk-tp-list{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:4px}
.fk-toolbar-periods .fk-tp-chk{display:inline-flex;align-items:center;justify-content:center;gap:3px;padding:4px 6px;background:#13202f;border:1px solid #2a3a4d;border-radius:4px;font-size:11px;color:var(--muted);cursor:pointer;user-select:none;white-space:nowrap}
.fk-toolbar-periods .fk-tp-chk input{width:12px;height:12px;cursor:pointer;margin:0}
.fk-toolbar-periods .fk-tp-chk.on{border-color:var(--accent);color:var(--accent)}
.fk-toolbar-periods .fk-tp-acts{display:flex;gap:6px;margin-top:6px;padding-top:6px;border-top:1px solid #223247}
.fk-toolbar-periods .fk-tp-all,.fk-toolbar-periods .fk-tp-none{flex:1;padding:3px 8px;font-size:11px;background:#13202f;border:1px solid #2a3a4d;border-radius:4px;color:var(--muted);cursor:pointer}
.fk-toolbar-periods .fk-tp-all:hover,.fk-toolbar-periods .fk-tp-none:hover{border-color:var(--accent);color:var(--accent)}
.fk-scan-hist{display:flex;align-items:center;gap:8px;font-size:12px;color:var(--muted)}
.fk-scan-hist label{display:inline-flex;align-items:center;gap:4px}
.fk-scan-hist input[type=date]{font-size:12px;padding:3px 6px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:4px}
.fk-scan-modehint{padding:5px 18px;font-size:11px;color:var(--muted);background:#0b1220;border-bottom:1px solid var(--border)}
.fk-scan-modehint.history{color:#f0a020}
.fk-scan-empty.hist{color:#f0a020}
.fk-bt-summary{padding:6px 10px;font-size:12px;color:#f0a020;background:rgba(240,160,32,.08);border-bottom:1px solid #2a3a4d;white-space:nowrap;overflow-x:auto}
.fk-scan-ctrl{display:flex;gap:10px;align-items:center;padding:8px 18px;background:#0b1220;border-bottom:1px solid var(--border);flex-wrap:wrap}
.fk-scan-ctrl select,.fk-scan-ctrl input,.fk-scan-ctrl button{font-size:13px;padding:5px 10px;border-radius:5px;background:#13202f;border:1px solid #2a3a4d;color:var(--text)}
.fk-scan-ctrl button{cursor:pointer;background:var(--accent);color:#05101c;border-color:var(--accent);font-weight:600}
.fk-scan-ctrl button.running{background:#ef4444;color:#fff;border-color:#ef4444}
.fk-scan-ctrl .fk-scan-history{display:inline-flex;align-items:center;padding:5px 10px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:5px;text-decoration:none;font-size:13px;cursor:pointer}
.fk-scan-ctrl .fk-scan-history:hover{border-color:var(--accent);color:var(--accent)}
.fk-scan-tplbar,.fk-scan-toolbar{display:flex;align-items:center;gap:8px;padding:6px 18px;background:#0b1220;border-bottom:1px solid var(--border);flex-wrap:wrap}
.fk-scan-toolbar .fk-tpl-chips{flex:1;min-width:120px}
.fk-scan-toolbar select,.fk-scan-toolbar input,.fk-scan-toolbar button,.fk-scan-toolbar .fk-scan-history{padding:5px 10px;border-radius:5px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);font-size:13px}
.fk-scan-toolbar button{cursor:pointer;background:var(--accent);color:#05101c;border-color:var(--accent);font-weight:600}
.fk-scan-toolbar button.running{background:#ef4444;color:#fff;border-color:#ef4444}
.fk-tpl-chips{display:flex;gap:6px;flex:1;overflow-x:auto;min-width:0}
.fk-tpl-chip{padding:4px 12px;background:#13202f;border:1px solid #2a3a4d;color:var(--muted);border-radius:14px;font-size:12px;cursor:pointer;white-space:nowrap;user-select:none}
.fk-tpl-chip:hover{color:var(--text);border-color:var(--accent)}
.fk-tpl-chip.active{background:var(--accent);color:#05101c;border-color:var(--accent);font-weight:600}
.fk-tpl-hint{color:var(--muted);font-size:12px}
.fk-tpl-settings{padding:5px 12px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:14px;font-size:12px;text-decoration:none;white-space:nowrap}
.fk-tpl-settings:hover{border-color:var(--accent);color:var(--accent)}
.fk-tpl-select{padding:5px 10px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:5px;font-size:12px;cursor:pointer;max-width:200px}
.fk-tpl-select:hover{border-color:var(--accent)}
/* [2026-10-09] 非当前模板有数据 -> 左上角聚合提示 */
.fk-tpl-toast-wrap{position:fixed;left:14px;top:14px;z-index:9999;display:flex;flex-direction:column;gap:8px;max-width:300px}
.fk-tpl-toast{display:flex;align-items:center;gap:10px;padding:9px 13px;background:linear-gradient(135deg,#1e3a5f,#14263f);border:1px solid var(--accent);border-radius:8px;color:var(--text);font-size:13px;box-shadow:0 6px 20px rgba(0,0,0,.5);cursor:pointer;animation:fkTplToastIn .25s ease}
.fk-tpl-toast:hover{border-color:var(--violet)}
.fk-tpl-toast .t-ico{font-size:16px}
.fk-tpl-toast .t-msg b{color:var(--accent)}
@keyframes fkTplToastIn{from{opacity:0;transform:translateX(-12px)}to{opacity:1;transform:translateX(0)}}
.fk-cfg-toggle{padding:5px 10px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:5px;font-size:12px;cursor:pointer;white-space:nowrap}
.fk-cfg-toggle:hover{border-color:var(--accent);color:var(--accent)}
.fk-cfg-toggle.open{border-color:var(--accent);color:var(--accent)}
.fk-scan-config{background:#0b1220;border-bottom:1px solid var(--border)}
.fk-scan-config summary{display:none}
.fk-scan-config[open] .fk-scan-rules{border-bottom:1px solid var(--border)}
.fk-scan-rules{padding:8px 18px;max-height:150px;overflow:auto;border-bottom:1px solid var(--border)}
.fk-scan-rule{display:inline-flex;align-items:center;gap:8px;margin:0 14px 6px 0;font-size:13px;color:var(--text);white-space:nowrap}
.fk-scan-rule input[type=checkbox]{width:15px;height:15px;cursor:pointer}
.fk-scan-rule .fk-rule-name{min-width:auto}
.fk-scan-rule .fk-rule-params{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.fk-scan-rule .fk-rule-params label{color:var(--muted);font-size:11px}
.fk-scan-rule .fk-rule-params input{width:56px;padding:3px 5px;background:#0d1424;border:1px solid #2a3a4d;color:var(--text);border-radius:4px;font-size:11px}
.fk-scan-result{flex:1;overflow:auto;padding:0}
.fk-scan-table{width:100%;border-collapse:collapse;font-size:12px;color:var(--text)}
.fk-scan-table thead{position:sticky;top:0;z-index:2}
.fk-scan-table th{padding:6px 8px;background:#0d1524;border-bottom:1px solid var(--border);color:var(--muted);font-weight:600;text-align:left;white-space:nowrap}
.fk-scan-table td{padding:5px 8px;border-bottom:1px solid #16202e;vertical-align:middle}
/* 紧凑密度：进一步收紧行距与标题/工具栏内边距, 一屏显示更多行 */
#fkScanFloat.fk-compact .fk-scan-section-title{padding:4px 18px}
#fkScanFloat.fk-compact .fk-scan-toolbar{padding-top:4px;padding-bottom:4px}
#fkScanFloat.fk-compact .fk-scan-table th{padding:4px 8px;font-size:11px}
#fkScanFloat.fk-compact .fk-scan-table td{padding:3px 8px;font-size:11px}
#fkScanFloat.fk-compact .fk-scan-table .col-time{width:78px}
.fk-scan-table tbody tr:hover{background:#111d2b}
.fk-scan-table .col-num{width:40px;text-align:center;color:var(--muted)}
.fk-scan-table .col-time{width:90px;font-variant-numeric:tabular-nums;white-space:nowrap}
.fk-scan-table .col-sym{font-weight:600;color:var(--text);cursor:pointer}
.fk-scan-table .col-sym:hover{color:var(--accent)}
.fk-scan-table .col-sym .sub{display:block;font-size:11px;color:var(--muted);font-weight:400}
/* [2026-10-12] 合约列: 交易所短码胶囊, 悬浮提示双代码, 点击复制(WH6 直接可敲的码) */
/* [2026-10-12] 合约代码悬浮提示: 平时不占版面, 悬停合约列浮现, 移开消失(用户裁定, 取代常驻胶囊) */
.fk-code-tip{position:fixed;left:0;top:0;z-index:99998;display:none;padding:6px 10px;border-radius:6px;background:#20242e;color:#e8e8e8;font-size:12px;line-height:1.7;box-shadow:0 4px 14px rgba(0,0,0,.45);white-space:nowrap}
.fk-code-tip b{color:var(--gold,#e8b339)}
.fk-code-tip .fk-tip-copy{margin-left:8px;padding:1px 8px;border-radius:4px;font-size:11px;font-weight:600;color:var(--gold,#e8b339);background:rgba(232,179,57,.12);border:1px solid rgba(232,179,57,.4);cursor:copy;user-select:none}
.fk-code-tip .fk-tip-copy:hover{background:rgba(232,179,57,.25)}
.fk-code-toast{position:fixed;left:50%;bottom:64px;transform:translateX(-50%);z-index:99999;padding:8px 18px;border-radius:8px;background:#2a2f3a;color:#fff;font-size:13px;box-shadow:0 4px 16px rgba(0,0,0,.4);pointer-events:none;transition:opacity .25s;opacity:0}
.fk-code-toast.show{opacity:1}
.fk-scan-table .col-dir{font-weight:700}
.fk-scan-table .col-dir.long{color:var(--up)}
.fk-scan-table .col-dir.short{color:var(--down)}
.fk-scan-table .col-price{font-variant-numeric:tabular-nums}
.fk-scan-table .col-change.up{color:#ff4d4f}
.fk-scan-table .col-change.down{color:#19c37d}
.fk-scan-table .col-signals{max-width:260px}
.fk-scan-table .col-signals .fk-scan-tag{display:inline-block;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:600;margin:0 4px 3px 0;white-space:nowrap}
.fk-scan-table .col-signals .fk-scan-tag.long{background:rgba(239,68,68,.12);color:var(--up);border:1px solid rgba(239,68,68,.25)}
.fk-scan-table .col-signals .fk-scan-tag.short{background:rgba(25,195,125,.12);color:var(--down);border:1px solid rgba(25,195,125,.25)}
/* [2026-10-16] MA60/MA20 标签(涨跌幅旁): 均线高于现价=绿↓, 低于现价=红↑(对齐参考端口径) */
.fk-scan-table .col-ma{width:76px;white-space:nowrap}
.fk-ma-tag{display:inline-block;margin-right:5px;font-size:11px;font-weight:700;font-variant-numeric:tabular-nums;white-space:nowrap;cursor:help}
.fk-ma-tag.ma-g{color:#19c37d}
.fk-ma-tag.ma-r{color:#ff4d4f}
.fk-ma-tag.ma-f{color:var(--muted)}
.fk-scan-table .col-desc{color:var(--muted);font-size:11px;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.fk-scan-periods{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.fk-scan-periods .fk-scan-pchk{display:inline-flex;align-items:center;gap:3px;padding:3px 8px;background:#13202f;border:1px solid #2a3a4d;border-radius:5px;font-size:12px;color:var(--text);cursor:pointer;user-select:none}
.fk-scan-periods .fk-scan-pchk input{width:13px;height:13px;cursor:pointer;margin:0}
.fk-scan-periods .fk-scan-pchk.on{border-color:var(--accent);color:var(--accent)}
.fk-scan-table .col-period{width:42px;text-align:center;color:var(--accent);font-weight:600;white-space:nowrap}
.fk-scan-empty{text-align:center;color:var(--muted);padding:40px 0;font-size:13px}
.fk-scan-section-title{display:flex;align-items:center;gap:8px;padding:6px 18px;background:#0b1220;border-bottom:1px solid var(--border);font-size:12px;color:var(--muted)}
.fk-scan-section-title .dot{width:6px;height:6px;border-radius:50%}
.fk-scan-section-title .dot.long{background:var(--up)}
.fk-scan-section-title .dot.short{background:var(--down)}
#fkScanAlerts{position:fixed;left:16px;top:16px;z-index:70;display:flex;flex-direction:column;gap:8px;max-width:320px}
.fk-scan-alert{padding:10px 14px;background:rgba(11,19,33,.98);border:1px solid var(--border);border-left:4px solid var(--accent);border-radius:8px;box-shadow:0 6px 20px rgba(0,0,0,.45);font-size:12px;color:var(--text);animation:fkAlertIn .25s ease}
.fk-scan-alert.long{border-left-color:var(--up)}
.fk-scan-alert.short{border-left-color:var(--down)}
.fk-scan-alert .fk-alert-title{font-weight:700;margin-bottom:2px}
.fk-scan-alert .fk-alert-body{color:var(--muted);font-size:11px}
@keyframes fkAlertIn{from{transform:translateX(-30px);opacity:0}to{transform:translateX(0);opacity:1}}
.fk-scan-alert{cursor:pointer;transition:opacity .25s}
.fk-scan-alert:hover{border-color:var(--accent)}
#fkNotifyCenter{display:flex;flex-direction:column;gap:6px;max-width:340px}
.fk-notify-pill{display:flex;align-items:center;gap:8px;padding:8px 12px;background:rgba(11,19,33,.98);border:1px solid var(--border);border-left:4px solid var(--accent);border-radius:8px;box-shadow:0 6px 20px rgba(0,0,0,.45);font-size:12px;color:var(--text);cursor:pointer;user-select:none}
.fk-notify-pill:hover{border-color:var(--accent)}
.fk-notify-pill .fk-nb-count{font-weight:700;color:var(--accent)}
.fk-notify-pill .fk-nb-txt{flex:1}
.fk-notify-pill .fk-nb-arrow{color:var(--muted);font-size:10px}
.fk-notify-list{max-height:46vh;max-width:300px;overflow-y:auto;background:rgba(11,19,33,.98);border:1px solid var(--border);border-radius:8px;box-shadow:0 6px 20px rgba(0,0,0,.45);padding:6px}
.fk-notify-group-title{padding:6px 8px;font-size:11px;color:var(--muted);border-bottom:1px solid var(--border);display:flex;justify-content:space-between}
.fk-notify-item{padding:8px 10px;border-radius:6px;margin-bottom:6px;background:#0b1220;border:1px solid var(--border);border-left:3px solid var(--accent);cursor:pointer;font-size:12px}
.fk-notify-item:hover{background:#13202f;border-color:var(--accent)}
.fk-notify-item.long{border-left-color:var(--up)}
.fk-notify-item.short{border-left-color:var(--down)}
.fk-notify-item .fk-ni-head{display:flex;align-items:center;gap:6px;margin-bottom:2px}
.fk-notify-item .fk-ni-name{font-weight:600}
.fk-notify-item.long .fk-ni-name{color:var(--up)}
.fk-notify-item.short .fk-ni-name{color:var(--down)}
.fk-notify-item .fk-ni-desc{color:var(--muted);font-size:11px;word-break:break-all}
.fk-notify-foot{display:flex;gap:6px;padding:4px 2px 0}
.fk-notify-foot button{flex:1;background:#13202f;color:#9fb3c8;border:1px solid #2a3a4d;border-radius:4px;padding:4px 6px;font-size:11px;cursor:pointer}
.fk-notify-foot button:hover{border-color:var(--accent);color:var(--text)}
.fk-notify-empty{padding:14px 10px;text-align:center;color:var(--muted);font-size:12px}
.fk-scan-sched{padding:6px 18px 10px;border-bottom:1px solid var(--border)}
.fk-scan-sched .fk-sched-title{cursor:pointer;color:var(--muted);font-size:12px;display:flex;align-items:center;gap:8px;user-select:none}
.fk-scan-sched .fk-sched-title .dot.sched{background:#f0b90b}
.fk-scan-sched .fk-sched-list{margin:6px 0}
.fk-scan-sched .fk-sched-row{display:flex;align-items:center;gap:8px;padding:6px 8px;background:#0b1220;border:1px solid var(--border);border-radius:6px;margin-bottom:6px;font-size:12px}
.fk-scan-sched .fk-sched-row .fk-sched-name{flex:1;font-weight:600}
.fk-scan-sched .fk-sched-row .fk-sched-meta{color:var(--muted);font-size:11px}
.fk-scan-sched .fk-sched-row.on{border-left:3px solid #19c37d}
.fk-scan-sched .fk-sched-row.off{border-left:3px solid #555;opacity:.7}
.fk-scan-sched .fk-sched-now{font-size:11px;color:var(--muted);margin-bottom:6px}
.fk-scan-sched .fk-sched-form{margin-top:8px;display:flex;flex-direction:column;gap:6px;background:#0b1220;padding:10px;border:1px solid var(--border);border-radius:8px}
.fk-scan-sched .fk-sched-form label{font-size:11px;color:var(--muted)}
.fk-scan-sched .fk-sched-form input,.fk-scan-sched .fk-sched-form select{background:#0f1830;border:1px solid var(--border);color:var(--text);border-radius:6px;padding:5px 8px;font-size:12px}
.fk-scan-sched .fk-sched-days{display:flex;gap:4px;flex-wrap:wrap}
.fk-scan-sched .fk-sched-days label{display:flex;align-items:center;gap:2px;background:#0f1830;border:1px solid var(--border);border-radius:4px;padding:2px 5px;cursor:pointer}
.fk-scan-sched .fk-sched-btns{display:flex;gap:8px;margin-top:4px}
.fk-scan-sched .fk-sched-btns button{flex:1}
</style>
'''


def _build_list_html():
    """期货品种列表页：实时行情表格 + 交易所/主连筛选 + 搜索。"""
    main_contracts = _get_main_contracts()
    # 筛选标签: 主连(全部) + 各交易所
    ex_order = [("", "主连"), ("SHFE", "上期所"), ("INE", "上期能源"),
                ("DCE", "大商所"), ("CZCE", "郑商所"), ("GFEX", "广期所"), ("CFFEX", "中金所")]
    ex_tabs = ""
    for ex, label in ex_order:
        active = " active" if ex == "" else ""
        ex_tabs += f'<button data-ex="{html.escape(ex)}" onclick="fkFilterEx(\'{ex}\')" class="{active.strip()}">{html.escape(label)}</button>'
    # 其他交易所(若有)
    known = {ex for ex, _ in ex_order}
    other_ex = sorted({ex for _, ex, _, _ in main_contracts if ex not in known})
    for ex in other_ex:
        ex_tabs += f'<button data-ex="{html.escape(ex)}" onclick="fkFilterEx(\'{ex}\')">{html.escape(ex)}</button>'

    # 表格骨架
    headers = ["名称", "最新", "涨跌", "涨幅%", "买价", "买量", "卖价", "卖量",
               "成交量", "持仓量", "日增仓", "开盘", "最高", "最低", "昨结", "动态%"]
    thead = "".join(f'<th>{h}</th>' for h in headers)

    doc = '<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><title>期货监控 - 主连列表</title></head><body>'
    doc += _OPT_SPECIAL_STYLE + short_term_quant._SQ_STYLE + _FK_STYLE
    doc += '<div class="opt-page">'
    doc += '<div class="fk-sticky-header">'
    doc += '<div class="page-title">期货监控</div>'
    doc += '<div class="page-sub">连续主力合约实时行情，点击 ▶ 展开旗下月份合约（与主表同列、含实时行情）</div>'
    doc += '<div class="sq-search"><input id="fkSearch" placeholder="搜索品种名称或代码，如 沪铜 / cu" oninput="fkTextFilter()"></div>'
    doc += f'<div class="sq-ex-tabs" id="fkExTabs">{ex_tabs}</div>'
    doc += '<div class="quote-status" id="fkStatus">正在连接行情…</div>'
    doc += '</div>'
    doc += '<div class="fk-table-wrap"><table class="fk-table" id="fkTable"><thead><tr><th style="width:20px"></th>' + thead + '</tr></thead><tbody id="fkTbody"><tr><td colspan="17">加载中…</td></tr></tbody></table></div>'
    doc += '</div>'
    doc += '<div class="opt-tabbar"><a class="active" href="?">期货列表</a></div>'
    doc += '''
    <script>
    var FK_DATA = [];
    var FK_FILTER_EX = "";
    var FK_FILTER_TEXT = "";
    var FK_EXPANDED = {};
    var FK_CONTRACTS_CACHE = {};
    var FK_CHILDREN_SIG = {};
    function fmtNum(n, d){
      if(n===null || n===undefined || n==="" || isNaN(n)) return "—";
      d = (d===undefined || d===null || isNaN(d) || d<0 || d>10) ? 0 : Math.floor(Number(d));
      return Number(n).toFixed(d);
    }
    function priceDec(p){ return (p!==null && p!==undefined && !isNaN(p) && p>=0 && p<10) ? 3 : 2; }
    function clsDir(change){ return change>0?"up":(change<0?"down":""); }
    function sign(v){ return v>0?"+":""; }
    function fkChildrenHtml(key, contracts){
      var html = '';
      for(var i=0;i<contracts.length;i++){
        var c = contracts[i];
        if(!c || !c.symbol) continue;
        var sym = c.symbol;
        var nm = (c.name || '') + ' ' + (c.contract_month || '');
        html += '<tr class="fk-child-row" data-parent="'+htmlEscape(key)+'" data-symbol="'+encodeURIComponent(sym)+'">'+
          '<td class="toggle child-toggle"></td>'+
          '<td class="name">'+htmlEscape(nm.trim())+'<div class="sub">'+htmlEscape(sym)+'</div></td>'+
          fkCells(c)+
        '</tr>';
      }
      return html;
    }
    function fkInsertChildren(key){
      var parent = document.querySelector('tr[data-expand-key="'+key+'"]');
      if(!parent) return;
      var olds = document.querySelectorAll('tr.fk-child-row[data-parent="'+key+'"]');
      for(var i=0;i<olds.length;i++){ if(olds[i].parentNode) olds[i].parentNode.removeChild(olds[i]); }
      var contracts = FK_CONTRACTS_CACHE[key] || [];
      if(contracts.length){
        parent.insertAdjacentHTML('afterend', fkChildrenHtml(key, contracts));
      } else {
        parent.insertAdjacentHTML('afterend', '<tr class="fk-child-row fk-child-loading-row" data-parent="'+htmlEscape(key)+'"><td class="toggle"></td><td class="name" colspan="16">加载 '+htmlEscape(key.split('-')[1])+' 月份合约行情…</td></tr>');
      }
    }
    function fkToggle(ex, code, el){
      var key = ex + '-' + code;
      var tr = el.closest('tr');
      if(FK_EXPANDED[key]){
        FK_EXPANDED[key] = false;
        var olds = document.querySelectorAll('tr.fk-child-row[data-parent="'+key+'"]');
        for(var i=0;i<olds.length;i++){ if(olds[i].parentNode) olds[i].parentNode.removeChild(olds[i]); }
        el.innerText = '▶';
        return;
      }
      FK_EXPANDED[key] = true;
      el.innerText = '▼';
      fkLoadContracts(ex, code);
    }
    function fkLoadContracts(ex, code){
      var key = ex + '-' + code;
      var url = 'api/contracts?exchange='+encodeURIComponent(ex)+'&code='+encodeURIComponent(code)+'&quotes=1';
      fetch(url).then(function(r){return r.json();}).then(function(d){
        if(!d.ok || !d.contracts || d.contracts.length===0){ return; }
        FK_CONTRACTS_CACHE[key] = d.contracts;
        fkInsertChildren(key);
      }).catch(function(e){
        var parent = document.querySelector('tr[data-expand-key="'+key+'"]');
        if(parent) parent.insertAdjacentHTML('afterend', '<tr class="fk-child-row fk-child-loading-row" data-parent="'+htmlEscape(key)+'"><td class="toggle"></td><td class="name" colspan="16">加载失败: '+htmlEscape(''+e)+'</td></tr>');
      });
    }
    function fkChildrenSig(contracts){
      var s = "";
      for(var i=0;i<contracts.length;i++){ s += contracts[i].symbol + ":" + contracts[i].last_price + ":" + contracts[i].change + ";"; }
      return s;
    }
    function fkRefreshChildren(){
      Object.keys(FK_EXPANDED).forEach(function(key){
        if(!FK_EXPANDED[key]) return;
        var parts = key.split('-');
        var ex = parts[0], code = parts[1];
        var url = 'api/contracts?exchange='+encodeURIComponent(ex)+'&code='+encodeURIComponent(code)+'&quotes=1';
        fetch(url).then(function(r){return r.json();}).then(function(d){
          if(d.ok && d.contracts && d.contracts.length){
            var sig = fkChildrenSig(d.contracts);
            if(sig === FK_CHILDREN_SIG[key]) return;
            FK_CHILDREN_SIG[key] = sig;
            FK_CONTRACTS_CACHE[key] = d.contracts;
            fkInsertChildren(key);
          }
        }).catch(function(e){});
      });
    }
    function htmlEscape(s){ return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
    function fkCells(q){
      var pc = q.change;
      var pct = q.change_pct;
      var amp = q.amplitude;
      var c = clsDir(pc);
      var di = clsDir(q.day_increase);
      return '<td class="num '+c+'">'+fmtNum(q.last_price, priceDec(q.last_price))+'</td>'+
        '<td class="num '+c+'">'+sign(pc)+fmtNum(pc, (Math.abs(pc||0)<10?2:1))+'</td>'+
        '<td class="num '+c+'">'+sign(pct)+fmtNum(pct,2)+'%</td>'+
        '<td class="num">'+fmtNum(q.bid_price1, priceDec(q.bid_price1))+'</td>'+
        '<td class="num">'+fmtNum(q.bid_volume1,0)+'</td>'+
        '<td class="num">'+fmtNum(q.ask_price1, priceDec(q.ask_price1))+'</td>'+
        '<td class="num">'+fmtNum(q.ask_volume1,0)+'</td>'+
        '<td class="num">'+fmtNum(q.volume,0)+'</td>'+
        '<td class="num">'+fmtNum(q.open_interest,0)+'</td>'+
        '<td class="num '+di+'">'+sign(q.day_increase)+fmtNum(q.day_increase,0)+'</td>'+
        '<td class="num">'+fmtNum(q.open, priceDec(q.open))+'</td>'+
        '<td class="num">'+fmtNum(q.high, priceDec(q.high))+'</td>'+
        '<td class="num">'+fmtNum(q.low, priceDec(q.low))+'</td>'+
        '<td class="num">'+fmtNum(q.pre_settlement, priceDec(q.pre_settlement))+'</td>'+
        '<td class="num">'+fmtNum(amp,2)+'%</td>';
    }
    function fkRow(q){
      var ex = q.exchange || "";
      var code = q.code || "";
      return '<tr data-symbol="'+encodeURIComponent(q.symbol)+'" data-ex="'+ex+'" data-name="'+(q.name||"").toLowerCase()+'" data-code="'+(code).toLowerCase()+'" data-expand-key="'+ex+'-'+code+'">'+
        '<td class="toggle"><span class="fk-toggle" data-ex="'+ex+'" data-code="'+code+'">▶</span></td>'+
        '<td class="name">'+(q.name||q.symbol)+'<div class="sub">'+q.symbol+'</div></td>'+
        fkCells(q)+
      '</tr>';
    }
    function fkChildRow(c, key){
      var sym = c.symbol;
      var nm = (c.name||'') + (c.contract_month?(' '+c.contract_month):'');
      return '<tr class="fk-child-row" data-parent="'+htmlEscape(key)+'" data-symbol="'+encodeURIComponent(sym)+'">'+
        '<td class="toggle child-toggle"></td>'+
        '<td class="name">'+htmlEscape(nm.trim())+'<div class="sub">'+htmlEscape(sym)+(c.expire_date?(' · 到期 '+htmlEscape(c.expire_date)):'')+'</div></td>'+
        fkCells(c)+
      '</tr>';
    }
    function fkRender(){
      try {
        var rows = FK_DATA.filter(function(q){
          var okEx = !FK_FILTER_EX || (q.exchange||"")===FK_FILTER_EX;
          var txt = FK_FILTER_TEXT.toLowerCase();
          var okText = !txt || ((q.name||"").toLowerCase().indexOf(txt)!==-1) || ((q.code||"").toLowerCase().indexOf(txt)!==-1);
          return okEx && okText;
        });
        var html = rows.length ? rows.map(fkRow).join("") : "<tr><td colspan='17'>无匹配合约</td></tr>";
        document.getElementById("fkTbody").innerHTML = html;
        document.getElementById("fkStatus").innerText = "行情更新 " + (FK_DATA.length?FK_DATA[0].__updated:"—") + " 共 "+rows.length+" 条";
        // 刷新后重新插回仍处于展开状态的月份合约行(数据来自缓存, 不重新请求)
        fkRestoreExpanded();
      } catch(e){
        document.getElementById("fkStatus").innerText = "渲染异常: " + e.message;
        console.error(e);
      }
    }
    function fkRestoreExpanded(){
      Object.keys(FK_EXPANDED).forEach(function(key){
        if(!FK_EXPANDED[key]) return;
        var parent = document.querySelector('tr[data-expand-key="'+key+'"]');
        if(!parent) return;
        var toggle = parent.querySelector('.fk-toggle');
        if(toggle) toggle.innerText = '▼';
        fkInsertChildren(key);
      });
    }
    function fkFilterEx(ex){
      FK_FILTER_EX = ex;
      var btns = document.querySelectorAll("#fkExTabs button");
      for(var i=0;i<btns.length;i++){
        btns[i].className = (btns[i].getAttribute("data-ex")===ex)?"active":"";
      }
      fkRender();
    }
    function fkTextFilter(){
      FK_FILTER_TEXT = document.getElementById("fkSearch").value.trim();
      fkRender();
    }
    var FK_FIRST_LOAD = true;
    var FK_LOADING = false;
    var FK_LAST_SIG = "";
    var FK_REFRESH_MS = 300;
    function fkSig(d){
      var qs = d.quotes || [];
      var s = String(qs.length) + "|" + (d.updated_at || "");
      for(var i=0;i<qs.length;i++){ s += "," + qs[i].last_price + "," + qs[i].change; }
      return s;
    }
    function fkLoad(){
      if(FK_LOADING) return;
      var url = FK_FIRST_LOAD ? "api/quotes?warm=1" : "api/quotes";
      FK_FIRST_LOAD = false;
      FK_LOADING = true;
      fetch(url).then(function(r){return r.json();}).then(function(d){
        if(d.ok){
          var updated = d.updated_at || "";
          FK_DATA = (d.quotes||[]).map(function(q){ q.__updated = updated; return q; });
          var sig = fkSig(d);
          if(sig !== FK_LAST_SIG){ FK_LAST_SIG = sig; fkRender(); }
          fkRefreshChildren();
        } else {
          document.getElementById("fkStatus").innerText = "行情异常: " + (d.error||"未知");
        }
      }).catch(function(e){
        document.getElementById("fkStatus").innerText = "请求失败: " + e;
      }).finally(function(){ FK_LOADING = false; });
    }
    document.getElementById("fkTbody").addEventListener("click", function(e){
      var toggle = e.target.closest(".fk-toggle");
      if(toggle){
        fkToggle(toggle.getAttribute("data-ex"), toggle.getAttribute("data-code"), toggle);
        return;
      }
      var tr = e.target.closest("tr[data-symbol]");
      if(tr) location.href = "?symbol=" + tr.getAttribute("data-symbol");
    });
    fkLoad();
    setInterval(fkLoad, FK_REFRESH_MS);
    </script>
    <style>
    .fk-table-wrap{overflow-x:auto;margin-top:10px;border:1px solid var(--border);border-radius:8px;background:var(--surface)}
    .fk-table{width:100%;border-collapse:collapse;font-size:13px;min-width:1100px}
    .fk-table th{position:sticky;top:0;background:#111a25;padding:10px 8px;text-align:right;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border);white-space:nowrap}
    .fk-table th:first-child{text-align:left}
    .fk-table td{padding:9px 8px;border-bottom:1px solid rgba(255,255,255,.04);text-align:right;white-space:nowrap}
    .fk-table td:first-child{text-align:left;cursor:pointer}
    .fk-table tbody tr:hover{background:rgba(45,212,255,.08);cursor:pointer}
    .fk-table .name{font-weight:600}
    .fk-table .sub{font-size:11px;color:var(--muted);font-weight:400}
    .fk-table .num{font-variant-numeric:tabular-nums}
    .fk-table .toggle{text-align:center;padding:0;cursor:default}
    .fk-table .toggle .fk-toggle{font-size:11px;color:var(--muted);cursor:pointer;padding:6px 8px;display:inline-block}
    .fk-table .toggle .fk-toggle:hover{color:var(--accent)}
    .quote-status{font-size:12px;color:var(--muted);padding:6px 0}
    .fk-sticky-header{position:sticky;top:0;z-index:100;background:var(--bg);padding:12px 0;border-bottom:1px solid var(--border)}
    .fk-child-row td{background:#070b12}
    .fk-child-row td.name{padding-left:30px;font-weight:600}
    .fk-child-row:hover td{background:rgba(45,212,255,.08);cursor:pointer}
    .fk-children-wrap{background:#070b12}
    .fk-child-loading-row td{color:var(--muted);font-size:12px;padding:10px 14px}
    .child-toggle{cursor:default}
    </style>
    '''
    doc += _fk_scan_panel_html()
    doc += _fk_scan_script()
    doc += '</body></html>'
    return doc


def _build_detail_html(symbol, period=None, mark_ts=""):
    # mark_ts: 监控回顾页深链带过来的信号时间("YYYY-MM-DD HH:MM:SS" 北京时间),
    # 用于在 K 线主图打一条竖直标记线, 定位当时触发信号的那根 K 线。
    mark_ts_ms = 0
    if mark_ts:
        try:
            dt = datetime.strptime(mark_ts, "%Y-%m-%d %H:%M:%S")
            dt_utc = dt - timedelta(hours=8)   # 北京时间 -> UTC  epoch ms(与 data.datetimes 的 ns 对齐)
            mark_ts_ms = int(dt_utc.timestamp() * 1000)
        except Exception:
            mark_ts_ms = 0
    name = symbol
    for ex, exname, items in short_term_quant.list_futures_products():
        for it in items:
            if it["symbol"] == symbol:
                name = it["name"]
                break
    back = '<a class="back-link" href="?">← 返回期货列表</a>'

    if period not in PERIODS:
        period = DEFAULT_PERIOD
    period_btns = ""
    for k, cfg in PERIODS.items():
        active = " active" if k == period else ""
        period_btns += f'<button class="{active.strip()}" data-p="{k}" onclick="fkLoad(\'{k}\')">{cfg["label"]}</button>'

    doc = '<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><title>期货监控 - ' + html.escape(symbol) + '</title>'
    doc += '<script src="static/echarts.min.js"></script><style>.ind-bar{display:flex;flex-wrap:wrap;gap:4px;align-items:center;padding:6px 0}.ind-label{color:var(--muted);font-size:12px;margin-right:4px}.ind-btn{background:#13202f;color:#9fb3c8;border:1px solid #2a3a4d;border-radius:4px;padding:3px 8px;font-size:12px;cursor:pointer}.ind-btn:hover{border-color:var(--accent)}.ind-btn.active{background:var(--accent);color:#06121f;border-color:var(--accent)}.ind-btn.fk-disabled{opacity:.55;cursor:not-allowed;background:#13202f!important;color:#9fb3c8!important;border-color:#2a3a4d!important}</style></head><body>'
    doc += _OPT_SPECIAL_STYLE + short_term_quant._SQ_STYLE + _FK_STYLE
    doc += '<div class="opt-page">'
    doc += f'<div class="page-title">{html.escape(name)} <span style="font-size:13px;color:var(--muted)">{html.escape(symbol)}</span></div>'
    doc += '<div class="page-sub">K线图 + 多周期切换 + 技术指标叠加（点击下方「指标」按钮勾选/取消需要显示的技术指标，主图叠加 MA/BOLL，其余指标各自独立副图）</div>'
    doc += back
    doc += '<div class="period-bar">'
    doc += period_btns
    doc += '<span id="fkLoadMore" class="ind-btn" onclick="fkLoadEarlier()" title="点击或把K线拖到最左端即可加载更早历史">加载更早</span>'
    doc += '<button class="fk-draw-btn" onclick="fkToggleDrawPanel(event)">✏ 划线分析</button>'
    doc += '<label class="fk-tipchk" title="不勾选: 光标信息面板固定停靠在图表右上角, 不遮挡正在看的那一段; 勾选: 面板跟随光标移动"><input type="checkbox" id="fkTipFollow" onchange="fkToggleTipFollow(this)"><span>提示框跟随光标</span></label>'
    doc += '</div>'
    doc += '<div class="ind-bar" id="fkIndBar"><span class="ind-label">指标:</span></div>'
    doc += '<div class="quote-bar" id="fkQuote"><span class="q-item">加载中…</span></div>'
    doc += '<div id="fkChartWrap"><div id="fkChart"></div><canvas id="fkDrawCanvas"></canvas>'
    doc += '<div class="fk-draw-panel" id="fkDrawPanel">'
    doc += '<h4>划线分析</h4>'
    doc += '<div class="fk-draw-tools">'
    doc += '<div class="fk-draw-tool" data-tool="line" onclick="fkSetDrawTool(\'line\')">直线</div>'
    doc += '<div class="fk-draw-tool" data-tool="ray" onclick="fkSetDrawTool(\'ray\')">射线</div>'
    doc += '<div class="fk-draw-tool" data-tool="segment" onclick="fkSetDrawTool(\'segment\')">线段</div>'
    doc += '<div class="fk-draw-tool" data-tool="hline" onclick="fkSetDrawTool(\'hline\')">水平</div>'
    doc += '<div class="fk-draw-tool" data-tool="vline" onclick="fkSetDrawTool(\'vline\')">垂直</div>'
    doc += '<div class="fk-draw-tool" data-tool="rect" onclick="fkSetDrawTool(\'rect\')">矩形</div>'
    doc += '<div class="fk-draw-tool" data-tool="text" onclick="fkSetDrawTool(\'text\')">文字</div>'
    doc += '</div>'
    doc += '<div class="fk-draw-colors">'
    doc += '<div class="fk-draw-color active" data-color="#ffd54f" style="background:#ffd54f" onclick="fkSetDrawColor(\'#ffd54f\')"></div>'
    doc += '<div class="fk-draw-color" data-color="#ff4d4f" style="background:#ff4d4f" onclick="fkSetDrawColor(\'#ff4d4f\')"></div>'
    doc += '<div class="fk-draw-color" data-color="#19c37d" style="background:#19c37d" onclick="fkSetDrawColor(\'#19c37d\')"></div>'
    doc += '<div class="fk-draw-color" data-color="#2dd4ff" style="background:#2dd4ff" onclick="fkSetDrawColor(\'#2dd4ff\')"></div>'
    doc += '<div class="fk-draw-color" data-color="#e056fd" style="background:#e056fd" onclick="fkSetDrawColor(\'#e056fd\')"></div>'
    doc += '<div class="fk-draw-color" data-color="#ffffff" style="background:#ffffff" onclick="fkSetDrawColor(\'#ffffff\')"></div>'
    doc += '</div>'
    doc += '<div class="fk-draw-actions"><button onclick="fkExitDraw()">退出</button><button onclick="fkClearDrawings()">清空</button></div>'
    doc += '</div></div>'
    doc += '</div>'
    doc += '<div class="opt-tabbar"><a href="?">期货列表</a></div>'
    doc += _fk_scan_panel_html()
    doc += _fk_detail_script(symbol, period, mark_ts_ms)
    doc += _fk_scan_script()
    doc += '</body></html>'
    return doc


def _fk_scan_panel_html():
    """返回全市场监控悬浮面板 HTML（列表页与详情页共用）。"""
    _porder = ["1m", "5m", "15m", "30m", "60m", "day", "week", "month", "quarter"]
    _pchecks = ""
    for _pk in _porder:
        _pc = PERIODS.get(_pk)
        if not _pc:
            continue
        _pchecks += '<label class="fk-scan-pchk on"><input type="checkbox" class="fk-period-cb" value="%s" checked onchange="fkOnPeriodToggle(this)">%s</label>' % (_pk, _pc["label"])
    period_checks = '<div class="fk-scan-periods" id="fkScanPeriods">' + _pchecks + '</div>'
    html = ('<div id="fkScanAlerts">'
            '<div id="fkNotifyCenter" style="display:none">'
            '<div class="fk-notify-pill" id="fkNotifyPill" onclick="fkNotifyToggle()" '
            'title="点击展开本时段全部信号通知，再点某条跳转到对应K线">'
            '<span class="fk-nb-txt"><b class="fk-nb-count" id="fkNotifyCount">0</b> 条信号通知</span>'
            '<span class="fk-nb-arrow" id="fkNotifyArrow">&#9662;</span>'
            '</div>'
            '<div class="fk-notify-list" id="fkNotifyList" style="display:none"></div>'
            '</div></div>')
    html += '<div id="fkScanFloat">'
    html += '<button class="fk-scan-toggle" onclick="fkToggleScanPanel()">监控</button>'
    html += '<div class="fk-scan-panel" id="fkScanPanel">'
    html += '<div class="fk-scan-resizer" id="fkScanResizer" onmousedown="fkStartDrag(event)" title="上下拖动调整面板高度"></div>'
    html += '<div class="fk-scan-head">'
    html += '<h4>全市场监控 <span class="fk-scan-count">共<b id="fkTotalCount">0</b>条 · 多<b id="fkHeadLong">0</b> · 空<b id="fkHeadShort">0</b></span></h4>'
    html += '<div class="fk-scan-mode">'
    html += '<button class="fk-mode-btn active" id="fkModeRealtime" data-mode="realtime" onclick="fkSetScanMode(\'realtime\')">实时</button>'
    html += '<button class="fk-mode-btn" id="fkModeHistory" data-mode="history" onclick="fkSetScanMode(\'history\')">历史回溯</button>'
    html += '</div>'
    # [2026-10-15 用户需求] 工具栏周期多选(下拉折叠): 勾选哪些周期, 下方表格就只显示哪些周期的数据。
    # 与折叠面板里的 fkScanPeriods 同一套状态(两处控件双向同步), 复用 fkOnPeriodToggle/fkRenderAgg,
    # 只做【显示过滤】不重扫(后端仍全量抓取), 切换即时生效。
    # [2026-10-15 追加] 横排 9 个复选框太占宽 -> 改为下拉: 按钮上显示已选摘要(全选时显示"全部"),
    # 点开面板内多选; 点外部/选中项自动收起(点复选框本身不收, 便于连勾多个)。
    html += '<div class="fk-toolbar-periods" id="fkToolbarPeriods" title="按周期筛选下方信号（可多选；只影响显示，不影响后台抓取）">'
    html += '<button class="fk-tp-trigger" id="fkTpTrigger" onclick="fkTogglePeriodDrop(event)">'
    html += '<span class="fk-tp-label">周期</span>'
    html += '<span class="fk-tp-summary" id="fkTpSummary">全部</span>'
    html += '<span class="fk-tp-arrow">&#9662;</span>'
    html += '</button>'
    html += '<div class="fk-tp-drop" id="fkTpDrop" style="display:none">'
    html += '<div class="fk-tp-list">'
    for _pk in _porder:
        _pc = PERIODS.get(_pk)
        if not _pc:
            continue
        html += '<label class="fk-tp-chk on"><input type="checkbox" class="fk-toolbar-period-cb" value="%s" checked onchange="fkOnToolbarPeriodToggle(this)">%s</label>' % (_pk, _pc["label"])
    html += '</div>'
    html += '<div class="fk-tp-acts">'
    html += '<button class="fk-tp-all" onclick="fkSetToolbarPeriods(true)">全选</button>'
    html += '<button class="fk-tp-none" onclick="fkSetToolbarPeriods(false)">清空</button>'
    html += '</div>'
    html += '</div>'
    html += '</div>'
    html += '<div class="fk-scan-head-right">'
    html += '<span id="fkScanStatus" style="color:var(--muted);font-size:12px">未开始</span>'
    # [2026-10-09 用户裁定] 工具栏整行(fk-scan-toolbar)移除, 仅保留「⚙设置」入口并移到面板第一行头部。
    html += '<a class="fk-tpl-settings fk-head-settings" id="fkScanSettingsLink" target="_blank" title="打开模板设置：管理 / 排序 / 指标">⚙ 设置</a>'
    html += '<button class="fk-scan-close" onclick="fkToggleScanPanel()">收起面板</button>'
    html += '<a class="fk-cfg-toggle fk-fs-btn" href="scan-monitor" target="_blank" style="text-decoration:none" title="整页独立展示监控（不遮挡K线）">⤢ 全屏</a>'
    html += '</div></div>'
    # [2026-10-09 用户裁定] 原工具栏行(设置/模板下拉/开始监控/历史复盘/指标周期设置/紧凑)整行删除。
    # 下方 .fk-scan-hist 历史回溯筛选容器一并移除(其入口 fkCfgToggle 已随工具栏删除)。
    html += '<div class="fk-scan-toolbar fk-toolbar-removed" id="fkTplBar" style="display:none"></div>'
    html += '<details class="fk-scan-config" id="fkScanConfig">'
    html += '<summary></summary>'
    html += period_checks
    html += '<div class="fk-scan-rules" id="fkScanRules">加载规则…</div>'
    html += '<div class="fk-scan-sched">'
    html += '<div class="fk-sched-title" onclick="fkToggleSched()"><span class="dot sched"></span>定时任务 <span id="fkSchedCount"></span></div>'
    html += '<div id="fkSchedBody" style="display:none">'
    html += '<div class="fk-sched-now" id="fkSchedNow"></div>'
    html += '<div class="fk-sched-list" id="fkSchedList"></div>'
    html += '<button onclick="fkShowSchedForm()">+ 新增定时任务</button>'
    html += '<div class="fk-sched-form" id="fkSchedForm" style="display:none">'
    html += '<label>任务名称</label><input id="fkSchedName" placeholder="如 夜盘自动监控">'
    html += '<label>关联模板（决定周期+规则+阈值）</label><select id="fkSchedTpl"><option value="">默认配置</option></select>'
    html += '<label>开始时间（北京时间，支持跨午夜如 21:00）</label><input type="time" id="fkSchedStart" value="21:00">'
    html += '<label>结束时间（北京时间，如 02:30 / 15:00）</label><input type="time" id="fkSchedEnd" value="02:30">'
    html += '<label>生效星期（默认全选）</label><div class="fk-sched-days" id="fkSchedDays">'
    _wd = [("0","一"),("1","二"),("2","三"),("3","四"),("4","五"),("5","六"),("6","日")]
    for _w, _n in _wd:
        html += '<label><input type="checkbox" class="fk-sched-day" value="%s" checked>周%s</label>' % (_w, _n)
    html += '</div>'
    html += '<div class="fk-sched-btns"><button onclick="fkSaveSched()">保存</button><button onclick="fkHideSchedForm()">取消</button></div>'
    html += '</div></div></div>'
    html += '</details>'
    html += '<div class="fk-scan-body">'
    html += '<div class="fk-scan-half fk-scan-long">'
    html += '<div class="fk-scan-section-title"><span class="dot long"></span>多头信号</div>'
    html += '<div class="fk-scan-result" id="fkLongResult"><div class="fk-scan-empty">暂无多头信号</div></div>'
    html += '</div>'
    html += '<div class="fk-scan-half fk-scan-short">'
    html += '<div class="fk-scan-section-title"><span class="dot short"></span>空头信号</div>'
    html += '<div class="fk-scan-result" id="fkShortResult"><div class="fk-scan-empty">暂无空头信号</div></div>'
    html += '</div>'
    html += '</div></div></div>'
    return html


def _fk_detail_script(symbol, period, mark_ts_ms=0):
    """生成期货K线详情页的 JS(指标选择面板 + 动态多副图)。"""
    sym_js = json.dumps(symbol)
    per_js = json.dumps(period)
    mts_js = int(mark_ts_ms or 0)
    js = (
        "<script>\n"
        + "var FK_SYMBOL = " + sym_js + ";\n"
        + "var FK_PERIOD = " + per_js + ";\n"
        + "var FK_MARK_TS_MS = " + str(mts_js) + ";\n"
        + '''var FK_CHART=null;
var FK_LAST=null;
var FK_SELECTED={MA:true,BOLL:true,VOL:true,OI:true,OID:true,MACD:true};
var FK_TIP_FOLLOW=false;   // 光标信息面板位置: false=固定停靠图表右上角(默认); true=跟随光标。localStorage 持久化
var FK_LOADED_COUNT=0;      // 当前已加载的历史根数
var FK_EARLIEST=false;      // 是否已到免费账户历史最早
var FK_LOADING_MORE=false;  // 防止"加载更早"并发
var FK_MAX_COUNT=1500;      // 单次最大历史根数(翻页每页固定 500, 不受此限瓶颈)
var FK_EARLIEST_DT=null;    // 当前窗口最左一根的 datetime(纳秒), 翻页时作为 end_dt 锚点
var FK_ZOOM={start:0,end:100}; // 当前缩放视图(百分比), 加载更早后据此保持原视图
var FK_LEGEND_SYNC=false;      // 防止 legendselectchanged 递归触发
var FK_MACD_ZERO_BAND=0;       // 监控面板设置的 MACD 距零线范围(±带宽)
var FK_MACD_ZERO_LINE=0;       // 监控面板设置的 MACD 零线位置(默认值0, 可设定为任意值如 800)
var FK_MACD_DIF_IDX=-1;        // MACD 副图 DIF 系列的 series 索引(供 setOption 按 id 定位)
var FK_MACD_AXIS_IDX=-1;       // MACD 副图对应的 grid/axis 索引(convertToPixel 用)
var FK_CROSS_DATA=[];          // 当前 K 线的 MACD 金叉/死叉交叉数据(供 dataZoom 拖拽时重绘标记)
var FK_CROSS_TIP_BOUND=false;   // 金叉/死叉自定义悬浮提示框的 mouseover/mouseout 监听是否已注册
var FK_IND={
  MA:{name:'MA',overlay:true,lines:[{k:'ma5',n:'MA5',c:'#f0b90b'},{k:'ma10',n:'MA10',c:'#2dd4ff'},{k:'ma20',n:'MA20',c:'#e056fd'},{k:'ma60',n:'MA60',c:'#ffffff'}]},
  BOLL:{name:'BOLL',overlay:true,lines:[{k:'bollUpper',n:'BOLL上',c:'#ffa940'},{k:'bollMid',n:'BOLL中',c:'#ffd666'},{k:'bollLower',n:'BOLL下',c:'#ffa940'}]},
  VOL:{name:'成交量',sub:true,type:'bar',k:'volume',c:'#3a6ea5'},
  OI:{name:'持仓量',sub:true,type:'bar',k:'oi',c:'#722ed1'},
  OID:{name:'日增仓',sub:true,type:'bar',k:'oiDiff',c:'#13c2c2'},
  MACD:{name:'MACD',sub:true,lines:[{k:'macdDif',n:'DIF',c:'#f0b90b'},{k:'macdDea',n:'DEA',c:'#2dd4ff'},{k:'macdBar',n:'MACD',c:'#ff4d4f',bar:true}]},
  RSI:{name:'RSI',sub:true,lines:[{k:'rsi6',n:'RSI6',c:'#f0b90b'},{k:'rsi12',n:'RSI12',c:'#2dd4ff'}]},
  KDJ:{name:'KDJ',sub:true,lines:[{k:'kdjK',n:'K',c:'#f0b90b'},{k:'kdjD',n:'D',c:'#2dd4ff'},{k:'kdjJ',n:'J',c:'#ff4d4f'}]},
  OBV:{name:'OBV',sub:true,type:'line',k:'obv',c:'#19c37d'},
  VR:{name:'VR',sub:true,type:'line',k:'vr',c:'#19c37d'},
  VROC:{name:'VROC',sub:true,type:'line',k:'vroc',c:'#19c37d'},
  WVAD:{name:'WVAD',sub:true,type:'line',k:'wvad',c:'#19c37d'},
  PVT:{name:'PVT',sub:true,type:'line',k:'pvt',c:'#19c37d'},
  AD:{name:'AD',sub:true,type:'line',k:'ad',c:'#19c37d'},
  VOLR:{name:'量比',sub:true,type:'line',k:'volRatio',c:'#fa8c16'},
  ATR:{name:'ATR',sub:true,type:'line',k:'atr14',c:'#fa8c16'},
  CCI:{name:'CCI',sub:true,type:'line',k:'cci14',c:'#fa8c16'}
};
function fkBuildIndBar(){
  var bar=document.getElementById('fkIndBar');
  var h='<span class="ind-label">指标:</span>';
  for(var key in FK_IND){
    var act=FK_SELECTED[key]?' active':'';
    h+='<span class="ind-btn'+act+'" data-ind="'+key+'" onclick="fkToggleInd(\\''+key+'\\')">'+FK_IND[key].name+'</span>';
  }
  bar.innerHTML=h;
}
function fkToggleInd(key){
  FK_SELECTED[key]=!FK_SELECTED[key];
  fkBuildIndBar();
  if(FK_LAST) fkRender(FK_LAST);
}
// 光标信息面板: 切换"跟随光标 / 固定停靠右上角"。
// 位置回调在每次显示时才读 FK_TIP_FOLLOW, 所以改完立刻生效, 不需要重绘图表(无闪屏);
// 只把当前已显示的那一个先藏掉, 免得它停在新旧位置中间。
function fkToggleTipFollow(el){
  FK_TIP_FOLLOW=!!(el && el.checked);
  try{ localStorage.setItem('fkTipFollow', FK_TIP_FOLLOW?'1':'0'); }catch(e){}
  if(FK_CHART){ try{ FK_CHART.dispatchAction({type:'hideTip'}); }catch(e){} }
}
function fkRestoreTipFollow(){
  var v='0';
  try{ v=localStorage.getItem('fkTipFollow')||'0'; }catch(e){}
  FK_TIP_FOLLOW=(v==='1');
  var cb=document.getElementById('fkTipFollow');
  if(cb) cb.checked=FK_TIP_FOLLOW;   // 默认不勾选 = 停靠右上角
}
function fkLocate(idx){
  if(!FK_CHART||!FK_LAST) return;
  var n=FK_LAST.categories.length; if(n<2) return;
  var pct=idx/(n-1)*100;
  var start=Math.max(0,pct-18), end=Math.min(100,pct+18);
  FK_CHART.dispatchAction({type:'dataZoom',start:start,end:end});
}
function fkRender(data, replace){
  if(replace===undefined) replace=true;
  FK_LAST=data;
  var chartDiv=document.getElementById('fkChart');
  var up='#ff4d4f', down='#19c37d';
  var subs=[], overlays=[];
  for(var k in FK_IND){
    if(FK_IND[k].overlay && FK_SELECTED[k]) overlays.push(k);
    else if(FK_IND[k].sub && FK_SELECTED[k]) subs.push(k);
  }
  var S=subs.length;
  chartDiv.style.height = (540 + S*120) + 'px';
  if(!FK_CHART){
    FK_CHART=echarts.init(chartDiv);
    // 把K线拖/缩到最左端时, 自动加载更早历史(对齐"别人能往左拉看更早")
    FK_CHART.on('dataZoom', function(e){
      var st=(e.batch&&e.batch[0])?e.batch[0].start:(e.start!=null?e.start:100);
      var en=(e.batch&&e.batch[0])?e.batch[0].end:(e.end!=null?e.end:100);
      FK_ZOOM={start:st,end:en};
      fkDrawOverlay();
      fkRefreshMacdCrossMarks();   // 缩放/拖拽后, 按当前坐标系重算金叉/死叉标记的像素位置, 使其跟随 K 线
      if(st<=1 && !FK_EARLIEST && !FK_LOADING_MORE){ setTimeout(fkLoadEarlier, 200); }
    });
    // 金叉/死叉标记的自定义悬浮提示框: 独立于主图 axis tooltip, 鼠标移到标记上即显示精确时间/数据
    // 注: 此面板位置跟随 markPoint 像素坐标(默认落在 MACD 副图区域), 按原始行为保留
    if(!FK_CROSS_TIP_BOUND){
      FK_CROSS_TIP_BOUND=true;
      FK_CHART.on('mouseover', function(p){
        if(p.componentType==='markPoint' && p.data && p.data._tip){
          var el=document.getElementById('fkCrossTip'); if(!el) return;
          el.innerHTML=p.data._tip; el.style.display='block';
          // 先按默认右下放置, 下一帧根据实际尺寸 + 容器边界做翻转/裁剪, 避免放大后超出画布
          el.style.left=(p.data.x+14)+'px'; el.style.top=(p.data.y-12)+'px';
          var host=FK_CHART.getDom();
          if(host){
            requestAnimationFrame(function(){
              var ew=el.offsetWidth||0, eh=el.offsetHeight||0;
              var cw=host.clientWidth||0, ch=host.clientHeight||0;
              if(!ew || !cw) return;
              var left=p.data.x+14, top=p.data.y-12;
              // 右边界溢出 -> 放到标记左侧
              if(left+ew > cw-4) left = p.data.x - ew - 14;
              // 左边界溢出 -> 靠左留边
              if(left < 4) left = 4;
              // 下边界溢出 -> 上移
              if(top+eh > ch-4) top = ch - eh - 4;
              // 上边界溢出 -> 下移
              if(top < 4) top = 4;
              el.style.left = left+'px'; el.style.top = top+'px';
            });
          }
        }
      });
      FK_CHART.on('mouseout', function(p){
        if(p.componentType==='markPoint' && p.data && p.data._tip){
          var el=document.getElementById('fkCrossTip'); if(el) el.style.display='none';
        }
      });
    }
    // 顶部 ECharts 图例点击也要同步到 FK_SELECTED, 否则点了会失效/下次重绘又出来
    FK_CHART.on('legendselectchanged', function(e){
      if(FK_LEGEND_SYNC) return;
      var name=e.name;
      for(var key in FK_IND){
        var cfg=FK_IND[key], matched=false;
        if(cfg.name && cfg.name===name) matched=true;
        if(!matched && cfg.lines){
          for(var li=0;li<cfg.lines.length;li++){ if(cfg.lines[li].n===name){ matched=true; break; } }
        }
        if(matched){
          FK_LEGEND_SYNC=true;
          FK_SELECTED[key]=!FK_SELECTED[key];
          fkBuildIndBar();
          if(FK_LAST) fkRender(FK_LAST, true);
          FK_LEGEND_SYNC=false;
          break;
        }
      }
    });
  }
  FK_CHART.resize();
  var topPad=6, gap=3, mainH=(S===0)?88:42;
  var subH=(S===0)?0:Math.max(8,(100-topPad-mainH-gap*(S+1))/S);
  var tops=[topPad]; var cur=topPad+mainH+gap;
  for(var i=0;i<S;i++){ tops.push(cur); cur+=subH+gap; }
  var grids=[{left:66,right:24,top:tops[0]+'%',height:mainH+'%'}];
  var xAxes=[{type:'category',data:data.categories,scale:true,axisLine:{lineStyle:{color:'#3a4a5e'}},axisLabel:{color:'#9fb3c8'},splitLine:{show:false}}];
  var yAxes=[{scale:true,splitLine:{lineStyle:{color:'#1a2535'}},axisLabel:{color:'#9fb3c8'}}];
  var series=[{name:'K线',type:'candlestick',data:data.kline,xAxisIndex:0,yAxisIndex:0,itemStyle:{color:up,color0:down,borderColor:up,borderColor0:down}}];
  var legendData=['K线'];
  for(var oi=0;oi<overlays.length;oi++){
    var ov=FK_IND[overlays[oi]];
    for(var li=0;li<ov.lines.length;li++){
      var ln=ov.lines[li];
      legendData.push(ln.n);
      series.push({name:ln.n,type:'line',data:data[ln.k],xAxisIndex:0,yAxisIndex:0,smooth:false,showSymbol:false,lineStyle:{width:1,color:ln.c}});
    }
  }
  var macdDifIdx=-1;
  for(var si=0;si<S;si++){
    var gi=si+1;
    grids.push({left:66,right:24,top:tops[gi]+'%',height:subH+'%'});
    xAxes.push({type:'category',gridIndex:gi,data:data.categories,axisLabel:{show:false},axisLine:{lineStyle:{color:'#3a4a5e'}},axisTick:{show:false}});
    yAxes.push({gridIndex:gi,scale:true,splitNumber:2,axisLabel:{color:'#9fb3c8'},axisLine:{show:false},axisTick:{show:false},splitLine:{lineStyle:{color:'#16202e'}}});
    var cfg=FK_IND[subs[si]];
    if(cfg.lines){
      for(var l2=0;l2<cfg.lines.length;l2++){
        var l3=cfg.lines[l2];
        legendData.push(l3.n);
        if(l3.bar){
          // MACD 柱状图: 每根柱子单独着色(正红负绿), series 级颜色固定为红色供图例使用
          var barData=data[l3.k].map(function(v){ return {value:v, itemStyle:{color:(v>=0)?'#ff4d4f':'#19c37d'}}; });
          series.push({name:l3.n,type:'bar',xAxisIndex:gi,yAxisIndex:gi,data:barData,itemStyle:{color:'#ff4d4f'}});
        } else {
          series.push({id:(l3.n==='DIF'?'fkMacdDif':undefined),name:l3.n,type:'line',data:data[l3.k],xAxisIndex:gi,yAxisIndex:gi,smooth:false,showSymbol:false,lineStyle:{width:1,color:l3.c}});
          if(subs[si]==='MACD' && l3.n==='DIF') macdDifIdx=series.length-1;
        }
      }
    } else if(cfg.type==='bar'){
      legendData.push(cfg.name);
      series.push({name:cfg.name,type:'bar',xAxisIndex:gi,yAxisIndex:gi,data:data[cfg.k],itemStyle:{color:cfg.c}});
    } else {
      legendData.push(cfg.name);
      series.push({name:cfg.name,type:'line',data:data[cfg.k],xAxisIndex:gi,yAxisIndex:gi,smooth:false,showSymbol:false,lineStyle:{width:1,color:cfg.c}});
    }
  }
  // ===== 形态信号: 支撑/压力位 + 顶/底背离 + MACD 金叉/死叉 =====
  var an = data.analysis||{support:[],resistance:[],divergences:[],macd_crosses:[]};
  var divs = an.divergences||[];
  var crosses = an.macd_crosses||[];
  var mg = subs.indexOf('MACD');
  var macdGrid = mg>=0 ? mg+1 : -1;
  // 主图: 支撑/压力位 水平线 (markLine)
  var mlData=[];
  for(var ri=0;ri<(an.resistance||[]).length;ri++){
    mlData.push({yAxis:an.resistance[ri].price, lineStyle:{color:'#ff4d4f',type:'dashed',width:1}, label:{formatter:'压力 '+an.resistance[ri].price, color:'#ff4d4f', position:'end', fontSize:11}});
  }
  for(var si2=0;si2<(an.support||[]).length;si2++){
    mlData.push({yAxis:an.support[si2].price, lineStyle:{color:'#19c37d',type:'dashed',width:1}, label:{formatter:'支撑 '+an.support[si2].price, color:'#19c37d', position:'end', fontSize:11}});
  }
  if(mlData.length) series[0].markLine={symbol:'none',silent:true,data:mlData,tooltip:{show:false}};
  // 信号回顾竖线标记(mark_ts, 来自监控回顾页深链): 在离信号时间最近的 K 线处打一条竖直虚线
  if(window.FK_MARK_TS_MS){
    var _dts=data.datetimes||[];
    if(_dts.length){
      var _tgt=(window.FK_MARK_TS_MS)*1000000;   // epoch ms(UTC) -> ns, 与 data.datetimes 对齐
      var _bi=-1,_bd=Infinity;
      for(var _di=0;_di<_dts.length;_di++){ var _dd=Math.abs((_dts[_di]||0)-_tgt); if(_dd<_bd){_bd=_dd;_bi=_di;} }
      if(_bi>=0){
        if(!series[0].markLine){ series[0].markLine={symbol:'none',silent:true,data:[],tooltip:{show:false}}; }
        series[0].markLine.data.push({xAxis:data.categories[_bi], lineStyle:{color:'#ffd54f',type:'dashed',width:1.5}, label:{show:true,formatter:'信号',color:'#ffd54f',position:'end',fontSize:11}});
      }
    }
  }
  // MACD 金叉/死叉标记: 这里只记录索引与交叉数据, 真正的像素定位在 FK_CHART.setOption 之后
  // 由 fkRefreshMacdCrossMarks() 用 convertToPixel 完成。这样在 dataZoom 缩放/拖拽时, 也能在
  // 事件中按"当前坐标系"重算像素坐标, 标记始终跟随 K 线移动(不会停在原地)。
  if(macdDifIdx>=0){ FK_MACD_DIF_IDX=macdDifIdx; FK_MACD_AXIS_IDX=macdGrid; FK_CROSS_DATA=crosses;
    // 零线范围带: 以用户设定的零线(zero_line)为中心, ±FK_MACD_ZERO_BAND 的阴影区 + 上下边界虚线 + 零线本体
    var zline=parseFloat(FK_MACD_ZERO_LINE)||0;
    var zband=parseFloat(FK_MACD_ZERO_BAND)||0;
    if(zband>0){
      series[macdDifIdx].markArea={
        silent:true,
        itemStyle:{color:'rgba(240,185,11,0.10)'},
        data:[[{yAxis:zline-zband},{yAxis:zline+zband}]],
        label:{show:false}
      };
      series[macdDifIdx].markLine={
        symbol:'none',silent:true,
        lineStyle:{color:'#f0b90b',type:'dashed',width:1},
        data:[
          {yAxis:zline+zband,label:{formatter:''+(zline+zband),color:'#f0b90b',fontSize:10}},
          {yAxis:zline-zband,label:{formatter:''+(zline-zband),color:'#f0b90b',fontSize:10}},
          {yAxis:zline,label:{formatter:'零线 '+zline,color:'#ffe08a',fontSize:10},lineStyle:{color:'#ffe08a',type:'solid',width:1.5}}
        ]
      };
    } else if(zline!==0){
      // 未设范围带宽, 但仍画出用户设定的零线位置
      series[macdDifIdx].markLine={
        symbol:'none',silent:true,
        lineStyle:{color:'#ffe08a',type:'solid',width:1.5},
        data:[{yAxis:zline,label:{formatter:'零线 '+zline,color:'#ffe08a',fontSize:10}}]
      };
    }
  }
  // 背离连线: 主图价格侧 + MACD 副图指标侧
  var priceColor={top:'#ff4d4f',bottom:'#19c37d'};
  for(var di=0;di<divs.length;di++){
    var d=divs[di]; var col=priceColor[d.type]; var lbl=(d.type==='top'?'顶背离':'底背离');
    series.push({type:'line',xAxisIndex:0,yAxisIndex:0,data:[{value:[d.x1,d.p1]},{value:[d.x2,d.p2]}],showSymbol:true,symbol:'circle',symbolSize:5,lineStyle:{color:col,width:1.5,type:'dashed'},itemStyle:{color:col},label:{show:true,formatter:lbl,position:'end',color:col,fontSize:12},tooltip:{show:false},silent:true,z:6});
    if(macdGrid>=0){
      series.push({type:'line',xAxisIndex:macdGrid,yAxisIndex:macdGrid,data:[{value:[d.x1,d.i1]},{value:[d.x2,d.i2]}],showSymbol:true,symbol:'circle',symbolSize:5,lineStyle:{color:col,width:1.5,type:'dashed'},itemStyle:{color:col},tooltip:{show:false},silent:true,z:6});
    }
  }
  var xIdxAll=[]; for(var xi=0;xi<grids.length;xi++) xIdxAll.push(xi);
  // 主图白色指标面板(轴 tooltip): 仅在鼠标位于 K 线主图(grid 0)区域时显示,
  // 落在下方 MACD/VOL 等副图区域时不显示(返回屏外坐标, 依赖 #fkChartWrap 的 overflow:hidden 裁掉)
  // 位置: 默认(不勾选)固定停靠在图表右上角 —— 面板不再跟着鼠标跑, 不遮挡正在看的那一段;
  //       勾选"提示框跟随光标"后恢复 point 跟随。由 FK_TIP_FOLLOW 控制, 开关实时生效(不重绘)。
  function fkMainGridRect(){
    try{ var g=FK_CHART.getModel().getComponent('grid',0); if(g && g.coordinateSystem) return g.coordinateSystem.getRect(); }catch(e){}
    return null;
  }
  var opt={
    backgroundColor:'transparent', animation:false,
    legend:{data:legendData,textStyle:{color:'#9fb3c8'},top:0,type:'scroll'},
    tooltip:{trigger:'axis',axisPointer:{type:'cross'},transitionDuration:0,
      position:function(point,params,dom,rect,size){
        var mr=fkMainGridRect();
        if(mr && (point[1]<mr.y || point[1]>mr.y+mr.height)) return [-99999,-99999];
        if(FK_TIP_FOLLOW) return point;              // 勾选: 跟随光标(原行为)
        var vw=(size&&size.viewSize)?size.viewSize[0]:0;
        var vh=(size&&size.viewSize)?size.viewSize[1]:0;
        var cw=(size&&size.contentSize)?size.contentSize[0]:0;
        var ch=(size&&size.contentSize)?size.contentSize[1]:0;
        if(!vw) return point;                        // 取不到尺寸时退回跟随, 不产生异常
        if(!cw) cw=236; if(!ch) ch=0;
        var x=vw-cw-12; if(x<8) x=8;                 // 贴右边缘(留 12px 内边距)
        var y=28;                                    // 让开顶部图例行
        if(ch && vh && y+ch>vh-8) y=Math.max(8, vh-ch-8);
        return [x,y];
      }},
    axisPointer:{link:[{xAxisIndex:'all'}]},
    grid:grids,
    xAxis:xAxes,
    yAxis:yAxes,
    dataZoom:[{type:'inside',xAxisIndex:xIdxAll,start:FK_ZOOM.start,end:FK_ZOOM.end},{type:'slider',xAxisIndex:xIdxAll,bottom:2,height:14,start:FK_ZOOM.start,end:FK_ZOOM.end,textStyle:{color:'#9fb3c8'}}],
    series:series
  };
  FK_CHART.setOption(opt, replace);   // replace=true 全量重建(切周期/切指标); false=合并模式(仅更新数据, 加载更早时丝滑拼接不闪屏)
  var q=data.quote||{};
  var h='';
  if(q.last!=null){
    var cls = q.change>0?'up':(q.change<0?'down':'');
    var sign = q.change>0?'+':'';
    h += '<span class="q-item">最新价<span class="q-val '+cls+'">'+q.last+'</span></span>';
    if(q.change!=null) h += '<span class="q-item">涨跌<span class="q-val '+cls+'">'+sign+q.change+' ('+sign+q.change_pct+'%)</span></span>';
    if(q.pre_close!=null) h += '<span class="q-item">昨收<span class="q-val">'+q.pre_close+'</span></span>';
  } else {
    h = '<span class="q-item">最新价<span class="q-val">—</span></span><span class="q-item">（无实时tick）</span>';
  }
  h += '<span class="q-item">周期<span class="q-val">'+data.period_label+'</span></span>';
  if(data.real_symbol && data.real_symbol!=FK_SYMBOL) h += '<span class="q-item">真实合约<span class="q-val">'+data.real_symbol+'</span></span>';
  document.getElementById('fkQuote').innerHTML = h;
  fkDrawOverlay();
  fkRefreshMacdCrossMarks();   // setOption 完成后, 把金叉/死叉标记定位到后一根 bar（长短线交汇的后一节点）
}
// 用 convertToPixel 按"当前坐标系"把 MACD 金叉/死叉标记定位到后一根 bar（长短线交汇的后一节点, X=idx, Y=DIF[i]）。
// 初始渲染后调用一次; 每次 dataZoom 缩放/拖拽也调用, 使标记随 K 线移动而不再静止。
function fkRefreshMacdCrossMarks(){
  if(!FK_CHART || FK_MACD_DIF_IDX<0) return;
  var data=FK_LAST; if(!data) return;
  // 确保自定义悬浮提示框容器存在(绝对定位于图表容器内, 不干扰主图 axis tooltip)
  var host=FK_CHART.getDom();
  if(host){
    if(getComputedStyle(host).position==='static') host.style.position='relative';
    if(!document.getElementById('fkCrossTip')){
      var tipEl=document.createElement('div'); tipEl.id='fkCrossTip';
      tipEl.style.cssText='position:absolute;display:none;z-index:60;pointer-events:none;background:rgba(18,20,28,0.96);border:1px solid #3a3f4b;border-radius:6px;padding:8px 10px;font-size:12px;color:#e8eaed;line-height:1.7;white-space:normal;max-width:260px;box-shadow:0 6px 16px rgba(0,0,0,0.5);';
      host.appendChild(tipEl);
    }
  }
  var crosses=FK_CROSS_DATA||[];
  var mp=[];
  function _v(arr,ix){ var v=(arr&&arr[ix]!=null)?arr[ix]:null; return (v==null)?'—':Number(v).toFixed(2); }
  for(var ci=0;ci<crosses.length;ci++){
    var cx=crosses[ci];
    var isGolden=cx.type==='golden';
    var col=isGolden?'#ff4d4f':'#19c37d';
    var lbl=isGolden?'金叉':'死叉';
    // X: 后一根 bar 整数下标; Y: 该 bar 的 DIF 值(长短线交汇的后一节点)
    var xpos=cx.idx;
    var yv=(cx.dif!=null)?cx.dif:(data.macdDif?data.macdDif[cx.idx]:null);
    if(yv==null || xpos==null) continue;
    var px=null, py=null;
    try{
      px = FK_CHART.convertToPixel({xAxisIndex:FK_MACD_AXIS_IDX}, xpos);
      py = FK_CHART.convertToPixel({yAxisIndex:FK_MACD_AXIS_IDX}, yv);
    }catch(e){ px=null; py=null; }
    if(px!=null && py!=null){
      // tooltip: 后一根 bar 的 DIF/DEA 与收盘(悬停标记时由自定义提示框显示)
      var tip = '<b style="color:'+col+'">'+lbl+'</b>（后一根 bar #'+xpos+'）<br>'
              + 'DIF: <b>'+_v(data.macdDif,xpos)+'</b> / DEA: '+_v(data.macdDea,xpos)+'<br>'
              + '收盘: '+(cx.price!=null?Number(cx.price).toFixed(2):'—');
      mp.push({_tip:tip, name:lbl, x:px, y:py, itemStyle:{color:col},
        symbol:isGolden?'triangle':'diamond', symbolSize:14, symbolRotate:isGolden?0:180,
        label:{show:true,formatter:lbl,position:'top',color:'#fff',fontSize:10,backgroundColor:col,padding:[2,4],borderRadius:3,offset:[0,-4]},
        tooltip:{show:false}});
    }
  }
  try{
    // 按 id 定位 DIF 系列, 仅更新 markPoint(不影响已有的 markArea/markLine);
    // silent:false 以触发 mouseover 供自定义提示框; tooltip.show:false 关闭 echarts 自带提示(用自定义 div)
    FK_CHART.setOption({series:[{id:'fkMacdDif', markPoint:{data:mp, silent:false, z:7, animation:false, tooltip:{show:false}}}]});
  }catch(e){}
}
function fkLoad(p, cnt){
  FK_PERIOD=p;
  var btns=document.querySelectorAll('.period-bar button');
  for(var i=0;i<btns.length;i++){ btns[i].className=(btns[i].getAttribute('data-p')===p)?'active':''; }
  var url='api/kline?symbol='+encodeURIComponent(FK_SYMBOL)+'&period='+p;
  if(cnt) url+='&count='+cnt;
  fetch(url).then(function(r){return r.json();}).then(function(d){
    if(d.ok){
      FK_LAST=d; fkRender(d);
      FK_LOADED_COUNT=d.categories.length;
      FK_EARLIEST_DT = (d.earliest_dt!=null)? d.earliest_dt : null;   // 记录最左根时间, 供翻页锚定
      if(!cnt){ FK_EARLIEST = !(d.has_more); FK_ZOOM={start:0,end:100}; }   // 切周期: 重置"已到最早"与视图
      if(cnt && d.categories.length<cnt){ FK_EARLIEST=true; }  // 返回少于请求=已到历史最早
      fkSetLoadBtn();
    } else { document.getElementById('fkQuote').innerHTML='<span class="q-item">'+(d.error||'无数据')+'</span>'; }
  }).catch(function(e){ document.getElementById('fkQuote').innerHTML='<span class="q-item">请求失败: '+e+'</span>'; });
}
// 监控面板调用: 设置 MACD 零线位置 / 范围带阈值, 并刷新当前图表
window.fkSetMacdZeroLine=function(v){
  FK_MACD_ZERO_LINE=parseFloat(v)||0;
  if(FK_CHART && FK_LAST) fkRender(FK_LAST, true);
};
window.fkSetMacdZeroBand=function(v){
  FK_MACD_ZERO_BAND=parseFloat(v)||0;
  if(FK_CHART && FK_LAST) fkRender(FK_LAST, true);
};
function fkSetLoadBtn(){
  var b=document.getElementById('fkLoadMore'); if(!b) return;
  if(FK_EARLIEST){ b.innerText='已到最早历史 ('+FK_LOADED_COUNT+'根)'; b.className='ind-btn fk-disabled'; }
  else if(FK_LOADING_MORE){ b.innerText='加载更早中…'; b.className='ind-btn'; }
  else { b.innerText='加载更早 ('+FK_LOADED_COUNT+'根)'; b.className='ind-btn'; }
}
function fkConcatHist(prev, more){
  // more = 更早的历史段(end_dt 之前), prev = 现有数据; 把 more 拼到 prev 前面
  if(!more || !more.categories || !more.categories.length) return prev;
  if(!prev || !prev.categories || !prev.categories.length) return more;
  var out={};
  for(var k in prev){ if(!Array.isArray(prev[k])) out[k]=prev[k]; }  // 标量字段保留 prev(如实时 quote)
  var md = more.datetimes||[];
  var pd = prev.datetimes||[];
  var cut = md.length;   // 默认去掉全部(若 more 全部与 prev 首根重叠)
  for(var i=md.length-1;i>=0;i--){ if(md[i] < pd[0]){ cut=i+1; break; } }  // 仅保留早于 prev 首根的更历史部分
  var N = md.length - cut;
  for(var k2 in prev){
    if(Array.isArray(prev[k2])){
      var m = Array.isArray(more[k2]) ? more[k2].slice(cut) : [];
      out[k2] = m.concat(prev[k2]);
    }
  }
  // 合并信号: 旧信号索引整体 +N(历史插在前面), 新段信号保持
  try{
    var pa = prev.analysis||{support:[],resistance:[],divergences:[],macd_crosses:[]};
    var ma = more.analysis||{support:[],resistance:[],divergences:[],macd_crosses:[]};
    function shiftIdx(arr){ if(!arr) return []; return arr.map(function(x){ var y={}; for(var z in x){ y[z]=x[z]; } if(y.idx!=null) y.idx+=N; if(y.x1!=null) y.x1+=N; if(y.x2!=null) y.x2+=N; return y; }); }
    out.analysis={ support:shiftIdx(pa.support).concat(ma.support||[]), resistance:shiftIdx(pa.resistance).concat(ma.resistance||[]), divergences:shiftIdx(pa.divergences).concat(ma.divergences||[]), macd_crosses:shiftIdx(pa.macd_crosses).concat(ma.macd_crosses||[]) };
  }catch(e){ out.analysis = prev.analysis; }
  out.earliest_dt = (more.earliest_dt!=null)? more.earliest_dt : (prev.earliest_dt!=null? prev.earliest_dt : null);
  out.has_more = more.has_more;
  out.loaded_count = (out.categories?out.categories.length:0);
  return out;
}
function fkLoadEarlier(){
  if(FK_EARLIEST || FK_LOADING_MORE) return;
  if(FK_EARLIEST_DT==null) return;   // 无锚点, 无法翻页
  FK_LOADING_MORE=true; fkSetLoadBtn();
  var prevLen=FK_LOADED_COUNT;
  var prevZoom={start:FK_ZOOM.start, end:FK_ZOOM.end};   // 加载前先记录当前缩放视图(可能放大过的任意区间)
  var page=500;
  var url='api/kline?symbol='+encodeURIComponent(FK_SYMBOL)+'&period='+FK_PERIOD+'&count='+page+'&end_dt='+FK_EARLIEST_DT;
  fetch(url).then(function(r){return r.json();}).then(function(d){
    FK_LOADING_MORE=false;
    if(d.ok && d.categories && d.categories.length){
      var merged=fkConcatHist(FK_LAST, d);
      var newLen=merged.categories.length;
      if(newLen<=prevLen){ FK_EARLIEST=true; fkSetLoadBtn(); return; }  // 无新增 => 已到历史最早
      var N=newLen-prevLen;
      // 保持当前视图 bar 不动: 旧数据索引整体右移 N 根(新历史插在左侧)
      if(prevLen>1 && newLen>1){
        var obS=prevZoom.start/100*(prevLen-1);
        var obE=prevZoom.end/100*(prevLen-1);
        FK_ZOOM={ start:(obS+N)/(newLen-1)*100, end:(obE+N)/(newLen-1)*100 };
      }
      FK_LAST=merged;
      FK_LOADED_COUNT=newLen;
      FK_EARLIEST_DT=(merged.earliest_dt!=null)?merged.earliest_dt:FK_EARLIEST_DT;
      if(d.has_more===false) FK_EARLIEST=true;   // 后端明确无更早
      fkRender(merged, false);   // 合并模式: 不重建整图, 丝滑拼接
      fkSetLoadBtn();
    } else if(d.ok){ FK_EARLIEST=true; fkSetLoadBtn(); }  // 返回空 => 到头
    else { fkSetLoadBtn(); }
  }).catch(function(e){ FK_LOADING_MORE=false; fkSetLoadBtn(); });
}
// ===== 划线分析 (透明 overlay canvas, 坐标用 ECharts 换算, 随缩放/翻页自动对齐) =====
var FK_DRAW={tool:'',color:'#ffd54f',active:false,drawings:[],temp:null,drag:null};
function fkToggleDrawPanel(e){ if(e) e.stopPropagation(); var p=document.getElementById('fkDrawPanel'); p.classList.toggle('show'); }
function fkSetDrawTool(t){
  FK_DRAW.tool=t; FK_DRAW.active=!!t;
  var btns=document.querySelectorAll('.fk-draw-tool'); for(var i=0;i<btns.length;i++){ btns[i].classList.toggle('active', btns[i].getAttribute('data-tool')===t); }
  var cv=document.getElementById('fkDrawCanvas'); if(cv){ cv.style.pointerEvents = t?'auto':'none'; fkSyncCanvas(); }
  var chart=document.getElementById('fkChart'); if(chart) chart.style.cursor = t?'crosshair':'';
}
function fkSetDrawColor(c){
  FK_DRAW.color=c;
  var sw=document.querySelectorAll('.fk-draw-color'); for(var i=0;i<sw.length;i++){ sw[i].classList.toggle('active', sw[i].getAttribute('data-color')===c); }
}
function fkExitDraw(){ FK_DRAW.active=false; FK_DRAW.tool=''; fkSetDrawTool(''); }
function fkClearDrawings(){ if(confirm('清空当前所有划线?')){ FK_DRAW.drawings=[]; fkDrawOverlay(); } }
function fkSyncCanvas(){
  var cv=document.getElementById('fkDrawCanvas'); if(!cv) return;
  var r=cv.getBoundingClientRect(); if(!r.width) r=document.getElementById('fkChart').getBoundingClientRect();
  cv.width=Math.round(r.width); cv.height=Math.round(r.height);
}
function fkGridRect(){
  try{
    var W=FK_CHART.getWidth(), H=FK_CHART.getHeight();
    var g=FK_CHART.getOption().grid[0];
    function px(v,tot){ if(typeof v==='number') return v; var s=String(v).replace('%',''); return parseFloat(s)/100*tot; }
    var left=px(g.left,W), right=px(g.right,W), top=px(g.top,H), height=px(g.height,H);
    return {x:left, y:top, w:(W-right)-left, h:height};
  }catch(e){ return {x:66,y:6,w:600,h:400}; }
}
function fkPxToData(px,py){ try{ return FK_CHART.convertFromPixel({xAxisIndex:0,yAxisIndex:0},[px,py]); }catch(e){ return null; } }
function fkDataToPx(xCat,yVal){
  var idx=FK_LAST.categories.indexOf(xCat); if(idx<0) return null;
  try{ var px=FK_CHART.convertToPixel({xAxisIndex:0}, idx); var py=FK_CHART.convertToPixel({yAxisIndex:0}, yVal); return [px,py]; }catch(e){ return null; }
}
function fkDrawOverlay(){
  var cv=document.getElementById('fkDrawCanvas'); if(!cv||!FK_CHART||!FK_LAST) return;
  fkSyncCanvas();
  var ctx=cv.getContext('2d'); ctx.clearRect(0,0,cv.width,cv.height);
  var rect=fkGridRect(); if(!rect) return;
  var gr={left:rect.x,top:rect.y,right:rect.x+rect.w,bottom:rect.y+rect.h};
  function seg(a,b,color,wid,dash){ ctx.strokeStyle=color; ctx.lineWidth=wid||1.5; ctx.setLineDash(dash||[]); ctx.beginPath(); ctx.moveTo(a[0],a[1]); ctx.lineTo(b[0],b[1]); ctx.stroke(); ctx.setLineDash([]); }
  function clip(){ ctx.save(); ctx.beginPath(); ctx.rect(gr.left,gr.top,gr.w,gr.h); ctx.clip(); }
  function one(d){
    ctx.strokeStyle=d.color; ctx.fillStyle=d.color; ctx.lineWidth=1.5;
    if(d.tool==='hline'){
      var py=FK_CHART.convertToPixel({yAxisIndex:0}, d.y1); if(py==null) return;
      clip(); seg([gr.left,py],[gr.right,py],d.color,1.3,[6,4]); ctx.restore();
    } else if(d.tool==='vline'){
      var pxi=FK_LAST.categories.indexOf(d.x1); if(pxi<0) return; var pxx=FK_CHART.convertToPixel({xAxisIndex:0}, pxi);
      clip(); seg([pxx,gr.top],[pxx,gr.bottom],d.color,1.3,[6,4]); ctx.restore();
    } else if(d.tool==='rect'){
      var p1=fkDataToPx(d.x1,d.y1), p2=fkDataToPx(d.x2,d.y2); if(!p1||!p2) return;
      clip(); ctx.strokeStyle=d.color; ctx.lineWidth=1.3; ctx.strokeRect(Math.min(p1[0],p2[0]),Math.min(p1[1],p2[1]),Math.abs(p2[0]-p1[0]),Math.abs(p2[1]-p1[1])); ctx.restore();
    } else if(d.tool==='text'){
      var pt=fkDataToPx(d.x1,d.y1); if(!pt) return;
      ctx.font='bold 13px Microsoft YaHei'; ctx.textBaseline='top'; ctx.fillStyle=d.color;
      ctx.fillText(d.text, pt[0]+3, pt[1]+3);
    } else {
      var a=fkDataToPx(d.x1,d.y1), b=fkDataToPx(d.x2,d.y2); if(!a||!b) return;
      clip();
      if(d.tool==='ray'){
        var dx=b[0]-a[0], dy=b[1]-a[1], k=dx!==0?dy/dx:0;
        var ex1,ey1,ex2,ey2;
        if(Math.abs(dx)<1e-6){ ex1=ex2=a[0]; ey1=gr.top; ey2=gr.bottom; }
        else { ex1=gr.left; ey1=a[1]+k*(gr.left-a[0]); ex2=gr.right; ey2=a[1]+k*(gr.right-a[0]); }
        seg([ex1,ey1],[ex2,ey2],d.color,1.5);
      } else { seg(a,b,d.color,1.5); }
      ctx.restore();
    }
  }
  for(var i=0;i<FK_DRAW.drawings.length;i++) one(FK_DRAW.drawings[i]);
  if(FK_DRAW.drag && FK_DRAW.temp){
    seg(FK_DRAW.drag[0], FK_DRAW.temp, FK_DRAW.color, 1.5, [5,4]);
  }
}
function fkDrawDown(e){
  if(!FK_DRAW.active || !FK_DRAW.tool) return;
  var cv=document.getElementById('fkDrawCanvas'); var r=cv.getBoundingClientRect();
  var px=e.clientX-r.left, py=e.clientY-r.top;
  if(FK_DRAW.tool==='text'){
    var txt=prompt('输入标注文字:','');
    if(txt){ var v=fkPxToData(px,py); if(v && FK_LAST.categories[Math.round(v[0])]!=null){ FK_DRAW.drawings.push({tool:'text',x1:FK_LAST.categories[Math.round(v[0])],y1:v[1],text:txt,color:FK_DRAW.color}); } }
    fkDrawOverlay(); return;
  }
  FK_DRAW.drag=[[px,py],[px,py]]; FK_DRAW.temp=[px,py];
}
function fkDrawMove(e){
  if(!FK_DRAW.drag) return;
  var cv=document.getElementById('fkDrawCanvas'); var r=cv.getBoundingClientRect();
  FK_DRAW.temp=[e.clientX-r.left, e.clientY-r.top]; fkDrawOverlay();
}
function fkDrawUp(e){
  if(!FK_DRAW.drag) return;
  var cv=document.getElementById('fkDrawCanvas'); var r=cv.getBoundingClientRect();
  var px=e.clientX-r.left, py=e.clientY-r.top; var a=FK_DRAW.drag[0];
  var v1=fkPxToData(a[0],a[1]), v2=fkPxToData(px,py);
  FK_DRAW.drag=null; FK_DRAW.temp=null;
  if(v1 && v2){
    var x1=FK_LAST.categories[Math.round(v1[0])], x2=FK_LAST.categories[Math.round(v2[0])];
    if(x1==null||x2==null){ fkDrawOverlay(); return; }
    var y1=v1[1], y2=v2[1];
    if(FK_DRAW.tool==='hline'){ y2=y1; x2=x1; }
    else if(FK_DRAW.tool==='vline'){ x2=x1; }
    if(FK_DRAW.tool!=='hline' && FK_DRAW.tool!=='vline' && Math.abs(px-a[0])<3 && Math.abs(py-a[1])<3){ fkDrawOverlay(); return; }
    FK_DRAW.drawings.push({tool:FK_DRAW.tool,x1:x1,y1:y1,x2:x2,y2:y2,color:FK_DRAW.color});
  }
  fkDrawOverlay();
}
(function(){
  var cv=document.getElementById('fkDrawCanvas');
  if(cv){ cv.addEventListener('mousedown', fkDrawDown); cv.addEventListener('mousemove', fkDrawMove); window.addEventListener('mouseup', fkDrawUp); }
})();
window.addEventListener('resize', function(){ if(FK_CHART){ FK_CHART.resize(); fkDrawOverlay(); } });
fkRestoreTipFollow();
fkBuildIndBar();
fkLoad(FK_PERIOD);
</script>
'''
    )
    return js


def _fk_scan_script():
    return r'''
<script>
// ===== 全市场监控悬浮面板 =====
var FK_SCAN={running:false,timer:null,timerToday:null,interval:15000,mode:'realtime',rules:[],lastAlerts:{},agg:{long:[],short:[],scanned:0,total:0},lastResult:null,audioCtx:null,scanning:false,alerts:[],notifyOpen:false,notifyCap:200,auto:null,alertArmed:false,selectedTemplateIds:[],defaultRules:[],optionalRules:[],userTemplates:[],curTemplateId:null,_tplToastLast:{}};
function fkToggleScanPanel(){
  var p=document.getElementById('fkScanFloat');
  var panel=document.getElementById('fkScanPanel');
  var was=p.classList.contains('expanded');
  if(was){p.classList.remove('expanded'); if(panel) panel.style.height='';}
  else{p.classList.add('expanded'); if(panel){panel.style.height='';}}
}
function fkStartDrag(e){
  e.preventDefault();
  var panel=document.getElementById('fkScanPanel');
  if(!panel) return;
  FK_SCAN._drag={startY:e.clientY, startH:panel.offsetHeight, panel:panel};
  panel.style.transition='none';
  document.addEventListener('mousemove',fkOnDrag);
  document.addEventListener('mouseup',fkStopDrag);
}
function fkOnDrag(e){
  if(!FK_SCAN._drag) return;
  var dy=FK_SCAN._drag.startY-e.clientY;
  var nh=FK_SCAN._drag.startH+dy;
  nh=Math.max(120,Math.min(window.innerHeight*0.92,nh));
  FK_SCAN._drag.panel.style.height=nh+'px';
}
function fkStopDrag(){
  if(FK_SCAN._drag){FK_SCAN._drag.panel.style.transition=''; FK_SCAN._drag=null;}
  document.removeEventListener('mousemove',fkOnDrag);
  document.removeEventListener('mouseup',fkStopDrag);
}
function fkApiBase(){
  var p=location.pathname;
  var i=p.indexOf('/futureskline/');
  return i>=0 ? p.substring(0,i)+'/futureskline/' : '/futureskline/';
}
function fkInitScanPanel(){
  var sl=document.getElementById('fkScanSettingsLink');
  if(sl) sl.href=fkApiBase()+'scan-settings';
  fkInitDensity();
  fetch(fkApiBase()+'api/scan/templates').then(function(r){return r.json();}).then(function(d){
    if(!d.ok)return;
    FK_SCAN.defaultRules = d.default_rules||[];
    FK_SCAN.optionalRules = d.templates||[];
    FK_SCAN.userTemplates = d.user_templates||[];
    try{ var _ct=localStorage.getItem('fkCurTpl'); FK_SCAN.curTemplateId = (_ct==null?'':_ct); }catch(e){ FK_SCAN.curTemplateId=''; }
    fkRenderTplSelect();   // [2026-10-09] 模板切换选择器
    FK_SCAN.rules = (FK_SCAN.defaultRules||[]).concat(FK_SCAN.optionalRules||[]);
    fkRenderScanRules();
    // 模板默认全选(2026-09-23 用户诉求: 模版默认全选)
    FK_SCAN.selectedTemplateIds = (FK_SCAN.userTemplates||[]).map(function(t){return t.id;});
    // [2026-09-24] 模板 chips 行已按用户裁定移除 -> 不再调 fkRefreshTemplateList 渲染;
    // 直接套用全选并集(条件 + 品种)。保存/删除模板后的 fkRefreshTemplateList 调用保留:
    // 其内建 #fkTplChips 缺元素守卫, 渲染自动跳过, 但仍会刷新 FK_SCAN.userTemplates 供并集使用。
    // 套用全选并集(条件 + 品种)
    fkApplySelectedTemplates(FK_SCAN.userTemplates);
    fkLoadSched();
  }).catch(function(e){document.getElementById('fkScanRules').innerHTML='<span style="color:#ef4444;font-size:12px">加载规则失败</span>';console.error('加载规则失败',e);});
}
function fkToggleScanConfig(){
  var d=document.getElementById('fkScanConfig');
  var btn=document.getElementById('fkCfgToggle');
  if(!d) return;
  if(d.open){ d.open=false; if(btn){btn.classList.remove('open'); btn.textContent='指标/周期设置 ▸';} }
  else { d.open=true; if(btn){btn.classList.add('open'); btn.textContent='指标/周期设置 ▾';} }
}
function fkToggleDensity(){
  var root=document.getElementById('fkScanFloat');
  if(!root) return;
  var compact=root.classList.toggle('fk-compact');
  try{ localStorage.setItem('fkScanDensity', compact?'compact':'comfort'); }catch(e){}
  var btn=document.getElementById('fkDensityBtn');
  if(btn) btn.textContent = compact? '紧凑' : '舒适';
}
function fkInitDensity(){
  var compact=false;
  try{ var v=localStorage.getItem('fkScanDensity'); compact = (v==='compact') || (v===null); }catch(e){}
  var root=document.getElementById('fkScanFloat');
  if(root && compact) root.classList.add('fk-compact');
  var btn=document.getElementById('fkDensityBtn');
  if(btn) btn.textContent = compact? '紧凑' : '舒适';
}
function fkHighlightChip(tid){
  var box=document.getElementById('fkTplChips');
  if(!box) return;
  box.querySelectorAll('.fk-tpl-chip').forEach(function(c){ c.classList.toggle('active', c.getAttribute('data-id')===String(tid)); });
}
function fkRenderScanRules(){
  var h='';
  FK_SCAN.rules.forEach(function(r,i){
    var checked=r.enabled?'checked':'';
    h+='<div class="fk-scan-rule">';
    h+='<input type="checkbox" id="fkRule_'+i+'" data-idx="'+i+'" '+checked+'>';
    h+='<span class="fk-rule-name">'+fkHtml(r.name)+'</span>';
    h+='<span class="fk-rule-params">';
    if(r.type==='macd_cross'){
      var zl=(r.params.zero_line!=null)?r.params.zero_line:'0';
      var th=(r.params.threshold!=null)?r.params.threshold:'';
      var op=(r.params.op!=null)?r.params.op:'gt';
      var ip=r.indicator_params||{};
      var macd_fast=(ip.fast!=null)?ip.fast:12;
      var macd_slow=(ip.slow!=null)?ip.slow:26;
      var macd_signal=(ip.signal!=null)?ip.signal:9;
      h+='<label title="MACD快线周期(默认12)">快线</label><input type="number" data-key="indicator_fast" value="'+macd_fast+'" style="width:42px">';
      h+='<label title="MACD慢线周期(默认26)">慢线</label><input type="number" data-key="indicator_slow" value="'+macd_slow+'" style="width:42px">';
      h+='<label title="MACD信号周期(DEA, 默认9)">信号</label><input type="number" data-key="indicator_signal" value="'+macd_signal+'" style="width:42px">';
      h+='<label title="零线(DIF/DEA基准): 默认0; 高价合约可设为典型 DIF(如沪铜800)以锚定真实波动区">零线</label><input type="number" step="0.1" data-key="zero_line" value="'+zl+'" placeholder="0" oninput="fkSyncMacdBandFromRules()">';
      h+='<select data-key="op" title="比较算子: 可切换 大于/大于等于/小于/小于等于">'+
           '<option value="gt"'+((op==='gt')?' selected':'')+'>大于 &gt;</option>'+
           '<option value="ge"'+((op==='ge')?' selected':'')+'>&ge; 大于等于</option>'+
           '<option value="lt"'+((op==='lt')?' selected':'')+'>小于 &lt;</option>'+
           '<option value="le"'+((op==='le')?' selected':'')+'>&le; 小于等于</option>'+
         '</select>';
      h+='<label title="比较阈值: 对 (DIF-零线) 做该比较; 留空=不过滤(显示全部金/死叉)">阈值</label><input type="number" step="0.1" data-key="threshold" value="'+th+'" placeholder="留空=全部">';
      var tmn=(r.params.trend_ma_n!=null)?r.params.trend_ma_n:60;
      var tlkb=(r.params.trend_lookback!=null)?r.params.trend_lookback:10;
      var tmode=(r.params.trend_mode!=null)?r.params.trend_mode:'match';
      h+='<label title="趋势判定均线周期(慢速): 用 MA(该值)在较长窗口上的整体斜率判趋势, 代表\'整体趋势\'; 金叉仅在该均线整体向上时成立, 死叉仅在该均线整体向下时成立 (需求人: 整体上升中的交叉算金叉)">趋势均线</label><input type="number" data-key="trend_ma_n" value="'+tmn+'" style="width:42px" title="慢速趋势均线周期, 默认60">';
      h+='<label title="趋势判定窗口: 取最近 N 根慢速均线取首尾斜率; 越大越平滑, 局部回调越不易翻转整体趋势">趋势窗口</label><input type="number" data-key="trend_lookback" value="'+tlkb+'" style="width:42px" title="趋势窗口根数, 默认10">';
      h+='<select data-key="trend_mode" title="交叉与趋势的判定模式: 方向匹配=仅 上穿+上升 算金叉 / 下穿+下降 算死叉(需求人2026-10-12裁定, 默认); 趋势驱动=上升趋势中任一交叉(上穿/下穿)都算金叉, 下降趋势中任一交叉都算死叉(2026-08-28 旧规则)">'+
           '<option value="trend"'+((tmode==='trend')?' selected':'')+'>趋势驱动</option>'+
           '<option value="match"'+((tmode==='match')?' selected':'')+'>方向匹配</option>'+
         '</select>';
    }
    if(r.type!=='macd_cross' && r.params && r.params.threshold!=null) h+='<label>阈值</label><input type="number" step="1" data-key="threshold" value="'+r.params.threshold+'">';
    if(r.params && r.params.recent_bars!=null) h+='<label>近N根</label><input type="number" data-key="recent_bars" value="'+r.params.recent_bars+'">';
    if(r.params && r.params.n!=null) h+='<label>周期</label><input type="number" data-key="n" value="'+r.params.n+'">';
    h+='</span></div>';
  });
  document.getElementById('fkScanRules').innerHTML=h||'暂无规则';
  fkSyncMacdBandFromRules();
}
function fkSyncMacdBandFromRules(){
  var line=0;
  FK_SCAN.rules.forEach(function(r,i){
    if((r.id==='macd_golden' || r.id==='macd_death') && r.params && r.params.zero_line!=null){
      var cb=document.getElementById('fkRule_'+i);
      if(!cb) return;
      var pEl=cb.parentElement;
      var zlInp=pEl.querySelector('input[data-key="zero_line"]');
      var zl=zlInp?parseFloat(zlInp.value)||0:0;
      if(zl>line) line=zl;
    }
  });
  if(typeof window.fkSetMacdZeroLine==='function') window.fkSetMacdZeroLine(line);
  if(typeof window.fkSetMacdZeroBand==='function') window.fkSetMacdZeroBand(0);
}
function fkHtml(s){return String(s).replace(/[&<>]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c];});}
function fkHtml(s){return String(s).replace(/[&<>]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c];});}
/* [2026-10-12] 交易所短码: INE.nr2612 -> nr2612 (WH6 直接可敲); 用 lastIndexOf 兼容 KQ.m@XXX.yyy 双点码; 无前缀原样返回 */
function fkShortCode(real){
  var s=String(real||'');
  var i=s.lastIndexOf('.');
  return i>=0 ? s.slice(i+1) : s;
}
/* 点击复制短码: clipboard API 不可用(http 非安全上下文)时回落 execCommand; stopPropagation 避免触发 fkOpenSym */
function fkCopyCode(ev,code){
  if(ev && ev.stopPropagation) ev.stopPropagation();
  if(ev && ev.preventDefault) ev.preventDefault();
  var done=function(){ fkCodeToast('已复制 '+code+'，可直接粘到 WH6'); };
  var fail=function(){
    try{
      var ta=document.createElement('textarea');
      ta.value=code; ta.style.cssText='position:fixed;left:-9999px;top:0';
      document.body.appendChild(ta); ta.select();
      var ok=document.execCommand('copy');
      document.body.removeChild(ta);
      if(ok){ done(); return; }
    }catch(e){}
    fkCodeToast('复制失败，代码: '+code);
  };
  try{
    if(navigator.clipboard && navigator.clipboard.writeText){
      navigator.clipboard.writeText(code).then(done,fail); return;
    }
  }catch(e){}
  fail();
}
function fkCodeToast(msg){
  var el=document.getElementById('fkCodeToast');
  if(!el){ el=document.createElement('div'); el.id='fkCodeToast'; el.className='fk-code-toast'; (document.body||document.documentElement).appendChild(el); }
  el.textContent=msg; el.classList.add('show');
  clearTimeout(el._tm);
  el._tm=setTimeout(function(){ el.classList.remove('show'); },1800);
}
/* [2026-10-12] 合约列悬浮代码提示(用户裁定: 不常驻, 悬停浮现/移开消失)。
   事件委托挂 document, 行每次重渲染无需重绑; 提示层全局单例。
   🔍 15:35 修复: 原实现 mousemove 跟随鼠标 → 鼠标永远追不上复制按钮; 改为出现时定位一次,
   悬停到提示层自身(#fkCodeTip)时保持显示, 离开行且不在提示层上才隐藏。 */
function _fkCodeTipEl(){
  var t=document.getElementById('fkCodeTip');
  if(!t){ t=document.createElement('div'); t.id='fkCodeTip'; t.className='fk-code-tip'; (document.body||document.documentElement).appendChild(t); }
  return t;
}
function fkCodeTipShow(ev){
  var t=ev.target;
  /* 悬停在提示层自身(移动到复制按钮上)时保持显示, 不隐藏不重定位 */
  if(t&&t.closest&&t.closest('#fkCodeTip')) return;
  var td=t&&t.closest?t.closest('.col-sym[data-code]'):null;
  if(!td){ fkCodeTipHide(); return; }
  var code=td.getAttribute('data-code')||'';
  var tip=_fkCodeTipEl();
  tip.innerHTML='交易所代码: <b>'+fkHtml(code)+'</b> ｜ 主连: '+fkHtml(td.getAttribute('data-main')||'')
    +'<span class="fk-tip-copy" onclick="fkCopyCode(event,\''+fkHtml(code)+'\')">⧉ 复制</span>';
  tip.style.display='block';
  /* 定位一次(鼠标右下方), 不再跟随 —— 用户才能把鼠标移到复制按钮上 */
  var x=(ev.clientX||0)+14, y=(ev.clientY||0)+18;
  var r=tip.getBoundingClientRect();
  if(x+r.width>window.innerWidth-8) x=window.innerWidth-8-r.width;
  if(y+r.height>window.innerHeight-8) y=(ev.clientY||0)-r.height-10;
  tip.style.left=x+'px'; tip.style.top=y+'px';
}
function fkCodeTipHide(){ var tip=document.getElementById('fkCodeTip'); if(tip) tip.style.display='none'; }
(function(){
  if(window._fkCodeTipBound) return; window._fkCodeTipBound=true;
  document.addEventListener('mouseover', fkCodeTipShow);
  /* 滚动/切窗时提示层位置会错位, 兜底隐藏 */
  window.addEventListener('scroll', fkCodeTipHide, true);
  window.addEventListener('blur', fkCodeTipHide);
})();
function fkDateStr(d){var p=function(n){return (n<10?'0':'')+n;};return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate());}
function fkSetScanMode(m){
  // 切换 实时 / 历史回溯: 实时=后台自动监控(只展示当天交易日信号); 历史=直接读库里已落库信号
  if(FK_SCAN.running){ fkStopScan(); }
  FK_SCAN.mode=m;
  var rt=document.getElementById('fkModeRealtime'), hs=document.getElementById('fkModeHistory');
  var btn=document.getElementById('fkScanBtn'), hint=document.getElementById('fkScanModeHint');
  var histBlock=document.getElementById('fkScanHist');
  if(m==='realtime'){
    if(rt)rt.classList.add('active'); if(hs)hs.classList.remove('active');
    if(histBlock)histBlock.style.display='none';
    if(btn){btn.disabled=false;}
    if(hint){hint.classList.remove('history'); hint.textContent='实时模式：后台自动拉取（交易时段自动启停），只展示当天交易日的信号；更早的到历史回溯看';}
    fkPollToday();
  }else{
    if(hs)hs.classList.add('active'); if(rt)rt.classList.remove('active');
    if(histBlock)histBlock.style.display='flex';
    if(btn){btn.textContent='查询历史'; btn.disabled=false; btn.classList.remove('running');}
    if(hint){hint.classList.add('history'); hint.textContent='历史回溯：直接列出已落库的历史信号，可按日期/周期/方向筛选（不再需要先扫描）';}
    var se=document.getElementById('fkHistEnd'), ss=document.getElementById('fkHistStart');
    var nd=new Date();
    if(se&&!se.value){ se.value=fkDateStr(nd); }
    if(ss&&!ss.value){ ss.value=fkDateStr(new Date(nd.getTime()-6*86400000)); }
    fkHistLoad();
  }
}
function fkToggleScan(){
  if(FK_SCAN.mode==='history'){ fkHistLoad(); }
  else { fkToggleAuto(); }
}
function fkScanIntervalForPeriod(period){
  // 按 K 线周期匹配扫描频率: 分钟线要快, 日线可慢
  var map={'1m':5000,'5m':8000,'15m':12000,'30m':15000,'60m':20000,'day':30000,'week':15000,'month':20000,'quarter':30000};
  return map[period] || 15000;
}
var FK_PERIOD_LABELS={'1m':'1分','5m':'5分','15m':'15分','30m':'30分','60m':'60分','day':'日','week':'周','month':'月','quarter':'季'};
function fkSelectedPeriods(){
  var cbs=document.querySelectorAll('#fkScanPeriods .fk-period-cb');
  var out=[];
  cbs.forEach(function(cb){ if(cb.checked) out.push(cb.value); });
  return out;
}
// [2026-09-28] 周期显示过滤: 仅展示用户在"周期"勾选框里勾选的周期; 取消勾选即在前端隐藏。
// 后端(自动监控)仍全量抓取所有周期 —— 显示过滤不重扫、切换即时生效、重新勾选自我修复。
function fkPeriodVisible(p){
  var sel=fkSelectedPeriods();
  if(!sel.length) return true;            // 极端情况全未勾选时不过滤, 避免空屏
  return sel.indexOf(p)>=0;
}
function fkOnPeriodToggle(cb){
  if(cb && cb.parentElement) cb.parentElement.classList.toggle('on', cb.checked);
  // [2026-10-15] 折叠面板与工具栏是同一套勾选状态 -> 双向同步
  if(typeof fkSyncToolbarPeriods==='function') fkSyncToolbarPeriods(cb ? cb.value : null, cb ? cb.checked : null);
  if(typeof fkUpdatePeriodSummary==='function') fkUpdatePeriodSummary();
  // 即时按新的周期勾选重新渲染(不重扫): 实时面板 + 通知中心
  if(typeof fkRenderAgg==='function') fkRenderAgg();
  if(typeof fkNotifyRender==='function') fkNotifyRender();
}
// ===== [2026-10-15 用户需求] 工具栏周期多选 =====
// 勾选哪些周期, 下方「多头/空头信号」表格就只显示这些周期的数据。
// 设计要点:
//   1) 与折叠面板 #fkScanPeriods 同一套勾选状态(双向同步), 不引入第二份真值;
//   2) 只做【显示过滤】, 后台自动监控仍按模板全量抓取 -> 切换即时生效、不重扫、不占天勤资源;
//   3) 一个周期都没勾 = 不过滤(显示全部), 避免误操作空屏; 想要空屏请用"清空"后再手动全不选语义。
//   4) 优先级: 用户在工具栏的勾选【覆盖】模板自带的周期列表(fkEffectivePeriods 返回 null 时回退模板周期)。
function fkToolbarSelectedPeriods(){
  var box=document.getElementById('fkToolbarPeriods');
  if(!box) return null;                 // 该页无工具栏控件 -> 不干预
  var cbs=box.querySelectorAll('.fk-toolbar-period-cb');
  if(!cbs.length) return null;
  var out=[];
  cbs.forEach(function(cb){ if(cb.checked) out.push(cb.value); });
  return out;                            // 空数组 = 全部不勾
}
function fkSyncToolbarPeriods(period, checked){
  var box=document.getElementById('fkToolbarPeriods');
  if(!box) return;
  box.querySelectorAll('.fk-toolbar-period-cb').forEach(function(cb){
    if(period && cb.value!==period) return;      // 定向同步
    cb.checked=!!checked;
    if(cb.parentElement) cb.parentElement.classList.toggle('on', cb.checked);
  });
}
function fkSetToolbarPeriods(all){
  var box=document.getElementById('fkToolbarPeriods');
  if(!box) return;
  box.querySelectorAll('.fk-toolbar-period-cb').forEach(function(cb){
    cb.checked=!!all;
    if(cb.parentElement) cb.parentElement.classList.toggle('on', !!all);
  });
  // 同步折叠面板那一份
  document.querySelectorAll('#fkScanPeriods .fk-period-cb').forEach(function(cb){
    cb.checked=!!all;
    if(cb.parentElement) cb.parentElement.classList.toggle('on', !!all);
  });
  if(typeof fkUpdatePeriodSummary==='function') fkUpdatePeriodSummary();
  if(typeof fkRenderAgg==='function') fkRenderAgg();
  if(typeof fkNotifyRender==='function') fkNotifyRender();
}
function fkOnToolbarPeriodToggle(cb){
  if(cb && cb.parentElement) cb.parentElement.classList.toggle('on', cb.checked);
  fkSyncToolbarPeriods(cb ? cb.value : null, cb ? cb.checked : null);
  // 同步折叠面板那一份
  document.querySelectorAll('#fkScanPeriods .fk-period-cb').forEach(function(s){
    if(!s || s.value!==cb.value) return;
    s.checked=cb.checked;
    if(s.parentElement) s.parentElement.classList.toggle('on', cb.checked);
  });
  if(typeof fkUpdatePeriodSummary==='function') fkUpdatePeriodSummary();
  if(typeof fkRenderAgg==='function') fkRenderAgg();
  if(typeof fkNotifyRender==='function') fkNotifyRender();
}
// ===== 下拉展开/收起(2026-10-15: 横排太占宽改下拉) =====
// 交互: 点触发按钮开合; 点页面外部收起; 按 Esc 收起。勾选复选框【不】自动收起(便于连勾多个)。
function fkTogglePeriodDrop(ev){
  if(ev && ev.stopPropagation) ev.stopPropagation();
  var box=document.getElementById('fkToolbarPeriods');
  var drop=document.getElementById('fkTpDrop');
  if(!box || !drop) return;
  var open = drop.style.display!=='none';
  drop.style.display = open ? 'none' : 'block';
  box.classList.toggle('open', !open);
  if(!open && typeof fkUpdatePeriodSummary==='function') fkUpdatePeriodSummary();
}
function fkClosePeriodDrop(){
  var box=document.getElementById('fkToolbarPeriods');
  var drop=document.getElementById('fkTpDrop');
  if(!box || !drop) return;
  if(drop.style.display==='none') return;
  drop.style.display='none';
  box.classList.remove('open');
}
function fkUpdatePeriodSummary(){
  var el=document.getElementById('fkTpSummary');
  if(!el) return;
  var sel=fkToolbarSelectedPeriods();
  if(!sel || !sel.length){ el.textContent='不过滤'; return; }
  if(sel.length>=9){ el.textContent='全部'; return; }   // 9 = 全周期全集
  var names=sel.map(function(p){ return (FK_PERIOD_LABELS&&FK_PERIOD_LABELS[p])||p; });
  el.textContent = names.length<=3 ? names.join('/') : names.slice(0,3).join('/')+'…'+(names.length-3);
}
// [2026-10-15] 全局只绑一次: 点外部收起 + Esc 收起(重复注入脚本时用标记防重)。
if(typeof window!=='undefined' && !window.__fkPeriodDropBound){
  window.__fkPeriodDropBound=true;
  document.addEventListener('click', function(e){
    var box=document.getElementById('fkToolbarPeriods');
    if(!box) return;
    if(box.contains && box.contains(e.target)) return;   // 点在控件内不关
    fkClosePeriodDrop();
  }, true);
  document.addEventListener('keydown', function(e){
    if(e.key==='Escape' || e.keyCode===27) fkClosePeriodDrop();
  });
}
function fkStartScan(){
  FK_SCAN.running=true;
  var btn=document.getElementById('fkScanBtn');   // [2026-10-09] 按钮已移除 -> 可为 null
  if(btn){ btn.textContent='停止监控'; btn.classList.add('running'); }
  var periods=fkSelectedPeriods();
  if(!periods.length){document.getElementById('fkScanStatus').textContent='请至少选择一个周期'; fkStopScan(); return;}
  var rules=fkCollectRules();
  var products=FK_SCAN.activeProducts||[];
  var prodHint=products.length?' · 限'+products.length+'品种':'';
  document.getElementById('fkScanStatus').textContent='启动多周期监控…('+periods.length+'个周期'+prodHint+', 一条线程一个周期)';
  var body='periods='+encodeURIComponent(periods.join(','))+'&rules='+encodeURIComponent(JSON.stringify(rules))+'&mode=realtime';
  if(products.length) body+='&products='+encodeURIComponent(products.join(','));
  fetch(fkApiBase()+'api/scan/multi/start',{
    method:'POST',
    headers:{'Content-Type':'application/x-www-form-urlencoded'},
    body:body
  }).then(function(r){return r.json();}).then(function(d){
    if(!d.ok){document.getElementById('fkScanStatus').textContent='启动失败:'+(d.error||''); fkStopScan(); return;}
    FK_SCAN.agg={long:[],short:[],scanned:0,total:0};
    FK_SCAN.lastAlerts={};
    FK_SCAN.alerts=[];
    FK_SCAN.notifyOpen=false;
    fkNotifyRender();
    if(FK_SCAN.timer) clearTimeout(FK_SCAN.timer);
    fkPollMonitor();
    FK_SCAN.timer=setTimeout(fkPollMonitor, 2000);
  }).catch(function(e){document.getElementById('fkScanStatus').textContent='启动异常:'+e; fkStopScan();});
}
function fkStopScan(){
  FK_SCAN.running=false;
  // [2026-10-09] #fkScanBtn(开始/停止监控按钮)已随工具栏整行移除 -> btn 可能为 null。
  // 后台自动监控切模式时会自动调本函数, 原直接解引用 btn.textContent 会抛 TypeError
  // 并中断后续清理(状态文案/timer/停线程), 必须加空值保护。
  var btn=document.getElementById('fkScanBtn');
  if(btn){
    if(FK_SCAN.mode==='history'){ btn.textContent='开始回溯'; }
    else { btn.textContent='开始监控'; btn.classList.remove('running'); }
    btn.disabled=false;
  }
  var stEl=document.getElementById('fkScanStatus'); if(stEl) stEl.textContent='已停止';
  if(FK_SCAN.timer){clearTimeout(FK_SCAN.timer); FK_SCAN.timer=null;}
  if(FK_SCAN.mode!=='history'){ fetch(fkApiBase()+'api/scan/multi/stop',{method:'POST'}).catch(function(){}); }
}
// 历史回溯: 单周期 + 日期区间, 一次性扫描历史 K 线做模型回溯(不报警/不入库)
function fkRunBacktest(){
  if(FK_SCAN.running) return;
  var periods=fkSelectedPeriods();
  if(!periods.length){document.getElementById('fkScanStatus').textContent='请至少选择一个周期'; return;}
  var start=document.getElementById('fkHistStart').value;
  var end=document.getElementById('fkHistEnd').value;
  if(!start||!end){document.getElementById('fkScanStatus').textContent='请选择起始日与结束日'; return;}
  FK_SCAN.running=true;
  var btn=document.getElementById('fkScanBtn'); btn.textContent='回溯中…'; btn.disabled=true;
  document.getElementById('fkScanStatus').textContent='历史回溯中…('+periods.join('/')+', '+start+'~'+end+')';
  var rules=fkCollectRules();
  var products=FK_SCAN.activeProducts||[];
  var body='period='+encodeURIComponent(periods[0])+'&rules='+encodeURIComponent(JSON.stringify(rules))+'&start='+encodeURIComponent(start)+'&end='+encodeURIComponent(end);
  if(products.length) body+='&products='+encodeURIComponent(products.join(','));
  fetch(fkApiBase()+'api/scan/backtest',{
    method:'POST',
    headers:{'Content-Type':'application/x-www-form-urlencoded'},
    body:body
  }).then(function(r){return r.json();}).then(function(d){
    FK_SCAN.running=false; btn.textContent='开始回溯'; btn.disabled=false;
    if(!d.ok){document.getElementById('fkScanStatus').textContent='回溯失败:'+(d.error||''); return;}
    fkRenderBacktest(d);
    var st=d.stats||{};
    document.getElementById('fkScanStatus').textContent='回溯完成: 共'+st.signal_count+'条信号(多'+st.long+'/空'+st.short+'), 耗时'+d.elapsed+'s ('+start+'~'+end+')';
  }).catch(function(e){FK_SCAN.running=false;btn.textContent='开始回溯';btn.disabled=false;document.getElementById('fkScanStatus').textContent='回溯异常:'+e;});
}
function fkRenderBacktest(d){
  var sigs=d.signals||[];
  var longs=sigs.filter(function(s){return s.side==='long';});
  var shorts=sigs.filter(function(s){return s.side==='short';});
  var st=d.stats||{};
  var html='<div class="fk-bt-summary">历史回溯 · '+d.period+' · '+d.start_date+'~'+d.end_date
    +' · 信号'+st.signal_count+'（多'+st.long+'/空'+st.short+'）· 扫描'+st.scanned+'/'+st.total+(st.failed?(' 失败'+st.failed):'')+' · 耗时'+d.elapsed+'s</div>';
  if(d.by_type && Object.keys(d.by_type).length){
    var bt=[]; for(var k in d.by_type) bt.push(k+':'+d.by_type[k]);
    html+='<div class="fk-bt-summary">类型分布: '+bt.join('  ')+'</div>';
  }
  document.getElementById('fkTotalCount').textContent=st.signal_count||0;
  document.getElementById('fkHeadLong').textContent=longs.length;
  document.getElementById('fkHeadShort').textContent=shorts.length;
  document.getElementById('fkLongResult').innerHTML = longs.length? fkRenderHistTable(longs) : '<div class="fk-scan-empty hist">该区间无多头信号</div>';
  document.getElementById('fkShortResult').innerHTML = shorts.length? fkRenderHistTable(shorts) : '<div class="fk-scan-empty hist">该区间无空头信号</div>';
}
function fkRenderHistTable(list){
  var html='<table class="fk-scan-table"><thead><tr>'
    +'<th class="col-num">序号</th><th class="col-period">周期</th><th class="col-sym">合约</th>'
    +'<th class="col-time">触发时间</th><th class="col-price">最新价</th>'
    +'<th class="col-change">涨跌幅</th><th class="col-ma">MA60/20</th><th class="col-dir">方向</th><th class="col-signals">信号</th></tr></thead><tbody>';
  list.forEach(function(s,idx){
    var side=s.side||'long';
    var clsDir=side==='short'?'down':'up';
    var sym=s.symbol||'';
    var disp=s.display_name||s.real_symbol||sym;
    var tt=s.trigger_time||'';
    // 触发时间仅显示 时分 (HH:MM)
    var timeHtml=tt.length>11?tt.slice(11):(tt||'—');
    var closeVal=s.close, change=s.change, changePct=s.change_pct;
    var priceHtml=(closeVal!=null&&!isNaN(closeVal))?Number(closeVal).toFixed(2):'—';
    var changeHtml='—', pctHtml='—';
    if(change!=null&&!isNaN(change)){ var sg=change>0?'+':''; changeHtml=sg+Number(change).toFixed(2); }
    if(changePct!=null&&!isNaN(changePct)){ var sg=changePct>0?'+':''; pctHtml=sg+Number(changePct).toFixed(2)+'%'; }
    // [2026-10-12] 合约列: 平时只显示名称/主连码; 悬停浮现代码提示层(与实时监控列表一致)
    var shortCode=fkShortCode(s.real_symbol||'');
    var cellAttr=shortCode?(' data-code="'+fkHtml(shortCode)+'" data-main="'+fkHtml(sym)+'"'):'';
    html+='<tr><td class="col-num">'+(idx+1)+'</td>'
      +'<td class="col-period">'+(FK_PERIOD_LABELS[s.period||'day']||s.period||'')+'</td>'
      +'<td class="col-sym"'+cellAttr+' onclick="fkOpenSym(\''+encodeURIComponent(sym)+'\',\''+(s.period||'day')+'\')">'+fkHtml(disp)+'<span class="sub">'+fkHtml(sym)+'</span></td>'
      +'<td class="col-time">'+timeHtml+'</td>'
      +'<td class="col-price">'+priceHtml+'</td>'
      +'<td class="col-change '+clsDir+'"><div class="pct">'+pctHtml+'</div><div class="pts">('+changeHtml+')</div></td>'
      +'<td class="col-ma">'+fkMaTags(closeVal, s.ma60, s.ma20)+'</td>'
      +'<td class="col-dir '+side+'">'+(side==='short'?'空头':'多头')+'</td>'
      +'<td class="col-signals"><span class="fk-scan-tag '+side+'">'+fkHtml(s.name||s.type||s.rule_name||s.signal_type||'')+'</span></td>'
      +'</tr>';
  });
  return html+'</tbody></table>';
}
// ===== 后台自动监控: 实时 tab 的数据源（不用手点，交易时段后端自动跑）=====
function fkUpdateAutoStatus(){
  var a=FK_SCAN.auto;
  var el=document.getElementById('fkScanStatus');
  var btn=document.getElementById('fkScanBtn');
  if(!a||!el) return;
  var running=(a.periods||[]).map(function(p){return FK_PERIOD_LABELS[p]||p;});
  var head;
  if(a.no_valid_account){ head='⚠ 系统无未到期的天勤账号，无法获取监控数据（请在账户管理中续费或添加有效账号）'; }
  else if(a.backfill&&a.backfill.running){ head='正在补扫启动后的今日信号… '+(a.backfill.progress||''); }
  else if(a.paused){ head='已暂停自动监控（点右侧按钮可恢复）'; }
  else if(a.trading_now){ head='后台自动监控中'+(running.length?(' · '+running.join('/')):' · 正在启动…'); }
  else { head='非交易时段（自动监控收盘后停止，仍展示当天信号）'; }
  if(a.trading_day) head+=' · 交易日 '+a.trading_day;
  if(a.symbols_used) head+=' · 覆盖 '+a.symbols_used+'/'+(a.symbols_total||a.symbols_used)+' 主连 × '+(a.auto_periods||[]).length+' 周期';
  if(a.pool_source) head+=' · 监控池: '+a.pool_source;
  if(a.pool_cut) head+=' · 已按优先级截断 '+a.pool_cut+' 只(会话 K 线预算上限)';
  if(a.budget_cap) head+=' · 会话 K 线对象 '+(a.budget||0)+'/'+a.budget_cap;
  el.textContent=head;
  if(btn && FK_SCAN.mode==='realtime'){
    btn.textContent = a.paused ? '恢复自动监控' : '暂停自动监控';
    if(a.paused){ btn.classList.remove('running'); } else { btn.classList.add('running'); }
  }
}
function fkToggleAuto(){
  var a=FK_SCAN.auto||{};
  var q=a.paused?'0':'1';
  var el=document.getElementById('fkScanStatus');
  if(el) el.textContent=(a.paused?'正在恢复自动监控…':'正在暂停自动监控…');
  fetch(fkApiBase()+'api/scan/auto?pause='+q,{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    FK_SCAN.auto=d.auto||null; fkUpdateAutoStatus();
  }).catch(function(e){ if(el) el.textContent='操作异常:'+e; });
}
function fkPollToday(){
  if(FK_SCAN.mode!=='realtime'){ return; }
  fetch(fkApiBase()+'api/scan/multi/today').then(function(r){return r.json();}).then(function(d){
    if(!d.ok) return;
    if(d.auto) FK_SCAN.auto=d.auto;
    var first=!FK_SCAN.alertArmed;   // 首轮只渲染不报警: 避免一开页面把当天历史信号全弹一遍
    FK_SCAN.agg={long:[],short:[],scanned:0,total:0};
    var results=d.results||{};
    Object.keys(results).forEach(function(p){
      var r=results[p]; if(r&&r.ok) fkMergeResult(r,p,first);
    });
    fkRenderAgg();
    fkUpdateAutoStatus();
    if(first) FK_SCAN.alertArmed=true;
  }).catch(function(e){});
}
// ===== 历史回溯: 直接读库里已落库信号 =====
function fkHistLoad(){
  var st=document.getElementById('fkHistStart'), en=document.getElementById('fkHistEnd');
  var pd=document.getElementById('fkHistPeriod'), sd=document.getElementById('fkHistSide');
  var el=document.getElementById('fkScanStatus');
  var qs='limit=500&offset=0';
  if(st&&st.value) qs+='&start='+encodeURIComponent(st.value+' 00:00');
  if(en&&en.value) qs+='&end='+encodeURIComponent(en.value+' 23:59');
  if(pd&&pd.value) qs+='&period='+encodeURIComponent(pd.value);
  if(sd&&sd.value) qs+='&side='+encodeURIComponent(sd.value);
  if(el) el.textContent='查询历史信号…';
  fetch(fkApiBase()+'api/scan/history?'+qs).then(function(r){return r.json();}).then(function(d){
    if(!d.ok){ if(el) el.textContent='查询失败:'+(d.error||''); return; }
    var rows=d.rows||[];
    // [2026-10-15] 工具栏周期多选: 历史回溯同样只显示勾选周期的数据(未指定单周期时生效)
    var tb=fkToolbarSelectedPeriods();
    if(tb && tb.length && !(pd&&pd.value)){
      rows=rows.filter(function(r){ return tb.indexOf(r.period)>=0; });
    }
    var longs=rows.filter(function(r){return r.side==='long';});
    var shorts=rows.filter(function(r){return r.side==='short';});
    document.getElementById('fkTotalCount').textContent=longs.length+shorts.length;
    document.getElementById('fkHeadLong').textContent=longs.length;
    document.getElementById('fkHeadShort').textContent=shorts.length;
    document.getElementById('fkLongResult').innerHTML = longs.length? fkRenderHistTable(longs) : '<div class="fk-scan-empty hist">该条件下无多头历史信号</div>';
    document.getElementById('fkShortResult').innerHTML = shorts.length? fkRenderHistTable(shorts) : '<div class="fk-scan-empty hist">该条件下无空头历史信号</div>';
    if(el) el.textContent='历史信号 共'+(longs.length+shorts.length)+'条（本页 '+rows.length+'，多 '+longs.length+' / 空 '+shorts.length+'）';
  }).catch(function(e){ if(el) el.textContent='查询异常:'+e; });
}
// 多周期监控: 后端每条线程一个周期持续扫描并异步入库, 前端每 2s 轮询渲染(不阻塞、不影响扫描速度)
function fkPollMonitor(){
  if(!FK_SCAN.running) return;
  var base=fkApiBase();
  Promise.all([
    fetch(base+'api/scan/multi/latest').then(function(r){return r.json();}),
    fetch(base+'api/scan/multi/status').then(function(r){return r.json();})
  ]).then(function(arr){
    var latest=arr[0], status=arr[1];
    FK_SCAN.agg={long:[],short:[],scanned:0,total:0};
    var anyNoLive=false, anyLive=false;
    // 当前盘次提示(日盘/夜盘/休市); 盘结束后实时面板清空, 数据到历史回溯查看
    var sess=(latest&&latest.session)?latest.session:null;
    var sessTxt='';
    if(sess){ sessTxt = sess.id ? (' · 本盘:'+sess.label) : ' · 休市(本盘已清理, 数据见历史回溯)'; }
    if(latest && latest.ok){
      var results=latest.results||{};
      Object.keys(results).forEach(function(p){
        var d=results[p]; if(d&&d.ok){ fkMergeResult(d,p); if(d.live_only && !d.live_data) anyNoLive=true; if(d.live_data) anyLive=true; }
      });
    }
    fkRenderAgg();
    var st=status&&status.status?status.status:{};
    var parts=[];
    Object.keys(st).forEach(function(p){
      var s=st[p];
      parts.push((FK_PERIOD_LABELS[p]||p)+':'+(s.last_scan?('✓'+s.last_count):'…')+(s.last_error?'⚠':''));
    });
    if(anyNoLive && !anyLive){
      document.getElementById('fkScanStatus').textContent='休市/无实时行情（实时模式仅监控当前 bar，不扫描历史）'+sessTxt;
    } else {
      document.getElementById('fkScanStatus').textContent='监控中 '+Object.keys(st).length+' 周期 | '+parts.join('  ')+sessTxt;
    }
  }).catch(function(e){ /* 单次轮询异常忽略, 下次继续 */ });
  if(FK_SCAN.running) FK_SCAN.timer=setTimeout(fkPollMonitor, 2000);
}
function fkCollectRules(){
  var rules=[];
  FK_SCAN.rules.forEach(function(r,i){
    var cb=document.getElementById('fkRule_'+i);
    var rule=JSON.parse(JSON.stringify(r)); rule.enabled=cb&&cb.checked;
    var row=cb?cb.parentElement:null;
    if(row){
      if(!rule.indicator_params) rule.indicator_params={};
      var inputs=row.querySelectorAll('input[data-key], select[data-key]');
      inputs.forEach(function(inp){
        var k=inp.getAttribute('data-key'), v=inp.value;
        if(k.indexOf('indicator_')===0){
          var ik=k.substring(10);
          rule.indicator_params[ik]= (v==='')?null:(v.indexOf('.')>=0?parseFloat(v):parseInt(v,10));
        } else {
          if(inp.tagName==='SELECT'){ rule.params[k]=v; }
          else if(v===''){ rule.params[k]=null; }
          else if(v.indexOf('.')>=0) rule.params[k]=parseFloat(v); else rule.params[k]=parseInt(v,10);
        }
      });
    }
    rules.push(rule);
  });
  return rules;
}
function fkRunBatch(){
  if(!FK_SCAN.running) return;
  if(FK_SCAN.scanning){ FK_SCAN.timer=setTimeout(fkRunBatch, FK_SCAN.interval); return; }
  FK_SCAN.scanning=true;
  var periods=fkSelectedPeriods();
  if(!periods.length){ fkStopScan(); return; }
  document.getElementById('fkScanStatus').textContent='扫描中…('+periods.length+'个周期)';
  var rules=fkCollectRules();
  var base=fkApiBase();
  var promises=periods.map(function(p){
    return fetch(base+'api/scan?period='+encodeURIComponent(p),{
      method:'POST',
      headers:{'Content-Type':'application/x-www-form-urlencoded'},
      body:'rules='+encodeURIComponent(JSON.stringify(rules))
    }).then(function(r){return r.json();}).then(function(d){ if(d&&d.ok) fkMergeResult(d,p); return d; })
      .catch(function(e){return null;});
  });
  Promise.all(promises).then(function(results){
    FK_SCAN.scanning=false;
    var agg={scanned:0,total:0};
    results.filter(Boolean).forEach(function(d){ if(d.stats){agg.scanned+=(d.stats.scanned||0);agg.total+=(d.stats.total||0);} });
    document.getElementById('fkScanStatus').textContent='上次:'+new Date().toLocaleTimeString()+' 共'+(FK_SCAN.agg.long.length+FK_SCAN.agg.short.length)+' 多'+FK_SCAN.agg.long.length+' 空'+FK_SCAN.agg.short.length+' ('+periods.join('/')+')';
    fkRenderAgg();
    if(FK_SCAN.running) FK_SCAN.timer=setTimeout(fkRunBatch, FK_SCAN.interval);
  });
}
function fkMergeResult(d,period,silent){
  (d.long||[]).forEach(function(it){ it.period=period; FK_SCAN.agg.long.push(it); if(!silent) fkCheckAlertsItem(it,'long',period); });
  (d.short||[]).forEach(function(it){ it.period=period; FK_SCAN.agg.short.push(it); if(!silent) fkCheckAlertsItem(it,'short',period); });
}
// ===== [2026-10-09] 模板视图过滤 + 非当前模板数据提示 =====
function fkCurTpl(){
  var id=FK_SCAN.curTemplateId;
  if(id==null||id==='') return null;
  var ts=FK_SCAN.userTemplates||[];
  for(var i=0;i<ts.length;i++){ if(String(ts[i].id)===String(id)) return ts[i]; }
  return null;
}
function fkRenderTplSelect(){
  var sel=document.getElementById('fkTplSelect');
  if(!sel) return;
  var ts=FK_SCAN.userTemplates||[];
  var cur=FK_SCAN.curTemplateId||'';
  var html='<option value="">全部模板(并集)</option>';
  ts.forEach(function(t){ html+='<option value="'+t.id+'">'+fkHtml(t.name)+'</option>'; });
  sel.innerHTML=html;
  sel.value=cur;
}
function fkOnTplSelect(sel){
  var v = sel ? sel.value : '';
  FK_SCAN.curTemplateId = v;
  try{ localStorage.setItem('fkCurTpl', v); }catch(e){}
  if(FK_SCAN.mode==='history'){ fkHistLoad(); }
  else { fkRenderAgg(); }
}
function fkSelectTpl(id){
  var sel=document.getElementById('fkTplSelect');
  if(sel) sel.value=String(id);
  fkOnTplSelect(sel||{value:String(id)});
}
function fkTplSymMatch(sym, tpl){
  if(!tpl || !tpl.products || !tpl.products.length) return true;
  var norm=(sym||'').split('@').pop();
  for(var i=0;i<tpl.products.length;i++){
    var p=tpl.products[i];
    if(p===sym || p===norm || (sym&&sym.indexOf(p)>=0)) return true;
  }
  return false;
}
function fkEffectivePeriods(tpl){
  // [2026-10-15 用户需求] 工具栏周期勾选优先级最高: 用户手动勾了哪些周期, 表格就只显示哪些。
  // 未做任何勾选(全不勾)时回退原口径(模板周期 -> 页面勾选框), 避免误操作导致空屏。
  var tb=fkToolbarSelectedPeriods();
  if(tb && tb.length) return tb;
  if(tpl && tpl.periods && tpl.periods.length) return tpl.periods;
  return fkSelectedPeriods();   // 全部模板模式: 沿用页面周期勾选框
}
function fkTplItemMatch(it, tpl, effPeriods){
  if(!fkTplSymMatch(it.symbol, tpl)) return false;
  if(effPeriods && effPeriods.length && effPeriods.indexOf(it.period)<0) return false;
  return true;
}
function fkTplSigMatch(item, sig, tpl){
  if(!tpl) return true;
  if(tpl.rules && tpl.rules.length){
    var rids={};
    for(var i=0;i<tpl.rules.length;i++){ var r=tpl.rules[i]; if(r&&r.id!=null) rids[r.id]=1; }
    if(!rids[sig.rule_id]) return false;
  }
  return true;
}
var FK_TPL_TOAST_COOLDOWN=30000;
function fkTplCheckOthers(agg){
  var cur=fkCurTpl();
  if(!cur) return;   // 全部模板模式不提示
  var others=(FK_SCAN.userTemplates||[]).filter(function(t){ return String(t.id)!==String(cur.id); });
  var now=Date.now();
  others.forEach(function(t){
    if(!t.products || !t.products.length) return;   // 无品种范围的模板不提示
    var cnt=0;
    (agg.long||[]).concat(agg.short||[]).forEach(function(it){
      if(fkTplSymMatch(it.symbol,t) && (!t.periods||!t.periods.length || t.periods.indexOf(it.period)>=0)) cnt++;
    });
    if(cnt>0){
      var last=FK_SCAN._tplToastLast[t.id]||0;
      if(now-last > FK_TPL_TOAST_COOLDOWN){
        FK_SCAN._tplToastLast[t.id]=now;
        fkTplToastShow(t, cnt);
      }
    }
  });
}
function fkTplToastShow(t, cnt){
  var wrap=document.getElementById('fkTplToastWrap');
  if(!wrap){ wrap=document.createElement('div'); wrap.id='fkTplToastWrap'; wrap.className='fk-tpl-toast-wrap'; (document.body||document.documentElement).appendChild(wrap); }
  var el=document.createElement('div');
  el.className='fk-tpl-toast';
  el.innerHTML='<span class="t-ico">🔔</span><span class="t-msg">模板 <b>'+fkHtml(t.name)+'</b> 有 '+cnt+' 条信号（点击切换查看）</span>';
  el.onclick=function(){ fkSelectTpl(t.id); if(el.parentNode) el.parentNode.removeChild(el); };
  wrap.appendChild(el);
  setTimeout(function(){ if(el.parentNode) el.parentNode.removeChild(el); }, 8000);
}
function fkRenderAgg(){
  // [2026-10-09] 模板视图过滤: 当前模板决定品种/周期口径(规则在 fkFlattenByTime 层过滤)
  var tpl=fkCurTpl();
  var effPeriods=fkEffectivePeriods(tpl);
  var longs=[], shorts=[];
  (FK_SCAN.agg.long||[]).forEach(function(it){ if(fkTplItemMatch(it,tpl,effPeriods)) longs.push(it); });
  (FK_SCAN.agg.short||[]).forEach(function(it){ if(fkTplItemMatch(it,tpl,effPeriods)) shorts.push(it); });
  fkTplCheckOthers(FK_SCAN.agg);   // 非当前模板有数据 -> 左上角聚合提示
  var total=longs.length+shorts.length;
  document.getElementById('fkTotalCount').textContent=total;
  document.getElementById('fkHeadLong').textContent=longs.length;
  document.getElementById('fkHeadShort').textContent=shorts.length;
  document.getElementById('fkLongResult').innerHTML = longs.length? fkRenderTable(longs,'long') : '<div class="fk-scan-empty">暂无多头信号</div>';
  document.getElementById('fkShortResult').innerHTML = shorts.length? fkRenderTable(shorts,'short') : '<div class="fk-scan-empty">暂无空头信号</div>';
}
function fkRenderScanResult(d){
  var longs=d.long||[], shorts=d.short||[], total=longs.length+shorts.length;
  document.getElementById('fkTotalCount').textContent=total;
  document.getElementById('fkHeadLong').textContent=longs.length;
  document.getElementById('fkHeadShort').textContent=shorts.length;
  longs.sort(function(a,b){ return fkFirstTime(b)-fkFirstTime(a); });
  shorts.sort(function(a,b){ return fkFirstTime(b)-fkFirstTime(a); });
  document.getElementById('fkLongResult').innerHTML = longs.length? fkRenderTable(longs,'long') : '<div class="fk-scan-empty">暂无多头信号</div>';
  document.getElementById('fkShortResult').innerHTML = shorts.length? fkRenderTable(shorts,'short') : '<div class="fk-scan-empty">暂无空头信号</div>';
}
function fkFirstTime(item){
  var t=(item.signals&&item.signals[0]&&item.signals[0].trigger_time)||item.trigger_time||'';
  return String(t).replace(/[^0-9]/g,'')||'0';
}
function fkFlattenByTime(list){
  /* 2026-09-24 15:28 用户裁定: 一条 = 一个品种 x 一个触发时间 x 一个状态(信号),
     完全不合并 —— 同一时间点有几个信号就几行, 每行只显示一个信号;
     全局按触发时间倒序(近的在上), 同一时刻按品种名稳定排序。
     [2026-10-09] 选中模板时, 仅保留该模板规则命中的信号(三要素全用, 规则维度在此层过滤)。 */
  var tpl=fkCurTpl();
  var rows=[];
  (list||[]).forEach(function(item){
    var sigs=(item.signals||[]);
    if(tpl){ sigs = sigs.filter(function(s){ return fkTplSigMatch(item, s, tpl); }); }
    sigs.forEach(function(s){
      rows.push({item:item, time:s.trigger_time||'', sig:s});
    });
  });
  rows.sort(function(a,b){
    var ka=String(a.time).replace(/[^0-9]/g,''), kb=String(b.time).replace(/[^0-9]/g,'');
    if(ka!==kb) return ka<kb?1:-1;
    var na=String(a.item.display_name||a.item.symbol||''), nb=String(b.item.display_name||b.item.symbol||'');
    if(na!==nb) return na<nb?-1:1;
    return (b.item.close||0)-(a.item.close||0);
  });
  return rows;
}
function fkRenderTable(list, side){
  var rows=fkFlattenByTime(list);
  var html='<table class="fk-scan-table"><thead><tr>'+
    '<th class="col-num">序号</th>'+
    '<th class="col-period">周期</th>'+
    '<th class="col-sym">合约</th>'+
    '<th class="col-time">触发时间</th>'+
    '<th class="col-price">最新价</th>'+
    '<th class="col-change">涨跌幅</th>'+
    '<th class="col-ma">MA60/20</th>'+
    '<th class="col-signals">触发信号</th>'+
    '</tr></thead><tbody>';
  rows.forEach(function(row,idx){
    html+=fkRenderRow(row.item, idx+1, side, row.time, row.sig);
  });
  return html+'</tbody></table>';
}
/* [2026-10-16] MA60/MA20 标签(涨跌幅旁): 均线高于现价=绿↓, 低于现价=红↑,
   与参考端口径一致(已用参考图13行x2标签实测对账: MA高于现价->绿, 低于->红)。
   title 悬浮显示均线绝对值与距现价差值, 平时不占版面。 */
function fkMaTag(label, ma, close){
  if(ma==null||close==null||isNaN(ma)||isNaN(close)) return '';
  var diff=ma-close, cls='ma-f', arrow='';
  if(diff>0){ cls='ma-g'; arrow='↓'; }
  else if(diff<0){ cls='ma-r'; arrow='↑'; }
  var tip='MA'+label+': '+Number(ma).toFixed(2);
  tip += diff>0 ? ' (高于现价 '+diff.toFixed(2)+')' : (diff<0 ? ' (低于现价 '+(-diff).toFixed(2)+')' : ' (与现价持平)');
  return '<span class="fk-ma-tag '+cls+'" title="'+tip+'">'+label+arrow+'</span>';
}
function fkMaTags(close, ma60, ma20){
  return fkMaTag('60', ma60, close)+fkMaTag('20', ma20, close);
}
function fkRenderRow(item,idx,side,oneTime,oneSig){
  var side=side||item.side||'long';
  var sym=item.symbol;
  var clsDir=side==='short'?'down':'up';
  var change=item.change, changePct=item.change_pct;
  var changeHtml='—', pctHtml='—';
  if(change!=null && !isNaN(change)){
    var s=change>0?'+':'';
    changeHtml=s+Number(change).toFixed(2);
  }
  if(changePct!=null && !isNaN(changePct)){
    var s=changePct>0?'+':'';
    pctHtml=s+Number(changePct).toFixed(2)+'%';
  }
  // 2026-09-24 15:28 用户裁定: 一条信号一行(单个 tag); 「方向」列删除(多空本就分两个面板)
  var sg=oneSig||{};
  var ss=sg.side==='short'?'short':(sg.side==='both'?'both':'long');
  var tag='<span class="fk-scan-tag '+ss+'">'+fkHtml(sg.name||sg.type||sg.rule_name||sg.signal_type||'')+'</span>';
  var t=oneTime||'';
  var timeHtml='—';
  if(t){
    if(t.length>11){
      var hm=t.slice(11);
      // 日线/周线/月线 bar 的天勤时间戳=交易日零点(2026-09-24 15:55 独立会话实铁),
      // 显示 00:00 没有信息量 -> 零点时分改显示日期 MM-DD; 分钟级信号照旧显示 HH:MM
      timeHtml = (hm.replace(':','')==='0000') ? t.slice(5,10) : hm;
    } else { timeHtml = t; }
  }
  // [2026-10-12] 合约列: 平时只显示名称/主连码; 悬停浮现代码提示层(交易所码可复制/主连码), 移开消失
  var shortCode=fkShortCode(item.real_symbol||'');
  var cellAttr=shortCode?(' data-code="'+fkHtml(shortCode)+'" data-main="'+fkHtml(sym)+'"'):'';
  return '<tr>'+
    '<td class="col-num">'+idx+'</td>'+
    '<td class="col-period">'+FK_PERIOD_LABELS[item.period||'day']+'</td>'+
    '<td class="col-sym"'+cellAttr+' onclick="fkOpenSym(\''+encodeURIComponent(sym)+'\',\''+(item.period||'day')+'\')">'+fkHtml(item.display_name||item.real_symbol||sym)+'<span class="sub">'+fkHtml(sym)+'</span></td>'+
    '<td class="col-time">'+timeHtml+'</td>'+
    '<td class="col-price">'+(item.close!=null?Number(item.close).toFixed(2):'—')+'</td>'+
    '<td class="col-change '+clsDir+'"><div class="pct">'+pctHtml+'</div><div class="pts">('+changeHtml+')</div></td>'+
    '<td class="col-ma">'+fkMaTags(item.close, item.ma60, item.ma20)+'</td>'+
    '<td class="col-signals">'+tag+'</td>'+
    '</tr>';
}
function fkCheckAlertsItem(item,side,period){
  if(!fkPeriodVisible(period)) return;   // [2026-09-28] 未勾选周期: 不推送/不响铃(后端仍全量抓取)
  (item.signals||[]).forEach(function(sig){
    if(side!=='both' && sig.side && sig.side!==side)return;
    // 用触发时间(或 idx 兜底)作为去重键: idx 会随 K 线窗口滚动而变, trigger_time 稳定
    var key=period+'|'+item.symbol+'|'+sig.type+'|'+(sig.trigger_time||sig.idx||'');
    if(FK_SCAN.lastAlerts[key]) return;
    FK_SCAN.lastAlerts[key]=Date.now();
    fkScanAlert(item,sig,side,period);
  });
  var now=Date.now();
  for(var k in FK_SCAN.lastAlerts){ if(now-FK_SCAN.lastAlerts[k]>3600000) delete FK_SCAN.lastAlerts[k]; }
}
function fkScanAlert(item,sig,side,period){
  var c=side==='short'?'#19c37d':'#ef4444';
  var pl=FK_PERIOD_LABELS[period]||period||'';
  var rec={
    ts:Date.now(),
    symbol:item.symbol||'',
    realSymbol:item.real_symbol||'',
    display:item.display_name||item.real_symbol||item.symbol||'',
    period:period||'', periodLabel:pl,
    side:side||'', sigName:sig.name||'', desc:sig.desc||'',
    trigger:sig.trigger_time||'', value:(sig.value!=null?sig.value:null)
  };
  fkNotifyPush(rec);

  // 预警不再以独立 toast 框弹出(避免左上角堆叠挡住图表), 统一折叠进通知中心徽标:
  // 徽标默认收起只显示 "N 条信号通知", 点击展开列表; 声音提示保留。
  fkScanPlaySound();
}

/* ===== 通知中心: 折叠存储 + 按时段分组展开 + 点击跳转K线 ===== */
function fkNotifyPush(rec){
  if(!FK_SCAN.alerts) FK_SCAN.alerts=[];
  FK_SCAN.alerts.unshift(rec);                       // 最新在前
  if(FK_SCAN.alerts.length>(FK_SCAN.notifyCap||200)) FK_SCAN.alerts.length=FK_SCAN.notifyCap;
  fkNotifyRender();
}
function fkNotifyBucket(ts){
  var d=new Date(ts);
  var hh=('0'+d.getHours()).slice(-2), mm=('0'+d.getMinutes()).slice(-2);
  return hh+':'+mm;                                  // 按"分钟"作为时段分组
}
function fkNotifyRender(){
  var nc=document.getElementById('fkNotifyCenter');
  var cnt=document.getElementById('fkNotifyCount');
  var list=document.getElementById('fkNotifyList');
  if(!nc||!cnt||!list) return;
  // [2026-09-28] 通知中心同样按勾选周期过滤(未勾选周期不进徽标计数/列表)
  var arr=(FK_SCAN.alerts||[]).filter(function(a){return fkPeriodVisible(a.period);});
  cnt.textContent=String(arr.length);
  nc.style.display=arr.length?'flex':'none';
  if(!arr.length){ list.style.display='none'; FK_SCAN.notifyOpen=false; return; }
  if(!FK_SCAN.notifyOpen){ list.style.display='none'; return; }

  // 按时段(分钟)分组
  var groups=[], map={};
  for(var i=0;i<arr.length;i++){
    var b=fkNotifyBucket(arr[i].ts);
    if(!map[b]){ map[b]=[]; groups.push(b); }
    map[b].push(arr[i]);
  }
  var h='';
  for(var g=0;g<groups.length;g++){
    var bk=groups[g], items=map[bk];
    h+='<div class="fk-notify-group-title"><span>时段 '+bk+'</span><span>'+items.length+' 条</span></div>';
    for(var j=0;j<items.length;j++){
      var it=items[j];
      var cls=(it.side==='short')?'short':'long';
      var dir=(it.side==='short')?'空':'多';
      h+='<div class="fk-notify-item '+cls+'" data-sym="'+fkHtml(it.symbol||it.realSymbol)+'" data-p="'+fkHtml(it.period)+'" onclick="fkGotoSymbol(this.getAttribute(\'data-sym\'),this.getAttribute(\'data-p\'))">'
        +'<div class="fk-ni-head"><span class="fk-ni-name">'+dir+' '+fkHtml(it.sigName)+'</span>'
        +'<span style="color:var(--muted)">'+(it.periodLabel?fkHtml(it.periodLabel)+' · ':'')+fkHtml(it.display)+'</span></div>'
        +'<div class="fk-ni-desc">'+(it.trigger?(it.trigger.length>11?it.trigger.slice(11):it.trigger):'')+'</div>'
        +'</div>';
    }
  }
  h+='<div class="fk-notify-foot"><button onclick="fkNotifyClear(event)">清空全部</button>'
    +'<button onclick="fkNotifyToggle(event)">收起</button></div>';
  list.innerHTML=h;
  list.style.display='block';
}
function fkNotifyToggle(e){
  if(e){ e.stopPropagation(); }
  FK_SCAN.notifyOpen=!FK_SCAN.notifyOpen;
  var ar=document.getElementById('fkNotifyArrow');
  if(ar) ar.innerHTML=FK_SCAN.notifyOpen?'&#9652;':'&#9662;';
  fkNotifyRender();
}
function fkNotifyClose(){
  FK_SCAN.notifyOpen=false;
  var ar=document.getElementById('fkNotifyArrow');
  if(ar) ar.innerHTML='&#9662;';
  var list=document.getElementById('fkNotifyList');
  if(list) list.style.display='none';
}
function fkNotifyClear(e){
  if(e){ e.stopPropagation(); }
  FK_SCAN.alerts=[];
  fkNotifyClose();
  fkNotifyRender();
}
// 合约名点击: 独立监控页有 fkMonPopup 则弹K线窗, 详情/列表页保持整页跳转
function fkOpenSym(sym,period){
  if(typeof fkMonPopup==='function'){ try{ fkMonPopup(sym,period); }catch(err){} return; }
  var u='?symbol='+encodeURIComponent(sym);
  if(period) u+='&period='+encodeURIComponent(period);
  location.href=u;
}
// 跳转到该合约对应周期的K线: 详情页站内异步切换(保留监控运行), 列表页整页跳转
function fkGotoSymbol(symbol,period){
  if(!symbol) return;
  try{
    if(typeof FK_SYMBOL!=='undefined' && typeof fkLoad==='function'){
      FK_SYMBOL=symbol;
      if(period) FK_PERIOD=period;
      FK_EARLIEST=false; FK_EARLIEST_DT=null; FK_LOADED_COUNT=0; FK_ZOOM={start:0,end:100};
      fkLoad(FK_PERIOD);
      var t=document.querySelector('.page-title');
      if(t) t.innerHTML=fkHtml(symbol)+' <span style="font-size:13px;color:var(--muted)">'+fkHtml(symbol)+'</span>';
      fkNotifyClose();
      return;
    }
  }catch(err){}
  var u='?symbol='+encodeURIComponent(symbol);
  if(period) u+='&period='+encodeURIComponent(period);
  location.href=u;
}
// 点击通知中心以外的区域时收起列表
document.addEventListener('click',function(e){
  var nc=document.getElementById('fkNotifyCenter');
  if(!nc||!FK_SCAN.notifyOpen) return;
  if(nc.contains(e.target)) return;
  fkNotifyClose();
});
function fkScanPlaySound(){
  try{
    if(!FK_SCAN.audioCtx)FK_SCAN.audioCtx=new (window.AudioContext||window.webkitAudioContext)();
    var ctx=FK_SCAN.audioCtx, osc=ctx.createOscillator(), gain=ctx.createGain();
    osc.type='sine'; osc.frequency.setValueAtTime(880,ctx.currentTime);
    osc.frequency.exponentialRampToValueAtTime(440,ctx.currentTime+0.15);
    gain.gain.setValueAtTime(0.3,ctx.currentTime); gain.gain.exponentialRampToValueAtTime(0.01,ctx.currentTime+0.25);
    osc.connect(gain); gain.connect(ctx.destination); osc.start(); osc.stop(ctx.currentTime+0.25);
  }catch(e){}
}
function fkRefreshTemplateList(){
  fetch(fkApiBase()+'api/scan/templates').then(function(r){return r.json();}).then(function(d){
    if(!d.ok) return;
    FK_SCAN.userTemplates = d.user_templates||[];
    var box=document.getElementById('fkTplChips');
    if(!box) return;
    // 首次进入默认全选(若用户已手动调整过则保留)
    if(!FK_SCAN.selectedTemplateIds || !FK_SCAN.selectedTemplateIds.length){
      FK_SCAN.selectedTemplateIds = FK_SCAN.userTemplates.map(function(t){return t.id;});
    }
    var html='';
    (FK_SCAN.userTemplates||[]).forEach(function(t){
      var on=(FK_SCAN.selectedTemplateIds.indexOf(t.id)>=0)?' active':'';
      html+='<span class="fk-tpl-chip'+on+'" data-id="'+t.id+'" onclick="fkOnTemplateChange('+t.id+')" title="点击切换是否纳入监控(默认全选)">'+fkHtml(t.name)+'</span>';
    });
    if(!html) html='<span class="fk-tpl-hint">暂无模板，点右上「⚙ 设置」新建</span>';
    box.innerHTML=html;
  }).catch(function(e){});
}
function fkOnTemplateChange(tid){
  // 多选切换: 点击在「纳入 / 剔除」间切换(2026-09-23 用户诉求: 模版默认全选、可手动增减)
  tid=parseInt(tid);
  var i=FK_SCAN.selectedTemplateIds.indexOf(tid);
  if(i>=0) FK_SCAN.selectedTemplateIds.splice(i,1);
  else FK_SCAN.selectedTemplateIds.push(tid);
  var box=document.getElementById('fkTplChips');
  if(box){
    box.querySelectorAll('.fk-tpl-chip').forEach(function(c){
      var id=parseInt(c.getAttribute('data-id'));
      c.classList.toggle('active', FK_SCAN.selectedTemplateIds.indexOf(id)>=0);
    });
  }
  var sel=FK_SCAN.userTemplates.filter(function(t){return FK_SCAN.selectedTemplateIds.indexOf(t.id)>=0;});
  fkApplySelectedTemplates(sel);
}
function _fkRuleKey(r,prefix){
  // 有 id 的规则按 id 归并(同 id 模板覆盖默认); 无 id 的自定义规则各自独立(加前缀防撞)
  if(r && r.id!=null) return String(r.id);
  return prefix+'|name|'+(r&&r.name!=null?r.name:(''+Math.random()));
}
function fkApplySelectedTemplates(templates){
  // 条件并集(2026-09-23 用户诉求: 勾选的条件全抓取):
  //   默认规则 + 可选预设 打底; 所选模板中 enabled(勾选)的规则按 id 覆盖(模板参数优先);
  //   模板里未勾选(enabled=false)的规则【不纳入】。
  var ruleMap={};
  (FK_SCAN.defaultRules||[]).forEach(function(r){ ruleMap[_fkRuleKey(r,'d')]=JSON.parse(JSON.stringify(r)); });
  (FK_SCAN.optionalRules||[]).forEach(function(r){ var k=_fkRuleKey(r,'o'); if(!(k in ruleMap)) ruleMap[k]=JSON.parse(JSON.stringify(r)); });
  (templates||[]).forEach(function(t){
    (t.rules||[]).forEach(function(r){
      if(r.enabled===false) return;            // 模板没勾选 -> 不抓取
      ruleMap[_fkRuleKey(r,'t')]=JSON.parse(JSON.stringify(r));
    });
  });
  FK_SCAN.rules=Object.keys(ruleMap).map(function(k){return ruleMap[k];});
  // 品种并集: 任一所选模板 products 为空(未勾品种) -> 全市场([])
  var anyEmpty=false, ps={};
  (templates||[]).forEach(function(t){
    var arr=t.products||[];
    if(!arr.length) anyEmpty=true;
    arr.forEach(function(p){ ps[p]=true; });
  });
  FK_SCAN.activeProducts = anyEmpty? [] : Object.keys(ps);
  fkRenderScanRules();
  var ph=document.getElementById('fkScanStatus');
  if(ph) ph.textContent='已套用模板('+(templates||[]).length+' 个, 默认全选): 条件 '+FK_SCAN.rules.length+' 项 · 品种 '+(FK_SCAN.activeProducts.length?FK_SCAN.activeProducts.length+' 只':'全市场');
}
// (已移除旧的 fkApplyTemplate / fkOnTemplateChange 单选逻辑; 改为 fkApplySelectedTemplates 多选并集)
function fkSaveTemplate(){
  var name=prompt('请输入模板名称（重复名称会新建）:');
  if(!name) return;
  var periods=fkSelectedPeriods();
  var rules=fkCollectRules();
  var products=FK_SCAN.activeProducts||[];
  var body='name='+encodeURIComponent(name)+'&periods='+encodeURIComponent(periods.join(','))+'&rules='+encodeURIComponent(JSON.stringify(rules));
  if(products.length) body+='&products='+encodeURIComponent(products.join(','));
  fetch(fkApiBase()+'api/scan/template/save',{
    method:'POST',
    headers:{'Content-Type':'application/x-www-form-urlencoded'},
    body:body
  }).then(function(r){return r.json();}).then(function(d){
    if(!d.ok){ document.getElementById('fkScanStatus').textContent='保存失败:'+(d.error||''); return; }
    fkRefreshTemplateList();
    document.getElementById('fkScanStatus').textContent='已保存模板: '+fkHtml(d.name||name);
  }).catch(function(e){ document.getElementById('fkScanStatus').textContent='保存异常:'+e; });
}
function fkDeleteTemplate(){
  var sel=document.getElementById('fkScanTemplate');
  var tid=sel.value;
  if(!tid){ alert('请先选择一个模板'); return; }
  if(!confirm('确定删除该模板吗？')) return;
  fetch(fkApiBase()+'api/scan/template/delete/'+encodeURIComponent(tid),{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    if(!d.ok){ document.getElementById('fkScanStatus').textContent='删除失败:'+(d.error||''); return; }
    sel.value='';
    fkRefreshTemplateList();
    document.getElementById('fkScanStatus').textContent='已删除模板';
  }).catch(function(e){ document.getElementById('fkScanStatus').textContent='删除异常:'+e; });
}
function fkToggleSched(){ var b=document.getElementById('fkSchedBody'); b.style.display=(b.style.display==='none')?'block':'none'; if(b.style.display==='block') fkLoadSched(); }
function fkLoadSched(){
  fetch(fkApiBase()+'api/scan/schedule/list').then(function(r){return r.json();}).then(function(d){
    if(!d.ok) return;
    document.getElementById('fkSchedNow').textContent='当前北京时间: '+(d.now||'')+(d.now?'（自动按窗口启停监控）':'');
    var tplSel=document.getElementById('fkSchedTpl');
    if(tplSel && tplSel.options.length<=1){
      fetch(fkApiBase()+'api/scan/templates').then(function(r){return r.json();}).then(function(td){
        (td.user_templates||[]).forEach(function(t){ var o=document.createElement('option'); o.value=t.id; o.textContent=t.name; tplSel.appendChild(o); });
      }).catch(function(e){});
    }
    var list=document.getElementById('fkSchedList');
    var html='';
    (d.schedules||[]).forEach(function(s){
      var cls=s.running?'on':'off';
      var days=fkDaysText(s.days_bit);
      html+='<div class="fk-sched-row '+cls+'"><div><div class="fk-sched-name">'+fkHtml(s.name)+(s.running?' <span style="color:#19c37d">●运行中</span>':'')+'</div>'+
        '<div class="fk-sched-meta">模板:'+(s.template_name||'默认')+' | '+s.start_hm+'–'+s.end_hm+' | 周'+days+'</div></div>'+
        '<button onclick="fkToggleSchedItem('+s.id+','+(s.enabled?0:1)+')">'+(s.enabled?'停':'启')+'</button>'+
        '<button onclick="fkDelSched('+s.id+')">删</button></div>';
    });
    if(!(d.schedules||[]).length) html='<div class="fk-sched-meta">暂无定时任务，点上方「+ 新增定时任务」</div>';
    list.innerHTML=html;
    document.getElementById('fkSchedCount').textContent=(d.schedules||[]).length?('('+(d.schedules||[]).length+')'):'';
  }).catch(function(e){});
}
function fkDaysText(bit){
  var names=['一','二','三','四','五','六','日']; var arr=[];
  for(var i=0;i<7;i++){ if(bit & (1<<i)) arr.push(names[i]); }
  return arr.join('');
}
function fkShowSchedForm(){ document.getElementById('fkSchedForm').style.display='flex'; }
function fkHideSchedForm(){ document.getElementById('fkSchedForm').style.display='none'; }
function fkSaveSched(){
  var name=document.getElementById('fkSchedName').value.trim();
  if(!name){ alert('请填写任务名称'); return; }
  var tpl=document.getElementById('fkSchedTpl').value;
  var start=document.getElementById('fkSchedStart').value;
  var end=document.getElementById('fkSchedEnd').value;
  if(!start||!end){ alert('请选择起止时间'); return; }
  var days=[]; document.querySelectorAll('.fk-sched-day:checked').forEach(function(c){ days.push(c.value); });
  var body='name='+encodeURIComponent(name)+'&template_id='+encodeURIComponent(tpl)+
    '&start_hm='+encodeURIComponent(start)+'&end_hm='+encodeURIComponent(end)+
    '&days='+encodeURIComponent(days.join(','))+'&enabled=1';
  fetch(fkApiBase()+'api/scan/schedule/save',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:body})
    .then(function(r){return r.json();}).then(function(d){
      if(!d.ok){ alert('保存失败:'+(d.error||'')); return; }
      fkHideSchedForm(); fkLoadSched();
    }).catch(function(e){ alert('保存异常:'+e); });
}
function fkToggleSchedItem(sid,en){
  fetch(fkApiBase()+'api/scan/schedule/toggle/'+sid+'?enabled='+en,{method:'POST'}).then(function(r){return r.json();}).then(function(d){ fkLoadSched(); }).catch(function(e){});
}
function fkDelSched(sid){
  if(!confirm('确定删除该定时任务吗？')) return;
  fetch(fkApiBase()+'api/scan/schedule/delete/'+sid,{method:'POST'}).then(function(r){return r.json();}).then(function(d){ fkLoadSched(); }).catch(function(e){});
}
fkInitScanPanel();
// 实时 tab 由后台自动监控驱动: 打开页面即轮询「当天信号」, 不需要点「开始监控」
setTimeout(function(){ fkPollToday(); }, 800);
FK_SCAN.timerToday=setInterval(fkPollToday, 3000);
</script>
'''




def scan_monitor_page():
    """全市场信号监控 - 独立整页：搬运期货K线页内悬浮监控面板，浮层改整页。"""
    root_css = ":root{--bg:#0b1120;--surface:#111827;--surface2:#1b2538;--border:#1e2d4a;--text:#e6edf7;--muted:#94a3b8;--accent:#38bdf8;--accent2:#f472b6;--violet:#c084fc;--up:#ef4444;--down:#19c37d;}"
    mon_css = (
        "<style>"
        "html,body{margin:0;padding:0;height:100%;background:var(--bg);color:var(--text);"
        "font-family:-apple-system,'Microsoft YaHei',sans-serif;overflow:hidden}"
        ".fk-mon-root{display:flex;flex-direction:column;height:100vh;box-sizing:border-box}"
        ".fk-mon-frame{flex:1;min-height:0;display:flex;flex-direction:column;padding:10px 12px 12px}"
        "#fkScanFloat.fk-monitor{position:static;right:auto;bottom:auto;z-index:auto;max-width:none;min-width:0;flex:1;min-height:0;display:flex;flex-direction:column}"
        "#fkScanFloat.fk-monitor .fk-scan-panel{position:relative;left:auto;right:auto;bottom:auto;height:auto!important;flex:1;min-height:0;display:flex;flex-direction:column;border-top:none;box-shadow:none}"
        "#fkScanFloat.fk-monitor.expanded .fk-scan-panel{height:auto}"
        "#fkScanFloat.fk-monitor .fk-scan-toggle{display:none}"
        "#fkScanFloat.fk-monitor .fk-scan-resizer{display:none}"
        "#fkScanFloat.fk-monitor .fk-scan-close{display:none}"
        "#fkScanFloat.fk-monitor .fk-fs-btn{display:none}"
        "#fkMonModal{position:fixed;inset:0;background:rgba(3,8,18,.74);z-index:9999;display:none;align-items:center;justify-content:center}"
        "#fkMonModal.on{display:flex}"
        ".fk-mon-dlg{width:92vw;height:92vh;background:var(--bg);border:1px solid var(--border);border-radius:10px;overflow:hidden;position:relative;display:flex;flex-direction:column;box-shadow:0 12px 48px rgba(0,0,0,.55)}"
        ".fk-mon-dlg-bar{display:flex;align-items:center;gap:10px;padding:8px 12px;background:var(--surface);border-bottom:1px solid var(--border);flex-shrink:0;font-size:13px;color:var(--text)}"
        ".fk-mon-dlg-bar .t{font-weight:700}"
        ".fk-mon-dlg-bar .x{margin-left:auto;cursor:pointer;color:var(--muted);font-size:18px;line-height:1;padding:0 4px}"
        ".fk-mon-dlg-bar .x:hover{color:var(--text)}"
        ".fk-mon-dlg iframe{flex:1;width:100%;border:0;background:var(--bg)}"
        "</style>"
    )
    # 复用 K 线页内悬浮监控面板 HTML，注入整页类（默认展开，屏蔽浮层/收起/拖拽手柄）
    panel = _fk_scan_panel_html().replace('<div id="fkScanFloat">', '<div id="fkScanFloat" class="expanded fk-monitor">', 1)
    doc = '<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><title>期货监控</title>'
    doc += '<style>' + root_css + '</style>'
    doc += _FK_STYLE
    doc += mon_css
    doc += '</head><body>'
    doc += '<div class="fk-mon-root">'
    doc += '<div class="fk-mon-frame">'
    doc += panel
    doc += '</div></div>'
    doc += ('<div id="fkMonModal"><div class="fk-mon-dlg">'
            '<div class="fk-mon-dlg-bar"><span class="t" id="fkMonTitle"></span>'
            '<span style="color:var(--muted)">期货K线</span>'
            '<span class="x" onclick="fkMonClose()" title="关闭 (Esc)">\u2715</span></div>'
            '<iframe id="fkMonIfr" src="about:blank"></iframe>'
            '</div></div>')
    # 2026-10-09 需求: 监控整页右下角加悬浮按钮「期货K线」-> 跳转原主连列表/K线页(?view=list)
    # [2026-10-09 用户裁定] 开【新标签页】跳转(target=_blank), 不在当前页把监控整页顶掉。
    # 同时保留 href 真实地址(便于中键/右键新窗口打开), onclick 仅做兜底拦截。
    doc += ('<a class="fk-goto-kline" id="fkGotoKline" target="_blank" '
            'href="javascript:void(0)" '
            'onclick="return fkGotoKline();" title="新标签页打开期货K线行情页">'
            '<span class="ico">📈</span><span class="txt">期货K线</span></a>')
    doc += ('<style>'
            # [2026-10-10 用户裁定] 默认收拢贴死右边缘(半胶囊, 右侧平贴边框), 移上去展开文字, 移开收回贴边。
            # 关键: 右边缘 right:0 全程固定不动(只改 border-radius/文字宽度), 避免 hover 时元素左移导致鼠标脱离->抖动。
            '.fk-goto-kline{position:fixed;right:0;bottom:18px;z-index:9998;display:flex;'
            'align-items:center;gap:0;padding:10px;border-radius:12px 0 0 12px;'
            'background:linear-gradient(135deg,#2563eb,#38bdf8);color:#fff;text-decoration:none;'
            'font-size:13px;font-weight:700;box-shadow:0 6px 20px rgba(37,99,235,.45);'
            'border:1px solid rgba(255,255,255,.18);border-right:none;'
            'transition:border-radius .22s ease,transform .15s,box-shadow .15s}'
            '.fk-goto-kline:hover{border-radius:26px;border-right:1px solid rgba(255,255,255,.18);'
            'transform:translateY(-2px);box-shadow:0 10px 26px rgba(37,99,235,.6)}'
            '.fk-goto-kline .txt{display:inline-block;max-width:0;opacity:0;margin-left:0;'
            'overflow:hidden;white-space:nowrap;transition:max-width .25s ease,opacity .2s ease,margin-left .25s ease}'
            '.fk-goto-kline:hover .txt{max-width:120px;opacity:1;margin-left:6px}'
            '.fk-goto-kline .ico{font-size:15px;line-height:1}'
            '</style>')
    doc += ('<script>'
            'function fkScanBase(){return location.pathname.replace(/scan-monitor.*$/,"");}'
            # [2026-10-09 用户裁定] 点「期货K线」= 开【新标签页】跳主连列表页(?view=list),
            # 不再当前页跳转(原来 location.href 会把监控整页顶掉, 用户原话"应该跳转到新的界面,
            # 不是在期货K线模板这里显示")。用 target=_blank 由浏览器新标签承载。
            'function fkGotoKline(){'
            'var w=window.open(fkScanBase()+"?view=list","_blank");'
            'if(!w){ location.href=fkScanBase()+"?view=list"; }/* 弹窗被拦 -> 同页兜底 */'
            'return false;}'
            'function fkMonPopup(sym,period){'
            'var m=document.getElementById("fkMonModal");if(!m)return;'
            'var base=fkScanBase();'
            'document.getElementById("fkMonIfr").src=base+"?symbol="+encodeURIComponent(sym||"")+"&period="+encodeURIComponent(period||"day");'
            'document.getElementById("fkMonTitle").textContent=sym||"";'
            'm.className="on";}'
            'function fkMonClose(){var m=document.getElementById("fkMonModal");if(!m)return;m.className="";'
            'document.getElementById("fkMonIfr").src="about:blank";}'
            'document.addEventListener("keydown",function(e){if(e.key==="Escape")fkMonClose();});'
            '</script>')
    doc += _fk_scan_script()
    doc += '</body></html>'
    return doc


# ==================== Flask 路由 ====================

def futures_kline_page():
    symbol = request.args.get("symbol", "").strip()
    if not symbol:
        # 2026-10-09 需求裁定: 打开期货K线默认落地【全市场信号监控整页】(原右下角"监控"
        # 按钮对应的界面); 原主连列表/K线行情退居 ?view=list, 由监控页悬浮按钮跳转进入。
        if request.args.get("view", "").strip() == "list":
            return _build_list_html()
        return scan_monitor_page()
    # 支持 ?symbol=X&period=5m : 通知中心跳转时带上信号所属周期
    period = request.args.get("period", "").strip()
    mark_ts = request.args.get("mark_ts", "").strip()
    return _build_detail_html(symbol, period or DEFAULT_PERIOD, mark_ts)


def api_kline_data():
    symbol = request.args.get("symbol", "").strip()
    period = request.args.get("period", DEFAULT_PERIOD).strip()
    if not symbol:
        return jsonify({"ok": False, "error": "缺少 symbol 参数"}), 400
    if period not in PERIODS:
        period = DEFAULT_PERIOD
    raw_count = request.args.get("count", "").strip()
    req_count = int(raw_count) if raw_count.isdigit() else None
    if req_count is not None:
        req_count = min(req_count, MAX_KLINE_COUNT)
    raw_end = request.args.get("end_dt", "").strip()
    end_dt = None
    if raw_end.isdigit():
        try:
            ns = int(raw_end)
            end_dt = pd.to_datetime(ns, unit="ns", utc=True).tz_convert("Asia/Shanghai").tz_localize(None).to_pydatetime()
        except Exception:
            end_dt = None
    slot = _get_slot()
    if slot is None:
        return jsonify({"ok": False, "error": "未分配有效账户"}), 503
    try:
        payload = _build_kline_payload(slot, symbol, period, req_count=req_count, end_dt=end_dt)
    except Exception as e:
        payload = {"ok": False, "error": f"K线生成异常: {e}", "trace": traceback.format_exc()}
    return jsonify(payload)


def api_quotes_data():
    """期货主连列表实时行情 API。?warm=1 时等待数据到达(首次), 否则直接读缓存。"""
    slot = _get_slot()
    if slot is None:
        return jsonify({"ok": False, "error": "未分配有效账户"}), 503
    warm = request.args.get("warm", "") == "1"
    main_contracts = _get_main_contracts()
    symbols = [s for s, _, _, _ in main_contracts]
    try:
        payload = _quote_payload(symbols, slot, wait_ready=warm)
        # 注入 exchange/code 便于前端筛选
        meta = {s: {"exchange": ex, "code": pc, "name": nm} for s, ex, pc, nm in main_contracts}
        for q in payload.get("quotes", []):
            sym = q.get("symbol", "")
            if sym in meta:
                q["exchange"] = meta[sym]["exchange"]
                q["code"] = meta[sym]["code"]
                q["name"] = meta[sym]["name"]
            q.pop("error", None)
            _enrich_change(q)
        payload["quotes"] = sorted(payload.get("quotes", []), key=lambda x: (x.get("exchange", ""), x.get("name", "")))
    except Exception as e:
        payload = {"ok": False, "error": f"行情获取异常: {e}", "trace": traceback.format_exc()}
    return jsonify(payload)


def api_contracts_data():
    """某品种下的月份合约列表 API。参数: exchange, code, quotes(=1 时附带实时行情)。
    返回 [{symbol, name, contract_month, expire_date, ...实时字段}, ...]。
    月份合约是标准可订阅期货代码, 与主连同源, 实时字段完全对齐主表标题。"""
    exchange = request.args.get("exchange", "").strip()
    code = request.args.get("code", "").strip()
    if not exchange or not code:
        return jsonify({"ok": False, "error": "缺少 exchange 或 code 参数"}), 400
    try:
        contracts = _get_product_contracts(exchange, code)
        # 附带实时行情: 月份合约与主连同源, 订阅后轮询线程持续推送
        if request.args.get("quotes", "") == "1" and contracts:
            slot = _get_slot()
            if slot is not None:
                syms = [c["symbol"] for c in contracts]
                warm = request.args.get("warm", "") == "1"
                try:
                    p = _quote_payload(syms, slot, wait_ready=warm)
                    qmap = {q.get("symbol"): q for q in p.get("quotes", [])}
                    quote_fields = ("last_price", "change", "change_pct", "bid_price1",
                                    "bid_volume1", "ask_price1", "ask_volume1", "volume",
                                    "open_interest", "day_increase", "open", "high", "low",
                                    "pre_settlement", "amplitude")
                    for c in contracts:
                        q = qmap.get(c["symbol"])
                        if q:
                            _enrich_change(q)
                            q.pop("error", None)
                            for fld in quote_fields:
                                if fld in q:
                                    c[fld] = q[fld]
                        c.setdefault("exchange", exchange)
                        c.setdefault("code", code)
                except Exception as e:
                    print(f"[合约行情] {exchange}.{code} 实时行情获取失败: {e}")
        return jsonify({"ok": True, "exchange": exchange, "code": code, "count": len(contracts), "contracts": contracts})
    except Exception as e:
        return jsonify({"ok": False, "error": f"读取合约列表异常: {e}", "trace": traceback.format_exc()}), 500


def api_scan_data():
    """期货全市场扫描接口。
    POST / GET 参数:
      symbols: 用逗号分隔的合约列表(可选, 默认主力合约)
      period: 扫描周期(默认 day)
      rules: JSON 字符串, 用户配置的规则数组
      max_workers: 并发数(默认 8, 上限 16)
      mode: realtime / history(默认 realtime); 仅 realtime 才写入历史复盘表
    返回: {ok, long:[{symbol,close,signals}], short:[...], stats, scan_time}
    """
    slot = _get_slot()
    if slot is None:
        return jsonify({"ok": False, "error": "未分配有效账户"}), 503

    period = request.args.get("period") or request.form.get("period") or "day"
    if period not in PERIODS:
        period = "day"

    # symbols 处理
    symbols = []
    raw_symbols = request.args.get("symbols", "") or request.form.get("symbols", "")
    if raw_symbols:
        symbols = [s.strip() for s in raw_symbols.split(",") if s.strip()]
    if not symbols:
        # 默认取主力合约; 为防止过多, 截断到前 300
        symbols = [s for s, _, _, _ in _get_main_contracts()][:300]

    # 品种白名单过滤
    raw_products = request.args.get("products", "") or request.form.get("products", "")
    products = [p.strip() for p in raw_products.split(",") if p.strip()] if raw_products else []
    if products:
        symbols = _filter_symbols_by_products(symbols, products)

    # rules 处理
    rules = None
    raw_rules = request.args.get("rules", "") or request.form.get("rules", "")
    if raw_rules:
        try:
            rules = json.loads(raw_rules)
        except Exception:
            rules = None

    max_workers = request.args.get("max_workers", type=int) or request.form.get("max_workers", type=int) or 8
    max_workers = max(1, min(16, int(max_workers)))

    mode = (request.args.get("mode") or request.form.get("mode") or "realtime").strip()
    if mode not in ("realtime", "history"):
        mode = "realtime"

    try:
        result = scan_engine.scan_symbols(symbols, slot, period=period, rules=rules, max_workers=max_workers)
        # 异步写入历史表, 不阻塞 HTTP 返回; 仅 realtime 模式入库
        if result and result.get("ok") and mode == "realtime":
            try:
                threading.Thread(
                    target=scan_history_db.save_scan_signals,
                    args=(result,),
                    daemon=True
                ).start()
            except Exception as e:
                print(f"[api_scan_data] 启动历史入库线程失败: {e}")
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": f"扫描异常: {e}", "trace": traceback.format_exc()}), 500


# ===== 多周期异步并发监控接口 =====
_MULTI_MON = None

def _get_multi_monitor():
    global _MULTI_MON
    if _MULTI_MON is None:
        _MULTI_MON = scan_engine.MultiPeriodMonitor(max_workers=24)
    return _MULTI_MON


# ==================== 后台自动监控（需求 2026-09-22） ====================
# 不用手点「开始监控」：交易时段(日盘 09:00-15:00 / 夜盘 21:00-02:30)自动起、收盘自动停；
# 面板「实时」只展示当天(交易日)信号，更早的到「历史回溯」里看。
# 会话安全：天勤单会话的 K 线对象有隐形预算(skill tqsdk-kline-pump-safe 实测 ~500 后静默)，
# 87 主连 × 7 组对象 ≈ 600 会超，故按 AUTO_TICK_SECONDS"每拍只起一个周期"渐进启动 + 预算守卫。
AUTO_PERIODS = ["1m", "5m", "15m", "30m", "60m", "day", "week", "month", "quarter"]
# ---- 会话 K 线对象预算(硬约束) ----
# 天勤单会话的"图表预算"实测约 500 个 K 线对象(见 skill tqsdk-kline-pump-safe: 超过后
# set_chart 静默失效, 甚至整条会话连行情/指数推送一起冻死)。页面图表与监控共用同一槽位,
# 故自动监控只吃其中一部分, 留余量给正在浏览的 K 线与自选订阅。
# 2026-09-24 18:05 实锤: 技术分析页预热对象(5 指数×6 周期=30 个)被监控订阅挤掉后,
# 页面 api/data 重建订阅等待 30s 超时 -> 整页 "no quote data"(实测同款请求 162ms vs 30s)。
# 2026-09-28 隔离: 监控改用独立账户(monitor 角色)连接, 与技术分析账户物理隔离,
# 不再与分析抢同一条连接的订阅容量 -> 预算恢复 420(监控池 cap 自动 60 只), 分析连接 100% 干净。
AUTO_BUDGET_OBJS = 420
# 每个标的占用的 K 线对象数(与 PERIODS 对齐):
#   1m/5m/15m/30m/60m 各 1 个(count=500)      = 5
#   日线 1 个(86400, count=500)                = 1
#   周/月/季 共用 1 个(86400, daily_count=2000) = 1
AUTO_OBJS_PER_SYMBOL = 7
# 启动某周期时"每个标的"新增的对象数(周/月/季 复用同一个日线对象, 不额外占用)
_AUTO_COST_BY_PERIOD = {"1m": 1, "5m": 1, "15m": 1, "30m": 1, "60m": 1,
                        "day": 1, "week": 1, "month": 0, "quarter": 0}
# [2026-10-09] 周期起动优先级(重→轻, 数字小=先起)。仅影响"同一拍起哪个周期"的顺序,
# 不改模板勾选口径、不改是否监控。原因见 _auto_tick 内 todo 处的注释:
#   周/月/季 cost=0(复用日线对象)最便宜 -> 最先; 分钟线每周期每标的各占 1 个对象最贵 -> 最后。
_AUTO_PERIOD_PRIORITY = ["month", "quarter", "day", "week",
                        "60m", "30m", "15m", "5m", "1m"]


def _auto_period_priority(periods_cfg=None):
    """按【重→轻】优先级返回本轮该起的周期顺序(AUTO_STAGGER 每拍取第一个)。

    入参 periods_cfg 为本拍已算出的周期并集(保持"模板勾选什么就监控什么"不变), 只重排先后:
      * 并集内的周期按 _AUTO_PERIOD_PRIORITY 排;
      * 并集里若有不在优先级表中的新周期(将来新增), 稳定地排在末尾, 不会漏起。
    periods_cfg 为空时自行读库(便于单测/兜底)。
    """
    if periods_cfg is None:
        try:
            periods_cfg = scan_template_db.monitor_periods() or AUTO_PERIODS
        except Exception as e:
            print("[自动监控] 读取周期并集失败, 按默认优先级:", e)
            periods_cfg = list(AUTO_PERIODS)
    rank = {p: i for i, p in enumerate(_AUTO_PERIOD_PRIORITY)}
    return sorted(periods_cfg, key=lambda p: rank.get(p, len(rank)))
AUTO_MAX_SYMBOLS = 0        # 0 = 按预算自动适配合约数; >0 = 手动钉死上限
AUTO_TICK_SECONDS = 20      # 守护轮询间隔(秒)
AUTO_STAGGER = True         # True = 每拍只启动一个周期，渐进建立 K 线对象
KLINE_BUDGET = AUTO_BUDGET_OBJS  # 运行期守卫: 已建对象 + 本轮新增 > 它 则暂停新增周期

# ============================================================
# 子合约纳入(2026-10-14 用户拍板"把子合约也弄进去")
# 背景: 监控池长期落在"主连档"(模板并集 87 品种 × 活跃月份 = 853 > cap=60),
#   所以子合约(jd2610 之类具体月份)**从未被监控** —— 用户看盘看的是子合约。
# 物理约束: 天勤单会话 K线对象上限 ~500(AUTO_BUDGET_OBJS=420 留余量),
#   全市场 936 只 × 7 周期 ≈ 6000+ 对象, 全月份档物理不可行。
# 本次方案(B 混合档):
#   主连池(60, 兜底全市场主连) + 子合约(模板品种的活跃月份, 只订【关键周期】省对象)
#   - [2026-10-15] 子合约周期跟设置走(_sub_periods()=模板勾选并集), 勾几个跑几个
#     而非 7 个, 同样预算能多纳约 2.3 倍只数;
#   - 子合约按"距到期近"排序(近月流动性最好、最贴近看盘), 再按品种轮转保公平;
#   - 池指纹带子合约, 模板变更能触发重建。
# 开关: AUTO_SUB_ENABLED=0 可一键回退到纯主连(环境变量, 便于排错对比)。
# ============================================================
AUTO_SUB_ENABLED = True
# [2026-10-15 用户裁定] 子合约周期不再硬编码 30m/60m/day 三档, 改为【跟着设置走】:
#   取"模板已勾选的周期"(扫描面板勾什么就监控什么), 再按对象预算裁剪;
#   若模板勾了全部 9 周期, 9 个都跑(预算够), 若只勾 1m/5m, 子合约就只跑 1m/5m。
# 预算不足时按 _AUTO_PERIOD_PRIORITY 的逆序(轻->重: month/quarter/day 最便宜)保留,
# 保证"设置里勾了且预算允许"的周期全部生效。
SUB_PERIODS_FALLBACK = ["30m", "60m", "day"]   # 读不到模板勾选时的兜底(原硬编码口径)
SUB_MAX_SYMBOLS = 40 # 子合约数量上限(占预算: 40×3=120 个对象)
SUB_PER_PRODUCT = 3   # 每个品种最多取几个活跃月份(近月优先), 防单品种吃满
SUB_KEEP_MIN = 12     # 子合约保底只数(预算冲突时优先保子合约, 减到这么多就不减了)
MAIN_KEEP_MIN = 40    # 主连保底只数(子合约让位后的下限)

_AUTO_LOCK = threading.Lock()
_AUTO_THREAD = None
_AUTO_STARTED = False
_AUTO_STATE = {
    "enabled": True,        # 总开关（False = 完全交回手动）
    "paused_day": None,     # 手动停止 -> 暂停到该交易日结束
    "sources": {},          # period -> 'auto' | 'manual'
    "last_tick": None,
    "last_action": "等待交易时段",
    "last_error": "",
    "budget": 0,
    "budget_cap": 0,
    "symbols_total": 0,
    "symbols_used": 0,
    "pool_source": "",      # 监控池来源: '' / 全市场 / 模板并集(N 品种)
    "pool_products": 0,     # 模板并集里的品种数(0 = 全市场)
    "pool_cut": 0,          # 因会话 K 线预算被截断的合约数
    # --- 混合档(2026-10-14 子合约) ---
    "pool_mode": "",        # 'main' | 'all-contracts' | 'mixed'
    "pool_sub_count": 0,    # 混合档里的子合约只数
    "pool_main_count": 0,   # 混合档里的主连只数
    "pool_sub_periods": [], # 子合约只订的周期(省 K 线对象)
    "pool_sig": "",         # 上一次的池指纹(逗号串), 用于检测模板变更
    "pool_change_at": 0,    # 上次因池变化重建的时刻(60s 冷却, 防抖动)
}


def _sub_contract_symbols(cand_products, budget_objs_left, per_objs=None):
    """按【混合档】挑子合约(具体月份), 返回 symbol 列表。

    选取口径(用户 2026-10-14 要求"把子合约也弄进去"; 2026-10-15 追加"范围跟着设置走"):
      1. 只从【模板勾选品种】里挑(空模板 -> 全市场, 但那样数量太大, 故只取主连);
      2. 每个品种取最近到期的 N 个活跃月份(SUB_PER_PRODUCT), 近月流动性最好、最贴近看盘;
      3. 对象预算守恒: 子合约每只占 per_objs 个对象(调用方按【实际周期成本】算好传入;
         月/季 cost=0 与日线共用对象, 不能简单用周期个数), 故可纳只数 <= budget_objs_left / per_objs;
      4. 品种间轮转取, 避免前几个品种吃满额度(一个模板品种1~10个近月时也能轮到后面)。

    返回 [] 表示预算不足/无模板/开关关闭 —— 调用方回退纯主连。
    """
    if not AUTO_SUB_ENABLED or not cand_products:
        return []
    if str(os.environ.get("AUTO_SUB_ENABLED", "1")).strip().lower() in ("0", "false", "no", "off"):
        return []
    if not per_objs:
        per_objs = sum(max(0, _AUTO_COST_BY_PERIOD.get(p, 1)) for p in _sub_periods()) or 1
    n_period = max(1, per_objs)
    max_n = SUB_MAX_SYMBOLS if SUB_MAX_SYMBOLS > 0 else budget_objs_left // n_period
    if max_n <= 0:
        return []
    # 2026-10-14 修正: 模板品种是按代码字母序排的, 前 N 个常同属一个交易所(SHFE...)，
    # 直接顺序取会让子合约全落在一个交易所、覆盖极差(实测 20 只全是 SHFE)。
    # 改为【按交易所轮转】: 先按交易所分组, 再交易所内取近月, 各交易所轮流取,
    # 保证每个交易所都有机会上。
    by_ex = {}
    for code in cand_products:
        ex, _, pc = code.partition(".")
        if not ex or not pc:
            continue
        by_ex.setdefault(ex, []).append(pc)
    # 交易所轮转顺序: 按该交易所能贡献的合约数降序(贡献多的一轮里先出), 同数按字母序
    ex_order = sorted(by_ex.keys(), key=lambda e: (-len(by_ex[e]), e))
    picked = []
    # 每个交易所内部: 只取 SUB_PER_PRODUCT 个近月品种(先算好缓存)
    per_ex_lists = {}
    for ex in ex_order:
        acc = []
        for pc in by_ex[ex]:
            if len(acc) >= SUB_PER_PRODUCT:
                break
            try:
                rows = _get_product_contracts(ex, pc, from_today=True)
            except Exception:
                continue
            if not rows:
                continue
            # 2026-10-14: 每品种只取【1 个最近月】用于跨交易所轮转, 保证覆盖面;
            #   若某交易所只有 1 个品种可取, 下面第二轮会再补它第 2/3 个近月。
            #   (原来一次取 3 个, 导致轮转前几轮就把额度吃掉, 只覆盖到 6 个品种)
            cnt = 0
            for r in rows:
                sym = (r or {}).get("symbol")
                if not sym or sym.startswith("KQ.m@"):
                    continue
                if sym in acc:
                    continue
                # 同月份优先标准合约(跳过 F 月均变体), 与深度补齐口径一致
                _cm = (r or {}).get("contract_month")
                if _cm and sym.endswith("F") and not _cm.endswith("F"):
                    _has_std = any(((x or {}).get("contract_month") == _cm)
                                   and not ((x or {}).get("symbol") or "").endswith("F")
                                   for x in rows)
                    if _has_std:
                        continue
                acc.append(sym)
                cnt += 1
                break
        per_ex_lists[ex] = acc
    # 品种深度: 每个交易所轮流取时, 按轮次给同一交易所换品种(而不是一直取同一品种的
    # 多个近月), 这样 40 只子合约能覆盖到 40 个不同品种/近月, 而不是 6 个品种 × 3 个月。
    # 实现: 一次把每个交易所可贡献的符号全列出来(按品种顺序), 轮转时按下标推进。
    for ex in ex_order:
        acc = per_ex_lists.get(ex) or []
        if len(acc) >= max_n:
            continue
        pc_list = by_ex.get(ex) or []
        taken = set(acc)
        for pc in pc_list:
            if len(acc) >= max_n:
                break
            try:
                rows = _get_product_contracts(ex, pc, from_today=True)
            except Exception:
                continue
            if not rows:
                continue
            for r in rows[:SUB_PER_PRODUCT]:
                sym = (r or {}).get("symbol")
                if not sym or sym.startswith("KQ.m@") or sym in taken:
                    continue
                # 2026-10-14: 同一 contract_month 优先标准合约, 跳过 "F" 月均变体
                #   (DCE 的月均结价合约, 如 DCE.v2611F/月均PVC2611, 与标准 v2611 同月份)。
                #   用户看盘看的是标准合约; 月均变体占额度且易混。
                _cm = (r or {}).get("contract_month")
                if _cm and sym.endswith("F") and not _cm.endswith("F"):
                    _has_std = any(((x or {}).get("contract_month") == _cm)
                                   and not ((x or {}).get("symbol") or "").endswith("F")
                                   for x in rows)
                    if _has_std:
                        continue
                acc.append(sym)
                taken.add(sym)
                if len(acc) >= max_n:
                    break
        per_ex_lists[ex] = acc
    # 轮转取, 每次每个交易所出一个
    idx = {ex: 0 for ex in ex_order}
    while len(picked) < max_n:
        progressed = False
        for ex in ex_order:
            if len(picked) >= max_n:
                break
            i = idx[ex]
            lst = per_ex_lists.get(ex) or []
            if i < len(lst):
                sym = lst[i]
                idx[ex] = i + 1
                if sym not in picked:
                    picked.append(sym)
                    progressed = True
        if not progressed:
            break
    return picked


def _monitor_symbols():
    """监控标的池 —— 自动监控与手动监控共用的【唯一口径】。

    2026-09-24 用户需求: 「监控所有的期货; 自己判断时间, 慢就保留主连但显示月份(如 2610)」。
    实测(tmp_perf_all_contracts.py, zhubajie 会话): 全市场未到期月份合约 936 只,
    建 K 线本身很快(0.10s/个), 但【K 线对象是硬约束】: 936×~7 周期 ≈ 6000+ 对象,
    远超天勤会话图表上限(~500, MAX_SLOT_SUBSCRIBE) —— 全月份档当前物理不可行。
    档位(自动判定, 2026-10-14 加混合档):
      A) 月份合约档: 候选品种的活跃月份总数 <= cap 时全上(模板品种少时自然生效);
      B) 混合档(当前): 主连池 + 子合约(模板品种近月, 只订关键周期省对象) ——
         用户看盘看的是子合约(jd2610 之类), 纯主连会对不上他看到的K线;
      C) 主连档: 前两者都装不下时的兜底。
    """
    try:
        mc = [s for s, _n, _e, _c in _get_main_contracts()]
    except Exception as e:
        print(f"[监控池] 取主连失败: {e}")
        return []
    total = len(mc)
    prods = []
    try:
        from services import scan_template_db as _std
        prods = _std.monitor_products() or []
    except Exception as e:
        print(f"[监控池] 读模板品种失败(退回全市场): {str(e)[:120]}")

    if AUTO_MAX_SYMBOLS and AUTO_MAX_SYMBOLS > 0:
        cap = AUTO_MAX_SYMBOLS
    else:
        cap = max(1, AUTO_BUDGET_OBJS // max(1, AUTO_OBJS_PER_SYMBOL))

    # ---- 自动档: 算候选品种的活跃月份合约总数, 装得下就全上(t_futures_expiry, 有缓存) ----
    if prods:
        cand, src0 = prods, "模板并集"
    else:
        cand, src0 = sorted({s.split("@", 1)[-1] for s in mc if "@" in s}), "全市场"
    month_syms = []
    for code in cand:
        ex, _, pc = code.partition(".")
        if not ex or not pc:
            continue
        try:
            for c in _get_product_contracts(ex, pc):
                month_syms.append(c["symbol"])
        except Exception:
            pass
    month_syms = sorted(set(month_syms))
    # ---- 档位选择: 全月份档 -> 混合档(主连+子合约) -> 纯主连档 ----
    mode, syms, src = "main", mc, src0 + "主连"
    subs = []
    if month_syms and len(month_syms) <= cap:
        # A) 模板品种少 -> 全部月份合约都纳, 不必混
        syms, src, mode = month_syms, "%s全部月份合约(%d)" % (src0, len(month_syms)), "all-contracts"
        print(f"[监控池] 月份合约档: {src} (cap={cap})")
    else:
        # B) 混合档: 主连打底 + 模板品种的近月子合约(周期跟着设置走)
        #    [2026-10-15] 周期不再硬编码: 主连周期 = 子合约周期 = 模板勾选并集(_periods_for_symbol),
        #    对象数按【实际周期数】精确核算(此前主连固定按 7 个算, 与实际 9 个不符, 会低估占用)。
        main_pers = _periods_for_symbol("KQ.m@X")
        n_main = max(1, len(main_pers))
        n_per_sub = max(1, len(_sub_periods()))
        # 每个标的的月线/季线 cost=0(与日线共用对象) -> 直接求和, 不能用 max(1, cost)
        #   (此前 max(1,cost) 把 cost=0 抬成 1, 每只多算 month+quarter 两个对象,
        #    实测 9 周期应为 7 个对象却算成 9 个 -> 混合池 468/420 超预算)。
        def _objs_for(pers):
            return sum(max(0, _AUTO_COST_BY_PERIOD.get(p, 1)) for p in pers) or 1
        main_objs_each = _objs_for(main_pers)
        sub_objs_each = _objs_for(_sub_periods())
        budget = AUTO_BUDGET_OBJS
        main_n = min(cap, max(MAIN_KEEP_MIN, budget // (2 * main_objs_each)))
        # 子合约可用额度 = 总预算 - 主连占用, 再按"每只 sub_objs_each 对象"折算只数
        left_objs = max(0, budget - main_n * main_objs_each)
        subs = _sub_contract_symbols(cand, left_objs, per_objs=sub_objs_each)
        if subs:
            mode = "mixed"
            # 🔴 预算硬约束(2026-10-14 / 2026-10-15): 混合后总对象数
            #   = 主连×main_objs_each + 子合约×sub_objs_each, **必须 <= AUTO_BUDGET_OBJS**,
            #   否则会话被顶到静默阈值(全线无行情)。超标时先减子合约(到保底), 再减主连(到保底)。
            while subs and (main_n * main_objs_each + len(subs) * sub_objs_each) > budget:
                if len(subs) > SUB_KEEP_MIN:
                    subs = subs[:-1]          # 先减子合约
                elif main_n > MAIN_KEEP_MIN:
                    main_n -= 1                # 再减主连
                else:
                    break
            syms = list(mc[:main_n]) + list(subs)
            src = "%s主连%d+子合约%d" % (src0, main_n, len(subs))
            total_objs = main_n * main_objs_each + len(subs) * sub_objs_each
            print("[监控池] 混合档: %s (子合约周期%s, 对象 %d/%d)" % (
                src, "/".join(_sub_periods()), total_objs, budget))
            print("[监控池] 子合约样例: %s" % (", ".join(subs[:6]) + (" ..." if len(subs) > 6 else "")))
        else:
            print(f"[监控池] 主连档: 月份合约 {len(month_syms)} 只 > cap={cap}, "
                  f"且子合约预算不足/无模板, 回退纯主连")

    if mode != "mixed":
        cut = max(0, len(syms) - cap)
        if cut:
            syms = syms[:cap]
    else:
        cut = 0
    with _AUTO_LOCK:
        _AUTO_STATE["symbols_total"] = total
        _AUTO_STATE["symbols_used"] = len(syms)
        _AUTO_STATE["pool_source"] = src
        _AUTO_STATE["pool_products"] = len(prods)
        _AUTO_STATE["pool_cut"] = cut
        _AUTO_STATE["pool_mode"] = mode
        _AUTO_STATE["pool_sub_count"] = len(subs)
        _AUTO_STATE["pool_main_count"] = len(syms) - len(subs)
        _AUTO_STATE["pool_sub_periods"] = list(_sub_periods()) if subs else []
    return syms


def _auto_symbols():
    """自动监控标的池 = 统一口径 `_monitor_symbols()`(模板并集 / 全市场, 按预算截断)。"""
    return _monitor_symbols()


def _sub_periods():
    """子合约实际要监控的周期集合(2026-10-15 用户裁定: 跟着【设置】走)。

    口径:
      1) 取"模板已勾选的周期"(扫描面板/模板 settings 勾什么就监控什么);
      2) 与可监控周期全集取交集(派生周期 week/month/quarter 已在 AUTO_PERIODS 内);
      3) 模板未勾 / 读不到 -> 回退 SUB_PERIODS_FALLBACK(原 30m/60m/day 口径, 保兼容)。
    这样用户在设置里勾 1m/5m, 子合约就只跑 1m/5m; 勾全 9 周期则子合约也跑 9 个
    (受 _sub_periods_affordable 的对象预算裁剪)。
    """
    try:
        allowed = _allowed_periods_or_none()
    except Exception:
        allowed = None
    if not allowed:
        return list(SUB_PERIODS_FALLBACK)
    out = [p for p in AUTO_PERIODS if p in set(allowed)]
    return out or list(SUB_PERIODS_FALLBACK)


def _periods_for_symbol(sym):
    """某标的在【混合档实时监控池】里实际被监控的周期集合。

    主连(KQ.m@ 前缀)走【模板勾选的周期并集】; 子合约(模板品种近月)走【同一批勾选周期】
    (2026-10-15 起两者口径统一, 都跟设置走, 仅标的集合不同), 补扫必须对齐这个口径,
    否则会产出"实时监控根本不监控的周期"的孤立信号(T2706 5m 假金叉事故同源)。
    """
    try:
        allowed = _allowed_periods_or_none()
    except Exception:
        allowed = None
    base = [p for p in AUTO_PERIODS if p in set(allowed)] if allowed else list(AUTO_PERIODS)
    return base or list(AUTO_PERIODS)


def _intraday_bar_closed(trigger_ns, period_duration, now_ns):
    """补扫过滤用: 日内周期的触发 bar 是否已收盘(bar_time + duration <= now)。

    非日内(day/week/month/quarter, duration>=86400)恒返回 True —— 这些"bar"跨整个周期、
    盘中恒为"形成中", 不应按收盘裁掉(否则日线及以上永远出不了信号)。
    用于拦截"正在形成 bar"的半截数据判出的次日消失假信号(TL2612 30m 09:30 事故同源)。
    """
    _dur = int(period_duration or 86400)
    if _dur >= 86400:
        return True
    return (int(trigger_ns) + _dur * 1_000_000_000) <= int(now_ns)


def _monitor_rules():
    """自动监控 / 手动 merge 用的【条件并集】: DEFAULT_RULES + 全部模板已勾选(enabled)规则。

    见 scan_template_db.monitor_rules 口径: 模版默认全选、勾选的条件全抓取。
    返回 None 表示"读取失败 -> 回落默认规则"(scan_engine 内部 rules=None 会用 DEFAULT_RULES)。
    """
    try:
        from services import scan_template_db as _std
        rules = _std.monitor_rules()
        return rules if rules else None
    except Exception as e:
        print(f"[监控池] 读模板条件失败(退回默认): {str(e)[:120]}")
        return None


def _rules_sig(rules):
    """规则的稳定签名(按 id 排序), 用于自动监控"条件变化即重建"判定。"""
    try:
        ids = sorted([str(r.get("id")) for r in (rules or []) if r.get("id") is not None])
        return "|".join(ids)
    except Exception:
        return ""


def _monitor_rules_by_symbol(symbols):
    """逐标的"最严规则"映射封装: 失败回落 {}（调用方再退默认规则）。

    见 scan_template_db.monitor_rules_by_symbol 口径: 每标的用对应模板规则,
    多模板重叠按"最严"合并去重。"""
    try:
        return scan_template_db.monitor_rules_by_symbol(symbols) or {}
    except Exception as e:
        print(f"[监控池] 读模板逐标的规则失败(退回默认): {str(e)[:120]}")
        return {}


def _kline_objs(slot):
    """会话里已建立的 K 线对象数（即"图表预算"占用）。"""
    try:
        return len(getattr(slot, "_kline_cache", {}) or {})
    except Exception:
        return 0


def auto_monitor_state():
    """给前端的状态快照。"""
    try:
        _ensure_today_backfill()   # 2026-09-24: 今天库空则后台补扫(内部幂等+节流)
    except Exception as _e:
        print(f"[自动监控] 补扫评估异常: {_e}")
    mon = _get_multi_monitor()
    with _AUTO_LOCK:
        st = dict(_AUTO_STATE)
        st["sources"] = dict(_AUTO_STATE.get("sources") or {})
    try:
        st["trading_now"] = scan_engine.is_trading_now()
        st["trading_day"] = str(scan_engine.trading_day_of())
        st["today_range"] = [str(x) for x in scan_engine.trading_day_range()]
    except Exception:
        st["trading_now"] = None
        st["trading_day"] = None
        st["today_range"] = None
    st["paused"] = bool(st.get("paused_day") and st["paused_day"] == st.get("trading_day"))
    st["running"] = mon.is_running()
    st["periods"] = mon.running_periods()
    # [2026-10-09] auto_periods 原为硬编码 AUTO_PERIODS 全集, 导致前端头部"覆盖 N 周期"
    # 恒显9 个周期(含模板没勾的周/月/季), 与实际监控口径不符。改为"实际生效的周期并集":
    # 模板有勾选 -> 并集; 无模板/全空 -> 回落全集(兼容旧行为)。
    _allow = _allowed_periods_or_none()
    st["auto_periods"] = list(_allow) if _allow else list(AUTO_PERIODS)
    st["session"] = mon.session_info()
    slot = _get_slot()
    st["budget"] = _kline_objs(slot) if slot is not None else 0
    st["budget_cap"] = KLINE_BUDGET
    st["objs_per_symbol"] = AUTO_OBJS_PER_SYMBOL
    # 2026-09-24 用户需求: 系统里一个未到期的天勤账号都没有 -> 不给看监控数据
    # (槽也没有且库中无有效账号; slot 非 None 时短路不查库)
    st["no_valid_account"] = bool(slot is None and not _any_valid_account_cached())
    with _BACKFILL_LOCK:
        st["backfill"] = {
            "date": _BACKFILL.get("date"),
            "running": bool(_BACKFILL.get("running")),
            "progress": _BACKFILL.get("progress") or "",
            "error": _BACKFILL.get("error") or "",
        }
    return st


def set_auto_monitor(enabled=None, pause=None):
    """暂停/恢复自动监控。

    pause=True  -> 暂停到当前交易日结束（用户手动「停止」时调用）
    pause=False -> 立即恢复
    """
    with _AUTO_LOCK:
        if enabled is not None:
            _AUTO_STATE["enabled"] = bool(enabled)
        if pause is True:
            try:
                _AUTO_STATE["paused_day"] = str(scan_engine.trading_day_of())
            except Exception:
                _AUTO_STATE["paused_day"] = "?"
        elif pause is False:
            _AUTO_STATE["paused_day"] = None
    try:
        _auto_tick()
    except Exception as e:
        print(f"[自动监控] 手动干预后评估异常: {e}")
    return auto_monitor_state()


def _auto_stop(mon, periods, why):
    if not periods:
        return
    try:
        mon.stop(periods)
    except Exception as e:
        print(f"[自动监控] 停止 {periods} 失败: {e}")
        return
    with _AUTO_LOCK:
        for p in periods:
            _AUTO_STATE["sources"].pop(p, None)
        _AUTO_STATE["last_action"] = f"{why}，已停 {','.join(periods)}"
    print(f"[自动监控] {why} -> 已停周期 {periods}")


def _auto_tick():
    """守护线程的每一拍：交易时段自动起（每拍最多起一个周期），休市/暂停则停自动起的周期。"""
    try:
        _ensure_today_backfill()   # 2026-09-24 16:55: 服务启动即补今日数据(独立会话, 不占实时)
    except Exception:
        pass
    mon = _get_multi_monitor()
    try:
        now = scan_engine._now_bj_naive()
        trading = scan_engine.is_trading_now(now)
        tday = str(scan_engine.trading_day_of(now))
    except Exception as e:
        trading, tday = False, None
        print(f"[自动监控] 判定交易时段失败: {e}")

    with _AUTO_LOCK:
        enabled = bool(_AUTO_STATE["enabled"])
        paused = bool(_AUTO_STATE["paused_day"] and _AUTO_STATE["paused_day"] == tday)
        srcs = dict(_AUTO_STATE.get("sources") or {})
        _AUTO_STATE["last_tick"] = datetime.now().strftime("%H:%M:%S")

    running = mon.running_periods()
    active = bool(enabled and trading and not paused)

    # 1) 该停的：自动起的，且(总开关关 / 休市 / 已暂停)
    if not active:
        stop_list = [p for p in running if srcs.get(p) == "auto"]
        if stop_list:
            why = "总开关关闭" if not enabled else ("已暂停到收盘" if paused else "非交易时段")
            _auto_stop(mon, stop_list, why)
        elif not enabled:
            pass
        return

    # 周期并集(全部模板勾选周期去重并集); 全空/读失败 -> 回落 AUTO_PERIODS 全集
    _periods = scan_template_db.monitor_periods()
    _periods_cfg = _periods if _periods else AUTO_PERIODS

    # 1.5) [2026-10-09 用户裁定] 该停的(之二): 模板已取消勾选的周期 —— 交易时段内也要停。
    # 背景: 原逻辑只在"休市/暂停/关总开关"时停, 模板勾选变更对【已在跑】的周期无效 ->
    # 取消勾选'周'后 week 线程仍在扫描并继续落库(实锤: 模板 update_time=15:38:54,
    # 而 week 信号 scan_time 仍到 15:00:46)。此处按并集差集停掉多余的 auto 周期。
    # 只停 source=="auto" 的, 手动起的周期(用户显式 start)不代用户停。
    if _periods:
        _stale = [p for p in running if p not in _periods_cfg and srcs.get(p) == "auto"]
        if _stale:
            _auto_stop(mon, _stale, "模板已取消勾选该周期")
            return      # 本拍只做停, 下一拍再补起需要的周期(避免同拍先停后起的竞争)

    # 2) 该起的：交易时段内，逐拍补一个周期(周期口径=模板并集, 无并集则全集)
    # [2026-10-09 修复] 按【重→轻】固定优先级排, 不再用模板勾选顺序:
    #   实锤根因: AUTO_STAGGER 每拍只起 todo[:1], 而原顺序 = monitor_periods() 返回序
    #   = 模板 JSON 里勾选顺序 -> month/quarter 常排第 7/8 位, 日内基本轮不到。
    #   实测库里 month/quarter 各仅 236 条(5m=12万/ week=8.9万), 相差 500 倍量级。
    #   排序依据: 周/月/季复用同一个日线对象(_AUTO_COST_BY_PERIOD=0, 零额外订阅),
    #   属最便宜的一档, 应最先起; 分钟线最贵(每周期每标的 1 个对象), 排最后渐进建立。
    #   只重排顺序, 不增删任何周期 -> 模板勾选口径与已起周期完全不受影响。
    todo = [p for p in _auto_period_priority(_periods_cfg) if p not in running]
    if not todo:
        with _AUTO_LOCK:
            if not _AUTO_STATE["last_action"].startswith("监控中"):
                _AUTO_STATE["last_action"] = "监控中（%d 个周期全部就位）" % len(_periods_cfg)
        return

    # 技术分析页优先(2026-09-24): 页面活跃期间暂不新建监控周期订阅(已起周期照常评估,
    # 不新建对象), 让页面预热/取数独占会话带宽; 页面停访 12s 后自动恢复逐拍补齐。
    if tech_active():
        with _AUTO_LOCK:
            _AUTO_STATE["last_action"] = "技术分析页优先中, 监控新起延后"
        return

    slot = _get_auto_slot()   # 2026-09-24: 含第⑤级自动挑号(库中有效账号), 不再干等网关注入
    if slot is None:
        with _AUTO_LOCK:
            _AUTO_STATE["last_error"] = "未分配有效账户"
            _AUTO_STATE["last_action"] = "等待有效天勤账户"
        return

    want = todo[:1] if AUTO_STAGGER else todo
    symbols = _auto_symbols()
    if not symbols:
        with _AUTO_LOCK:
            _AUTO_STATE["last_error"] = "主连列表为空"
        return
    # ---- 2026-10-14 混合档: 子合约只订关键周期(省 K 线对象), 与主连分批启动 ----
    # 监控池现在是 [主连...]+[子合约...] 拼接; [2026-10-15] 两者周期同源(跟设置走)。
    # 不分批的话子合约会跟着起全周期 -> 对象数翻倍超预算 -> 全盘崩。
    _sub_cnt = 0
    with _AUTO_LOCK:
        _sub_cnt = int(_AUTO_STATE.get("pool_sub_count") or 0)
    _main_syms, _sub_syms = symbols, []
    if _sub_cnt > 0:
        _main_syms, _sub_syms = symbols[:len(symbols) - _sub_cnt], symbols[len(symbols) - _sub_cnt:]
    # [2026-10-15 用户裁定"范围跟着设置走"] 自本轮起, 子合约周期 = 主连周期 = 模板勾选并集
    #   (_sub_periods() 与 _periods_for_symbol() 同源), 因此【不存在"共享周期"之分】:
    #   任何周期都要【主连+子合约同周期同线程】一起起。
    #   (此前"共享周期才起全池、非共享只起主连"的前提是子合约只跑 30m/60m/day, 现已不成立;
    #    若保留旧分支, 用户只勾 1m/5m 时子合约会被主连排除在起周期之外 -> 子合约无数据。)
    _cur_syms = symbols
    # 逐标的"最严规则"映射(用户裁定: 每模板监控自己的规则, 重叠标的取最严一套去重);
    # 读取失败返回 {} -> 该标的回落 DEFAULT_RULES。数据仍按标的各取自带 slot(不重复取)。
    # 2026-10-15 修复: 规则按【全池】读一次, 起周期只用本拍 _cur_syms 的子集;
    # 指纹(_sig)也必须按全池算 —— 否则 _cur_syms 随 want[0] 在主连/子合约间切换时,
    # 规则签名换口径 -> sig 抖 -> "池/条件变化"误触发 -> 第3拍(day)停掉重来,
    # 永远走不到第4个周期(实锤: 220 次重启循环, K线对象恒0, 实时ingest零写入)。
    _pool_rules = _monitor_rules_by_symbol(symbols)
    if _pool_rules:
        _cur_set = set(_cur_syms)
        _symbol_rules = {k: v for k, v in _pool_rules.items() if k in _cur_set}
    else:
        _symbol_rules = _monitor_rules_by_symbol(_cur_syms)   # 读失败兜底(与原行为一致)
    # [2026-10-10] 自动监控"条件变化即重建"指纹改用逐标的规则映射签名;
    # 任一款标的规则/阈值变化 -> 签名变 -> 停掉自动起的周期, 下一拍按新口径重建。
    # 周期并集变化同样纳入指纹(修复"未勾周期仍被监控")。
    # 目的: 用户改完模板/删模板/改勾选后【无需重启服务】, 一个 tick(20s) 内自动换池/换条件/换周期。
    # 60s 冷却: 防止读库瞬断导致的"全市场 <-> 模板并集"抖动引发反复重建。
    # 🔴 指纹三要素(标的串/全池规则签名/周期并集)必须【与拍次无关】地稳定:
    #    任何一项随 want[0] 切换口径, "条件变化即重建"就会退化成"每拍都变"的死循环。
    _sig = ",".join(symbols) + "#" + scan_template_db._rules_by_symbol_sig(_pool_rules or _symbol_rules) + "#" + (
        ",".join(_periods) if _periods else "ALL")
    _now_t = time.time()
    with _AUTO_LOCK:
        _prev_sig = _AUTO_STATE.get("pool_sig") or ""
        _AUTO_STATE["pool_sig"] = _sig
        _last_chg = float(_AUTO_STATE.get("pool_change_at") or 0)
    if _prev_sig and _prev_sig != _sig and (_now_t - _last_chg) > 60:
        with _AUTO_LOCK:
            _AUTO_STATE["pool_change_at"] = _now_t
            _src_now = _AUTO_STATE.get("pool_source") or ""
        _stop_list = [p for p in running if srcs.get(p) == "auto"]
        if _stop_list:
            _auto_stop(mon, _stop_list, "监控池/条件变化(%s)" % _src_now)
            print("[自动监控] 监控池/条件变化 -> 停 %s, 下一拍按新口径(%d 只)重建"
                  % (_stop_list, len(symbols)))
            return
    used = _kline_objs(slot)
    with _AUTO_LOCK:
        _AUTO_STATE["budget"] = used
        _AUTO_STATE["budget_cap"] = KLINE_BUDGET
    # 预算守卫: 本轮启动会新建的对象数(周/月/季 复用日线对象 -> 0)。超预算就不新增,
    # 但已起的周期继续跑 —— 优雅降级, 绝不把会话顶到"静默"阈值。
    cost = len(_cur_syms) * _AUTO_COST_BY_PERIOD.get(want[0], 1)
    if KLINE_BUDGET and used + cost > KLINE_BUDGET:
        with _AUTO_LOCK:
            _AUTO_STATE["last_action"] = ("会话 K 线对象 %d + 本轮 %d 超预算 %d，暂停新增周期"
                                          % (used, cost, KLINE_BUDGET))
        print(f"[自动监控] K 线对象 {used} + 本轮 {cost} > 预算 {KLINE_BUDGET}，暂不新增周期")
        return
    try:
        mon.start(want, _cur_syms, None, mode="realtime",
                  symbol_rules=_symbol_rules, symbol_tpl=_symbol_rules)
    except Exception as e:
        with _AUTO_LOCK:
            _AUTO_STATE["last_error"] = str(e)[:200]
        print(f"[自动监控] 启动 {want} 失败: {e}")
        return
    with _AUTO_LOCK:
        for p in want:
            _AUTO_STATE["sources"][p] = "auto"
        _AUTO_STATE["last_error"] = ""
        _kind = "主连+子合约" if (_sub_syms and _cur_syms is symbols) else (
            "子合约" if (_sub_syms and _cur_syms is _sub_syms) else "主连")
        _AUTO_STATE["last_action"] = "已自动启动 %s（%d %s，待起 %d 个周期）" % (
            ",".join(want), len(_cur_syms), _kind, max(0, len(todo) - len(want)))
    print(f"[自动监控] 交易时段自动启动 {want} × {len(_cur_syms)} {_kind}（K线对象 {used}）"
          + (" [主连%d+子合约%d]" % (len(_main_syms), len(_sub_syms)) if _sub_syms else ""))


def _auto_loop():
    while True:
        try:
            _auto_tick()
        except Exception as e:
            print(f"[自动监控] 循环异常: {e}")
        time.sleep(AUTO_TICK_SECONDS)


def start_auto_monitor():
    """启动后台自动监控守护线程（幂等）。在 register_routes 调用一次。"""
    global _AUTO_THREAD, _AUTO_STARTED
    if _AUTO_STARTED:
        return
    _AUTO_STARTED = True
    _AUTO_THREAD = threading.Thread(target=_auto_loop, daemon=True, name="auto-monitor")
    _AUTO_THREAD.start()
    print("[自动监控] 后台守护已启动：交易时段自动拉取(全周期×主连)，无需手动开始"
          f"（每 {AUTO_TICK_SECONDS}s 一拍，渐进启动）")


def _parse_periods(src):
    """从 query/form/JSON 解析周期列表(支持逗号串或数组)。"""
    if isinstance(src, list):
        return [str(p) for p in src]
    if isinstance(src, str):
        return [p.strip() for p in src.split(",") if p.strip()]
    return []


def api_scan_multi_start():
    """启动多周期监控: 每个周期一条守护线程, 按自身节奏循环扫描并异步入库。"""
    mon = _get_multi_monitor()
    slot = _get_slot()
    if slot is None:
        return jsonify({"ok": False, "error": "未分配有效账户"}), 503

    periods = _parse_periods(
        request.args.get("periods") or request.form.get("periods")
    )
    if not periods and request.is_json:
        j = request.get_json(silent=True) or {}
        periods = _parse_periods(j.get("periods"))
    if not periods:
        periods = ["day"]

    # scope: merge(默认) = 与自动监控同一口径(全部模板品种并集 / 全市场);
    #        single = 按显式传参跑(单模板试跑), 保留原行为。
    scope = (request.args.get("scope") or request.form.get("scope") or "merge").strip()
    raw_syms = request.args.get("symbols", "") or request.form.get("symbols", "")
    if raw_syms:
        symbols = [s.strip() for s in raw_syms.split(",") if s.strip()]
    elif scope == "single":
        symbols = [s for s, _, _, _ in _get_main_contracts()][:300]
    else:
        symbols = _monitor_symbols()

    raw_products = request.args.get("products", "") or request.form.get("products", "")
    products = [p.strip() for p in raw_products.split(",") if p.strip()] if raw_products else []
    if products and scope == "single":
        symbols = _filter_symbols_by_products(symbols, products)

    rules = None
    raw_rules = request.args.get("rules", "") or request.form.get("rules", "")
    if raw_rules:
        try:
            rules = json.loads(raw_rules)
        except Exception:
            rules = None
    # 手动 merge 且前端未显式传规则 -> 套用【逐标的最严规则】(每标的用对应模板规则, 重叠取最严一套);
    # single 模式(试跑)保持原样(用前端勾选或默认全局 rules, 不按模板逐标的拆分)。
    symbol_rules = None
    if rules is None and scope == "merge":
        symbol_rules = _monitor_rules_by_symbol(symbols)

    mode = (request.args.get("mode") or request.form.get("mode") or "realtime").strip()
    if mode not in ("realtime", "history"):
        mode = "realtime"

    status = mon.start(periods, symbols, rules, mode=mode,
                       symbol_rules=symbol_rules, symbol_tpl=symbol_rules)
    return jsonify({"ok": True, "status": status, "mode": mode})


def api_scan_backtest():
    """历史回溯: 按日期区间对指定周期的历史 K 线做全量信号扫描(模型回溯)。
    与实时监控区分: 不报警/不发声/不入库。"""
    slot = _get_slot()
    if slot is None:
        return jsonify({"ok": False, "error": "未分配有效账户"}), 503
    period = (request.args.get("period") or request.form.get("period") or "day").strip()
    if period not in scan_engine.VALID_PERIODS:
        period = "day"
    start_date = (request.args.get("start") or request.form.get("start") or "").strip()
    end_date = (request.args.get("end") or request.form.get("end") or "").strip()
    if not start_date or not end_date:
        return jsonify({"ok": False, "error": "请同时提供 start 与 end 日期"}), 400
    raw_syms = request.args.get("symbols", "") or request.form.get("symbols", "")
    if raw_syms:
        symbols = [s.strip() for s in raw_syms.split(",") if s.strip()]
    else:
        symbols = [s for s, _, _, _ in _get_main_contracts()][:300]
    raw_products = request.args.get("products", "") or request.form.get("products", "")
    products = [p.strip() for p in raw_products.split(",") if p.strip()] if raw_products else []
    if products:
        symbols = _filter_symbols_by_products(symbols, products)
    rules = None
    raw_rules = request.args.get("rules", "") or request.form.get("rules", "")
    if raw_rules:
        try:
            rules = json.loads(raw_rules)
        except Exception:
            rules = None
    try:
        result = scan_engine.backtest_symbols(
            symbols, slot, period=period, rules=rules,
            start_date=start_date, end_date=end_date,
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": f"回溯失败: {e}"}), 500


def api_scan_multi_stop():
    """停止多周期监控(可指定周期, 缺省全部)。"""
    mon = _get_multi_monitor()
    periods = _parse_periods(request.args.get("periods") or request.form.get("periods")) or None
    status = mon.stop(periods)
    return jsonify({"ok": True, "status": status})


def api_scan_multi_status():
    """返回各周期监控状态。"""
    mon = _get_multi_monitor()
    return jsonify({"ok": True, "running": mon.is_running(), "status": mon.status()})


def _allowed_periods_or_none():
    """当前允许下发/启动的周期并集(模板口径)。返回 None = 不限制(无模板/全空, 回落全集)。

    [2026-10-09 用户裁定] 周期口径统一为"模板并集":
      * 所有模板都没勾某周期 -> 该周期不监控、也不下发;
      * 但凡有一个模板勾了   -> 该周期监控且下发;
      * 返回 None(无模板, 或全部模板都没勾任何周期) -> 调用方按"不限制"处理(兼容旧兜底)。
    """
    try:
        return scan_template_db.monitor_periods()
    except Exception as e:
        print("[监控] 读取模板周期并集失败, 回落不限制:", e)
        return None


def _filter_periods_payload(results, allowed=None):
    """按模板周期并集裁剪 {period: {...}} 载荷, 只保留允许的周期键。

    [2026-10-09 用户裁定] 即使库里/内存里已有某周期信号(历史遗留, 或运行期模板未勾前已起),
    只要当前模板并集里没有该周期, 就不许传给前端。
    """
    if not isinstance(results, dict) or allowed is None:
        return results
    allow = set(allowed)
    return {p: v for p, v in results.items() if p in allow}


def api_scan_multi_latest():
    """返回【本盘累积】结果(开盘->当前时间, 按触发时间排序)供前端轮询渲染。

    ?period=X 取单周期; ?order=asc 改为从开盘正序(默认 desc 最新在前)。
    盘结束(日盘过15:00 / 夜盘过02:30)自动清空显示, 数据已在库, 到历史回溯查看。
    [2026-10-09] 结果按模板周期并集过滤: 模板没勾的周期不下发(运行期已起也拦掉)。
    """
    mon = _get_multi_monitor()
    period = request.args.get("period", "")
    order = (request.args.get("order", "desc") or "desc").lower()
    sess = mon.session_info()
    allowed = _allowed_periods_or_none()
    if period:
        # 指定单周期也守并集: 模板没勾该周期 -> 直接返回空
        if allowed is not None and period not in set(allowed):
            return jsonify({"ok": True, "period": period, "result": None, "session": sess})
        return jsonify({"ok": True, "period": period,
                        "result": mon.session_latest(period, order=order),
                        "session": sess})
    results = _filter_periods_payload(mon.session_latest(order=order), allowed)
    return jsonify({"ok": True, "results": results, "session": sess})


def api_scan_multi_today():
    """【当天(交易日)】信号视图: 库中当天已落库 ∪ 内存实时累积，供「实时」tab 轮询。

    与 /multi/latest 的区别: latest 是"本盘"（收盘即清空），today 覆盖整个交易日
    [前一天21:00, 当天15:00]，休市也看得到当天信号；次日自动切到新交易日。
    [2026-10-09] 结果按模板周期并集过滤: 模板没勾的周期不下发(运行期已起也拦掉)。
    """
    mon = _get_multi_monitor()
    period = request.args.get("period", "")
    order = (request.args.get("order", "desc") or "desc").lower()
    auto = auto_monitor_state()
    # 2026-09-24 用户需求: 系统无未到期天勤账号 -> 不给看监控数据(列表置空, 前端有提示条)
    if auto.get("no_valid_account"):
        empty = {"ok": True, "period": period or "day", "long": [], "short": [],
                 "stats": {"long_count": 0, "short_count": 0}, "no_valid_account": True}
        if period:
            return jsonify({"ok": True, "period": period, "result": empty, "auto": auto})
        return jsonify({"ok": True, "results": {}, "auto": auto})
    # [2026-10-09] 周期并集随载荷下发, 前端头部"覆盖 N 周期"直接用, 避免二次读库
    allowed = _allowed_periods_or_none()
    if allowed is not None:
        auto["periods_allowed"] = list(allowed)
    if period:
        if allowed is not None and period not in set(allowed):
            return jsonify({"ok": True, "period": period,
                            "result": {"long": [], "short": [],
                                       "stats": {"long_count": 0, "short_count": 0}},
                            "auto": auto})
        return jsonify({"ok": True, "period": period,
                        "result": mon.trading_day_view(period, order=order),
                        "auto": auto})
    results = _filter_periods_payload(mon.trading_day_view(order=order), allowed)
    return jsonify({"ok": True, "results": results, "auto": auto})


def api_scan_auto():
    """自动监控状态；?pause=1/0 暂停(到下一交易日)/恢复，?enabled=1/0 切总开关。"""
    raw_e = request.args.get("enabled", request.form.get("enabled"))
    raw_p = request.args.get("pause", request.form.get("pause"))
    if raw_e is None and raw_p is None:
        return jsonify({"ok": True, "auto": auto_monitor_state()})
    enabled = None if raw_e in (None, "") else (str(raw_e).lower() in ("1", "true", "yes", "on"))
    pause = None if raw_p in (None, "") else (str(raw_p).lower() in ("1", "true", "yes", "on"))
    return jsonify({"ok": True, "auto": set_auto_monitor(enabled=enabled, pause=pause)})


def api_scan_templates():
    """返回扫描规则模板和默认规则 + 用户保存的扫描模板列表。"""
    try:
        user_templates = scan_template_db.list_templates()
    except Exception as e:
        user_templates = []
        print(f"[api_scan_templates] 加载用户模板失败: {e}")
    return jsonify({
        "ok": True,
        "default_rules": scan_engine.DEFAULT_RULES,
        "templates": scan_engine.OPTIONAL_RULE_TEMPLATES,
        "user_templates": user_templates,
    })


def api_scan_template_save():
    """保存当前扫描配置为一个命名模板。"""
    try:
        name = (request.form.get("name") or request.args.get("name", "")).strip()
        tid = request.form.get("id") or request.args.get("id", "")
        periods = _parse_periods(request.form.get("periods") or request.args.get("periods", ""))
        raw_rules = request.form.get("rules") or request.args.get("rules", "")
        rules = json.loads(raw_rules) if raw_rules else []
        raw_products = request.form.get("products") or request.args.get("products", "")
        products = [p.strip() for p in raw_products.split(",") if p.strip()] if raw_products else []
        pri_raw = request.form.get("priority") or request.args.get("priority")
        priority = None
        if pri_raw is not None and str(pri_raw).strip() != "":
            try:
                priority = int(float(pri_raw))
            except Exception:
                priority = None
        result = scan_template_db.save_template(
            name=name,
            periods=periods,
            rules=rules,
            products=products,
            tid=int(tid) if tid and str(tid).isdigit() else None,
            priority=priority,
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": f"保存失败: {e}"}), 500


def api_kline_settings():
    """读取/保存「计算根数 / 取值根数」设置(2026-10-13 用户拍板, 仅期货监控线)。

    GET  -> 返回全量设置 + 校验 + 派生周期换算预览(前端设置卡片直接渲染)
    POST -> 保存(与本模块其它接口同款: GET 也能保存, 兼容前端 form 提交)
    """
    try:
        from services import kline_settings as _ks
        if request.method == "GET" and not (request.args.get("calc_bars") or
                                           request.args.get("take_bars")):
            return jsonify(_ks.describe())

        calc_raw = request.form.get("calc_bars") or request.args.get("calc_bars")
        take_raw = request.form.get("take_bars") or request.args.get("take_bars")
        calc = take = None
        if calc_raw:
            try:
                calc = {k: int(v) for k, v in json.loads(calc_raw).items()}
            except Exception:
                calc = None
        if take_raw:
            try:
                take = {k: int(v) for k, v in json.loads(take_raw).items()}
            except Exception:
                take = None
        if calc is None and take is None:
            return jsonify(_ks.describe())
        out = _ks.set_bars(calc_bars=calc, take_bars=take)
        out["saved"] = True
        return jsonify(out)
    except Exception as e:
        return jsonify({"ok": False, "error": f"保存失败: {e}"}), 500


def api_kline_settings_reset():
    """恢复标准默认值。"""
    try:
        from services import kline_settings as _ks
        out = _ks.set_bars(
            calc_bars={p: _ks.DEFAULT_CALC_BARS[p] for p in _ks.PERIODS},
            take_bars={p: _ks.DEFAULT_TAKE_BARS[p] for p in _ks.PERIODS})
        out["saved"] = True
        return jsonify(out)
    except Exception as e:
        return jsonify({"ok": False, "error": f"恢复失败: {e}"}), 500


def api_scan_template_move(tid):
    """上移/下移模板（调整优先级顺序）。direction=up|down。"""
    try:
        direction = (request.args.get("direction") or request.form.get("direction") or "up").strip()
        return jsonify(scan_template_db.move_template(tid, direction))
    except Exception as e:
        return jsonify({"ok": False, "error": f"移动失败: {e}"}), 500


def api_scan_template_load(tid):
    """按 ID 加载完整模板。"""
    try:
        tpl = scan_template_db.load_template(tid)
        if not tpl:
            return jsonify({"ok": False, "error": "模板不存在"}), 404
        return jsonify({"ok": True, "template": tpl})
    except Exception as e:
        return jsonify({"ok": False, "error": f"加载失败: {e}"}), 500


def api_scan_template_delete(tid):
    """删除指定模板。"""
    try:
        result = scan_template_db.delete_template(tid)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": f"删除失败: {e}"}), 500


def api_scan_schedule_save():
    """新增/更新定时爬取任务。
    POST: name, template_id(可空), start_hm(HH:MM), end_hm(HH:MM),
          days(逗号0-6或位掩码), enabled(0/1), sid(可选=更新)。"""
    try:
        name = (request.form.get("name") or request.args.get("name") or "").strip()
        template_id = (request.form.get("template_id") or request.args.get("template_id") or "").strip()
        start_hm = (request.form.get("start_hm") or request.args.get("start_hm") or "09:00").strip()
        end_hm = (request.form.get("end_hm") or request.args.get("end_hm") or "15:00").strip()
        days_raw = (request.form.get("days") or request.args.get("days") or "0,1,2,3,4,5,6").strip()
        enabled = request.form.get("enabled") or request.args.get("enabled")
        sid = (request.form.get("sid") or request.args.get("sid") or "").strip()
        # days 解析: 逗号列表 -> 位掩码(Mon=1<<0); 纯数字 -> 直接用
        days_bit = 127
        if "," in days_raw or " " in days_raw:
            days_bit = 0
            for p in days_raw.replace(" ", "").split(","):
                p = p.strip()
                if p.isdigit():
                    days_bit |= (1 << int(p))
        elif days_raw.isdigit():
            days_bit = int(days_raw)
        en = 1
        if enabled is not None:
            en = 1 if str(enabled) in ("1", "true", "True", "on") else 0
        tid = int(template_id) if template_id else None
        sid_int = int(sid) if sid else None
        result = scan_schedule_db.save_schedule(
            name=name, template_id=tid, start_hm=start_hm, end_hm=end_hm,
            days_bit=days_bit, enabled=en, sid=sid_int
        )
        # 保存后立即评估一次, 让启停即时反映
        try:
            scan_engine._run_schedule_once()
        except Exception:
            pass
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": f"保存失败: {e}"}), 500


def api_scan_schedule_list():
    """返回所有定时任务 + 当前北京时间 + 各任务运行状态。"""
    try:
        scheds = scan_schedule_db.list_schedules()
        running = scan_engine.schedule_status()
        now = None
        try:
            import datetime as _dt
            now = _dt.datetime.now(_dt.timezone(_dt.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            now = None
        for s in scheds:
            s["running"] = bool(running.get(str(s["id"]), False))
        return jsonify({"ok": True, "schedules": scheds, "now": now})
    except Exception as e:
        return jsonify({"ok": False, "error": f"查询失败: {e}"}), 500


def api_scan_schedule_delete(sid):
    try:
        # 先停掉运行中的 monitor
        try:
            scan_engine._SCHED_MONITORS.pop(int(sid), None)
        except Exception:
            pass
        return jsonify(scan_schedule_db.delete_schedule(sid))
    except Exception as e:
        return jsonify({"ok": False, "error": f"删除失败: {e}"}), 500


def api_scan_schedule_toggle(sid):
    enabled = request.args.get("enabled") or request.form.get("enabled")
    try:
        if enabled is not None:
            result = scan_schedule_db.toggle_schedule(
                sid, 1 if str(enabled) in ("1", "true", "True", "on") else 0)
        else:
            result = scan_schedule_db.toggle_schedule(sid)
        # 立即评估一次, 让启停即时生效
        try:
            scan_engine._run_schedule_once()
        except Exception:
            pass
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": f"切换失败: {e}"}), 500


def api_scan_history():
    """查询历史扫描信号。
    GET 参数:
      start, end: 扫描时间范围(YYYY-MM-DD 或 YYYY-MM-DD HH:MM)
      period: 周期(day/60m/30m/15m/5m/1m)
      symbol: 合约代码(模糊匹配 symbol/real_symbol)
      side: long/short
      signal_type: 信号类型(macd_golden/macd_death/top_divergence/...)
      limit, offset: 分页
    """
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    period = request.args.get("period", "")
    symbol = request.args.get("symbol", "")
    side = request.args.get("side", "")
    signal_type = request.args.get("signal_type", "")
    limit = request.args.get("limit", type=int) or 200
    offset = request.args.get("offset", type=int) or 0
    limit = max(1, min(1000, int(limit)))
    offset = max(0, int(offset))
    try:
        result = scan_history_db.query_history(
            start=start, end=end, period=period, symbol=symbol,
            side=side, signal_type=signal_type, limit=limit, offset=offset
        )
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": f"查询失败: {e}", "trace": traceback.format_exc()}), 500


def api_scan_history_stats():
    """统计聚合: 在同样筛选条件下返回汇总与分布(供历史复盘页统计看板)。"""
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    period = request.args.get("period", "")
    symbol = request.args.get("symbol", "")
    side = request.args.get("side", "")
    signal_type = request.args.get("signal_type", "")
    try:
        stats = scan_history_db.query_stats(
            start=start, end=end, period=period, symbol=symbol,
            side=side, signal_type=signal_type
        )
        return jsonify({"ok": True, **stats})
    except Exception as e:
        return jsonify({"ok": False, "error": f"统计失败: {e}", "trace": traceback.format_exc()}), 500


def scan_settings_page():
    """悬浮监控模板设置页（单独模块）。

    在这里集中管理扫描模板：新建/编辑/复制/删除，以及调整优先级（↑/↓）。
    模板按 (priority DESC, create_time DESC) 排序，即“级别高的、后创建的放前面”。
    悬浮面板只显示模板名，指标/周期设置在面板内折叠；完整编辑在此页完成。
    """
    _porder = ["1m", "5m", "15m", "30m", "60m", "day", "week", "month", "quarter"]
    _pchecks = ""
    for _pk in _porder:
        _pc = PERIODS.get(_pk)
        if not _pc:
            continue
        _pchecks += '<label class="fk-set-pchk"><input type="checkbox" class="fk-period-cb" value="%s">%s</label>' % (_pk, _pc["label"])

    # 品种白名单：按交易所分组生成勾选列表（代码格式 EX.PC，如 CZCE.MA）
    _EX_LABELS = {"SHFE": "上期所", "INE": "上期能源", "DCE": "大商所",
                  "CZCE": "郑商所", "GFEX": "广期所", "CFFEX": "中金所"}
    _ex_order = ["SHFE", "INE", "DCE", "CZCE", "GFEX", "CFFEX"]
    _prod_groups = {}
    try:
        for _sym, _ex, _pc, _nm in _get_main_contracts():
            _prod_groups.setdefault(_ex, []).append((_pc, _nm))
    except Exception:
        _prod_groups = {}
    _products_html = '<div class="fk-set-prod-scroll">'
    for _ex in _ex_order:
        if _ex not in _prod_groups:
            continue
        _ex_label = _EX_LABELS.get(_ex, _ex)
        _products_html += ('<div class="fk-set-prod-group" data-ex="%s">'
                           '<label class="fk-set-prod-ex"><input type="checkbox" class="fk-prod-ex-cb" onchange="fkToggleEx(this)"> %s</label>'
                           '<div class="fk-set-prod-items">' % (_ex, _ex_label))
        for _pc, _nm in sorted(_prod_groups[_ex], key=lambda kv: kv[1]):
            _code = "%s.%s" % (_ex, _pc)
            _products_html += ('<label class="fk-set-prod-item"><input type="checkbox" class="fk-prod-cb" value="%s"> %s</label>' % (_code, _nm))
        _products_html += '</div></div>'
    for _ex in _prod_groups:
        if _ex in _ex_order:
            continue
        _products_html += ('<div class="fk-set-prod-group" data-ex="%s">'
                           '<label class="fk-set-prod-ex"><input type="checkbox" class="fk-prod-ex-cb" onchange="fkToggleEx(this)"> %s</label>'
                           '<div class="fk-set-prod-items">' % (_ex, _ex))
        for _pc, _nm in sorted(_prod_groups[_ex], key=lambda kv: kv[1]):
            _code = "%s.%s" % (_ex, _pc)
            _products_html += ('<label class="fk-set-prod-item"><input type="checkbox" class="fk-prod-cb" value="%s"> %s</label>' % (_code, _nm))
        _products_html += '</div></div>'
    _products_html += '</div>'

    page = (
        '<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><title>期货监控 · 模板设置</title>'
        + _FK_STYLE
        + '''<style>
      :root{--bg:#0b1120;--surface:#111827;--surface2:#1b2538;--border:#1e2d4a;--text:#e6edf7;--muted:#94a3b8;--accent:#38bdf8;--accent2:#f472b6;--violet:#c084fc;--up:#ef4444;--down:#19c37d;}
      .fk-set-wrap{max-width:1400px;margin:0 auto;padding:18px;color:var(--text)}
      .fk-set-head{display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:16px}
      .fk-set-back{padding:5px 12px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:6px;text-decoration:none;font-size:13px}
      .fk-set-back:hover{border-color:var(--accent);color:var(--accent)}
      .fk-set-head h2{margin:0;font-size:18px}
      .fk-set-hint{color:var(--muted);font-size:12px}
      .fk-set-tabbar{display:flex;gap:4px;margin:0 0 16px;border-bottom:1px solid var(--border)}
      .fk-set-tab{padding:9px 20px;background:transparent;border:none;border-bottom:2px solid transparent;color:var(--muted);font-size:14px;font-weight:600;cursor:pointer;transition:color .15s,border-color .15s}
      .fk-set-tab:hover{color:var(--text)}
      .fk-set-tab.active{color:var(--accent);border-bottom-color:var(--accent)}
      .fk-set-panel{display:block}
      .fk-set-grid{display:grid;grid-template-columns:minmax(260px,300px) minmax(420px,1fr) minmax(200px,260px);gap:16px;align-items:start}
      .fk-set-card{background:#0b1220;border:1px solid var(--border);border-radius:10px;overflow:hidden}
      .fk-set-card-title{padding:10px 14px;background:#0d1524;border-bottom:1px solid var(--border);font-size:13px;font-weight:600;color:var(--text);display:flex;align-items:center;gap:10px}
      .fk-set-card-title .fk-set-add{margin-left:auto;padding:4px 12px;font-size:12px;font-weight:600;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:5px;cursor:pointer}
      .fk-set-card-title .fk-set-add:hover{border-color:var(--accent);color:var(--accent);background:#16273a}
      .fk-set-row.active{background:#132033;box-shadow:inset 3px 0 0 var(--accent)}
      .fk-set-list{padding:8px}
      .fk-set-empty{padding:24px;text-align:center;color:var(--muted);font-size:13px}
      .fk-set-row{display:flex;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid #16202e}
      .fk-set-row:last-child{border-bottom:none}
      .fk-set-row-order{display:flex;flex-direction:column;gap:2px;flex:none}
      .fk-set-row-order button{width:24px;height:20px;font-size:11px;line-height:1;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:4px;cursor:pointer}
      .fk-set-row-order button:hover{border-color:var(--accent);color:var(--accent)}
      .fk-set-row-main{flex:1;min-width:0;cursor:pointer}
      .fk-set-row-name{font-size:14px;font-weight:600;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
      .fk-set-row-meta{font-size:11px;color:var(--muted);margin-top:2px}
      .fk-set-row-acts{display:flex;gap:6px;flex:none}
      .fk-set-row-acts .mini{padding:4px 10px;font-size:12px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:5px;cursor:pointer}
      .fk-set-row-acts .mini:hover{border-color:var(--accent);color:var(--accent)}
      .fk-set-row-acts .mini.danger:hover{border-color:#ef4444;color:#ef4444}
      .fk-set-edit-body{padding:14px}
      .fk-set-edit-body label{display:block;margin:12px 0 5px;font-size:12px;color:var(--muted)}
      .fk-set-edit-body input[type=text],.fk-set-edit-body input:not([type]){width:100%;padding:7px 10px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:5px;font-size:13px;box-sizing:border-box}
      .fk-set-periods{display:flex;flex-wrap:wrap;gap:8px}
      .fk-set-periods .fk-set-pchk{display:inline-flex;align-items:center;gap:4px;font-size:13px;color:var(--text);cursor:pointer}
      .fk-scan-rules{padding:8px;background:#0b1220;border:1px solid #16202e;border-radius:6px;margin-top:6px;max-height:none;overflow:auto}
      .fk-set-edit-actions{display:flex;gap:10px;margin-top:16px}
      .fk-set-edit-actions button{padding:8px 18px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;border:none}
      #fkSetSaveBtn{background:var(--accent);color:#05101c}
      .fk-set-edit-actions .ghost{background:#13202f;border:1px solid #2a3a4d;color:var(--text)}
      .fk-set-edit-actions .ghost:hover{border-color:var(--accent);color:var(--accent)}
      .fk-set-msg{margin-top:10px;font-size:12px;color:#ef4444;min-height:16px}
      .fk-set-msg.ok{color:#19c37d}
      /* K线根数设置卡片：整行全宽突出，低于阈值只标黄不拦截(用户 2026-10-13 裁定) */
      .fk-set-grid>.fk-set-card.fk-kls-card{grid-column:1 / -1}
      .fk-kls-card .fk-set-card-title .fk-kls-badge{margin-left:auto;padding:3px 12px;font-size:11px;font-weight:600;color:#05101c;background:var(--accent);border-radius:20px}
      .fk-kls-intro{padding:12px 14px;background:#0d1524;border-bottom:1px solid var(--border);font-size:12px;line-height:1.95;color:var(--muted)}
      .fk-kls-intro b{color:var(--text)}
      .fk-kls-intro .hl{color:#f59e0b}
      table.fk-kls-table{width:100%;border-collapse:collapse;font-size:13px;margin:0}
      table.fk-kls-table thead th{padding:11px 14px;text-align:left;color:var(--text);font-weight:600;background:#0d1524;border-bottom:2px solid var(--accent)}
      table.fk-kls-table tbody td{padding:10px 14px;border-bottom:1px solid var(--border);vertical-align:middle}
      table.fk-kls-table tbody tr:hover{background:#0d1524}
      .fk-kls-per{font-weight:600;color:var(--text)}
      .fk-kls-per small{color:var(--muted);font-weight:400;margin-left:4px}
      .fk-kls-input{display:flex;align-items:center;gap:8px}
      table.fk-kls-table input[type=number]{width:92px;padding:8px 8px;background:var(--surface2,#1f2937);border:1px solid var(--border,#27344f);color:var(--text);border-radius:6px;font-size:13px;text-align:center}
      table.fk-kls-table input.warn{color:#f59e0b;border-color:#f59e0b;box-shadow:inset 0 0 0 1px #f59e0b}
      .fk-kls-unit{color:var(--muted);font-size:11px;white-space:nowrap}
      .fk-kls-warncell{font-size:11px;color:#94a3b8;line-height:1.6}
      .fk-kls-warncell .bad{color:#f59e0b;display:block;margin-top:4px}
      .fk-kls-q{position:relative;display:inline-block;width:15px;height:15px;line-height:14px;text-align:center;border:1px solid var(--muted);border-radius:50%;color:var(--muted);font-size:10px;cursor:help;margin-left:5px;user-select:none;vertical-align:middle}
      .fk-kls-q:hover{color:var(--accent);border-color:var(--accent)}
      .fk-kls-q .fk-kls-tip{display:none;position:absolute;left:18px;top:-6px;width:300px;background:#1f2937;border:1px solid var(--border);border-radius:6px;padding:10px 12px;color:var(--text);font-size:11px;line-height:1.7;z-index:50;box-shadow:0 8px 22px rgba(0,0,0,.5);font-weight:400;text-align:left;white-space:normal}
      .fk-kls-q:hover .fk-kls-tip{display:block}
      .fk-kls-actions{padding:13px 14px;display:flex;gap:10px;align-items:center;background:#0b1220;border-top:1px solid var(--border)}
      .fk-kls-actions button{padding:8px 18px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;border:none;background:var(--accent);color:#05101c}
      .fk-kls-actions .ghost{background:#13202f;border:1px solid #2a3a4d;color:var(--text)}
      .fk-kls-actions .ghost:hover{border-color:var(--accent);color:var(--accent)}
      .fk-kls-warnbox{margin:0;padding:12px 14px;background:#1a1407;border-top:1px solid #3a2a08;font-size:11px;color:#f59e0b;line-height:1.8}
      .fk-set-prod-scroll{max-height:calc(100vh - 200px);overflow-y:auto;padding:10px 12px}
      .fk-set-prod-group{margin-bottom:10px}
      .fk-set-prod-ex{display:flex;align-items:center;gap:6px;font-size:13px;font-weight:600;color:var(--text);margin-bottom:5px;cursor:pointer}
      .fk-set-prod-items{display:grid;grid-template-columns:1fr;gap:4px 8px;padding-left:8px}
      .fk-set-prod-item{display:flex;align-items:center;gap:4px;font-size:12px;color:var(--muted);cursor:pointer;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
      .fk-set-prod-item:hover{color:var(--text)}
      .fk-set-prod-item input{flex:none}
      .fk-set-prod-hint{font-size:11px;color:var(--muted);padding:8px 12px;border-bottom:1px solid #16202e}
      @media(max-width:1260px){.fk-set-grid{grid-template-columns:minmax(260px,300px) 1fr}}
      @media(max-width:1260px){.fk-set-grid>.fk-set-card.fk-kls-card{grid-column:1 / -1;max-height:none}}
      @media(max-width:880px){.fk-set-grid{grid-template-columns:1fr}}
      @media(max-width:880px){.fk-set-grid>.fk-set-card.fk-kls-card{grid-column:auto;max-height:none}}
    </style></head><body>
    <div class="fk-set-wrap">
      <div class="fk-set-head">
        <a class="fk-set-back" id="fkSetBack" href="#">← 返回行情</a>
        <h2>悬浮监控 · 模板设置</h2>
        <span class="fk-set-hint">设置好的模板带优先级，级别高的、后创建的排在前面；悬浮面板只显示模板名，指标/周期在面板内折叠</span>
      </div>
      <div class="fk-set-tabbar">
        <button type="button" class="fk-set-tab active" data-tab="tpl" onclick="fkSetTab('tpl')">模板设置</button>
        <button type="button" class="fk-set-tab" data-tab="kls" onclick="fkSetTab('kls')">数据限制</button>
      </div>
      <div class="fk-set-panel" id="fkTabTpl">
      <div class="fk-set-grid">
        <div class="fk-set-card">
          <div class="fk-set-card-title">模板列表（按优先级）<button class="fk-set-add" onclick="fkSetNew()" title="新建一个空白模板">＋ 新增</button></div>
          <div id="fkSetList" class="fk-set-list">加载中…</div>
        </div>
        <div class="fk-set-card">
          <div class="fk-set-card-title"><span id="fkSetEditTitle">新建模板</span></div>
          <div class="fk-set-edit-body">
            <label>模板名称</label>
            <input id="fkSetName" type="text" placeholder="如 MACD超短盘前扫描">
            <label>监控周期</label>
            <div class="fk-set-periods" id="fkSetPeriods">'''
        + _pchecks
        + '''</div>
            <label>指标规则（勾选并填参数，与悬浮面板一致）</label>
            <div class="fk-scan-rules" id="fkScanRules">加载规则…</div>
            <div class="fk-set-edit-actions">
              <button id="fkSetSaveBtn" onclick="fkSetSave()">保存模板</button>
              <button class="ghost" onclick="fkSetNew()">清空新建</button>
            </div>
            <div id="fkSetMsg" class="fk-set-msg"></div>
          </div>
        </div>
        <div class="fk-set-card">
          <div class="fk-set-card-title">监控品种（勾选=只监控这些）</div>
          <div class="fk-set-prod-hint">默认不勾=监控全部主力合约</div>
          <div id="fkSetProducts">''' + _products_html + '''</div>
        </div>
      </div>
      </div>
      <div class="fk-set-panel" id="fkTabKls" style="display:none">
      <div class="fk-set-card fk-kls-card">
          <div class="fk-set-card-title">K线根数设置<span class="fk-kls-badge">信号计算口径</span></div>
          <div class="fk-kls-intro" id="fkKlsNote">加载中…</div>
          <table class="fk-kls-table">
            <thead><tr>
              <th style="width:150px">周期</th>
              <th style="width:210px">计算根数<span class="fk-kls-q" id="fkKlsQCalc">?<span class="fk-kls-tip" id="fkKlsTipCalc"></span></span></th>
              <th style="width:210px">取值根数<span class="fk-kls-q" id="fkKlsQTake">?<span class="fk-kls-tip" id="fkKlsTipTake"></span></span></th>
              <th>实际可用 / 说明</th>
            </tr></thead>
            <tbody id="fkKlsBody"><tr><td colspan="4" style="color:#94a3b8">加载中…</td></tr></tbody>
          </table>
          <div class="fk-kls-actions">
            <button onclick="fkKlsSave()">保存根数设置</button>
            <button class="ghost" onclick="fkKlsReset()">恢复默认</button>
            <span id="fkKlsMsg" class="fk-set-msg"></span>
          </div>
          <div class="fk-kls-warnbox" id="fkKlsWarn"></div>
        </div>
      </div>
    </div>
    <script>
    var FK_SCAN={rules:[]};
    var FK_SET_EDIT_ID=null;
    function fkSetTab(name){
      document.querySelectorAll('.fk-set-tab').forEach(function(b){ b.classList.toggle('active', b.getAttribute('data-tab')===name); });
      var t=document.getElementById('fkTabTpl'), k=document.getElementById('fkTabKls');
      if(t) t.style.display=(name==='tpl')?'':'none';
      if(k) k.style.display=(name==='kls')?'':'none';
      if(name==='kls' && typeof fkKlsLoad==='function') fkKlsLoad();
    }
    function fkApiBase(){
      var p=location.pathname;
      var i=p.indexOf('/futureskline/');
      return i>=0 ? p.substring(0,i)+'/futureskline/' : '/futureskline/';
    }
    function fkHtml(s){return String(s).replace(/[&<>]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c];});}
    function fkSyncMacdBandFromRules(){}
    function fkRenderScanRules(){
      var h='';
      FK_SCAN.rules.forEach(function(r,i){
        var checked=r.enabled?'checked':'';
        h+='<div class="fk-scan-rule">';
        h+='<input type="checkbox" id="fkRule_'+i+'" data-idx="'+i+'" '+checked+'>';
        h+='<span class="fk-rule-name">'+fkHtml(r.name)+'</span>';
        h+='<span class="fk-rule-params">';
        if(r.type==='macd_cross'){
          var zl=(r.params.zero_line!=null)?r.params.zero_line:'0';
          var th=(r.params.threshold!=null)?r.params.threshold:'';
          var op=(r.params.op!=null)?r.params.op:'gt';
          var ip=r.indicator_params||{};
          var macd_fast=(ip.fast!=null)?ip.fast:12;
          var macd_slow=(ip.slow!=null)?ip.slow:26;
          var macd_signal=(ip.signal!=null)?ip.signal:9;
          h+='<label title="MACD快线周期(默认12)">快线</label><input type="number" data-key="indicator_fast" value="'+macd_fast+'" style="width:42px">';
          h+='<label title="MACD慢线周期(默认26)">慢线</label><input type="number" data-key="indicator_slow" value="'+macd_slow+'" style="width:42px">';
          h+='<label title="MACD信号周期(DEA, 默认9)">信号</label><input type="number" data-key="indicator_signal" value="'+macd_signal+'" style="width:42px">';
          h+='<label title="零线(DIF/DEA基准): 默认0">零线</label><input type="number" step="0.1" data-key="zero_line" value="'+zl+'" placeholder="0">';
          h+='<select data-key="op" title="比较算子">'+
               '<option value="gt"'+((op==='gt')?' selected':'')+'>大于 &gt;</option>'+
               '<option value="ge"'+((op==='ge')?' selected':'')+'>&ge; 大于等于</option>'+
               '<option value="lt"'+((op==='lt')?' selected':'')+'>小于 &lt;</option>'+
               '<option value="le"'+((op==='le')?' selected':'')+'>&le; 小于等于</option>'+
             '</select>';
          h+='<label title="比较阈值: 留空=不过滤(显示全部金/死叉)">阈值</label><input type="number" step="0.1" data-key="threshold" value="'+th+'" placeholder="留空=全部">';
          var tmn=(r.params.trend_ma_n!=null)?r.params.trend_ma_n:60;
          var tlkb=(r.params.trend_lookback!=null)?r.params.trend_lookback:10;
          var tmode=(r.params.trend_mode!=null)?r.params.trend_mode:'match';
          h+='<label title="趋势判定均线周期(慢速)">趋势均线</label><input type="number" data-key="trend_ma_n" value="'+tmn+'" style="width:42px">';
          h+='<label title="趋势判定窗口">趋势窗口</label><input type="number" data-key="trend_lookback" value="'+tlkb+'" style="width:42px">';
          h+='<select data-key="trend_mode" title="交叉与趋势判定模式">'+
               '<option value="trend"'+((tmode==='trend')?' selected':'')+'>趋势驱动</option>'+
               '<option value="match"'+((tmode==='match')?' selected':'')+'>方向匹配</option>'+
             '</select>';
        }
        if(r.type!=='macd_cross' && r.params && r.params.threshold!=null) h+='<label>阈值</label><input type="number" step="1" data-key="threshold" value="'+r.params.threshold+'">';
        if(r.params && r.params.recent_bars!=null) h+='<label>近N根</label><input type="number" data-key="recent_bars" value="'+r.params.recent_bars+'">';
        if(r.params && r.params.n!=null) h+='<label>周期</label><input type="number" data-key="n" value="'+r.params.n+'">';
        h+='</span></div>';
      });
      var box=document.getElementById('fkScanRules');
      if(box) box.innerHTML=h||'暂无规则';
    }
    function fkCollectRules(){
      var rules=[];
      FK_SCAN.rules.forEach(function(r,i){
        var cb=document.getElementById('fkRule_'+i);
        var rule=JSON.parse(JSON.stringify(r)); rule.enabled=cb&&cb.checked;
        var row=cb?cb.parentElement:null;
        if(row){
          if(!rule.indicator_params) rule.indicator_params={};
          var inputs=row.querySelectorAll('input[data-key], select[data-key]');
          inputs.forEach(function(inp){
            var k=inp.getAttribute('data-key'), v=inp.value;
            if(k.indexOf('indicator_')===0){
              var ik=k.substring(10);
              rule.indicator_params[ik]= (v==='')?null:(v.indexOf('.')>=0?parseFloat(v):parseInt(v,10));
            } else {
              if(inp.tagName==='SELECT'){ rule.params[k]=v; }
              else if(v===''){ rule.params[k]=null; }
              else if(v.indexOf('.')>=0) rule.params[k]=parseFloat(v); else rule.params[k]=parseInt(v,10);
            }
          });
        }
        rules.push(rule);
      });
      return rules;
    }
    function fkSetSelectedPeriods(){
      var cbs=document.querySelectorAll('#fkSetPeriods .fk-period-cb');
      var out=[];
      cbs.forEach(function(cb){ if(cb.checked) out.push(cb.value); });
      return out;
    }
    function fkCollectProducts(){
      var out=[];
      document.querySelectorAll('.fk-prod-cb:checked').forEach(function(cb){ out.push(cb.value); });
      return out;
    }
    function fkSetApplyProducts(tpl){
      var pMap={}; (tpl.products||[]).forEach(function(p){ pMap[p]=true; });
      document.querySelectorAll('.fk-prod-cb').forEach(function(cb){ cb.checked=!!pMap[cb.value]; });
      // 同步更新交易所级全选框
      document.querySelectorAll('.fk-set-prod-group').forEach(function(g){
        var items=g.querySelectorAll('.fk-prod-cb');
        var checked=g.querySelectorAll('.fk-prod-cb:checked');
        var exCb=g.querySelector('.fk-prod-ex-cb');
        if(exCb) exCb.checked = items.length>0 && checked.length===items.length;
      });
    }
    function fkToggleEx(cb){
      var g=cb.closest('.fk-set-prod-group'); if(!g) return;
      g.querySelectorAll('.fk-prod-cb').forEach(function(item){ item.checked=cb.checked; });
    }
    function fkSetApplyTemplate(tpl){
      if(!tpl||!tpl.rules) return;
      var pMap={}; (tpl.periods||[]).forEach(function(p){pMap[p]=true;});
      document.querySelectorAll('#fkSetPeriods .fk-period-cb').forEach(function(cb){ cb.checked=!!pMap[cb.value]; });
      fkSetApplyProducts(tpl);
      var ruleMap={}; (tpl.rules||[]).forEach(function(r){ruleMap[r.id]=r;});
      FK_SCAN.rules.forEach(function(r,i){
        if(ruleMap[r.id]){ FK_SCAN.rules[i]=ruleMap[r.id]; delete ruleMap[r.id]; }
      });
      Object.keys(ruleMap).forEach(function(k){ FK_SCAN.rules.push(ruleMap[k]); });
      fkRenderScanRules();
    }
    function fkSetMsg(msg,ok){ var m=document.getElementById('fkSetMsg'); if(!m)return; m.textContent=msg; m.className='fk-set-msg'+(ok?' ok':''); }
    // [2026-10-09 用户裁定] 顶部「＋ 新增」按钮 + 原"清空新建"按钮 统一走本函数:
    // 进入【新增模式】——编辑面板清空(名称/周期/规则/品种全不勾), 标题回到"新建模板",
    // 列表高亮取消。保存时因 FK_SET_EDIT_ID=null 会新建而非覆盖。
    function fkSetNew(){
      FK_SET_EDIT_ID=null;
      var n=document.getElementById('fkSetName'); if(n)n.value='';
      var t=document.getElementById('fkSetEditTitle'); if(t)t.textContent='新建模板';
      var sb=document.getElementById('fkSetSaveBtn'); if(sb)sb.textContent='保存模板';
      // 周期全不勾
      document.querySelectorAll('#fkSetPeriods .fk-period-cb').forEach(function(cb){cb.checked=false;});
      // 品种/交易所全不勾
      document.querySelectorAll('.fk-prod-cb').forEach(function(cb){cb.checked=false;});
      document.querySelectorAll('.fk-prod-ex-cb').forEach(function(cb){cb.checked=false;});
      // 规则回到"默认规则集"且全部不勾(还原成新建态, 不残留上一个模板的规则与参数)
      if(FK_SCAN.defaultRules) fkSetRenderRules(FK_SCAN.defaultRules);
      fkSetMarkActive(null);
      fkSetMsg('新增模板：填写后点「保存模板」',true);
    }
    // 列表高亮当前正在编辑的模板(新增模式下不高亮任何行)
    function fkSetMarkActive(tid){
      document.querySelectorAll('#fkSetList .fk-set-row').forEach(function(row){
        row.classList.toggle('active', String(tid)===row.getAttribute('data-id'));
      });
    }
    // 用一份完整规则集重渲染规则区(新增模式复位用)
    function fkSetRenderRules(rules){
      FK_SCAN.rules = rules||[];
      fkRenderScanRules();
    }
    function fkSetReset(){ fkSetNew(); }
    function fkSetRenderList(tpls){
      var box=document.getElementById('fkSetList'); if(!box)return;
      if(!tpls.length){ box.innerHTML='<div class="fk-set-empty">暂无模板，右侧新建一个</div>'; return; }
      var h='';
      tpls.forEach(function(t){
        var act=(FK_SET_EDIT_ID!=null&&String(FK_SET_EDIT_ID)===String(t.id))?' active':'';
        h+='<div class="fk-set-row'+act+'" data-id="'+t.id+'">'
          +'<div class="fk-set-row-order">'
            +'<button title="上移" onclick="fkSetMove('+t.id+',\\'up\\')">↑</button>'
            +'<button title="下移" onclick="fkSetMove('+t.id+',\\'down\\')">↓</button>'
          +'</div>'
          +'<div class="fk-set-row-main" onclick="fkSetEdit('+t.id+')" title="点击载入该模板到编辑面板">'
            +'<div class="fk-set-row-name">'+fkHtml(t.name)+'</div>'
            +'<div class="fk-set-row-meta">优先级 '+t.priority+' · 更新 '+(t.update_time||'')+'</div>'
          +'</div>'
          +'<div class="fk-set-row-acts">'
            +'<button class="mini" onclick="fkSetEdit('+t.id+')">编辑</button>'
            +'<button class="mini" onclick="fkSetCopy('+t.id+')">复制</button>'
            +'<button class="mini danger" onclick="fkSetDelete('+t.id+')">删除</button>'
          +'</div></div>';
      });
      box.innerHTML=h;
    }
    function fkSetInit(){
      var b=document.getElementById('fkSetBack'); if(b) b.href=fkApiBase();
      fetch(fkApiBase()+'api/scan/templates').then(function(r){return r.json();}).then(function(d){
        if(!d.ok){ var b=document.getElementById('fkSetList'); if(b)b.innerHTML='<div class="fk-set-empty">加载失败</div>'; return; }
        FK_SCAN.defaultRules=d.default_rules||[];   // [2026-10-09] 存一份"未编辑"基线, 供新增模式复位
        FK_SCAN.rules=FK_SCAN.defaultRules.concat(d.templates||[]);
        fkRenderScanRules();
        var tpls=d.user_templates||[];
        fkSetRenderList(tpls);
        // [2026-10-09 用户裁定] 默认进来直接编辑【第一个模板】, 不再停在空的新建面板。
        // 列表按 (priority DESC, create_time DESC) 排序, tpls[0] 就是优先级最高的那条。
        if(tpls.length){ fkSetEdit(tpls[0].id); }
        else { fkSetNew(); }   // 一个模板都没有 -> 停在新增面板
      }).catch(function(e){ var b=document.getElementById('fkSetList'); if(b)b.innerHTML='<div class="fk-set-empty">加载失败:'+e+'</div>'; });
    }
    function fkSetEdit(tid){
      fetch(fkApiBase()+'api/scan/template/load/'+tid).then(function(r){return r.json();}).then(function(d){
        if(!d.ok){ fkSetMsg('加载失败:'+(d.error||''),false); return; }
        var t=d.template;
        FK_SET_EDIT_ID=t.id;
        var n=document.getElementById('fkSetName'); if(n)n.value=t.name||'';
        var ti=document.getElementById('fkSetEditTitle'); if(ti)ti.textContent='编辑：'+(t.name||'');
        var sb=document.getElementById('fkSetSaveBtn'); if(sb)sb.textContent='保存修改';
        fkSetApplyTemplate(t);
        fkSetMarkActive(t.id);   // [2026-10-09] 列表高亮跟随当前编辑对象
        fkSetMsg('已载入模板，改完点保存',true);
      }).catch(function(e){ fkSetMsg('加载异常:'+e,false); });
    }
    function fkSetCopy(tid){
      fetch(fkApiBase()+'api/scan/template/load/'+tid).then(function(r){return r.json();}).then(function(d){
        if(!d.ok){ fkSetMsg('复制失败',false); return; }
        var t=d.template;
        FK_SET_EDIT_ID=null;
        var n=document.getElementById('fkSetName'); if(n)n.value=(t.name||'')+' 副本';
        var ti=document.getElementById('fkSetEditTitle'); if(ti)ti.textContent='新建模板（从副本）';
        var sb=document.getElementById('fkSetSaveBtn'); if(sb)sb.textContent='保存模板';
        fkSetApplyTemplate(t);
        fkSetMarkActive(null);   // 副本属新增态, 不高亮任何现有模板
        fkSetMsg('已复制为副本，改完点保存即新建',true);
      }).catch(function(e){ fkSetMsg('复制异常:'+e,false); });
    }
    function fkSetSave(){
      var n=document.getElementById('fkSetName'); var name=n?n.value.trim():'';
      if(!name){ fkSetMsg('请填写模板名称',false); return; }
      var periods=fkSetSelectedPeriods();
      var rules=fkCollectRules();
      var products=fkCollectProducts();
      var body='name='+encodeURIComponent(name)+'&periods='+encodeURIComponent(periods.join(','))+'&rules='+encodeURIComponent(JSON.stringify(rules));
      if(products.length) body+='&products='+encodeURIComponent(products.join(','));
      if(FK_SET_EDIT_ID!=null) body+='&id='+encodeURIComponent(FK_SET_EDIT_ID);
      fetch(fkApiBase()+'api/scan/template/save',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:body})
        .then(function(r){return r.json();}).then(function(d){
          if(!d.ok){ fkSetMsg('保存失败:'+(d.error||''),false); return; }
          FK_SET_EDIT_ID=(d.id!=null)?d.id:FK_SET_EDIT_ID;
          var ti=document.getElementById('fkSetEditTitle'); if(ti)ti.textContent='编辑：'+(d.name||name);
          fkSetMsg('已保存：'+(d.name||name),true);
          fkSetReloadList();
        }).catch(function(e){ fkSetMsg('保存异常:'+e,false); });
    }
    // pickFirst: true 时重载完自动载入首个模板(删除当前模板后自动落到新的第一条)。
    // 注意 fkSetEdit 是异步的, 这里不做等待——它自己会 render 面板与高亮。
    function fkSetReloadList(pickFirst){
      fetch(fkApiBase()+'api/scan/templates').then(function(r){return r.json();}).then(function(d){
        if(!d.ok) return;
        var tpls=d.user_templates||[];
        fkSetRenderList(tpls);
        if(pickFirst){
          if(tpls.length){ fkSetEdit(tpls[0].id); }
          else { fkSetNew(); }
        }
      }).catch(function(){});
    }
    function fkSetMove(tid,dir){
      fetch(fkApiBase()+'api/scan/template/move/'+tid+'?direction='+dir,{method:'POST'}).then(function(r){return r.json();}).then(function(d){
        if(!d.ok){ fkSetMsg('移动失败:'+(d.error||''),false); return; }
        fkSetReloadList();
      }).catch(function(e){ fkSetMsg('移动异常:'+e,false); });
    }
    function fkSetDelete(tid){
      if(!confirm('确定删除该模板吗？')) return;
      fetch(fkApiBase()+'api/scan/template/delete/'+tid,{method:'POST'}).then(function(r){return r.json();}).then(function(d){
        if(!d.ok){ fkSetMsg('删除失败:'+(d.error||''),false); return; }
        var wasEditing = (FK_SET_EDIT_ID===tid);
        fkSetReloadList(wasEditing);   // 删掉正在编辑的那条 -> 自动载入新的首个模板
        fkSetMsg('已删除',true);
      }).catch(function(e){ fkSetMsg('删除异常:'+e,false); });
    }
    // ===== K线根数设置(计算根数 / 取值根数) =====
    // 2026-10-13 用户拍板: 计算根数=取多少根算指标; 取值根数=最近N根已收盘内出现状态就记录。
    // 低于45根只数字标黄, 不拦截(仍可保存)。
    var FK_KLS=null;
    function fkKlsMarkWarn(el){
      var minc=(FK_KLS&&FK_KLS.min_calc_bars)||45;
      var v=parseInt(el.value,10);
      var bad=(isNaN(v)||v<1);
      if(el.classList.contains('fk-kls-calc')) bad=bad||(v<minc);
      el.classList.toggle('warn',bad);
    }
    // 日/周/月/季 计算根数互相联动换算(用户 2026-10-13 拍板④-2):
    //   1 根该周期 ≈ 多少根日线(与 kline_settings._DERIVED_DAY_FACTOR 同口径)
    var FK_KLS_DERIVED = {day:1, week:5, month:21, quarter:63};
    function fkKlsSync(el){
      var p = el.getAttribute('data-p');
      if(!(p in FK_KLS_DERIVED)){ fkKlsMarkWarn(el); return; }   // 分钟级不参与联动
      var v = parseInt(el.value,10);
      if(isNaN(v) || v < 1){ fkKlsMarkWarn(el); return; }
      var dayVal = v * FK_KLS_DERIVED[p];                          // 折算成"日线根数"作基准
      document.querySelectorAll('.fk-kls-calc').forEach(function(inp){
        var tp = inp.getAttribute('data-p');
        if(!(tp in FK_KLS_DERIVED) || tp === p) return;            // 跳过分钟级与自身
        var tv = Math.max(1, Math.round(dayVal / FK_KLS_DERIVED[tp]));
        inp.value = tv;
        fkKlsMarkWarn(inp);
      });
      fkKlsMarkWarn(el);
    }
    function fkKlsRender(d){
      FK_KLS=d;
      var minc=d.min_calc_bars||45;
      var note=document.getElementById('fkKlsNote');
      if(note){
        note.innerHTML='<b>计算根数</b>：从K线仓库取多少根 bar 去算指标，低于 <b>'+minc+'</b> 根时 MACD 处于种子期，可能产生假金叉/假死叉 —— 数字会<span class="hl">标黄</span>，仍可保存。'
          +'<br><b>取值根数</b>：算完后，最近 N 根内出现状态就记录该信号（每周期一个值）。';
      }
      var tc=document.getElementById('fkKlsTipCalc'), tt=document.getElementById('fkKlsTipTake');
      if(tc) tc.textContent=(d.hints&&d.hints.calc)||'';
      if(tt) tt.textContent=(d.hints&&d.hints.take)||'';
      var body=document.getElementById('fkKlsBody');
      if(!body) return;
      if(!(d.items&&d.items.length)){
        body.innerHTML='<tr><td colspan="4" style="color:#94a3b8">暂无数据</td></tr>';
        return;
      }
      body.innerHTML=d.items.map(function(it){
        var cw=(it.calc_check&&it.calc_check.warnings)||[];
        var tw=(it.take_check&&it.take_check.warnings)||[];
        var w=cw.concat(tw);
        var note2='';
        if(it.preview) note2=fkHtml(it.preview.hint);
        if(it.eff_calc_bars<minc){
          note2='<span style="color:#f59e0b">实际仅 '+it.eff_calc_bars+' 根，不足 '+minc+' 根，该周期不出信号</span>';
        }
        return '<tr>'
          +'<td><span class="fk-kls-per">'+fkHtml(it.label)+'</span>'+(it.derived?'<small>(派生)</small>':'')+'</td>'
          +'<td><div class="fk-kls-input"><input type="number" min="1" step="1" class="fk-kls-calc'+(cw.length?' warn':'')+'" data-p="'+it.period+'" value="'+it.calc_bars+'" oninput="fkKlsSync(this)"><span class="fk-kls-unit">根</span></div></td>'
          +'<td><div class="fk-kls-input"><input type="number" min="1" step="1" class="fk-kls-take" data-p="'+it.period+'" value="'+it.take_bars+'" oninput="fkKlsMarkWarn(this)"><span class="fk-kls-unit">根</span></div></td>'
          +'<td class="fk-kls-warncell">'+note2+(w.length?'<span class="bad">'+fkHtml(w[0])+'</span>':'')+'</td>'
          +'</tr>';
      }).join('');
      var wn=document.getElementById('fkKlsWarn');
      if(wn){
        wn.textContent='周/月/季由日线重采样：周≈日线÷5，月≈日线÷21，季≈日线÷63；'
          +'月线需日线≥945 根才够 '+minc+' 根门槛。日/周/月/季任一改动会自动联动换算。';
      }
    }
    function fkKlsCollect(){
      var calc={},take={};
      document.querySelectorAll('.fk-kls-calc').forEach(function(el){
        var v=parseInt(el.value,10); if(!isNaN(v)&&v>0) calc[el.getAttribute('data-p')]=v;
      });
      document.querySelectorAll('.fk-kls-take').forEach(function(el){
        var v=parseInt(el.value,10); if(!isNaN(v)&&v>0) take[el.getAttribute('data-p')]=v;
      });
      return {calc_bars:calc,take_bars:take};
    }
    function fkKlsMsg(txt,ok){
      var el=document.getElementById('fkKlsMsg');
      if(!el) return;
      el.textContent=txt;
      el.className=ok?'fk-set-msg ok':'fk-set-msg';
    }
    function fkKlsSave(){
      var p=fkKlsCollect();
      var body=new URLSearchParams();
      body.set('calc_bars',JSON.stringify(p.calc_bars));
      body.set('take_bars',JSON.stringify(p.take_bars));
      fetch(fkApiBase()+'api/kline/settings',{method:'POST',
        headers:{'Content-Type':'application/x-www-form-urlencoded'},body:body.toString()})
        .then(function(r){return r.json();})
        .then(function(d){
          if(d&&d.ok!==false){ fkKlsRender(d); fkKlsMsg('已保存，下一轮扫描生效',true); }
          else { fkKlsMsg('保存失败:'+((d&&d.error)||''),false); }
        }).catch(function(e){ fkKlsMsg('保存异常:'+e,false); });
    }
    function fkKlsReset(){
      if(!confirm('恢复标准默认值？')) return;
      fetch(fkApiBase()+'api/kline/settings/reset',{method:'POST'})
        .then(function(r){return r.json();})
        .then(function(d){
          if(d&&d.ok!==false){ fkKlsRender(d); fkKlsMsg('已恢复默认',true); }
          else { fkKlsMsg('恢复失败:'+((d&&d.error)||''),false); }
        }).catch(function(e){ fkKlsMsg('恢复异常:'+e,false); });
    }
    function fkKlsLoad(){
      fetch(fkApiBase()+'api/kline/settings')
        .then(function(r){return r.json();})
        .then(function(d){ if(d&&d.ok!==false) fkKlsRender(d); })
        .catch(function(e){ console.error('加载根数设置失败',e); });
    }
    fkSetInit();
    fkKlsLoad();
    </script>
    </body></html>'''
    )
    return page


def scan_history_page():
    """历史复盘查询页面。"""
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    opts = '<option value="">全部周期</option>'
    for k, v in PERIODS.items():
        opts += f'<option value="{html.escape(k)}">{html.escape(v["label"])}</option>'
    side_opts = '<option value="">全部方向</option><option value="long">多头</option><option value="short">空头</option>'
    type_opts = '<option value="">全部类型</option>'
    for t, n in (
        ("macd_golden", "MACD金叉"), ("macd_death", "MACD死叉"),
        ("top_divergence", "顶背离"), ("bottom_divergence", "底背离"),
        ("rsi_oversold", "RSI超卖"), ("rsi_overbought", "RSI超买"),
        ("kdj_oversold", "KDJ超卖"), ("kdj_overbought", "KDJ超买"),
        ("boll_lower", "BOLL下轨"), ("boll_upper", "BOLL上轨"),
        ("cci_oversold", "CCI超卖"), ("cci_overbought", "CCI超买"),
        ("vol_ratio", "量比异动"), ("obv_high", "OBV新高"), ("wvad_positive", "WVAD转正"), ("vroc", "量变动速率"),
    ):
        type_opts += f'<option value="{html.escape(t)}">{html.escape(n)}</option>'

    page = '''<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><title>期货监控 · 信号历史复盘</title>'''
    page += _OPT_SPECIAL_STYLE + short_term_quant._SQ_STYLE + _FK_STYLE
    page += '''<style>
      .fk-hist-wrap{max-width:1600px;margin:0 auto;padding:18px}
      .fk-hist-filters{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:16px;padding:14px;background:#0b1220;border:1px solid var(--border);border-radius:8px}
      .fk-hist-filters label{color:var(--muted);font-size:13px}
      .fk-hist-filters input,.fk-hist-filters select{padding:6px 10px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:5px;font-size:13px}
      .fk-hist-filters button{padding:6px 16px;background:var(--accent);color:#05101c;border:none;border-radius:5px;font-weight:600;cursor:pointer;font-size:13px}
      .fk-hist-count{color:var(--muted);font-size:13px;margin-bottom:10px}
      .fk-hist-table{width:100%;border-collapse:collapse;font-size:13px;color:var(--text)}
      .fk-hist-table th{padding:10px;background:#0d1524;border-bottom:1px solid var(--border);color:var(--muted);font-weight:600;text-align:left}
      .fk-hist-table td{padding:9px 10px;border-bottom:1px solid #16202e;vertical-align:middle}
      .fk-hist-table tr:hover{background:#111d2b}
      .fk-hist-side{font-weight:700}
      .fk-hist-side.long{color:var(--up)}
      .fk-hist-side.short{color:var(--down)}
      .fk-hist-change{font-variant-numeric:tabular-nums}
      .fk-hist-change.up{color:#ff4d4f}
      .fk-hist-change.down{color:#19c37d}
      .fk-hist-change .pct{font-weight:600}
      .fk-hist-change .pts{color:var(--muted);font-size:11px}
      .fk-hist-empty{text-align:center;color:var(--muted);padding:40px}
      .fk-hist-pagination{display:flex;gap:10px;align-items:center;margin-top:14px}
      .fk-hist-pagination button{padding:5px 12px;background:#13202f;border:1px solid #2a3a4d;color:var(--text);border-radius:5px;cursor:pointer}
      .fk-hist-pagination button:disabled{opacity:.4;cursor:not-allowed}
      .fk-stats-title{font-size:14px;color:var(--text);font-weight:600;margin:6px 0 10px}
      .fk-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:14px}
      .fk-stat-card{background:#0d1524;border:1px solid var(--border);border-radius:10px;padding:14px 16px}
      .fk-stat-card .v{font-size:26px;font-weight:700;color:var(--text)}
      .fk-stat-card .l{font-size:12px;color:var(--muted);margin-top:4px}
      .fk-stat-card.long .v{color:var(--up)}
      .fk-stat-card.short .v{color:var(--down)}
      .fk-dist{background:#0b1220;border:1px solid var(--border);border-radius:10px;padding:14px 16px;margin-bottom:14px}
      .fk-dist h4{margin:0 0 10px;font-size:13px;color:var(--muted);font-weight:600}
      .fk-dist-row{display:flex;align-items:center;gap:8px;margin:5px 0;font-size:12px}
      .fk-dist-row .name{width:96px;color:var(--text);flex:none;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
      .fk-dist-bar{height:14px;background:#13202f;border-radius:3px;overflow:hidden;flex:1}
      .fk-dist-bar > i{display:block;height:100%;background:var(--accent)}
      .fk-dist-row .cnt{width:48px;text-align:right;color:var(--muted)}
    </style></head><body>
    <div class="fk-hist-wrap">
      <div class="page-title">监控信号历史复盘</div>
      <div class="page-sub">按扫描时间、周期、合约、方向、信号类型筛选历次扫描命中的信号</div>
      <div class="fk-hist-filters">
        <label>开始</label><input type="datetime-local" id="hStart" value="''' + yesterday + '''T00:00">
        <label>结束</label><input type="datetime-local" id="hEnd" value="''' + today + '''T23:59">
        <label>周期</label><select id="hPeriod">''' + opts + '''</select>
        <label>合约</label><input type="text" id="hSymbol" placeholder="代码/名称" style="width:120px">
        <label>方向</label><select id="hSide">''' + side_opts + '''</select>
        <label>类型</label><select id="hType">''' + type_opts + '''</select>
        <button onclick="fkHistQuery(0)">查询</button>
        <a class="fk-scan-history" href="." style="margin-left:auto">← 返回行情</a>
      </div>
      <div class="fk-stats-title">统计看板（按上方筛选条件聚合）</div>
      <div class="fk-stats" id="hStatCards"></div>
      <div class="fk-dist"><h4>按周期分布</h4><div id="hDistPeriod"></div></div>
      <div class="fk-dist"><h4>按信号类型分布</h4><div id="hDistType"></div></div>
      <div class="fk-dist"><h4>按日期分布</h4><div id="hDistDay"></div></div>
      <div class="fk-hist-count" id="hCount">点击查询加载数据</div>
      <div id="hTableWrap"><div class="fk-hist-empty">请选择条件并点击查询</div></div>
      <div class="fk-hist-pagination" id="hPage"></div>
    </div>
    <script>
    var FK_H={limit:50,offset:0,total:0};
    function fkApiBase(){
      var p=location.pathname;
      var i=p.indexOf('/futureskline/');
      return i>=0 ? p.substring(0,i)+'/futureskline/' : '/futureskline/';
    }
    function fkHistQuery(offset){
      FK_H.offset=offset||0;
      fkHistStats();
      var qs='start='+encodeURIComponent(document.getElementById('hStart').value)
        +'&end='+encodeURIComponent(document.getElementById('hEnd').value)
        +'&period='+encodeURIComponent(document.getElementById('hPeriod').value)
        +'&symbol='+encodeURIComponent(document.getElementById('hSymbol').value)
        +'&side='+encodeURIComponent(document.getElementById('hSide').value)
        +'&signal_type='+encodeURIComponent(document.getElementById('hType').value)
        +'&limit='+FK_H.limit+'&offset='+FK_H.offset;
      fetch(fkApiBase()+'api/scan/history?'+qs).then(function(r){return r.json();}).then(function(d){
        if(!d.ok){document.getElementById('hTableWrap').innerHTML='<div class="fk-hist-empty">查询失败:'+(d.error||'')+'</div>'; return;}
        FK_H.total=d.total;
        document.getElementById('hCount').textContent='共 '+d.total+' 条 (显示第 '+(FK_H.offset+1)+'-'+(FK_H.offset+d.rows.length)+' 条)';
        if(!d.rows.length){document.getElementById('hTableWrap').innerHTML='<div class="fk-hist-empty">无数据</div>'; fkHistPage(); return;}
        var html='<table class="fk-hist-table"><thead><tr><th>扫描时间</th><th>周期</th><th>合约</th><th>方向</th><th>信号类型</th><th>触发时间</th><th>最新价</th><th>涨跌幅</th><th>MA60/20</th></tr></thead><tbody>';
        d.rows.forEach(function(r){
          var tt=r.trigger_time||'';
          var timeHtml=tt.length>11?tt.slice(11):(tt||'-');
          var closeVal=r.close_price!=null?r.close_price:r.close;
          var chgVal=r.change, pctVal=r.change_pct;
          var sig=r.signal||{};
          var priceHtml=(closeVal!=null&&!isNaN(closeVal))?Number(closeVal).toFixed(2):'-';
          var changeHtml='—', pctHtml='—';
          if(chgVal!=null&&!isNaN(chgVal)){ var sg=chgVal>0?'+':''; changeHtml=sg+Number(chgVal).toFixed(2); }
          if(pctVal!=null&&!isNaN(pctVal)){ var sg=pctVal>0?'+':''; pctHtml=sg+Number(pctVal).toFixed(2)+'%'; }
          var clsDir=(r.side||'')==='short'?'down':'up';
          html+='<tr><td>'+(r.scan_time||'-')+'</td><td>'+(r.period||'-')+'</td><td>'+(r.display_name||r.real_symbol||r.symbol)+'<br><span style="color:#94a3b8;font-size:11px">'+(r.real_symbol||r.symbol)+'</span></td><td class="fk-hist-side '+(r.side||'')+'">'+(r.side==='long'?'多头':'空头')+'</td><td>'+(r.rule_name||r.signal_type)+'</td><td>'+timeHtml+'</td><td>'+priceHtml+'</td><td class="fk-hist-change '+clsDir+'"><div class="pct">'+pctHtml+'</div><div class="pts">('+changeHtml+')</div></td><td>'+fkMaTags(closeVal, sig.ma60, sig.ma20)+'</td></tr>';
        });
        html+='</tbody></table>';
        document.getElementById('hTableWrap').innerHTML=html;
        fkHistPage();
      }).catch(function(e){document.getElementById('hTableWrap').innerHTML='<div class="fk-hist-empty">请求异常:'+e+'</div>';});
    }
    function fkHistStats(){
      var qs='start='+encodeURIComponent(document.getElementById('hStart').value)
        +'&end='+encodeURIComponent(document.getElementById('hEnd').value)
        +'&period='+encodeURIComponent(document.getElementById('hPeriod').value)
        +'&symbol='+encodeURIComponent(document.getElementById('hSymbol').value)
        +'&side='+encodeURIComponent(document.getElementById('hSide').value)
        +'&signal_type='+encodeURIComponent(document.getElementById('hType').value);
      fetch(fkApiBase()+'api/scan/history/stats?'+qs).then(function(r){return r.json();}).then(function(d){
        if(!d.ok) return;
        var cards=[
          {l:'总信号数',v:d.total},
          {l:'多头',v:d.long_count,c:'long'},
          {l:'空头',v:d.short_count,c:'short'},
          {l:'涉及合约',v:d.distinct_symbols}
        ];
        document.getElementById('hStatCards').innerHTML=cards.map(function(c){
          return '<div class="fk-stat-card '+(c.c||'')+'"><div class="v">'+(c.v||0)+'</div><div class="l">'+c.l+'</div></div>';
        }).join('');
        fkRenderDist('hDistPeriod', d.by_period||[], function(x){return x.period;});
        fkRenderDist('hDistType', d.by_type||[], function(x){return x.name;});
        fkRenderDist('hDistDay', d.by_day||[], function(x){return x.day;});
      }).catch(function(e){});
    }
    function fkRenderDist(id, arr, nameFn){
      var box=document.getElementById(id);
      if(!box) return;
      if(!arr||!arr.length){box.innerHTML='<div class="fk-hist-empty" style="padding:14px">无数据</div>';return;}
      var max=Math.max.apply(null, arr.map(function(x){return x.cnt;}));
      box.innerHTML=arr.map(function(x){
        var pct=max>0?Math.round(x.cnt/max*100):0;
        return '<div class="fk-dist-row"><span class="name" title="'+fkHistEsc(nameFn(x))+'">'+fkHistEsc(nameFn(x))+'</span><span class="fk-dist-bar"><i style="width:'+pct+'%"></i></span><span class="cnt">'+x.cnt+'</span></div>';
      }).join('');
    }
    function fkHistEsc(s){return String(s==null?'':s).replace(/[&<>]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c];});}
    function fkHistPage(){
      var total=FK_H.total, limit=FK_H.limit, off=FK_H.offset;
      var pages=Math.ceil(total/limit)||1;
      var cur=Math.floor(off/limit)+1;
      var html='<button '+(off>0?'':'disabled')+' onclick="fkHistQuery('+(off-limit)+')">上一页</button>';
      html+='<span style="color:var(--muted);font-size:13px">第 '+cur+' / '+pages+' 页</span>';
      html+='<button '+((off+limit)<total?'':'disabled')+' onclick="fkHistQuery('+(off+limit)+')">下一页</button>';
      document.getElementById('hPage').innerHTML=html;
    }
    fkHistStats();
    </script></body></html>'''
    return page


def serve_static(filename):
    return send_from_directory(_STATIC_DIR, filename)


def register_routes(app, slots, default_key, get_slot=None, create_slot=None):
    global _SLOTS, _DEFAULT_KEY, _GET_SLOT, _MAKE_SLOT
    _SLOTS = slots
    _DEFAULT_KEY = default_key
    _GET_SLOT = get_slot
    _MAKE_SLOT = create_slot   # 2026-09-24: main.get_slot 注入, 供第⑤级自动挑号开槽
    # 2026-09-28 隔离: 启动期幂等加列 + 自动指派 monitor/primary 角色
    try:
        _ensure_account_roles()
    except Exception as _e:
        print(f"[监控隔离] 角色指派异常(可忽略): {_e}")
    app.add_url_rule("/futureskline", view_func=futures_kline_page, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/kline", view_func=api_kline_data, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/quotes", view_func=api_quotes_data, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/contracts", view_func=api_contracts_data, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan", view_func=api_scan_data, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/templates", view_func=api_scan_templates, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/template/save", view_func=api_scan_template_save, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/kline/settings", view_func=api_kline_settings, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/kline/settings/reset", view_func=api_kline_settings_reset, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/template/load/<int:tid>", view_func=api_scan_template_load, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/template/delete/<int:tid>", view_func=api_scan_template_delete, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/template/move/<int:tid>", view_func=api_scan_template_move, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/schedule/save", view_func=api_scan_schedule_save, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/schedule/list", view_func=api_scan_schedule_list, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/schedule/delete/<int:sid>", view_func=api_scan_schedule_delete, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/schedule/toggle/<int:sid>", view_func=api_scan_schedule_toggle, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/history", view_func=api_scan_history, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/history/stats", view_func=api_scan_history_stats, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/multi/start", view_func=api_scan_multi_start, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/backtest", view_func=api_scan_backtest, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/multi/stop", view_func=api_scan_multi_stop, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/multi/status", view_func=api_scan_multi_status, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/multi/latest", view_func=api_scan_multi_latest, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/multi/today", view_func=api_scan_multi_today, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/api/scan/auto", view_func=api_scan_auto, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/futureskline/scan-history", view_func=scan_history_page, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/scan-settings", view_func=scan_settings_page, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/scan-monitor", view_func=scan_monitor_page, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/futureskline/static/<path:filename>", view_func=serve_static, methods=["GET"], strict_slashes=False)
    # 启动定时爬取调度线程(幂等)
    try:
        scan_engine.start_scan_scheduler()
    except Exception as e:
        print(f"[期货K线] 启动调度线程失败: {e}")
    # 启动后台自动监控守护(幂等): 交易时段自动拉取, 不用手点「开始监控」
    try:
        start_auto_monitor()
    except Exception as e:
        print(f"[期货K线] 启动自动监控失败: {e}")
    print("[期货K线] 路由已注册: /futureskline, /futureskline/api/kline, /futureskline/api/quotes, /futureskline/api/contracts, /futureskline/api/scan, /futureskline/api/scan/templates, /futureskline/api/scan/template/{save,load,delete}, /futureskline/api/scan/schedule/{save,list,delete,toggle}, /futureskline/api/scan/history, /futureskline/api/scan/history/stats, /futureskline/api/scan/multi/{start,stop,status,latest}, /futureskline/scan-history, /futureskline/static/<file>")
