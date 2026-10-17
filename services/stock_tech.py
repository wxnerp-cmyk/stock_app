# -*- coding: utf-8 -*-
"""
股票多周期技术分析矩阵模块 (stock_tech)

功能:
- 在「股票」主服务下新增一个模块, 展示 1分/5分/15分/30分/60分/日线 六周期 × 多种指标的矩阵表。
- 指标全部复用 indicators / ta_patterns / futures_kline, 不重复造轮子。
- 支撑/压力位按需求人规则: 用户设置 MA 周期, 算 MA 值; 当前价 > MA => 支撑位, 否则 => 压力位。
- 颈线: 【2026-09-18 已从界面移除】原为"用户自行设置最多 6 条关键价位线", 已被预设点位替代; 后端 neckline= 解析通道保留(死代码, 便于随时恢复), 界面不再有入口。
- 预设点位: 用户手填, 不参与计算, 仅展示。
- 形态识别(M头/W底/U底/V底/头肩顶/头肩底/箱体突破/箱体跌破): 基于枢轴点的简化模式匹配模型。
- 黄金分割: 取近 N 根 K 线高低点, 按 0/0.236/0.382/0.5/0.618/0.786/1 计算回撤位。
- 跳空: 检测最近一个向上/向下跳空。
- 背离: 价格与 MACD(及 RSI) 方向相反(顶/底背离)。

数据来源: 天勤 TqSdk, 经 stock_app 的 AccountSlot 单一连接。
网关路径约定(与 stock_kline 一致):
- 页面 JS 用 apiBase()=location.pathname 截到 /stocktech/ 做前缀, 自动兼容 直连(:5088) 与 网关(:9000/api/gw/stocktech)
"""
import os
import time
import json
import collections
import threading
import concurrent.futures
from datetime import datetime

import pandas as pd
import numpy as np
from flask import request, jsonify, send_from_directory, Response

from services import indicators
from services import ta_patterns
from services import futures_kline
from services import tx_kline   # 腾讯 HTTP K线兜底源(天勤不存在的标的, 如科创50)


# ===================== 全局状态 =====================
_SLOTS = None
_DEFAULT_KEY = None
_GET_SLOT = None
_MAKE_SLOT = None
_STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")

# 临时性能埋点(TECH_DEBUG=1 时打印各环节耗时); 定位性能问题用, 默认关闭
DEBUG_TIMING = bool(os.environ.get("TECH_DEBUG"))

# 六周期(复用 futures_kline.PERIODS 的 1m..day)
TECH_PERIODS = [
    ("1m", "1分", 60, 90),
    ("5m", "5分", 300, 90),
    ("15m", "15分", 900, 90),
    ("30m", "30分", 1800, 90),
    ("60m", "60分", 3600, 90),
    ("day", "日线", 86400, 90),
]

# 默认分析标的(需求人截图: 上证指数 + 上证50/沪深300/中证1000 + 创业板 + 科创50)
# 2026-09-07 实测改版: 当前账号(Mr qia, trial)只有【指数】权限 —— 期货主连/ETF 全部无权限
#   (KQ.i.CFFEX.IH 报合约不存在, SZSE.159915/科创50 等 35s 长等待仍超时, 已排除冷启动误判)。
#   故改用指数代码接入, 右栏 4 个标的即可出真实数据且秒级返回(不走 15s 超时等待)。
#   实测该账号有权限的指数仅 5 个: SSE.000001 / 000016 / 000300 / 000852 / SZSE.399006。
# 2026-09-11 复核: 科创50【指数】(000688)在天勤合约库里【根本不存在】—— SSE.000688 与 CSI.000688
#   均直接报 "contains non-existent instrument"(不是权限问题, 换任何账号都拿不到);
#   把天勤全部 500 个指数拉出来看, 中文名含"科创"的 0 个。故该指数只能走外部源(见下)。
# 2026-09-12 用户先裁 B(改用天勤同主题 ETF SSE.588000), 见到实盘后【改回指数口径(A)】——
#   因为页面上要看的"科创50"是【指数】(收盘 1553.39), 而 ETF(588000) 报的是基金二级市场价 ~1.639,
#   两者量级差 1000 倍, 与百度/行情软件对不上。故仍走腾讯 HTTP 源取【真指数 000688】。
#
# 2026-09-14 修复"技术分析页跳几下就不刷新": 根因是 admin 绑定的 Mr qia 天勤会话会【周期性静默】
# (约 90s 不收包)才自动重连, 静默窗口里所有走天勤源的标的行情冻住(当时一度把 6 个指数全改走 tx)。
#
# 2026-09-14 用户裁定(最终口径, 目标: 核心数据 1 秒以内 / 订阅推送):
#   ① 科创50(SSE.000688) 天勤合约库【不存在】, 保留走腾讯 HTTP 源(不受此限);
#   ② 其余 5 个指数【改回天勤 AccountSlot】, 用其"订阅 + 服务器推送"模型:
#      quote 对象由该账户专属泵线程 wait_update 持续更新, 请求线程只读缓存引用(零阻塞、无往返),
#      _live_price() 每轮请求直接取推送后的最新报价覆盖"最新价/前收/涨跌" => 毫秒级新鲜;
#      MA/BOLL/MACD/形态等重计算仍走 0.6s 缓存, 不因实时价被迫重算(用户: 复杂计算可缓存/异步)。
#   ③ 天勤会话静默由 main.py _maybe_reconnect_on_silence(market-aware, 盘中 25s/45s)自愈兜底。
# src: 缺省 "tq" = 走天勤 AccountSlot(订阅推送); "tx" = 走腾讯 HTTP K线源(仅天勤无此标的时用)
# 2026-09-24 实锤改版: 上证指数/创业板指改走 tx 源 —— admin 绑定 zhubajie(free)后, 免费权限
#   仅 4 指数(000016/000300/000905/000852), 000001/399006 天勤拒给 -> 页面"暂无K线数据"全空
#   (09-07 当时绑 trial 账号全通故好使; 账号一换必挂)。tx 源不吃天勤权限、不受账号切换影响,
#   与科创50 同模式(09-14 用户已接受 tx 源口径); 且不占天勤订阅, 监控配额更宽裕。
# 2026-09-28 二次实锤(新鲜度监控 78 样本): 腾讯 qt 快照"生成->到达"平均 3.2s 旧(东财仅 1.2s),
#   tx 源报价天然比百度系慢 2~3s —— 用户实测"百度比我们快两三秒"。而技术分析实际账户是
#   Mr qia(free, 权限恰覆盖 000016/000300/000905/000852), 故这 3 指数改回天勤订阅源:
#   报价 = 泵线程 wait_update 推送(毫秒级, get_quote 只读缓存零往返), K线/指标同源。
#   ⚠️ 000001/399006/000688 免费权限不含, 必须保留 tx, 勿改! (2026-09-24 教训)
DEFAULT_INSTRUMENTS = [
    {"name": "上证指数", "symbol": "SSE.000001", "src": "tx"},
    {"name": "上证50", "symbol": "SSE.000016"},
    {"name": "沪深300", "symbol": "SSE.000300"},
    {"name": "中证1000", "symbol": "SSE.000852"},
    {"name": "创业板指", "symbol": "SZSE.399006", "src": "tx"},
    {"name": "科创50", "symbol": "SSE.000688", "src": "tx"},
]

# 标的 -> 数据源 映射(compute_period / 预热 / 取数入口共用)
_SRC = {it["symbol"]: (it.get("src") or "tq") for it in DEFAULT_INSTRUMENTS}


def _is_http_src(symbol):
    """该标的是否走 HTTP 兜底源(不走天勤)。"""
    return _SRC.get(symbol) == "tx"


def _live_price(slot, symbol):
    """返回该标的【订阅推送】的实时报价 (最新价, 前收); 拿不到返回 (None, None)。

    tq 源: 天勤 quote 对象 —— 由该账户专属泵线程 wait_update 持续推送更新, 请求线程只读缓存
           引用(零阻塞、无网络往返), 因此读到的就是"推送到达即最新"的值(毫秒级)。
    tx 源: 腾讯 qt 实时报价接口(2s 缓存兜底)。
    本函数只取价、不做任何重计算, 专门用于保证"核心基本数据"永远新鲜。
    """
    if _is_http_src(symbol):
        try:
            return tx_kline.fetch_quote(symbol)
        except Exception:
            return None, None
    if slot is None or getattr(slot, "client", None) is None:
        return None, None
    try:
        es = getattr(slot, "ensure_subscribed", None)
        if es is not None:
            es([symbol])                      # 仅登记待订阅, 真正 get_quote 由泵线程执行
        q = slot.client.get_quote(symbol)      # 只读缓存引用, 绝不阻塞
        if q is None:
            return None, None
        last = _round(getattr(q, "last_price", None))
        pre = _round(getattr(q, "pre_close", None))
        if last is None or last <= 0:
            return None, None
        return last, (pre if (pre is not None and pre > 0) else None)
    except Exception:
        return None, None

# 计算缓存 (symbol|period|ma|neckline_levels -> (ts, payload)), 只缓存【重计算】(MA/BOLL/MACD/
# 形态/背离…); "最新价/前收/涨跌"不依赖它, 每轮请求都从推送报价单独覆盖(见 compute_instrument)。
#
# 2026-09-14 用户裁定: 目标"核心数据 1 秒以内" —— 重计算改为【stale-while-revalidate】:
#   请求永远立即返回缓存值(哪怕略旧), 过期后由后台线程重算, 绝不让请求等计算。这样即使
#   6 标的并发(GIL 串行), 每个请求也只花"读缓存 + 贴实时价"的时间(个位数 ms), 页面 500ms
#   轮询不再被计算拖住; 指标的新鲜度由后台按 TTL 保证(数据源本身 ~3s 才变, TTL=2s 足够)。
_CACHE = {}
_CACHE_LOCK = threading.Lock()
# 2026-09-28 实锤改版: TTL 2.0 -> 30.0。原 TTL=2s 使「重算需求 ≈ 54 key × ~0.5s / 2s = 13 倍 GIL」,
#   2 个 techbg worker 长期 100% 满载, 把整个 stock_app 进程的 GIL 挤爆 —— 技术分析 api_tech_data
#   等 30s 超时(页面"时有时无")、股票 api/kline 60s 网关超时(指标区全空) 同根因。
#   TTL=30s 后重算需求降 ~15 倍; 且请求本来就用实时价覆盖价格字段(2026-09-14 裁定),
#   重指标(MACD/形态等) 30s 新鲜度与股票模块 30s 兜底口径一致。
_CACHE_TTL = 30.0
# 上限(2026-09-15): cache_key 里含 MA/颈线/阈值/自定义价位/【指标参数】—— 用户每改一次参数就
# 多出一批新键, 旧键再也不会命中。给个上限防内存长期膨胀: 超出时按写入时间淘汰最旧的 1/4。
_CACHE_MAX = 1500


def _cache_put(cache_key, res):
    with _CACHE_LOCK:
        _CACHE[cache_key] = (time.time(), res)
        if len(_CACHE) > _CACHE_MAX:
            for _kk, _vv in sorted(_CACHE.items(), key=lambda kv: kv[1][0])[:max(1, _CACHE_MAX // 4)]:
                _CACHE.pop(_kk, None)

# 后台重算线程池(与请求用的 _EXECUTOR 分开, 避免重算抢占请求线程) + 同 key 去重
_BG_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="techbg")
_BG_REFRESHING = set()
_BG_LOCK = threading.Lock()


def _refresh_period_bg(slot, symbol, key, label, dur, cnt,
                       user_ma_periods, neckline_levels, preset_points,
                       user_thresholds, cache_key, custom_levels=None, ind_params=None):
    """后台重算某周期(同一 cache_key 去重), 页面永不因它等待。"""
    with _BG_LOCK:
        if cache_key in _BG_REFRESHING:
            return
        _BG_REFRESHING.add(cache_key)

    def _work():
        try:
            r = compute_period(slot, symbol, key, label, dur, cnt,
                               user_ma_periods, neckline_levels, preset_points, user_thresholds,
                               custom_levels, ind_params)
            if isinstance(r, dict) and not r.get("error"):
                _cache_put(cache_key, r)
        except Exception:
            pass
        finally:
            with _BG_LOCK:
                _BG_REFRESHING.discard(cache_key)

    try:
        _BG_EXECUTOR.submit(_work)
    except Exception:
        with _BG_LOCK:
            _BG_REFRESHING.discard(cache_key)


# 服务端取数超时保护: 无权限/不存在的合约底层会卡 ~36s(6s×6等待), 用独立线程+超时快速失败,
# 避免前端长时间挂起。仅作用于本模块, 不动共享的 futures_kline/main.py。
# 取数线程池: 2026-09-11 由 4 提到 6 —— 多标的页正好 6 个标的, 提到 6 可一轮并发算完,
# 消除第二轮排队(实测冷算整轮 0.72~0.90s -> 目标 <0.6s), 单核占用实测仅 ~17%, 余量充足。
_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=6, thread_name_prefix="tech")
_FETCH_TIMEOUT = 30
# [2026-10-10] 同步冷算在途去重表: key = symbol|ma|颈线|预设|阈值|自定义|指标参数。
# 客户端 abort 不会取消服务端已在跑的 compute_instrument, 冷合约时前端 500ms 节拍会不断重发,
# 没有这张表就会在 6 线程池上堆出十几个重复冷算互相抢 GIL(实测热标 SSE.000300 被拖到 18s)。
_INFLIGHT = {}
_INFLIGHT_LOCK = threading.Lock()
_INFLIGHT_MAX = 200

# 后台预热: 天勤 K 线初始数据是按"串行节奏"逐个下发的(并发订阅会相互挤掉 6s 窗口导致部分标的无数据)。
# 因此启动后把全部 默认标的×周期 的 K 线【串行】预订阅并等数据到位, 之后页面请求直接命中缓存(对象已建好且被
# wait_update 持续刷新), 既秒级返回又绝不触发并发订阅竞态。预热完成后 _warm_done=True。
_warm_lock = threading.Lock()
_warm_thread = None
_warm_done = False
_preload_done = False
# 预加载改为【优先队列 + worker】模型:
#   _PRELOAD_QUEUE 存放待预热标的, LEFT(固定标) 首进, 随后默认标, 最后候选标; 用户打开页面时
#   客户端通过 /stocktech/api/preload_priority 把自己缓存的标插到队首 => "打开即见, 不干等"。
#   _USER_REQ_ACTIVE: 当前正在处理的用户 api/data 请求数; worker 看到 >0 就多让路, 绝不饿死前端。
#   _USER_COLD: 用户请求正在冷算的标的集合; worker 遇到就跳过(交给用户请求填充), 避免同标的双算抢 GIL。
_PRELOAD_QUEUE = collections.deque()
_PRELOAD_SEEN = set()
_PRELOAD_TS = {}          # 各标"默认参数"上次算完时间戳; 续热循环据此跳过仍在 TTL 内的标
_PRELOAD_LOCK = threading.Lock()
# 前端默认首请求参数(必须与前端 MAP / INDC 默认值保持一致, 否则 cache_key 不同 -> 预热/预热命中失效):
#   MA = '2,10,20,60'(MAP 默认); 指标 = INDC 默认(macd/kdj/boll/rsi 均非空)。
_PRELOAD_DFLT_MA = [2, 10, 20, 60]
_PRELOAD_DFLT_IND = {"macd": "12,26,9", "kdj": "9,3,3", "boll": "20,20,2", "rsi": "6,12,24"}
_USER_REQ_ACTIVE = 0
_USER_REQ_LOCK = threading.Lock()
_USER_COLD = set()


def _warm_scan_klines():
    """后台串行预热全部默认标的×周期的 K 线订阅(规避并发订阅致交付失效)。

    必须在扫描账号 Slot 真正在线(client 已建好)后才开始, 否则会拿到未连接的 init Slot
    导致 request_kline_obj 全部返回 None。故先轮询等待在线 Slot(启动早期 slot 可能尚未连接)。"""
    global _warm_done
    slot = None
    for _ in range(60):  # 等扫描账号上线(最多 ~180s)
        slot = _get_slot_for_scan()
        if slot is not None and getattr(slot, "status", "") == "online" and getattr(slot, "client", None) is not None:
            break
        slot = None
        time.sleep(3)
    if slot is None:
        print("[技术分析] 预热中止: 启动 3 分钟内未拿到已连接的扫描账号 Slot", flush=True)
        _warm_done = True
        return
    print("[技术分析] 开始串行预热 K 线订阅(%d 标的 × %d 周期)..." % (len(DEFAULT_INSTRUMENTS), len(TECH_PERIODS)), flush=True)
    ok = 0
    for inst in DEFAULT_INSTRUMENTS:
        sym = inst["symbol"]
        for key, label, dur, cnt in TECH_PERIODS:
            try:
                # A股/指数 60m 实际订阅的是 30m 合成源(kline_source_spec), 与取数同 key
                src_dur, src_cnt = futures_kline.kline_source_spec(sym, dur, cnt)
                obj = slot.request_kline_obj(sym, src_dur, src_cnt)
                if obj is None:
                    continue
                dl = time.time() + 12
                while time.time() < dl and not slot._kline_ready(obj):
                    time.sleep(0.2)
                ok += 1
            except Exception:
                pass
    _warm_done = True
    print("[技术分析] K 线预热完成: %d 个对象就绪" % ok, flush=True)


def _ensure_warm():
    """历史兼容空实现: K 线预建已迁移到 main.py 的 startup_poll(扫描账号 Slot 连接时确定性
    顺序预建, 早于 pump 循环、绝不竞态/冻泵)。此处不再后台预热, 仅保留空壳以兼容既有调用点。"""
    return


def _is_default_cached(symbol):
    """默认参数(ma=2,10,20,60 + 空颈线/预设/自定义 + 默认阈值 + INDC 默认指标)下, 该标的日线周期是否已热。
    以 day 周期默认键为代理: 一次 compute_instrument 会把全部 6 周期都写缓存, 故 day 热≈全热。
    天勤源标的返回 False(它的就绪判定依赖 K 线对象, 不走此代理, 由 worker 自行等)。
    键必须与 compute_instrument 的 8 段拼法完全一致(且对齐前端默认请求), 否则命中不到 → 误判"冷"。"""
    if not _is_http_src(symbol):
        return False
    # 默认参数键(对齐前端 MAP=2,10,20,60 与 INDC 默认值; 与 _preload_worker 预热同一套):
    #   symbol|周期|MA|颈线|预设|阈值签名|自定义签名|指标签名
    _thr_sig = ";".join("%s:%s,%s" % (k, v[0], v[1])
                        for k, v in sorted((parse_thresholds("") or {}).items()))
    _cust_sig = ";".join("%s:%s" % ((it or {}).get("name", ""), (it or {}).get("value", ""))
                         for it in [])
    _ind_sig = ind_params_from_args(_PRELOAD_DFLT_IND).get("sig", "")
    _k = "%s|%s|%s|%s|%s|%s|%s|%s" % (symbol, "day", ",".join(map(str, _PRELOAD_DFLT_MA)),
                                      "", "", _thr_sig, _cust_sig, _ind_sig)
    with _CACHE_LOCK:
        return _CACHE.get(_k) is not None


def _build_preload_order():
    """去重的有序预加载清单: LEFT(固定)首, 默认次, 候选末(与 _preload_tech_cache 原口径一致)。"""
    seen, order = set(), []
    for it in (DEFAULT_INSTRUMENTS[0:1] + DEFAULT_INSTRUMENTS[1:] + EXTRA_INDEX_INSTRUMENTS):
        s = it["symbol"]
        if s in seen:
            continue
        seen.add(s)
        order.append(it)
    return order


def _reseed_preload_queue():
    """续热用: 把全部已知标的重新入队; 循环体对仍在 TTL 内的标会跳过, 不重复算、不抢 GIL。"""
    order = _build_preload_order()
    with _PRELOAD_LOCK:
        _PRELOAD_QUEUE.clear()
        _PRELOAD_SEEN.clear()
        for it in order:
            s = it["symbol"]
            if s in _PRELOAD_SEEN:
                continue
            _PRELOAD_SEEN.add(s)
            _PRELOAD_QUEUE.append(it)


def _preload_sym_fresh(sym):
    """该标"默认参数"缓存是否仍在 TTL 内(续热循环据此跳过, 省 GIL)。"""
    ts = _PRELOAD_TS.get(sym)
    return ts is not None and (time.time() - ts) < _CACHE_TTL


def _preload_tech_cache():
    """启动预热(改为优先队列模型): 构建【LEFT 固定标首 → 默认标 → 候选标】的去重队列,
    再起一个 daemon worker 串行消费。用户打开页面时客户端通过 /stocktech/api/preload_priority
    把自己缓存的标插到队首 => 打开即见, 不干等。详见模块顶部 _PRELOAD_QUEUE 注释。

    关键约束(与天勤同线程铁律 / GIL 预算强相关):
    - worker 必须等默认账号 Slot 在线且天勤源标的 K 线数据就绪后再算, 否则踩雷/缓存空值。
    - 每算完一个标的让出 GIL; 且 _USER_REQ_ACTIVE>0(有用户请求活跃)时多睡, 绝不饿死前端/行情泵。
    - worker 遇到 _USER_COLD(用户正在冷算的标的)直接跳过, 避免同标的双算抢 GIL。
    - cache_key 不含账号 → 用默认账号算一次, 所有用户(含不同登录账号)命中同一份缓存。
    """
    import time as _t
    # 1) 构建优先队列: LEFT(固定)首, 默认其余, 候选末(去重逻辑抽到 _build_preload_order)
    _order = _build_preload_order()
    with _PRELOAD_LOCK:
        _PRELOAD_QUEUE.clear()
        _PRELOAD_SEEN.clear()
        for it in _order:
            if it["symbol"] in _PRELOAD_SEEN:
                continue
            _PRELOAD_SEEN.add(it["symbol"])
            _PRELOAD_QUEUE.append(it)
    print("[技术分析] 预加载队列就绪(%d 标的, LEFT=%s 优先), 启动 worker..." %
          (len(_order), DEFAULT_INSTRUMENTS[0]["symbol"]), flush=True)
    _t2 = threading.Thread(target=_preload_worker, daemon=True)
    _t2.start()


def _preload_worker():
    """串行消费 _PRELOAD_QUEUE 的后台 worker: 把指标计算结果填入 _CACHE。
    与 _warm_scan_klines 同口径先等默认账号 Slot 在线; 天勤源标的等 K 线对象就绪再算。"""
    import time as _t
    global _preload_done
    slot = None
    for _ in range(60):  # 等扫描账号上线(最多 ~180s)
        slot = _get_slot_for_scan("")
        if slot is not None and getattr(slot, "status", "") == "online" \
                and getattr(slot, "client", None) is not None:
            break
        slot = None
        _t.sleep(3)
    if slot is None:
        print("[技术分析] 预加载中止: 启动 3 分钟内未拿到已连接的默认账号 Slot", flush=True)
        with _PRELOAD_LOCK:
            _preload_done = True
        return
    _day = next((p for p in TECH_PERIODS if p[0] == "day"), None)
    _ok = 0
    _prev_computed = 0
    while True:
        with _PRELOAD_LOCK:
            if not _PRELOAD_QUEUE:
                # 续热: 队列空不退出, 重新入队全部已知标的; 循环体对仍在 TTL 内的标会跳过。
                _reseed_preload_queue()
                if not _PRELOAD_QUEUE:
                    _t.sleep(5)
                    continue
                # 上一轮若没算任何标(默认参数缓存全在 TTL 内) => 缓存已热, 休眠避免空转抢 GIL
                if _prev_computed == 0:
                    _t.sleep(3)
                _prev_computed = 0
            it = _PRELOAD_QUEUE.popleft()
        sym = it["symbol"]
        nm = it.get("name") or sym
        # 续热跳过: 该标默认参数缓存仍在 TTL 内 -> 不重复算(省 GIL, 不饿死行情泵/前端)
        if _preload_sym_fresh(sym):
            _t.sleep(0.05)
            continue
        try:
            # 用户正在冷算该标 => 跳过, 交给用户请求填充(避免双算抢 GIL)
            with _USER_REQ_LOCK:
                if sym in _USER_COLD:
                    _t.sleep(0.4)
                    continue
            if not _is_http_src(sym):
                # 天勤源: 必须等其日线 K 线对象数据就绪再算(否则缓存空值)
                from services import futures_kline as _fk
                if _day is not None:
                    src_dur, src_cnt = _fk.kline_source_spec(sym, _day[2], _day[3])
                    obj = slot._kline_cache.get((sym, src_dur, src_cnt, None))
                    if obj is None:
                        # 对象未预建(如候选指数中证500, 启动期 K 线预建只覆盖默认标):
                        # 线程安全登记到 slot 线程创建(其权限预检会拦截无权限标的, 不毒化会话)
                        try:
                            obj = slot.request_kline_obj(sym, src_dur, src_cnt)
                        except Exception:
                            obj = None
                    if obj is None:
                        _dl = _t.time() + 12
                        while _t.time() < _dl:
                            obj = slot._kline_cache.get((sym, src_dur, src_cnt, None))
                            if obj is not None:
                                break
                            _t.sleep(0.3)
                    if obj is None:
                        print("[技术分析] 预加载跳过(天勤源 K 线不可用/无权限): %s" % sym, flush=True)
                        _t.sleep(0.4)
                        continue
                    _dl = _t.time() + 15
                    while _t.time() < _dl and not slot._kline_ready(obj):
                        _t.sleep(0.3)
                    if not slot._kline_ready(obj):
                        print("[技术分析] 预加载跳过(天勤源 K 线 15s 未就绪): %s" % sym, flush=True)
                        _t.sleep(0.4)
                        continue
            # 必须与【前端默认首请求】参数完全一致(MA=MAP 默认 2,10,20,60; 指标=INDC 默认值),
            # 否则 cache_key 不同 -> 预热缓存永不被前端命中, 每次首开都冷算(曾导致"打开等 1 分钟")。
            compute_instrument(slot, sym, nm, _PRELOAD_DFLT_MA, [], [],
                               parse_thresholds(""), [], ind_params_from_args(_PRELOAD_DFLT_IND))
            _PRELOAD_TS[sym] = _t.time()
            _ok += 1
            _prev_computed += 1
            print("[技术分析] 预加载完成: %s %s" % (sym, nm), flush=True)
        except Exception as _e:
            print("[技术分析] 预加载单标异常 %s: %s" % (sym, str(_e)[:80]), flush=True)
        # 让路: 有用户请求活跃时多睡, 避免饿死行情泵/前端/收盘补扫
        with _USER_REQ_LOCK:
            _ua = _USER_REQ_ACTIVE
        _t.sleep(1.0 if _ua > 0 else 0.4)
    print("[技术分析] 指标缓存预加载完成: %d 标的就绪" % _ok, flush=True)


# ==================== 形态总数(网关导航徽标数据源, 2026-10-14) ====================
# 用户需求: "形态总数放在技术分析后面显示, 即使用户在别的模块也能看到总数"。
# 网关内容区是单 iframe 换 src —— 一切到别的模块, 技术分析页面(含悬浮球/顶栏徽标)
# 就被销毁, 页面内的总数停止更新。故总数改由【服务端常驻聚合】提供:
#   - 每 15s 读一遍指标预热缓存 _CACHE(默认参数键), 按前端 patList() 的同一口径
#     逐周期数形态 chip(shapes/背离/MACD·KDJ 交叉/RSI·KDJ 提示), 加总写入 _PAT_TOTAL;
#   - 只读缓存, 零新增天勤订阅/计算, 不挤占 GIL(与技术分析预热同源, 不添新负载);
#   - 月份合约(股指期货)只在技术分析页打开期间才进缓存, 用【粘滞计数】_PAT_LAST 保留
#     其最后一次值, 避免用户切到别的模块时总数跳水; 指数本身由预热 worker 持续保温, 恒 Live。
_PAT_TOTAL = {"total": 0, "ts": 0.0, "ready": 0, "scope": 0}
_PAT_TOTAL_LOCK = threading.Lock()
_PAT_LAST = {}                      # symbol -> 最近一次形态数(粘滞, 只增不清零)
_PAT_WORKER_STARTED = False
# 页面上报值(2026-10-17): 多标页每拍把【当前页 scope】的形态总数 POST 上来,
# 导航徽标优先展示它 —— 用户诉求"左侧模块数字与页面顶部不一致":
#   徽标原口径 = 全局(DEFAULT 6 + EXTRA 11 候选 + 月份合约), 页面顶部 = [LEFT]+RIGHT+已加载子合约,
#   两边注定对不上(实测 45 vs 12)。统一为"页面上报即所见": 页面开着时两边严格同源;
#   页面关闭后保留最后值(age 增长, 网关侧自动标"数据陈旧"); 服务重启后回落聚合值兜底。
_PAT_PAGE = {"total": 0, "ts": 0.0, "ready": 0, "scope": 0, "src": "agg"}
_PAT_PAGE_LOCK = threading.Lock()


def _pat_count_period(pe):
    """与前端 patList() 逐字同口径: 数【单个周期】结果里的形态 chip 数。"""
    if not isinstance(pe, dict) or pe.get("error"):
        return 0
    n = len(pe.get("shapes") or [])
    if pe.get("divergence"):
        n += 1
    macd = pe.get("macd")
    if isinstance(macd, dict) and macd.get("cross"):
        n += 1
    kdj = pe.get("kdj")
    if isinstance(kdj, dict) and kdj.get("cross"):
        n += 1
    for r in (pe.get("rsi_kdj") or []):
        if isinstance(r, dict) and r.get("label"):
            n += 1
    return n


def _pat_default_key(symbol, period):
    """默认参数(MA=2,10,20,60 + 空颈线/预设/自定义 + 空阈值 + INDC 默认)下的缓存键,
    与 compute_instrument 的 8 段拼法逐字一致(对齐前端默认首请求, 否则命中不到)。"""
    _thr_sig = ";".join("%s:%s,%s" % (k, v[0], v[1])
                        for k, v in sorted((parse_thresholds("") or {}).items()))
    _ind_sig = ind_params_from_args(_PRELOAD_DFLT_IND).get("sig", "")
    return "%s|%s|%s|%s|%s|%s|%s|%s" % (
        symbol, period, ",".join(map(str, _PRELOAD_DFLT_MA)), "", "", _thr_sig, "", _ind_sig)


def _pat_scope_symbols():
    """聚合范围 = 默认 6 指数 + 候选 11 指数 + 各指数旗下股指期货月份合约(仅缓存里已有的)。"""
    syms, seen = [], set()
    for it in (DEFAULT_INSTRUMENTS + EXTRA_INDEX_INSTRUMENTS):
        if it["symbol"] not in seen:
            seen.add(it["symbol"])
            syms.append(it["symbol"])
    # 月份合约只从 _FUT_MONTHS_CACHE 取(页面打开过才有); 未缓存的产品不在此触发查询,
    # 不给天勤添短期会话 —— 月份计数靠粘滞值延续(技术分析页打开期间会自然刷新)。
    for _prod, _pname in INDEX_FUT_PRODUCT.values():
        for m in ((_FUT_MONTHS_CACHE.get(_prod) or {}).get("months") or []):
            s = m.get("symbol") if isinstance(m, dict) else m
            if s and s not in seen:
                seen.add(s)
                syms.append(s)
    return syms


def _pat_total_refresh():
    """聚合一轮形态总数(纯内存读缓存, 无网络无计算)。返回总数。"""
    total, ready = 0, 0
    for sym in _pat_scope_symbols():
        cnt = None
        for key, _label, _dur, _cnt in TECH_PERIODS:
            with _CACHE_LOCK:
                hit = _CACHE.get(_pat_default_key(sym, key))
            if hit is not None:
                cnt = (cnt or 0) + _pat_count_period(hit[1])
        if cnt is not None:            # 有缓存(哪怕 0 个形态) -> 刷新粘滞值
            _PAT_LAST[sym] = cnt
            ready += 1
        total += _PAT_LAST.get(sym, 0)   # 无缓存(如月份合约离开页面后): 保留粘滞值
    with _PAT_TOTAL_LOCK:
        _PAT_TOTAL.update({"total": total, "ts": time.time(),
                           "ready": ready, "scope": len(_PAT_LAST)})
    return total


def _pat_total_worker():
    while True:
        try:
            _pat_total_refresh()
        except Exception as _e:
            print("[技术分析] 形态总数聚合异常(已忽略): %s" % str(_e)[:80], flush=True)
        time.sleep(15)


def start_pat_total_worker():
    """启动形态总数聚合线程(幂等)。由 register_routes 调用一次。"""
    global _PAT_WORKER_STARTED
    if _PAT_WORKER_STARTED:
        return
    _PAT_WORKER_STARTED = True
    threading.Thread(target=_pat_total_worker, daemon=True, name="tech-pat-total").start()
    print("[技术分析] 形态总数聚合线程已启动(网关导航徽标数据源, 15s 一拍)", flush=True)


def api_pattern_total():
    """GET /stocktech/api/pattern_total —— 网关导航"技术分析"项后面的形态总数。

    取值优先级(2026-10-17): 页面上报值(_PAT_PAGE) > 服务端聚合值(_PAT_TOTAL)。
    页面上报值 = 多标页 patCheck() 的 _patTotal, 与页面顶部"🔔 形态 N"逐帧同源;
    页面从未打开(或服务刚重启)时回落聚合线程的全局口径兜底。
    """
    with _PAT_PAGE_LOCK:
        snap = dict(_PAT_PAGE)
    if snap.get("ts"):
        ts = snap.get("ts") or 0
        age = max(0, int(time.time() - ts)) if ts else -1
        try:
            upd = datetime.fromtimestamp(ts).strftime("%H:%M:%S") if ts else "--:--:--"
        except Exception:
            upd = "--:--:--"
        return jsonify({
            "ok": True,
            "total": int(snap.get("total") or 0),
            "ready": int(snap.get("ready") or 0),
            "scope": int(snap.get("scope") or 0),
            "age": age,
            "updated_at": upd,
            "src": "page",
        })
    with _PAT_TOTAL_LOCK:
        snap = dict(_PAT_TOTAL)
    ts = snap.get("ts") or 0
    age = max(0, int(time.time() - ts)) if ts else -1
    try:
        upd = datetime.fromtimestamp(ts).strftime("%H:%M:%S") if ts else "--:--:--"
    except Exception:
        upd = "--:--:--"
    return jsonify({
        "ok": True,
        "total": int(snap.get("total") or 0),
        "ready": int(snap.get("ready") or 0),
        "scope": int(snap.get("scope") or 0),
        "age": age,
        "updated_at": upd,
        "src": "agg",
    })


def api_pattern_total_report():
    """POST /stocktech/api/pattern_total —— 多标页每拍上报当前页形态总数。

    body: {"total": int, "rows": [[sym, n], ...], "scope": int}
      total = 页面顶部徽标/悬浮球展示的同一个数(patCheck 的 _patTotal);
      rows  = 有形态的标的明细(ready = len(rows));
      scope = 本拍统计范围的标的数([LEFT]+RIGHT+已发现月份合约)。
    只写内存, 零数据库/零天勤; 网关导航徽标(20s 轮询 GET)读它。
    """
    data = request.get_json(silent=True) or {}
    try:
        total = int(data.get("total") or 0)
        scope = int(data.get("scope") or 0)
        rows = data.get("rows") or []
    except Exception:
        return jsonify({"ok": False, "msg": "bad payload"}), 400
    if total < 0 or total > 99999 or scope < 0 or scope > 500:
        return jsonify({"ok": False, "msg": "out of range"}), 400
    if not isinstance(rows, list) or len(rows) > 500:
        return jsonify({"ok": False, "msg": "bad rows"}), 400
    with _PAT_PAGE_LOCK:
        _PAT_PAGE.update({"total": total, "ts": time.time(),
                          "ready": len(rows), "scope": scope, "src": "page"})
    return jsonify({"ok": True, "total": total})


# ===================== 指标补充 =====================
# 可由用户配置阈值的指标(2026-09-08 需求: 参数由用户填, 超过上限=超买, 低于下限=超卖, 中间灰)
# 🔴 2026-09-20 用户裁定【翻转着色语义】: 超买=绿 / 超卖=红。
#   理由: 新加的 RSI/KDJ 提示按用户的"红=偏多 / 绿=偏空"口径(超卖红、超买绿),
#   若阈值着色仍是"超买红/超卖绿", 同一格里会出现 "KDJ 数字是绿" 与 "红字 KDJ超卖" 自相矛盾。
#   翻转后全页只剩一套口径: 红=偏多(支撑/金叉/超卖/做多) / 绿=偏空(压力/死叉/超买/做空)。
#   映射点只有前端 thrCls() 一处(thrW 与 kdjCell 都走它), 后端 _thr_state 只回状态字符串。
# 判定取值: kdj->J值, rsi->RSI, psy->PSY, vr->量比
# 注: 2026-09-11 起 MACD 指标行已从技术分析页移除, macd 阈值同步停用
THR_KEYS = ("kdj", "rsi", "psy", "vr")


def parse_thresholds(raw):
    """解析阈值参数 'kdj:80,20;rsi:70,30' -> {'kdj':(80.0,20.0), 'rsi':(70.0,30.0)}。

    hi/lo 可单边留空: 'kdj:80,'=只判上限, 'kdj:,20'=只判下限。
    """
    out = {}
    for seg in (raw or "").split(";"):
        seg = seg.strip()
        if not seg or ":" not in seg:
            continue
        k, _, v = seg.partition(":")
        k = k.strip().lower()
        if k not in THR_KEYS:
            continue
        parts = (v or "").split(",")
        hi = lo = None
        try:
            if parts and parts[0].strip():
                hi = float(parts[0])
            if len(parts) > 1 and parts[1].strip():
                lo = float(parts[1])
        except Exception:
            continue
        if hi is None and lo is None:
            continue
        out[k] = (hi, lo)
    return out


# ===================== 指标参数(2026-09-15 用户要求: 指标参数可自定义) =====================
# 默认值 = 主流看盘软件默认: MACD(12,26,9) / KDJ(9,3,3) / BOLL(20,20,2) / RSI三线(6,12,24)。
# BOLL 三个数 = (N 统计周期, P 中轨均线周期, M 标准差倍数); P 缺省等于 N(即标准 BOLL)。
# RSI 三线只用于做多/做空差值信号(2026-09-21 用户裁定方案A); 背离仍固定 RSI14(业界默认, 口径不变)。
# 用户在前端浮层里改, 经 &macd=&kdj=&boll=&rsi= 分段下发(见 parse_ind_params)。
DEF_MACD = (12, 26, 9)
DEF_KDJ = (9, 3, 3)
DEF_BOLL = (20, 20, 2)
DEF_RSI_SIG = (6, 12, 24)

# 合法区间(宽到够用, 窄到防挂): 超界一律回落默认值, 而不是报错 —— 用户手抖填 0/负数不能把页面打挂。
_IND_LIMITS = {
    "macd": (1, 200),   # 每个分量
    "kdj": (1, 200),    # N; M1/M2 上限 100
    "boll": (2, 250),   # N / P
    "rsi": (2, 100),    # 信号三线周期, 要求 短<中<长
}


def parse_ind_params(raw):
    """解析 'macd=12,26,9&kdj=9,3,3&boll=20,20,2' 里的三段 → dict。

    入参 raw 为 {key: "a,b,c"} 或已解析的值; 任一分量非法/缺失 → 该指标整体回落默认。
    返回 {"macd": (f,s,sig), "kdj": (n,m1,m2), "boll": (n,p,k), "rsi": (s,m,l), "sig": "..."}。
    """
    src = raw if isinstance(raw, dict) else {}

    def _nums(k, cnt):
        v = src.get(k)
        if v is None:
            return None
        parts = [x.strip() for x in str(v).split(",")]
        if len(parts) != cnt:
            return None
        out = []
        for x in parts:
            try:
                out.append(float(x))
            except Exception:
                return None
        return out

    m = _nums("macd", 3)
    macd = DEF_MACD
    if m and _IND_LIMITS["macd"][0] <= m[0] <= _IND_LIMITS["macd"][1] \
            and _IND_LIMITS["macd"][0] <= m[1] <= _IND_LIMITS["macd"][1] \
            and 1 <= m[2] <= 100 and 0 < m[0] < m[1]:
        macd = (int(m[0]), int(m[1]), int(m[2]))

    k = _nums("kdj", 3)
    kdj = DEF_KDJ
    if k and _IND_LIMITS["kdj"][0] <= k[0] <= _IND_LIMITS["kdj"][1] \
            and 1 <= k[1] <= 100 and 1 <= k[2] <= 100:
        kdj = (int(k[0]), int(k[1]), int(k[2]))

    b = _nums("boll", 3)
    boll = DEF_BOLL
    if b:
        n, p, kk = b
        if p == 0:            # 允许 P 填 0 表示"与 N 相同"
            p = n
        if _IND_LIMITS["boll"][0] <= n <= _IND_LIMITS["boll"][1] \
                and _IND_LIMITS["boll"][0] <= p <= _IND_LIMITS["boll"][1] \
                and 0.1 <= kk <= 10:
            # 倍数取整时存 int(2 而非 2.0) —— 前端回显/URL 更干净
            kk_v = int(kk) if float(kk).is_integer() else float(kk)
            boll = (int(n), int(p), kk_v)

    r = _nums("rsi", 3)
    rsi_sig = DEF_RSI_SIG
    if r and all(_IND_LIMITS["rsi"][0] <= x <= _IND_LIMITS["rsi"][1] for x in r) \
            and r[0] < r[1] < r[2]:
        rsi_sig = (int(r[0]), int(r[1]), int(r[2]))

    return {"macd": macd, "kdj": kdj, "boll": boll, "rsi": rsi_sig,
            "sig": "m%s|k%s|b%s|r%s" % (",".join(map(str, macd)), ",".join(map(str, kdj)),
                                        ",".join(map(str, boll)), ",".join(map(str, rsi_sig)))}


def ind_params_from_args(args):
    """从 Flask request.args 取 ind=... 三段。兼容 ind=MACD 用 &macd=&kdj=&boll= 分开传。"""
    raw = {}
    for key in ("macd", "kdj", "boll", "rsi"):
        v = args.get(key)
        if v:
            raw[key] = v
    # 也支持一次性 ind=12,26,9;9,3,3;20,20,2 (三段分号隔开)
    one = (args.get("ind") or "").strip()
    if one:
        segs = one.split(";")
        for key, seg in zip(("macd", "kdj", "boll", "rsi"), segs):
            if seg.strip() and not raw.get(key):
                raw[key] = seg.strip()
    return parse_ind_params(raw)


DEFAULT_IND_PARAMS = parse_ind_params({})


def _thr_block(user_thresholds, values):
    """按用户阈值给各指标算着色状态, 返回 {key: {v, hi, lo, st}} (未配置阈值的指标不返回)。"""
    out = {}
    if not user_thresholds:
        return out
    for key, val in (values or {}).items():
        cfg = user_thresholds.get(key)
        if not cfg:
            continue
        hi, lo = cfg
        st = _thr_state(val, hi, lo)
        if st:
            out[key] = {"v": val, "hi": hi, "lo": lo, "st": st}
    return out


def _thr_state(v, hi, lo):
    """阈值判定: 超上限='over'(超买), 低于下限='under'(超卖), 其间='mid'(灰)。

    2026-09-20 用户裁定翻转着色语义 —— over 现在出【绿】、under 出【红】(见 thrCls)。
    本函数只负责状态判定, 不含任何颜色, 故翻转不影响这里。"""
    if v is None:
        return None
    try:
        v = float(v)
    except Exception:
        return None
    if hi is not None and v > hi:
        return "over"
    if lo is not None and v < lo:
        return "under"
    return "mid"


def psy(close, n=12):
    """心理线 PSY = 近 n 日上涨天数占比 * 100。"""
    s = pd.Series(close, dtype="float64")
    up = (s.diff() > 0).astype(float)
    win = up.rolling(n, min_periods=n).sum()
    return indicators.to_list(win / n * 100.0)


def _last_non_none(arr):
    if arr is None:
        return None
    for v in reversed(arr):
        if v is not None:
            return v
    return None


def _round(v, d=4):
    if v is None:
        return None
    try:
        return round(float(v), d)
    except Exception:
        return None


# 黄金分割"命中"容差: 现价与分割位的相对偏离 <= 该比例即视为"刚好在分割点上"(标黄)
GOLDEN_HIT_TOL = 0.001   # 0.1%
GOLDEN_RATIOS = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]


def golden_levels(hh, ll, cur):
    """按区间高低点计算黄金分割位，并逐位判定角色。

    规则(与 MA / 颈线保持一致):
      现价 > 分割位 -> 分割位在下方 -> 支撑(support, 红)
      现价 < 分割位 -> 分割位在上方 -> 压力(resist,  绿)
      现价落在分割位容差内(GOLDEN_HIT_TOL) -> 命中(ontarget, 黄)
    """
    items = []
    if hh is None or ll is None:
        return items
    rng = hh - ll
    for r in GOLDEN_RATIOS:
        v = _round(hh - r * rng)
        if v is None:
            continue
        if cur is not None and v and abs(cur - v) <= abs(v) * GOLDEN_HIT_TOL:
            role = "ontarget"
        elif cur is not None and cur > v:
            role = "support"
        else:
            role = "resist"
        items.append({"ratio": r, "value": v, "role": role})
    return items


# 价位相对现价的角色容差(与前端 LVL_TOL 保持一致): 现价落在该比例内视为"命中"。
LEVEL_HIT_TOL = 0.001   # ±0.1%

# 事件型形态信号(MACD/KDJ金叉死叉、背离)只在最近 N 根 K 线内保鲜。
# N=1 表示: 新 K 线形成后, 旧的金叉/死叉/背离自动清理, 不再一直挂在形态行。
SIGNAL_FRESH_BARS = 1

# 🔴 背离不能用上面那把尺子(2026-09-17 修"背离从来不显示"的根因):
#   SIGNAL_FRESH_BARS=1 的语义是"信号发生在最后一根 K 线上"; KDJ/MACD 交叉直接用相邻两根
#   的值比较, 可以做到。但【背离依赖枢轴】—— 枢轴要等右侧 order 根走完才能确认, 天然滞后。
#   详见 compute_period 背离段注释。
DIV_PIVOT_ORDER = 5     # 背离枢轴窗口(order): 与形态识别 detect_shapes 同口径, 滤掉噪声小枢轴
DIV_FRESH_BARS = 15     # 背离确认点(后一个枢轴 x2)落在最近 15 根 K 线内即算有效

# 🔴 背离的【噪声门槛】(2026-09-17, 用户以参考端对照 IH2612 1m 的形态行投诉后追加):
#   投诉用例(上证50 -> 子合约 CFFEX.IH2612 1m, 末根 15:00 收 2792.40): 形态行报"顶背离",
#   而它的两个枢轴高点是 2791.80(14:45) -> 2792.00(14:53) —— 相差 0.20 点,
#   恰好是 IH 的【一个最小变动价位(1 tick)】, 占价 0.007%; RSI 只差 1.13;
#   而且末根已经【越过第二个高点】(2792.40 > 2792.00) —— "顶已被刷新还叫顶背离"。
#   同一格里 MACD金叉 + KDJ金叉(两个多头信号都是真的)与"顶背离"并列, 明显自相矛盾。
#
#   三条门槛(各自可回退到旧行为):
#     ① DIV_NO_BREAK    顶背离要求第二个枢轴之后【没有更高的高点】(价格已创新高 = 顶已破, 该背离失效);
#                        底背离要求之后没有更低的低点。**置 0 = 关闭, 恢复"不管后面有没有被刷新"**。
#                        实测(10 标的 x 6 周期 + IH2612 1m, 共 61 格): win=15 时 18 -> 18 格,
#                        win=60 时 44 -> 38 格 —— 是【精准过滤】, 只挡已经失效的那些, 不误杀。
#     ② DIV_MIN_RSI     |ΔRSI| >= 2.0 (0~100 刻度)。实测 11 个 fresh 候选里只挡掉 1 个(0.58 那个)。
#                        **置 0 = 关闭**。
#     ③ DIV_MIN_MACD_ATR |ΔDIF| >= 0.20 x ATR14 —— DIF 是【价格单位】(上证日线的 0.3 点与 IH 1m 的
#                        0.3 点完全不是一个量级), 必须按 ATR 归一才能跨品种/周期比较。
#                        IH2612 1m 的 ATR14 = 1.02 -> 门槛 0.20 点。**置 0 = 关闭**。
#   组合效果(61 格样本): "有背离的格子" 30% -> 23% (win=15), 既不空也不刷屏。
DIV_NO_BREAK = 1
DIV_MIN_RSI = 2.0
DIV_MIN_MACD_ATR = 0.20

# KDJ 交叉的"最小分离度"(单位: KDJ 的 0~100 刻度)。2026-09-17 用户以参考端对照提出。
#   背景: 上证指数 60m 末根 K=62.18 / D=63.24 —— 前一根 K-D 仅 +0.21, 末根 -1.06,
#   数值上确实"下穿"了, 但两根线几乎重合, 参考端图上看着根本没分叉 -> 用户判为误报。
#   这类"擦边交叉"属噪声级: 实测 5 标的 x 4 周期共 916 个交叉, 交叉后 |K-D| 的 25 分位 = 1.17,
#   10 分位 = 0.46。取 1.5 挡掉约 31% 的擦边交叉, 保留 69% 幅度明确的交叉。
#   **置 0 = 恢复旧行为(任意幅度都报)**, 随时可回退。
KDJ_CROSS_MIN_GAP = 1.5

# ===== RSI(6/12/24) 三线合一 · 差值做多做空 · KDJ 超买超卖 (2026-09-20 用户规格) =====
#   阈值【逐字照抄用户原话】, 不做任何"优化":
#     低位: 6RSI<13,   12RSI<22,   24RSI<30  -> 满足任意 2 条=二合一(红), 满足 3 条=三合一(红)
#     高位: 6RSI>94,   12RSI>87,   24RSI>80  -> 满足任意 2 条=二合一(绿), 满足 3 条=三合一(绿)
#     差值: 6R-12R<=-13 或 6R-24R<=-20 -> 做多(红);  6R-12R>=13 或 6R-24R>=20 -> 做空(绿)
#     KDJ : K/D/J 三值【同时】<20 -> 超卖(红); 三值【同时】>80 -> 超买(绿)
#
# 🔴 用户裁定(2026-09-20): 六类信号【各自独立判定、互不抑制】。
#    穷举确认"红绿并存"真实存在, 不是理论顾虑:
#       S6=35/S12=21/S24=29   -> 低位满足 2 条(二合一, 红) + 6R-12R=+14(做空, 绿)
#       S6=80/S12=95/S24=100  -> 高位满足 2 条(二合一, 绿) + 6R-12R=-15(做多, 红)
#       S6=50/S12=65/S24=25   -> 6R-12R=-15(做多, 红) + 6R-24R=+25(做空, 绿)
#    用户原话"按照需求人的来, 优于之前的逻辑" -> 【不加优先级、不做互斥】, 照实全部下发。
#    互斥的组合(推导所得, 供回归断言): 三合一红<->做空、三合一绿<->做多、二合一红<->二合一绿。
#
# 🔴 适用范围(用户裁定): 六组【全部】只在 60m 以上级别判 —— 1m/5m/15m/30m 一律不判(返回空)。
#    标的侧无需过滤: 本页标的池天然全是股指(6 默认指数 + 11 扩展指数 + IF/IH/IC/IM 月份合约)。
RSI_SIG_PERIODS = ("60m", "day")
RSIG_S6_LO, RSIG_S12_LO, RSIG_S24_LO = 13.0, 22.0, 30.0
RSIG_S6_HI, RSIG_S12_HI, RSIG_S24_HI = 94.0, 87.0, 80.0
RSIG_GAP12, RSIG_GAP24 = 13.0, 20.0
KDJ_SIG_LO, KDJ_SIG_HI = 20.0, 80.0


def level_role(cur, lvl, tol=LEVEL_HIT_TOL):
    """单一价位相对现价的角色(与前端 lvlRole 同一套规则)。

    现价 > 价位 -> 支撑(sup, 红) / 现价 < 价位 -> 压力(pres, 绿) / 恰在该位(±tol) -> 命中(hit)。
    无值或价位为 0 返回空串。"""
    try:
        if cur is None or lvl is None or not lvl:
            return ""
        if abs(cur - lvl) <= abs(lvl) * tol:
            return "hit"
        return "sup" if cur > lvl else "pres"
    except Exception:
        return ""


# --------------------- 跳空最小有效幅度(2026-09-17 修) ---------------------
# 用户对质: "你显示跳空了上证指数, 5/15 分钟的, 实际没有"。
# 实测确认(上证指数 3879 点): 原实现报出的 5m 跳空只有 0.22 点(占价 0.0057%)、
# 15m 只有 0.08 点(0.0021%) —— 数值上确实存在价差, 但没有交易者会把它叫跳空。
#
# 🔴 根因: 原实现「只要 low[i] > high[i-1] 就判跳空」, 对缺口幅度【没有任何要求】。
#   连续竞价中相邻两根 K 线共享同一个成交价, 区间本不该脱节; 之所以还能扫出 0.0x 点的
#   "缺口", 来自两类噪声:
#     ① 隔夜开盘的极小跳空(真实但无意义, 如 0.08 点);
#     ② 行情源/采样粒度毛刺 —— 腾讯源 1m 甚至能在【盘中】扫出 0.03~0.31 点的假跳空
#        (连续竞价里理论上不可能出现, 属数据 artifact)。
#   实测 16 指数 × 6 周期 × 90 根窗口共 586 个候选缺口: 噪声集中在 0.001~0.10 × ATR14
#   且占价 ≤ 0.09%; 真实缺口 ≥ 0.20 × ATR14 或 ≥ 价格的 0.10%(两者必居其一)。
#
# 判据: 缺口须 ≥ 【两者中较小的那个】, 等价于"两个尺度同时不达标才算噪声"。
#   为什么是 min 而不是 max: 单用 ATR 会误杀日线大缺口 —— 深证成指日线一个 63.3 点的
#   跳空(占价 0.47%)因为其日线 ATR 高达 277 点, 被 0.25×ATR=69 判成噪声; 单用百分比
#   又会漏放低波动标的。取 min = 只可能少放噪声, 绝不会误杀真缺口。
GAP_MIN_ATR = 0.30      # 缺口 ≥ 本周期 ATR(14) 的 30%(约"一根典型波幅的 1/3")
GAP_MIN_PCT = 0.0010    # 或 ≥ 价格的 0.10%(满足其一即可)


def _gap_min_size(highs, lows, closes, cur):
    """跳空的最小有效幅度(点数)。两个尺度都拿不到时返回 None —— 调用方据此【不做跳空判定】,
    宁可不显示也不误报(与"宁可不出"的形态识别 v2 同一取舍)。"""
    floors = []
    try:
        _a = _last_non_none(indicators.atr(highs, lows, closes, 14))
    except Exception:
        _a = None
    if _a and _a > 0:
        floors.append(GAP_MIN_ATR * _a)
    if cur and cur > 0:
        floors.append(GAP_MIN_PCT * cur)
    return min(floors) if floors else None


# ---------------- MACD 零轴 x 关键位「确认 / 临近」信号(2026-09-17 用户新增) ----------------
# 用户原话: "形态里面增加几个类型, 一个是 MACD 的上下零轴有关, 用于确认突破压力、
#   跌破支撑、临近压力、临近支撑; 绿色显示突破即可, 长文字你可以自己做一个简化"。
#
# 四类信号对称设计 —— MACD 零轴 = 多空【确认器】, 关键位 = 【几何最近】的上下方价位:
#   ① 突破压力: 现价站上【刚被上穿】的价位(穿越发生在最近 ZONE_BREAK_BARS 根内)
#               且 DIF 在零轴上方(多头确认)          -> 绿(chip pres,      文案"突破")
#   ② 临近压力: 现价仍在【最近上方价位】之下, 但距离 <= 门槛
#               且近 N 根净上行(向上逼近)            -> 绿(chip pres soft, 文案"近压力")
#   ③ 跌破支撑: 现价跌破【刚被下穿】的价位, 且 DIF 在零轴下方
#                                                    -> 红(chip sup,      文案"跌破")
#   ④ 临近支撑: 现价仍在【最近下方价位】之上, 但距离 <= 门槛
#               且近 N 根净下行(向下逼近)            -> 红(chip sup soft, 文案"近支撑")
#   压力侧/支撑侧各自最多出 1 个, 且"突破/跌破"优先于"临近" —— 一格最多 2 个 chip。
#   chip 文案(2026-09-17 用户定): 已【发生】的两条用动词("突破"/"跌破"),
#   只是【贴近】的两条用名词("近压力"/"近支撑") —— 语义上一眼可分。
#   配色沿用中国惯例(涨/多=红, 跌/空=绿)在该模块的既有映射: 压力侧=绿, 支撑侧=红。
#
# 🔴 取价位必须用【纯数值口径】, 不能用 levels 里的 role:
#   role 是 level_role(cur, lvl) 按现价【实时】算的 —— 价格一旦站上某位, 该位立刻变成 sup,
#   于是"刚被突破的那条压力"在 levels 里已经"不是压力"了, 按 role 取会永远取到突破后的
#   新压力(更上面那条), "突破压力"这个信号根本不可能成立。所以: 上穿事件在下方位价里找,
#   下穿事件在上方位价里找。
#
# 门槛口径与跳空(GAP_MIN_*)一致: 取 min(ZONE_NEAR_ATR*ATR14, ZONE_NEAR_PCT*现价) ——
#   两个尺度同时不达标才算"不临近", 只可能少报, 不会误杀(低波动标的靠百分比尺度兜住)。
ZONE_NEAR_ATR = 0.50      # "临近" = 距关键位 <= 0.5 x ATR(14)
ZONE_NEAR_PCT = 0.0050    #          或 <= 价格的 0.50%
ZONE_BREAK_BARS = 3       # 穿越须发生在最近 3 根 K 线内(事件保鲜, 同 SIGNAL_FRESH_BARS 的思路)

# "临近"的【方向】判据回看根数(2026-09-17 用户以参考端 K 线图对照后追加):
#   原来的"临近"只看【静态距离】—— 只要最近的上方位在门槛内就报"临近压力"。
#   实测误报: 上证指数 60m 收 3875.60, 上方最近位 BOLL 中轨 3879.61 只差 4.01 点
#   (< 门槛 8.66 点) 就报了"临近压力"; 但价格最近 3 根是 3879.94 -> 3875.60 (净 -4.34 点),
#   明明是【回落远离】压力位。参考端判词: "价格是回落走势, 没有向上逼近压力, 不触发"。
#   故: 临近压力 要求价格近 ZONE_APPROACH_BARS 根净【上行】(向上逼近);
#       临近支撑 要求净【下行】(向下逼近)。方向不符 -> 不报(宁可不出)。
ZONE_APPROACH_BARS = 3

# 🔴 「突破/跌破」必须穿一条【真位】(2026-09-17, 用户以参考端对照 IH2612 1m 的形态行投诉后追加):
#   投诉用例: 形态行报"压力"(即突破压力), 穿的位是 MA2 @ 14:59 —— 末根 2791.40 -> 2792.40,
#   MA2 = (2791.40+2792.40)/2 = 2791.90 恰在两根收盘之间。
#   **MA2 这种自指位(值恒等于最近两根收盘的中间) => 任何阳线都"上穿 MA2"、任何阴线都"下穿 MA2"**,
#   报出来的"突破压力/跌破支撑"其实什么都没突破(和之前修过的"MA2 导致临近刷屏"同一族问题)。
#   判据: 这条位必须真的当过压力/支撑 —— 回看穿越前的 ZONE_BREAK_HOLD_M 根,
#   其中至少 ZONE_BREAK_HOLD_K 根的【当根位值】在收盘价的另一侧。
#   实测(10 标的 x 6 周期 + 2 个期货, 共 141 次穿越):
#      驻留度分布 0:10 / 1:26 / 2:34 / 3:33 / 4:31 / 5:7
#      要求 >= 4/5 -> 保留 38/141 (27%);  其 MA2 类自指位几乎全是 1~3/5(被挡)
#      用户上次确认正确的"上证 60m 跌破支撑(MA10 @ 09-17 14:00)" = 4/5 -> **保留** ✓
#   回退: ZONE_BREAK_HOLD_K = 0 即完全恢复旧行为。
ZONE_BREAK_HOLD_M = 5
ZONE_BREAK_HOLD_K = 4

# 🔴 「自指位」硬排除(2026-09-17 二次修, 真机核验发现的漏网)。
#   上面 ZONE_BREAK_HOLD_* 的注释写了"MA2 类自指位几乎全是 1~3/5(被挡)" ——
#   "几乎"两个字就是漏洞: _held 数的是【当根收盘 vs 当根位值】, 而 MA2_j 恒落在
#   (c[j-1], c[j]) 之间, 于是 `c[j] < MA2_j` 退化成"当根是不是阴线"。**连续 5 根里出现
#   4 根同向, 就把自指位凑过了 4/5 的门槛。** 真机实测(2026-09-17):
#     上证50(SSE.000016) 1m -> 形态行报"压力", title = "突破压力：MA2 2844.04，
#       上穿 09-17 14:59（穿越前 5 根里 4 根收在该位下方）" —— 又一次什么都没突破。
#   统计(6 标的 x 5 周期 = 30 组, 窗口 20 根): 位值落在【最近两根收盘区间内】的比例
#     MA2 = 1.00 (最小值也 1.00, 由构造必然) | MA3 = 0.45~0.80 | MA5 = 0.20~0.55
#     MA10 = 0.10~0.45 | MA20 = 0.00~0.30 | MA60 = 0.00~0.45 | BOLL中轨 = 0.00~0.30
#   => 取 0.90 作阈值: MA2 必被挡, 与次高的 MA3(0.80) 还留 0.10 裕度。
#   只作用于【带逐根序列的位】(MA/BOLL 这类会走动的位); 颈线/预设点位/自定义/跳空缺口
#   是恒定价位, 本来就是"价格要去够的一条线", 不参与该判据。
#   排除范围 = 四类信号的全部候选(突破/跌破 + 临近), 因为自指位当"最近的上下方位"时
#   距离恒近于 0, "临近压力：MA2，距现价 0.00%" 同样是废话。
#   回退: ZONE_LEVEL_INSIDE_MAX = 1.0 即关闭(只 1.00 才算自指, 实际只剩 MA2 一类)。
ZONE_LEVEL_INSIDE_WIN = 20      # 统计窗口(根)
ZONE_LEVEL_INSIDE_MAX = 0.90    # 占比 >= 该值 -> 判为自指位, 退出四类信号候选


def detect_zone_signals(c, h, l, dif, levels, cur, atr14=None, dates=None, level_series=None):
    """MACD 零轴 x 关键位 的四类确认信号。返回 dict(见上方常量的语义说明)。

    pres / sup : 最近的上方 / 下方价位描述(纯数值口径, 不看 role), 形如
                 {"value","name","group","dist_pct"} —— 无则 None
    brk        : "pres"(突破压力) / "sup"(跌破支撑) / None
    near       : "pres"(临近压力) / "sup"(临近支撑) / None
    dif/state  : DIF 最新值与零轴位置("above"/"below")
    zero_dir   : 最近一次 DIF 穿越零轴的方向 "up"/"down"(供 chip 的 title 交代零轴何时翻的)
    brk_at     : 被穿越的那个价位描述(含 at=日期标签), 供 title 显示"穿了哪条位"

    level_series: {level["key"]: [逐根位值]} —— 会走动的位(MA/BOLL)必须传, 详见 _lv_at。
    """
    out = {"pres": None, "sup": None, "brk": None, "near": None,
           "dif": None, "state": None, "zero_dir": None, "zero_at": None, "brk_at": None,
           "approach": None}
    try:
        n = len(c)
        if n < 5 or not levels:
            return out

        dif_last = _last_non_none(dif)
        if dif_last is not None:
            out["dif"] = _round(dif_last, 3)
            out["state"] = "above" if dif_last > 0 else "below"

        # 最近一次 DIF 穿越零轴(只往回找 60 根, 再久远就不提了 —— title 里"零轴什么时候翻的")
        for i in range(n - 1, max(0, n - 60), -1):
            a = dif[i - 1] if i - 1 < len(dif) else None
            b = dif[i] if i < len(dif) else None
            if a is None or b is None:
                continue
            if a <= 0 < b:
                out["zero_dir"], out["zero_at"] = "up", (dates[i] if dates else None)
                break
            if a >= 0 > b:
                out["zero_dir"], out["zero_at"] = "down", (dates[i] if dates else None)
                break

        def _desc(x, at=None):
            v = float(x["value"])
            return {"value": _round(v), "name": str(x.get("name") or x.get("group") or ""),
                    "group": str(x.get("group") or ""),
                    "dist_pct": round((v - cur) / cur * 100, 2), "at": at}

        # ---- 自指位排除(见常量 ZONE_LEVEL_INSIDE_* 的实测数据) ----
        #   只对【带逐根序列的位】生效: MA/BOLL 这类会走动的位。恒定价位(颈线/预设/自定义/
        #   跳空缺口)不参与 —— 它们本来就是"价格要去够的一条线", 不存在与价格同源的问题。
        _lser0 = level_series or {}

        def _self_ref(x):
            ser = _lser0.get(x.get("key"))
            if not ser:
                return False
            if ZONE_LEVEL_INSIDE_MAX >= 1.0:
                return False            # 门槛置 1.0 = 关闭(退化为"只认恒等式", 实际等于不启用)
            lo = max(1, n - ZONE_LEVEL_INSIDE_WIN)
            hit = tot = 0
            for j in range(lo, n):
                if j >= len(ser):
                    continue
                v = ser[j]
                if v is None:
                    continue
                try:
                    v = float(v)
                except Exception:
                    continue
                if v != v:
                    continue
                tot += 1
                a, b2 = c[j - 1], c[j]
                if min(a, b2) <= v <= max(a, b2):
                    hit += 1
            return tot > 0 and (hit / float(tot)) >= ZONE_LEVEL_INSIDE_MAX

        ups = [x for x in levels
               if x.get("value") and float(x["value"]) > cur and not _self_ref(x)]
        dns = [x for x in levels
               if x.get("value") and float(x["value"]) < cur and not _self_ref(x)]

        floors = []
        if atr14 and atr14 > 0:
            floors.append(ZONE_NEAR_ATR * atr14)
        if cur and cur > 0:
            floors.append(ZONE_NEAR_PCT * cur)
        near_gap = min(floors) if floors else None

        # 🔴 位值必须取【第 i 根当时的值】, 不能拿"今天的位值"去比历史收盘价。
        #   均线/布林是【会走动的】: 实测(2026-09-17 上证 60m)现行实现用今天的 MA20=3879.61
        #   去比 11:30 那根收盘 3877.46, 判出"11:30 下穿 MA20"; 而 11:30 当时 MA20=3885.65,
        #   价格早已在其下方 —— 是**凭空造出来的穿越事件**。故:
        #     会走动的位(MA/BOLL) -> 由调用方经 level_series 传逐根序列;
        #     恒定位(颈线/预设点位/跳空缺口/自定义) -> 用标量 value。
        _lser = level_series or {}

        def _lv_at(x, i):
            """位 x 在第 i 根上的值; 取不到返回 None。"""
            ser = _lser.get(x.get("key"))
            if ser is not None and 0 <= i < len(ser):
                v = ser[i]
                if v is None:
                    return None
                try:
                    v = float(v)
                except Exception:
                    return None
                return None if v != v else v        # NaN 自比不等
            try:
                return float(x["value"])
            except Exception:
                return None

        def _held(x, i, up):
            """位 x 在第 i 根被穿越前, 是否【真的当过】压力/支撑。返回 (ok, 命中根数, 有效根数)。

            回看穿越前 ZONE_BREAK_HOLD_M 根, 数其中"当根位值在收盘价另一侧"的根数:
              上穿(up)  -> 数 c[j] < 位值[j]  的根数(价格在它下方 = 它当过压力)
              下穿(!up) -> 数 c[j] > 位值[j]  的根数(价格在它上方 = 它当过支撑)
            位值一律取【当根值】(与穿越判定同一口径, 不能用末根值回比历史)。
            ⚠️ 这条门槛对自指位【不严密】(2026-09-17 真机核验后确认): MA2_j 恒在 (c[j-1], c[j])
               之间, 于是 `c[j] < MA2_j` 等价于"当根是阴线", 连续同向根数够多就能凑过门槛。
               自指位现在由 ZONE_LEVEL_INSIDE_* 在候选集层面【硬排除】, 不依赖这条计数。
               本条门槛退化为"挡掉驻留度不足的弱穿越"(如价格长期贴着位子来回磨)。
            取不到位值的根不计入分母; 若有效根数为 0(均线还没热)则不启用该门槛(宁可不改)。
            """
            m = min(ZONE_BREAK_HOLD_M, i)
            if ZONE_BREAK_HOLD_K <= 0 or m <= 0:
                return True, 0, 0
            cnt, valid = 0, 0
            for j in range(i - m, i):
                lvj = _lv_at(x, j)
                if lvj is None:
                    continue
                valid += 1
                if (c[j] < lvj) if up else (c[j] > lvj):
                    cnt += 1
            if valid <= 0:
                return True, 0, 0
            return (cnt >= min(ZONE_BREAK_HOLD_K, valid)), cnt, valid

        def _crossed(x, up, ret_at=False):
            """收盘价口径: 最近 ZONE_BREAK_BARS 根内是否有位 x 的上穿/下穿(位值按当根取),
            且该位确实"当过压力/支撑"(_held, 见常量 ZONE_BREAK_HOLD_* 的说明)。
            返回 True/False(ret_at=False) 或 (命中, 日期标签, (驻留命中数, 有效根数))(ret_at=True)。"""
            lo = max(1, n - ZONE_BREAK_BARS)
            for i in range(n - 1, lo - 1, -1):
                lvl = _lv_at(x, i)
                if lvl is None:
                    continue
                a, b = c[i - 1], c[i]
                if (up and a <= lvl < b) or ((not up) and a >= lvl > b):
                    # 🔴 自指位(MA2/MA3 这种值恒贴近收盘的)不算真突破 —— 见常量区说明。
                    okh, hc, hv = _held(x, i, up)
                    if not okh:
                        return (False, None, None) if ret_at else False
                    hh = (hc, hv) if hv else None
                    return (True, (dates[i] if dates else None), hh) if ret_at else True
            return (False, None, None) if ret_at else False

        def _same(a, b):
            return a is not None and b is not None and abs(a - b) < 1e-9

        # ① 突破压力: 在【下方位价】里找刚被上穿的那条(离现价最近的优先), 且 DIF 在零轴上方。
        #    ⚠️ 被穿那条还必须"真的当过压力"(_held) —— 否则继续往外找更远的一条,
        #       不能因为最近的一条是自指位(MA2)就把整个"突破压力"判成"没有"。
        _brkv_pres = None
        if dns and (dif_last is None or dif_last > 0):
            for x in sorted(dns, key=lambda y: -float(y["value"])):
                ok, at, hold = _crossed(x, True, ret_at=True)
                if ok:
                    out["brk"], out["brk_at"] = "pres", _desc(x, at)
                    if hold:
                        out["brk_at"]["hold"] = ("穿越前 %d 根里 %d 根收在该位下方"
                                                 % (hold[1], hold[0]))
                    _brkv_pres = float(x["value"])
                    break
        # ② 跌破支撑: 在【上方位价】里找刚被下穿的那条(离现价最近的优先), 且 DIF 在零轴下方
        _brkv_sup = None
        if ups and (dif_last is None or dif_last < 0):
            for x in sorted(ups, key=lambda y: float(y["value"])):
                ok, at, hold = _crossed(x, False, ret_at=True)
                if ok:
                    out["brk"], out["brk_at"] = "sup", _desc(x, at)
                    if hold:
                        out["brk_at"]["hold"] = ("穿越前 %d 根里 %d 根收在该位上方"
                                                 % (hold[1], hold[0]))
                    _brkv_sup = float(x["value"])
                    break

        # 最近的上方/下方价位。🔴 必须排除"刚被穿越的那条" —— 否则同一条位会既报
        # "突破压力"、又因为价格已站上去而被当成下方支撑再报一次"临近支撑"(实测踩到,
        # 上证 5m: brk=sup 的同时 near=pres 指着同一条 MA)。排除后两侧描述各自干净。
        # 说明: 突破压力的那条位必然落在 dns(价格已在它上方) -> 从 dns2 排除;
        #       跌破支撑的那条必然落在 ups(价格已在它下方) -> 从 ups2 排除。
        ups2 = [x for x in ups if not _same(float(x["value"]), _brkv_sup)]
        dns2 = [x for x in dns if not _same(float(x["value"]), _brkv_pres)]
        P = min(ups2, key=lambda x: float(x["value"])) if ups2 else None   # 最近的上方价位
        S = max(dns2, key=lambda x: float(x["value"])) if dns2 else None   # 最近的下方价位
        out["pres"] = _desc(P) if P else None
        out["sup"] = _desc(S) if S else None

        # ③④ 临近压力/临近支撑: 距离贴近 **且价格正朝该位逼近** —— 最多提示 1 个。
        #    "更近者优先"而不是"压力优先": 两侧都贴近时报更值得关注的那个, 也避免一格塞 2 个。
        #    ⚠ 落在【命中带】(±LEVEL_HIT_TOL)以内的价位不参与"临近" —— 那种情况压力/支撑表里的
        #      价位 chip 已经标黄("命中"), 再报一次"临近压力 距 0.01%"纯属重复; 而且用户若把 MA
        #      设成 MA2(几乎等于现价本身), 不排除的话每一格都会挂上"临近"。
        #      副作用(可接受): 若 near_gap 比命中带还窄(极低波动标的), 该侧就不报临近了。
        #    🔴 方向判据(2026-09-17 追加, 见 ZONE_APPROACH_BARS 注释): 只看静态距离会把
        #      "价格回落远离压力位"报成"临近压力"(实测误报)。故:
        #        临近压力 -> 要求近 N 根净上行(向上逼近); 临近支撑 -> 要求净下行。
        _hit_gap = abs(cur) * LEVEL_HIT_TOL
        _dmove = (c[-1] - c[-1 - ZONE_APPROACH_BARS]) if n > ZONE_APPROACH_BARS else 0.0
        out["approach"] = _round(_dmove, 3)
        _np, _nv = None, None
        if out["brk"] != "pres" and P is not None and near_gap is not None:
            _p = float(P["value"]) - cur
            if _hit_gap < _p <= near_gap and _dmove > 0:
                _np, _nv = "pres", _p
        if out["brk"] != "sup" and S is not None and near_gap is not None:
            _s = cur - float(S["value"])
            if _hit_gap < _s <= near_gap and _dmove < 0 and (_nv is None or _s < _nv):
                _np, _nv = "sup", _s
        out["near"] = _np
        return out
    except Exception:
        return out


def _bar_time_labels(df, fmt):
    """把 df["datetime"] 列渲染成【北京时间】标签列表；取不到返回 None。

    ⚠️ 两个行情源的 datetime 列【语义不同】, 不能无脑套 _dt_local(2026-09-17 实测踩坑):
      · 天勤源: UTC 纳秒【整数】(int64)       -> 必须 _dt_local(ns, utc=True) 再转 Asia/Shanghai
      · 腾讯源: 已是北京时间 naive datetime64 -> 直接格式化
    而 _dt_local 对 datetime64 输入是【静默】处理的(unit 参数对 datetime64 无效, utc=True
    把 naive 当 UTC 再 +8h) —— 走腾讯源的标的(科创50 等 11 个指数)时间会平白多 8 小时:
    实测腾讯 10:15 被渲染成 18:15。故此处必须按 dtype 分流。
    """
    try:
        s = df["datetime"]
    except Exception:
        return None
    try:
        if pd.api.types.is_numeric_dtype(s):
            dl = futures_kline._dt_local(s)           # 天勤: UTC 纳秒
        else:
            dl = pd.to_datetime(s)                    # 腾讯: 已是北京时间
            if getattr(dl.dt, "tz", None) is not None:
                dl = dl.dt.tz_convert("Asia/Shanghai")
        return [t.strftime(fmt) for t in dl]
    except Exception:
        return None


def _gap_bar_label(df, i, duration):
    """缺口所在 K 线的时间标签(日线 %m-%d / 分钟 %m-%d %H:%M)。取不到返回空串。

    为什么要带日期: 跳空是【事件】不是【位置】—— 只给一个价位, 用户无法判断它是今天开盘
    跳的还是三天前跳的(与形态 chip 必须带日期同因, 见 detect_shapes 的 notes)。"""
    labs = _bar_time_labels(df, "%m-%d" if duration >= 86400 else "%m-%d %H:%M")
    try:
        return labs[i] or ""
    except Exception:
        return ""


def detect_gap(highs, lows, closes, cur):
    """最近的【有意义】跳空。返回 {"type","size","idx","edge","floor"}；无则 {"type": None}。

    向上跳空: low[i] > high[i-1] -> 缺口【下沿】= 前一根最高价(回补至此常获支撑)
    向下跳空: high[i] < low[i-1] -> 缺口【上沿】= 前一根最低价(反弹至此常遇压力)
    幅度门槛见 GAP_MIN_ATR / GAP_MIN_PCT —— 缺了它会把 0.0x 点的报价毛刺当跳空(2026-09-17 修)。

    抽成独立函数是为了可被回归脚本直接调用（旧行为 = 把 GAP_MIN_ATR/GAP_MIN_PCT 置 0 即复刻）。
    """
    gap = {"type": None}
    floor_ = _gap_min_size(highs, lows, closes, cur)
    if floor_ is None:
        return gap
    for i in range(len(lows) - 1, 0, -1):
        gsz, gtype, gedge = None, None, None
        if lows[i] is not None and highs[i - 1] is not None and lows[i] > highs[i - 1]:
            gsz, gtype, gedge = lows[i] - highs[i - 1], "up", highs[i - 1]
        elif highs[i] is not None and lows[i - 1] is not None and highs[i] < lows[i - 1]:
            gsz, gtype, gedge = lows[i - 1] - highs[i], "down", lows[i - 1]
        if gsz is not None and gsz >= floor_:
            return {"type": gtype, "size": _round(gsz), "idx": i,
                    "edge": _round(gedge), "floor": _round(floor_, 2)}
    return gap


# ===================== 单周期计算 =====================
def _tail2(seq):
    """取序列末两个【非 None】值 (缺失则返回 None)。"""
    vals = [x for x in (seq or []) if x is not None]
    if len(vals) < 2:
        return None, None
    return vals[-2], vals[-1]


def kdj_cross_of(kk, kd):
    """由 K/D 序列判定最新一根是否发生金叉/死叉。返回 (cross, gap)。

      cross = "golden" / "death" / None      （判定带【最小分离度】门槛, 见 KDJ_CROSS_MIN_GAP）
      gap   = 末根 K-D(无论是否判定为交叉都给) —— 前端 chip 的 title 用它交代"这次交叉有多实"

    为什么要分离度门槛(2026-09-17, 用户拿参考端对照上证 60m 提出):
      末根 K=62.18/D=63.24, 前一根 K-D 仅 +0.21 —— 数值上确实下穿了, 但两根线几乎重合,
      参考端图上看着根本没分叉, 用户判为误报。这类"擦边交叉"是噪声级(实测 916 个交叉的
      |K-D| 25 分位 = 1.17), 故要求交叉后至少拉开 KDJ_CROSS_MIN_GAP 才算数。
      KDJ_CROSS_MIN_GAP=0 即恢复"任意幅度都报"的旧行为。
    """
    k0, k1 = _tail2(kk)
    d0, d1 = _tail2(kd)
    if k1 is None or d1 is None:
        return None, None
    g2 = k1 - d1
    if k0 is None or d0 is None:
        return None, _round(g2, 2)
    if abs(g2) < KDJ_CROSS_MIN_GAP:
        return None, _round(g2, 2)
    g1 = k0 - d0
    if g1 <= 0 < g2:
        return "golden", _round(g2, 2)
    if g1 >= 0 > g2:
        return "death", _round(g2, 2)
    return None, _round(g2, 2)


def rsi_kdj_signals(s6, s12, s24, k=None, d=None, j=None, period_key=None):
    """RSI(6/12/24) 三线合一 · 差值做多做空 · KDJ 超买超卖 —— 6 类提示(2026-09-20 用户规格)。

    返回 [{"code","label","dir","reason"}, ...]
      code   : rsi_mix2 / rsi_mix3 / rsi_long / rsi_short / kdj_lo / kdj_hi
      dir    : "up"   = 红(偏多)     |  "down" = 绿(偏空)   —— 与全页统一口径一致
      reason : 中文依据(实际数值 + 满足哪几条), 供前端 chip 的 title

    阈值语义与"并存/互斥"的实证见模块常量 RSIG_* / KDJ_SIG_* 上方注释。
    period_key 非 None 时做适用范围过滤(RSI_SIG_PERIODS); 传 None = 不过滤(单测直接调)。
    六类各自独立判定、互不抑制(用户裁定), 故返回列表里可能同时出现红与绿。
    """
    out = []
    if period_key is not None and period_key not in RSI_SIG_PERIODS:
        return out
    try:
        s6 = None if s6 is None else float(s6)
        s12 = None if s12 is None else float(s12)
        s24 = None if s24 is None else float(s24)
        k = None if k is None else float(k)
        d = None if d is None else float(d)
        j = None if j is None else float(j)
    except Exception:
        return out
    _fmt = lambda x: ("—" if x is None else "%.2f" % x)

    # ---- 组 1/2: 三线合一 —— "数满足条数"(低位与高位各自独立计数) ----
    if s6 is not None and s12 is not None and s24 is not None:
        vals = "6R=%s、12R=%s、24R=%s" % (_fmt(s6), _fmt(s12), _fmt(s24))
        lo_hit = []
        if s6 < RSIG_S6_LO:
            lo_hit.append("6R<13")
        if s12 < RSIG_S12_LO:
            lo_hit.append("12R<22")
        if s24 < RSIG_S24_LO:
            lo_hit.append("24R<30")
        hi_hit = []
        if s6 > RSIG_S6_HI:
            hi_hit.append("6R>94")
        if s12 > RSIG_S12_HI:
            hi_hit.append("12R>87")
        if s24 > RSIG_S24_HI:
            hi_hit.append("24R>80")
        if len(lo_hit) >= 2:
            _three = (len(lo_hit) == 3)
            out.append({"code": "rsi_mix3" if _three else "rsi_mix2",
                        "label": "RSI三合一" if _three else "RSI二合一",
                        "dir": "up",
                        "reason": "低位满足 %d 条（%s）｜门槛 6R<13 / 12R<22 / 24R<30｜%s"
                                  % (len(lo_hit), "、".join(lo_hit), vals)})
        if len(hi_hit) >= 2:
            _three = (len(hi_hit) == 3)
            out.append({"code": "rsi_mix3" if _three else "rsi_mix2",
                        "label": "RSI三合一" if _three else "RSI二合一",
                        "dir": "down",
                        "reason": "高位满足 %d 条（%s）｜门槛 6R>94 / 12R>87 / 24R>80｜%s"
                                  % (len(hi_hit), "、".join(hi_hit), vals)})

        # ---- 组 3/4: 差值做多 / 做空 ----
        d12 = s6 - s12
        d24 = s6 - s24
        dvals = "6R-12R=%+.2f、6R-24R=%+.2f" % (d12, d24)
        _long = []
        if d12 <= -RSIG_GAP12:
            _long.append("6R-12R≤-13")
        if d24 <= -RSIG_GAP24:
            _long.append("6R-24R≤-20")
        if _long:
            out.append({"code": "rsi_long", "label": "RSI做多", "dir": "up",
                        "reason": "满足 %s｜%s" % ("、".join(_long), dvals)})
        _short = []
        if d12 >= RSIG_GAP12:
            _short.append("6R-12R≥13")
        if d24 >= RSIG_GAP24:
            _short.append("6R-24R≥20")
        if _short:
            out.append({"code": "rsi_short", "label": "RSI做空", "dir": "down",
                        "reason": "满足 %s｜%s" % ("、".join(_short), dvals)})

    # ---- 组 5/6: KDJ 三值【同时】超卖 / 超买 ----
    if k is not None and d is not None and j is not None:
        kvals = "K=%s、D=%s、J=%s" % (_fmt(k), _fmt(d), _fmt(j))
        if k < KDJ_SIG_LO and d < KDJ_SIG_LO and j < KDJ_SIG_LO:
            out.append({"code": "kdj_lo", "label": "KDJ超卖", "dir": "up",
                        "reason": "三值同时 <20｜%s" % kvals})
        if k > KDJ_SIG_HI and d > KDJ_SIG_HI and j > KDJ_SIG_HI:
            out.append({"code": "kdj_hi", "label": "KDJ超买", "dir": "down",
                        "reason": "三值同时 >80｜%s" % kvals})
    return out


def div_gate_reason(src, d, highs, lows, atr14=None):
    """背离候选的【噪声门槛】判定: 通过返回 ""，被挡返回中文原因(便于日志与回归断言)。

    三条门槛的语义与定值理由见模块常量 DIV_NO_BREAK / DIV_MIN_RSI / DIV_MIN_MACD_ATR。
    src: "MACD" / "RSI"; d: ta_patterns.detect_divergence 的单个结果。
    """
    try:
        typ = str(d.get("type") or "")
        x2 = int(d.get("x2", 0))
        if DIV_NO_BREAK and typ == "top":
            tail = [float(x) for x in highs[x2 + 1:] if x == x]
            if tail and max(tail) > float(d["p2"]):
                return ("顶背离后价格已创新高(%.2f > 第二高点 %.2f), 顶已被刷新 -> 背离失效"
                        % (max(tail), float(d["p2"])))
        if DIV_NO_BREAK and typ == "bottom":
            tail = [float(x) for x in lows[x2 + 1:] if x == x]
            if tail and min(tail) < float(d["p2"]):
                return ("底背离后价格已创新低(%.2f < 第二低点 %.2f), 底已被刷新 -> 背离失效"
                        % (min(tail), float(d["p2"])))
        delta = abs(float(d.get("i2", 0)) - float(d.get("i1", 0)))
        if src == "RSI":
            if DIV_MIN_RSI > 0 and delta < DIV_MIN_RSI:
                return "RSI 差 %.2f < 门槛 %.2f(噪声级)" % (delta, DIV_MIN_RSI)
        else:
            if DIV_MIN_MACD_ATR > 0 and atr14 and atr14 > 0 and delta < DIV_MIN_MACD_ATR * atr14:
                return ("DIF 差 %.3f < 门槛 %.3f (%.2f x ATR14 %.3f, 噪声级)"
                        % (delta, DIV_MIN_MACD_ATR * atr14, DIV_MIN_MACD_ATR, atr14))
    except Exception as e:
        return "门槛判定异常: %s" % e
    return ""


def compute_period(slot, symbol, period_key, label, duration, count,
                   user_ma_periods, neckline_levels, preset_points,
                   user_thresholds=None, custom_levels=None, ind_params=None):
    """返回单个周期的指标字典。失败返回带 error 的 dict。

    ind_params: parse_ind_params() 的结果(含 macd/kdj/boll 三段)。None = 用默认 (12,26,9)/(9,3,3)/(20,20,2)。
    """
    ip = ind_params or DEFAULT_IND_PARAMS
    _tf0 = time.perf_counter()
    try:
        if _is_http_src(symbol):
            # HTTP 兜底源(腾讯): 天勤不存在/无权限的标的走这里, 不碰 AccountSlot, slot 可为 None
            df = tx_kline.fetch_df(symbol, period_key, count)
        elif period_key == "60m" and futures_kline.uses_ashare_60m_sessions(symbol):
            # A股/指数 60m(2026-09-15 用户裁定 A): 天勤 60m 按自然小时切(5 根/天), 主流看盘
            # 按交易时段切(4 根/天) -> KDJ/MACD 回看窗口不同, 结论会分歧。故改用天勤 30m
            # 合成标准交易时段柱(10:30/11:30/14:00/15:00), 与东财/同花顺/文华逐值一致。
            df, real = futures_kline._fetch_kline_60m_std(slot, symbol, count)
        else:
            df, real = futures_kline._fetch_kline_df(slot, symbol, duration, count)
    except Exception as e:
        return {"error": "K线拉取失败: %s" % e}
    if DEBUG_TIMING:
        print("[TECHDBG] %s %-4s fetch=%.1fms rows=%s" % (
            symbol, period_key, (time.perf_counter() - _tf0) * 1000,
            0 if df is None else len(df)), flush=True)
    if df is None or df.empty:
        return {"error": "暂无K线数据"}

    df = df[df["close"] > 0].copy()
    if len(df) < 30:
        return {"error": "K线数量不足(<30)"}

    o = df["open"].astype(float).tolist()
    h = df["high"].astype(float).tolist()
    l = df["low"].astype(float).tolist()
    c = df["close"].astype(float).tolist()
    v = df["volume"].astype(float).tolist()

    cur = c[-1]
    prev = c[-2] if len(c) >= 2 else c[-1]
    # 实时价覆盖"当前价/前收": K 线柱收尾刷新太粗(约7~10s一跳), 直接用【订阅推送】的报价。
    # tq 源 = 天勤 quote 对象(泵线程 wait_update 持续推送, 零往返); tx 源 = 腾讯 qt 实时报价。
    # 指标(MA/BOLL/MACD 等)仍基于 K 线柱计算, 不受影响。
    q_last, q_prev = _live_price(slot, symbol)
    if q_last is not None:
        cur = q_last
    if q_prev is not None:
        prev = q_prev
    change_pct = round((cur - prev) / prev * 100, 2) if prev else 0.0

    # 均线 (用户设置)
    ma_vals = {}
    ma_ser = {}
    for p in user_ma_periods:
        _ser = indicators.ma(c, p)
        ma_ser[p] = _ser
        ma_vals[p] = _last_non_none(_ser)
    # 布林带 (上轨/中轨/下轨) —— 参数 (N 统计周期, P 中轨均线周期, M 倍数), 默认 (20,20,2)
    _b_n, _b_p, _b_k = ip["boll"]
    bu, bm, bl = indicators.boll(c, _b_n, _b_k, p=_b_p)
    boll = {
        "upper": _round(_last_non_none(bu)),
        "mid": _round(_last_non_none(bm)),
        "lower": _round(_last_non_none(bl)),
    }

    # 摆动指标
    rsi_arr = indicators.rsi(c, 14)      # 只算一次: rsi 字段 + 下方 RSI 背离共用, 避免重复计算
    rsi = _round(_last_non_none(rsi_arr), 2)
    psyv = _round(_last_non_none(psy(c, 12)), 2)

    # MACD —— 参数 (快, 慢, 信号), 默认 (12,26,9)
    _m_f, _m_s, _m_sig = ip["macd"]
    dif, dea, bar = indicators.macd(c, _m_f, _m_s, _m_sig)
    macd_crosses = ta_patterns.detect_macd_cross(dif, dea, c)
    last_macd_cross = None
    if macd_crosses:
        last = macd_crosses[-1]
        # 2026-09-15: 只保留发生在最近 N 根 K 线内的交叉, 否则新 K 线到来后旧金叉一直挂着。
        if last.get("idx", 0) >= len(c) - SIGNAL_FRESH_BARS:
            last_macd_cross = last["type"]
    macd = {
        "dif": _round(_last_non_none(dif), 3),
        "dea": _round(_last_non_none(dea), 3),
        "bar": _round(_last_non_none(bar), 3),
        "cross": last_macd_cross,
    }

    # KDJ —— 参数 (N, M1, M2), 默认 (9,3,3)
    _k_n, _k_m1, _k_m2 = ip["kdj"]
    kk, kd, kj = indicators.kdj(h, l, c, _k_n, _k_m1, _k_m2)
    k_last = _last_non_none(kk)
    d_last = _last_non_none(kd)
    kdj_cross, kdj_gap = kdj_cross_of(kk, kd)
    kdj = {
        "k": _round(k_last, 2),
        "d": _round(d_last, 2),
        "j": _round(_last_non_none(kj), 2),
        "cross": kdj_cross,
        # 交叉幅度(K-D): 前端 chip 的 title 直接展示 —— 用户拿参考端对照时,
        # "幅度 0.8 的擦边交叉"和"幅度 8 的明确交叉"一眼可分。
        "gap": kdj_gap,
    }

    # RSI 三线合一 / 差值做多做空 / KDJ 超买超卖 (2026-09-20 用户规格)
    #   只算 60m 与日线(用户裁定: 六组全部限 60m 以上), 其余周期直接给空列表 —— 省三次 ewm 递推。
    #   RSI 用与上方 RSI14 完全同族的 Wilder 递推(indicators.rsi), 周期用 ip["rsi"] 三线
    #   (面板可调, 默认 6/12/24; 2026-09-21 方案A)。背离不受影响, 仍走上方固定 RSI14。
    _s6 = _s12 = _s24 = None
    if period_key in RSI_SIG_PERIODS:
        _r1, _r2, _r3 = ip["rsi"]
        _s6 = _last_non_none(indicators.rsi(c, _r1))
        _s12 = _last_non_none(indicators.rsi(c, _r2))
        _s24 = _last_non_none(indicators.rsi(c, _r3))
    rsi_kdj = rsi_kdj_signals(_s6, _s12, _s24, k=k_last, d=d_last,
                              j=_last_non_none(kj), period_key=period_key)

    # 支撑/压力 (用户 MA + 颈线): 当前价>MA => 支撑, 否则压力
    sr = []
    for p in user_ma_periods:
        val = ma_vals.get(p)
        if val is None:
            continue
        role = "support" if cur > val else "resist"
        sr.append({"label": "MA%d" % p, "value": _round(val), "role": role, "type": "MA"})
    # 颈线: 用户设置的关键价位线(最多6条), 每条独立判定支撑/压力
    for idx, lvl in enumerate(neckline_levels[:6], 1):
        if lvl is None:
            continue
        role = "support" if cur > lvl else "resist"
        sr.append({"label": "Neck%d" % idx, "value": _round(lvl), "role": role, "type": "neckline"})

    # 黄金分割
    look = min(len(c), 120)
    hh = max(h[-look:])
    ll = min(l[-look:])
    rng = hh - ll
    ratios = GOLDEN_RATIOS
    # 每个分割位独立判定: 现价 > 分割位 => 支撑(红), 现价 < 分割位 => 压力(绿),
    # 现价落在分割位容差内(GOLDEN_HIT_TOL) => 命中(黄)。
    _g_items = golden_levels(hh, ll, cur)
    golden = {
        "hh": _round(hh), "ll": _round(ll),
        "levels": [it["value"] for it in _g_items],   # 兼容旧字段(纯价格数组)
        "items": _g_items,                            # 带支撑/压力/命中角色
        "cur": _round(cur),
        "tol": GOLDEN_HIT_TOL,
    }

    # 跳空 (最近一个)。2026-09-14: 跳空同时作为【价位】参与支撑/压力(用户要求), 故带上边缘价:
    #   向上跳空 -> 缺口【下沿】= 前一根最高价(回补到此处通常获支撑)
    #   向下跳空 -> 缺口【上沿】= 前一根最低价(反弹到此处通常遇压力)
    # 2026-09-17 修: 增加【最小有效幅度】门槛(见 GAP_MIN_ATR / GAP_MIN_PCT), 判定逻辑在 detect_gap。
    #   原实现无门槛, 把隔夜 0.08 点的价差和行情源毛刺都当跳空报了(用户拿 5m/15m 上证指数对质)。
    gap = detect_gap(h, l, c, cur)
    if gap.get("type"):
        gap["at"] = _gap_bar_label(df, gap["idx"], duration)

    # 日期标签(2026-09-17 提前到此处): 背离 / 形态 / 零轴信号的 chip title 都要用, 故在背离之前先算好。
    # 2026-09-17 修: 原实现无条件套 _dt_local, 而它只对天勤的 UTC 纳秒正确 —— 走腾讯源的
    # 标的(科创50 等 11 个指数)本列已是 datetime64 北京时间, 会被静默 +8h(09:30 -> 17:30),
    # 于是形态 chip 的日期一直偏 8 小时。统一收敛到 _bar_time_labels(按 dtype 分流)。
    _dlabels = _bar_time_labels(df, "%m-%d" if duration >= 86400 else "%m-%d %H:%M")

    # 背离 (MACD / RSI)
    # 🔴 2026-09-17 修「背离永远不显示」(用户: "背离你确认一遍有问题没, 最近没看到有显示背离形态"):
    #   旧判据 _fresh(x2) = (x2 >= len(c) - SIGNAL_FRESH_BARS), SIGNAL_FRESH_BARS=1
    #   -> 要求"后一个枢轴落在最后一根 K 线上"。而枢轴由 find_pivots(order) 产出, 它只在
    #   range(order, n-order) 内取点, 故【最大枢轴索引 = n-order-1 = n-4】, 永远够不到 n-1
    #   -> 判据恒假 -> div 恒为 None -> 形态行里背离 chip 一次都没出现过(静默, 无报错)。
    #   取证: 6 标的 x 4 周期(各 300 根, 腾讯源真实行情) 共 106 个真实背离事件, 旧规则命中 0 个。
    #   定稿两条:
    #     ① 枢轴 order 3 -> DIV_PIVOT_ORDER(5): 与形态识别同口径, 滤掉噪声小枢轴(噪声枢轴会造出
    #        "两个相隔 3 根的小高点"式的伪背离);
    #     ② 保鲜改用独立常量 DIV_FRESH_BARS(15): 背离确认点落在最近 15 根内即算有效 ——
    #        枢轴天然滞后 order 根, 不能沿用"上一根才成立"的尺子。
    #   实测(order=5, gap<=15): 24 个标/周期里 13 格有背离, 既不空也不刷屏。
    # 性能: MACD/RSI 两次背离检测的枢轴完全相同(同 h/l/order) —— 只算一次并复用。
    _piv_h = [(i, v) for i, v, k in ta_patterns.find_pivots(h, DIV_PIVOT_ORDER) if k == "high"]
    _piv_l = [(i, v) for i, v, k in ta_patterns.find_pivots(l, DIV_PIVOT_ORDER) if k == "low"]
    div_macd = ta_patterns.detect_divergence(h, l, dif, order=DIV_PIVOT_ORDER, lookback=120,
                                             piv_h=_piv_h, piv_l=_piv_l)
    div_rsi = ta_patterns.detect_divergence(h, l, rsi_arr, order=DIV_PIVOT_ORDER, lookback=120,
                                            piv_h=_piv_h, piv_l=_piv_l)
    # ATR(14) —— 背离的 MACD 噪声门槛(DIV_MIN_MACD_ATR)与下方 zone 信号共用, 只算一次。
    _atr14 = None
    try:
        _atr14 = _last_non_none(indicators.atr(h, l, c, 14))
    except Exception:
        _atr14 = None
    div, div_note = None, None
    _cands = [("MACD", d) for d in div_macd] + [("RSI", d) for d in div_rsi]
    _cands = [t for t in _cands if t[1].get("x2", 0) >= len(c) - DIV_FRESH_BARS]
    # 噪声门槛(2026-09-17): 挡掉"1 tick 级价差 / 指标几乎没差 / 顶底已被刷新"的伪背离。
    # 见常量 DIV_NO_BREAK / DIV_MIN_RSI / DIV_MIN_MACD_ATR 的注释(含投诉用例与实测数据)。
    _cands = [t for t in _cands if not div_gate_reason(t[0], t[1], h, l, _atr14)]
    if _cands:
        # 取最近形成的一个(x2 最大); 同一点位 MACD 优先(用户主要看 MACD 背离)
        _cands.sort(key=lambda t: (-t[1]["x2"], 0 if t[0] == "MACD" else 1))
        _src, _d = _cands[0]
        div = _d["type"]
        _lbl = lambda i: (_dlabels[i] if _dlabels and 0 <= i < len(_dlabels) else ("第%d根" % (i + 1)))
        # 长文字只在 chip 的 title 里: "MACD 顶背离：09-10 14:00 高点 3901.20 → 09-15 10:30 高点 3912.50（DIF 0.520 → 0.310）"
        div_note = "%s %s背离：%s %.2f → %s %.2f（%s %.3f → %.3f）" % (
            _src, ("顶" if div == "top" else "底"),
            _lbl(_d["x1"]), _d["p1"], _lbl(_d["x2"]), _d["p2"],
            ("DIF" if _src == "MACD" else "RSI"), _d["i1"], _d["i2"])

    # 形态识别(2026-09-15: 同时产出"关键点位 + 日期"注记, 供前端 chip 的 title 展示)。
    # 为什么必须要日期: 形态是【事件】不是【位置】—— chip 只给一个名字时, 用户会把"M头"
    # 读成"最近这几根是M头", 而实际上破位柱离两峰可能已经过去十几根(用户 2026-09-15 反馈)。
    _shape_notes = {}
    shapes = detect_shapes(h, l, c, order=5, lookback=min(len(c), 90),
                           dates=_dlabels, notes=_shape_notes, period_key=period_key)


    # 量能
    vol_last = _round(v[-1], 2) if v else None
    vol_ratio = _round(_last_non_none(indicators.vol_ratio(v, 5)), 2)

    # ---- 三模块布局(2026-09-12 用户裁定, 2026-09-14 迭代) ----
    # 价位类指标(BOLL / 跳空 / 颈线 / MA / 预设点位 / 自定义)统一成 levels 列表, 供前端按"压力/支撑"分成两个板块渲染:
    #   压力(pres, 绿) = 价位在当前价【上方】; 支撑(sup, 红) = 价位在当前价【下方】;
    #   命中(hit) = 现价恰在该位(±0.1%), 由前端按"角色是否刚发生变化"决定标黄还是稳态色。
    # group 决定前端分行, key 是"角色变化"状态跟踪的稳定标识。
    # 2026-09-14 变化:
    #   ① MA 不再按周期各占一行, 统一进 group="MA" —— 前端做成"MA区": 一格竖排该侧全部 MA,
    #      最近的排在最下方(压力区)/最上方(支撑区), 并显示 MA 序号与离现价的距离;
    #   ② 跳空从"指标模块"移入价位模块(group="跳空");
    #   ③ 新增自定义价位(用户＋号面板添加, group=用户起的名字)。
    levels = []
    for _lk, _ln, _lv in (("boll_upper", "上轨", boll["upper"]),
                          ("boll_mid", "中轨", boll["mid"]),
                          ("boll_lower", "下轨", boll["lower"])):
        levels.append({"group": "BOLL", "key": _lk, "name": _ln,
                       "value": _lv, "role": level_role(cur, _lv)})
    # 🔴 2026-09-17(三改) 用户要求: "跳空这一行都不要了" —— 价位模块里不再下发
    #   group="跳空" 的价位。原实现把"跳空缺口边缘价"当一条价位塞进支撑/压力表(2026-09-14 改),
    #   现整行移除。gap 本体仍在返回值 "gap": gap 里照常给出(判定逻辑/门槛 GAP_MIN_* 一个字没动),
    #   只是不再作为表格里的一行出现 —— 将来若要在别处复用, 数据还在, 不必回滚这段。
    for idx, lvl in enumerate(neckline_levels[:6], 1):
        if lvl is None:
            continue
        levels.append({"group": "颈线", "key": "neck_%d" % idx, "name": "颈线%d" % idx,
                       "value": _round(lvl), "role": level_role(cur, lvl)})
    for p in user_ma_periods:
        _lv = _round(ma_vals.get(p))
        levels.append({"group": "MA", "key": "ma_%d" % p, "name": "MA%d" % p,
                       "value": _lv, "role": level_role(cur, _lv)})
    for i, x in enumerate(preset_points, 1):
        _lv = _round(x)
        levels.append({"group": "预设点位", "key": "preset_%d" % i, "name": "点位%d" % i,
                       "value": _lv, "role": level_role(cur, _lv)})
    for i, it in enumerate(custom_levels or []):
        if not isinstance(it, dict):
            continue
        try:
            _lv = _round(float(it.get("value")))
        except Exception:
            continue
        _nm = (str(it.get("name") or "")).strip() or ("自选%d" % (i + 1))
        levels.append({"group": _nm, "key": "custom_%d" % i, "name": _nm,
                       "value": _lv, "role": level_role(cur, _lv)})

    # MACD 零轴 x 关键位 的四类确认信号(2026-09-17 用户要求):
    #   突破压力 / 临近压力 / 跌破支撑 / 临近支撑 —— 语义与判据见 detect_zone_signals 上方注释。
    # 必须放在 levels 之后: 它读的正是这份"价位清单"(纯数值口径取最近的上方/下方位价 ——
    # 不能用 levels 的 role, 原因见常量区说明)。
    # ⚠️ _atr14 已在【上方背离段】算好并复用(两处共用一份, 别在这里重复算 —— 会白跑一遍 ATR),
    #   "临近"门槛与背离的 MACD 噪声门槛用的是同一个 ATR14。
    # 会走动的关键位(MA/BOLL)的【逐根序列】。只作为 level_series 传给判定函数, **不进 levels** ——
    #   否则 6 条 MA x 几百根的数字会跟着 levels 一起下发给前端, JSON 体积白白翻几十倍。
    def _ser_list(x):
        try:
            return x.tolist()
        except Exception:
            try:
                return list(x)
            except Exception:
                return None

    level_series = {}
    for _p in user_ma_periods:
        _s = _ser_list(ma_ser.get(_p))
        if _s:
            level_series["ma_%d" % _p] = _s
    for _k, _s in (("boll_upper", bu), ("boll_mid", bm), ("boll_lower", bl)):
        _l = _ser_list(_s)
        if _l:
            level_series[_k] = _l
    # 🔴 2026-09-17(三改) 用户要求: 形态行不再要"突破压力/跌破支撑/临近压力/临近支撑"四个 chip,
    #   "改为对 BOLL 的处理, 如果是跌破支撑和突破压力, 就高亮显示该数值就好"。
    #   落地: 判定范围【收敛到 BOLL 三条轨】—— 只传 BOLL 的 levels 进去, 于是
    #     zone.brk / zone.brk_at 只可能是上轨/中轨/下轨, 前端据 brk_at.key 把 BOLL 行里
    #     【那一条轨的数值】高亮(突破=绿 .pres, 跌破=红 .sup)。
    #   判定口径一个字没改: 仍要求"穿越发生在最近 ZONE_BREAK_BARS 根内 + 穿越前驻留度
    #     ZONE_BREAK_HOLD_* + DIF 与方向同侧确认(突破 DIF>0 / 跌破 DIF<0)", 也没了自指位问题
    #     (BOLL 中轨=MA20 的 frac ≤0.30, 远低于 ZONE_LEVEL_INSIDE_MAX=0.90)。
    #   ⚠ near/pres/sup 仍按原样算(BOLL 口径), 前端已不再消费 —— 留着是为了 zone 结构完整,
    #     将来若要恢复"临近"提示不必再改后端。
    zone = detect_zone_signals(c, h, l, dif,
                               [x for x in levels if x.get("group") == "BOLL"],
                               cur, atr14=_atr14, dates=_dlabels, level_series=level_series)

    return {
        "label": label,
        "last": _round(cur),
        # 2026-09-11: 前收/涨跌额必须由后端直接下发。前端曾用"两位小数的 change_pct"反推涨跌额
        # (last-last/(1+pct/100)), 舍入误差被放大: 上证指数 3888.11 由 pct=-1.18 反推得 -46.43,
        # 真值 = 3888.11-3934.40 = -46.29。原始前收就在手里(c[-2]), 不再让前端反算。
        "pre_close": _round(prev),
        "change": _round(cur - prev),
        "change_pct": change_pct,
        "ma": {str(p): _round(ma_vals.get(p)) for p in user_ma_periods},
        "boll": boll,
        # 本周期实际生效的指标参数(前端展示/自查用; 参数改动经 cache_key 触发重算)
        "ind": {"macd": list(ip["macd"]), "kdj": list(ip["kdj"]), "boll": list(ip["boll"]),
                "rsi": list(ip["rsi"])},
        "rsi": rsi,
        "psy": psyv,
        "macd": macd,
        "kdj": kdj,
        # RSI(6/12/24) 与 6 类提示 (2026-09-20)。rsi_sig 原先"仅供前端/验收脚本核对为什么出
        #   这个信号, 不参与渲染"; 2026-09-23 用户要求 RSI 行显示 6/12/24 三值 -> rsi_sig 现
        #   直接参与渲染(前端 rsiCell 逐值按各自固定门槛着色)。rsi_kdj 为 [] 表示该周期不判
        #   (1m~30m)或六类都没命中; 对应 rsi_sig 三值也是 null, 前端显示 —。
        "rsi_sig": {"s6": _round(_s6, 2), "s12": _round(_s12, 2), "s24": _round(_s24, 2)},
        "rsi_kdj": rsi_kdj,
        "support_resistance": sr,
        "levels": levels,          # 三模块布局: 价位类指标的统一下发(压力/支撑分区用)
        "golden": golden,
        "gap": gap,
        "divergence": div,
        "divergence_note": div_note,   # 背离的关键点位 + 日期(MACD/RSI, 供 chip 的 title)
        "zone": zone,                  # MACD 零轴 x 关键位: 突破压力/临近压力/跌破支撑/临近支撑
        "shapes": shapes,
        "shape_notes": _shape_notes,   # {形态名: "左峰 09-01 14:00 3442.48 → ..."} 供 chip 的 title
        "volume": vol_last,
        "vol_ratio": vol_ratio,
        "preset": [_round(x) for x in preset_points],
        "thr": _thr_block(user_thresholds, {
            "kdj": kdj.get("j"), "rsi": rsi, "psy": psyv,
            "macd": macd.get("bar"), "vr": vol_ratio,
        }),
        # [2026-10-10] 指标区 VR/PSY/RSI/KDJ 的【方向】状态(1 做多 / -1 做空 / 0 中性)。
        #   客户要求: 正常用白色, 有做多信号变红、做空变绿, 且只判日线。
        #   为什么不能用 thr(用户可配阈值)着色: ① 用户没配阈值时 thr 为空 => 无色可上;
        #     ② thr 是"位置"语义(超买绿/超卖红), 与客户要的"方向"语义(多红/空绿)相反。
        #   故此处自带固定门槛, 与用户配置无关, 保证任何时候都能给出确定方向。
        #   psy_dir 门槛沿用行业惯例 PSY<25 超卖(偏多) / >75 超买(偏空);
        #   vr_dir 量比无方向含义(放量下跌是空), 恒为 0 => VR 行不着色(且该行已按要求隐藏)。
        "psy_dir": _psy_dir(psyv),
        "rsi_dir": _rsi_dir(_s6, _s12, _s24),
        "kdj_dir": _kdj_dir(k_last, d_last),
    }


def _psy_dir(psyv):
    """PSY 方向: <25 超卖=做多(1) / >75 超买=做空(-1) / 居中=0。"""
    try:
        if psyv is None:
            return 0
        v = float(psyv)
    except Exception:
        return 0
    if v< 25:
        return 1
    if v > 75:
        return -1
    return 0


def _rsi_dir(s6, s12, s24):
    """RSI(6/12/24) 方向: 三线中【有两条以上】同处低位/高位才算方向(与形态格"二合一"同思路)。

    ⚠ 低位与高位必须【互斥】才输出方向: 若两类同时达到(如 6R=10/12R=20 已超卖, 24R=85 已超买),
    说明三条线严重分叉, 此时任何方向都是臆测 -> 返回 0(中性白)。
    修复记录: 2026-10-10 首版未做互斥, (10, 20, 85) 被误判"做多", 已补互斥。
    """
    vals = [x for x in (s6, s12, s24) if x is not None]
    if len(vals) < 2:
        return 0
    lo = [x for x in vals if x < 30]
    hi = [x for x in vals if x > 80]
    if len(lo) >= 2 and len(hi) == 0:
        return 1
    if len(hi) >= 2 and len(lo) == 0:
        return -1
    return 0


def _kdj_dir(k, d):
    """KDJ 方向: K>D 多头(1) / K<D 空头(-1) / 相等或缺失=0。"""
    try:
        if k is None or d is None:
            return 0
        kf, df = float(k), float(d)
    except Exception:
        return 0
    if kf > df:
        return 1
    if kf < df:
        return -1
    return 0


# ===================== 形态识别 =====================
# 2026-09-14 重写(用户反馈"M头/U底都不对", 实测确认旧算法有严重误报):
#   旧算法实测 45 条真实序列命中 33 例(73%), 其中 U底 25 例 —— 连【单边下跌】(左端高出低点
#   32%~40%、现价仅高 5%)都被判成"U底"; M头则在【谷深仅 0.6%~1.1% 的平顶】上触发, 其中
#   一例两峰之间【根本没有枢轴谷】。根因: 旧判据几乎是"窗口里有低点且现价高于它"的恒真式,
#   且 M头/头肩 只要求 0.5% 的颈线缓冲、不看中间谷深。
#   新算法: 每个形态都给【结构必要条件】, 缺一不可; 宁可不出, 不可出错。
#   实测(45 条真实序列)命中 6 例, 逐例核对图形均成立;
#   合成用例 11/11 通过(含"平顶不得判M头""单边下跌不得判U底/V底""崩盘弱反弹不得判U底")。
MIN_VALLEY = 0.030     # 双顶/双底: 中间谷(峰)相对两顶(底)的最小深度/高度
PEAK_TOL = 0.030       # 双顶两峰 / 双底两谷 高度差上限
SHOULDER_TOL = 0.040   # 头肩: 两肩高度差上限
HEAD_MIN = 0.015       # 头肩: 头比肩至少高出(低出) 1.5%
NECK_TOL = 0.050       # 头肩: 两谷(峰)颈线高度差上限
BREAK_BUF = 0.005      # 颈线突破缓冲 0.5%
BREAK_MAX_AGE = 0.55   # 跌破/突破颈线距今不得超过窗口的 55%(避免老形态长期挂榜)

# 2026-09-15 追加(用户反馈"创业板指 60m 一路在跌, 为什么标 M头"):
#   旧判据只有【局部结构】4 条(两峰等高 / 中间谷深 / 峰间峰后无更高高 / 跌破颈线),
#   唯独缺【趋势位置】—— 实测(5 指数 x 6 周期 = 30 格): 全场唯一命中就是创业板指 60m,
#   而它: 两峰离窗口最高点 -8.14%(其余 29 格全在 -3.4% 以内)、峰前只涨 +2.48%
#   却做出 4.64% 的谷深。即【单边下跌台阶里的最后两级 lower-high】被当成了双顶。
#   教科书双顶/双底是【趋势末端的反转】, 故补两条前置条件(缺一不可):
REV_NEAR = 0.05        # 双顶两峰须落在窗口最高点下方 5% 内(双底两谷须在窗口最低点上方 5% 内)
REV_PRIOR = 1.0        # 形态前的反向走势幅度须 >= 形态自身深度 x 1.0(先涨才有顶, 先跌才有底)
V_RECENT = 0.45        # V底: 最低点须落在窗口后 45%
V_FAST = 0.32          # V底: 从最低点到最新不超过窗口的 32%(尖)
V_REBOUND = 0.060      # V底: 反弹幅度 ≥6%
V_DROP = 0.040         # V底: 前一波下跌幅度 ≥4%
V_SLOPE = 1.0          # V底: 反弹斜率须 ≥ 下跌斜率(排除"单边下跌+弱反弹")
U_POS = (0.28, 0.68)   # U底: 最低点位置区间(居中)
U_FLAT_BAND = 0.15     # U底: "底部带" = 窗口振幅的 15%
U_FLAT_MIN = 0.16      # U底: 底部带内 bar 占比 ≥16%(平缓, 区别于 V)
U_ARM = 0.045          # U底: 两臂(左端/现价)须高于低点 ≥4.5%
U_SYM = 3.0            # U底: 左右臂高度比上限(排除"崩盘后弱反弹")
# 箱体判定参数(2026-09-23 重写)。原实现三处硬伤:
#   ① 只用收盘价定上下沿(收盘最高/最低) → 真实阻力被低估(实测某指数真实振幅 6.1%被缩成 5.0%),
#      且"下沿"高于真实支撑 → 出现"最低价根本没破"的假下破;
#   ② 振幅门槛固定 5.5%, 但"50 根极差"随周期量级差 ~50 倍(实测中位 1m 0.33% / day 17.8%)
#      → 1m 100% 通过(门槛形同虚设)、日线 0% 通过(永不触发);
#   ③ 突破只看"末 10 根内曾否越界", 不看"现在是否仍在箱外" → 破而复返仍报形态。
#   另: 无"横盘"约束 → 缓步单边趋势(累计极差也小)被当成箱体。
BOX_RANGE = {"1m": 0.0035, "5m": 0.0110, "15m": 0.0220,
             "30m": 0.0330, "60m": 0.0550, "day": 0.1475}   # 各周期 p25(实测)
BOX_RANGE_DEF = 0.055  # 周期不在表内时的兜底
BOX_BREAK = 0.010      # 箱体突破幅度下限(要求决定性越界, 排除贴边噪声)
BOX_MID = 0.25         # 上下沿"带"宽 = 箱高的 25%(按 bar 中点把每根判成 T/B/M)
BOX_ALT_MIN = 2        # 上下沿至少交替 2 次 → 排除"贴着一边走的缓步趋势"
BOX_DRIFT_REL = 0.60   # |净趋势| / 振幅 上限(相对箱高, 排除方向性漂移)


def _bars_since_break(closes, level, above, n):
    """最后一次收盘价在 level 的(上/下)方距今多少根。"""
    for k in range(n - 1, -1, -1):
        if above and closes[k] > level:
            return n - 1 - k
        if (not above) and closes[k] < level:
            return n - 1 - k
    return n


def _dlab(dates, i):
    """形态注记用的一根 K 线标签(可读日期); dates 缺省时退回"第N根"。"""
    if not dates:
        return "第%d根" % (i + 1)
    try:
        return str(dates[i])
    except Exception:
        return "第%d根" % (i + 1)


def _f2(x):
    try:
        return "%.2f" % float(x)
    except Exception:
        return str(x)


def detect_shapes(highs, lows, closes, order=5, lookback=150, dates=None, notes=None,
                  period_key=None):
    """反转/持续形态识别(严格版)。返回 0 或 1 个形态名。

    支撑事实(全部为结构必要条件, 缺一不可):
      M头     两枢轴高近似等高(≤3%) + 中间【有枢轴谷】且谷深≥3% + 两峰间/峰后无更高高点
              + 现价跌破该谷(颈线) 0.5% 以上 + 距跌破不超过窗口 55%
              + 【趋势位置】两峰须贴近窗口最高点(REV_NEAR) 且峰前须有 ≥ 谷深 的上涨(REV_PRIOR)
      W底     M头镜像(两谷贴近窗口最低点 + 谷前须有 ≥ 中间峰高 的下跌)
      头肩顶   三高中间最高(高出肩≥1.5%) + 两肩差≤4% + 两谷颈线差≤5% + 谷深≥3% + 跌破颈线
              + 【趋势位置】头须贴近窗口最高点
      头肩底   头肩顶镜像
      V底     最低点落在窗口后 45% + 距最低点≤窗口 32% + 反弹≥6% + 前跌≥4%
              + 【反弹斜率 ≥ 下跌斜率】(尖底特征, 排除单边下跌里的弱反弹)
      U底     最低点居中(28%~68%) + 底部带(窗口振幅 15%)内 bar 占比≥16%(圆底平缓)
              + 两臂均高于低点≥4.5% 且两臂高度比≤3 + 近 10 根均值高于前 10 根(右臂上行)
      箱体     前 50 根【最高/最低价口径】振幅<该周期门槛(p25) + 上下沿反复交替≥2次(排除缓步趋势)
              + 无净漂移(|趋势|≤0.6×振幅) + 末段决定性越界≥1.0% 且【末根收盘仍在箱外】
    多形态同时成立时: 反转形态 > V/U > 箱体; 同类取置信分高者。只返回 1 个。

    dates / notes: 可选。传入 dates(每根 K 线的可读标签) 且 notes 为 dict 时, 会把命中形态的
    【关键点位与日期】写进 notes(前端 chip 的 title 用), 让"形态是事件不是位置"一目了然。
    """
    if notes is not None:
        notes.clear()
    n = len(closes)
    if n < 2 * order + 1:
        return []
    lb = min(n, lookback)
    start = n - lb
    ph = [(i, v) for i, v, k in ta_patterns.find_pivots(highs, order) if k == "high" and i >= start]
    pl = [(i, v) for i, v, k in ta_patterns.find_pivots(lows, order) if k == "low" and i >= start]
    cur = float(closes[-1])
    GAP = 2 * order + 1
    seg = closes[start:]
    cands = []   # (优先类, 置信分, 名称)

    def lows_between(i1, i2):
        return [(i, v) for i, v in pl if i1 < i < i2]

    def highs_between(i1, i2):
        return [(i, v) for i, v in ph if i1 < i < i2]

    # ---------- M头 (双顶) ----------
    if len(ph) >= 2:
        a, b = ph[-2], ph[-1]
        if b[0] - a[0] >= GAP and abs(a[1] - b[1]) / max(a[1], b[1]) <= PEAK_TOL:
            tb = lows_between(a[0], b[0])
            if tb:
                ti, tv = min(tb, key=lambda x: x[1])
                depth = (min(a[1], b[1]) - tv) / max(tv, 1e-9)
                between_max = max(highs[a[0]:b[0] + 1])
                after_max = max(highs[b[0]:]) if b[0] < n - 1 else b[1]
                # 趋势位置(2026-09-15 追加): 顶部形态必须发生在【窗口高位区】, 且【先涨后顶】。
                # 缺这两条时, 单边下跌里的最后两级 lower-high 会被误判成双顶。
                win_max = max(highs[start:])
                base = min(lows[start:a[0] + 1])
                prior_up = (a[1] - base) / max(base, 1e-9)
                if (depth >= MIN_VALLEY
                        and max(a[1], b[1]) >= win_max * (1 - REV_NEAR)
                        and prior_up >= depth * REV_PRIOR
                        and between_max <= max(a[1], b[1]) * 1.005
                        and after_max <= max(a[1], b[1]) * 1.01
                        and cur < tv * (1 - BREAK_BUF)
                        and _bars_since_break(closes, tv, True, n) <= BREAK_MAX_AGE * lb):
                    cands.append((3, depth + (PEAK_TOL - abs(a[1] - b[1]) / max(a[1], b[1])), "M-Top"))
                    if notes is not None:
                        notes["M-Top"] = ("左峰 %s %s → 右峰 %s %s(等高差 %.2f%%) → 颈线 %s @ %s → 现价已破"
                                          "〔两峰距窗口最高点 %+.1f%% / 峰前涨 %.1f%%〕"
                                          % (_dlab(dates, a[0]), _f2(a[1]), _dlab(dates, b[0]), _f2(b[1]),
                                             abs(a[1] - b[1]) / max(a[1], b[1]) * 100,
                                             _f2(tv), _dlab(dates, ti),
                                             (max(a[1], b[1]) / win_max - 1) * 100, prior_up * 100))

    # ---------- 头肩顶 ----------
    if len(ph) >= 3:
        a, b, c = ph[-3], ph[-2], ph[-1]
        if b[0] - a[0] >= GAP and c[0] - b[0] >= GAP and b[1] > a[1] and b[1] > c[1]:
            sh = abs(a[1] - c[1]) / max(a[1], c[1])
            head = (b[1] - max(a[1], c[1])) / b[1]
            t1 = lows_between(a[0], b[0]); t2 = lows_between(b[0], c[0])
            if t1 and t2 and sh <= SHOULDER_TOL and head >= HEAD_MIN:
                v1 = min(t1, key=lambda x: x[1])[1]
                v2 = min(t2, key=lambda x: x[1])[1]
                neck = (v1 + v2) / 2.0
                depth = (min(a[1], c[1]) - max(v1, v2)) / max(max(v1, v2), 1e-9)
                after_max = max(highs[c[0]:]) if c[0] < n - 1 else c[1]
                # 趋势位置(2026-09-15 追加): 头必须是窗口高位区的头, 否则只是半山腰的三级台阶。
                if (depth >= MIN_VALLEY and abs(v1 - v2) / max(neck, 1e-9) <= NECK_TOL
                        and b[1] >= max(highs[start:]) * (1 - REV_NEAR)
                        and after_max <= max(a[1], c[1]) * 1.01
                        and cur < neck * (1 - BREAK_BUF)
                        and _bars_since_break(closes, neck, True, n) <= BREAK_MAX_AGE * lb):
                    cands.append((3, depth + head + (SHOULDER_TOL - sh), "H&S-Top"))
                    if notes is not None:
                        notes["H&S-Top"] = ("左肩 %s %s → 头 %s %s → 右肩 %s %s → 颈线 %s → 现价已破"
                                            % (_dlab(dates, a[0]), _f2(a[1]), _dlab(dates, b[0]), _f2(b[1]),
                                               _dlab(dates, c[0]), _f2(c[1]), _f2(neck)))

    # ---------- W底 (双底) ----------
    if len(pl) >= 2:
        a, b = pl[-2], pl[-1]
        if b[0] - a[0] >= GAP and abs(a[1] - b[1]) / max(abs(a[1]), abs(b[1])) <= PEAK_TOL:
            tb = highs_between(a[0], b[0])
            if tb:
                ti, tv = max(tb, key=lambda x: x[1])
                height = (tv - max(a[1], b[1])) / max(abs(tv), 1e-9)
                between_min = min(lows[a[0]:b[0] + 1])
                after_min = min(lows[b[0]:]) if b[0] < n - 1 else b[1]
                # 趋势位置(2026-09-15 追加, M头镜像): 底部形态须在窗口低位区, 且【先跌后底】。
                win_min = min(lows[start:])
                base = max(highs[start:a[0] + 1])
                prior_dn = (base - a[1]) / max(abs(base), 1e-9)
                if (height >= MIN_VALLEY
                        and min(a[1], b[1]) <= win_min * (1 + REV_NEAR)
                        and prior_dn >= height * REV_PRIOR
                        and between_min >= min(a[1], b[1]) * 0.995
                        and after_min >= min(a[1], b[1]) * 0.99
                        and cur > tv * (1 + BREAK_BUF)
                        and _bars_since_break(closes, tv, False, n) <= BREAK_MAX_AGE * lb):
                    cands.append((3, height + (PEAK_TOL - abs(a[1] - b[1]) / max(abs(a[1]), abs(b[1]))), "W-Bot"))
                    if notes is not None:
                        notes["W-Bot"] = ("左底 %s %s → 右底 %s %s(等高差 %.2f%%) → 颈线 %s → 现价已突破"
                                          "〔两底距窗口最低点 %+.1f%% / 底前跌 %.1f%%〕"
                                          % (_dlab(dates, a[0]), _f2(a[1]), _dlab(dates, b[0]), _f2(b[1]),
                                             abs(a[1] - b[1]) / max(abs(a[1]), abs(b[1])) * 100,
                                             _f2(tv),
                                             (min(a[1], b[1]) / win_min - 1) * 100, prior_dn * 100))

    # ---------- 头肩底 ----------
    if len(pl) >= 3:
        a, b, c = pl[-3], pl[-2], pl[-1]
        if b[0] - a[0] >= GAP and c[0] - b[0] >= GAP and b[1] < a[1] and b[1] < c[1]:
            sh = abs(a[1] - c[1]) / max(abs(a[1]), abs(c[1]))
            head = (min(a[1], c[1]) - b[1]) / max(abs(min(a[1], c[1])), 1e-9)
            t1 = highs_between(a[0], b[0]); t2 = highs_between(b[0], c[0])
            if t1 and t2 and sh <= SHOULDER_TOL and head >= HEAD_MIN:
                v1 = max(t1, key=lambda x: x[1])[1]
                v2 = max(t2, key=lambda x: x[1])[1]
                neck = (v1 + v2) / 2.0
                height = (min(v1, v2) - max(a[1], c[1])) / max(abs(min(v1, v2)), 1e-9)
                after_min = min(lows[c[0]:]) if c[0] < n - 1 else c[1]
                # 趋势位置(2026-09-15 追加, 头肩顶镜像)
                if (height >= MIN_VALLEY and abs(v1 - v2) / max(abs(neck), 1e-9) <= NECK_TOL
                        and b[1] <= min(lows[start:]) * (1 + REV_NEAR)
                        and after_min >= min(a[1], c[1]) * 0.99
                        and cur > neck * (1 + BREAK_BUF)
                        and _bars_since_break(closes, neck, False, n) <= BREAK_MAX_AGE * lb):
                    cands.append((3, height + head + (SHOULDER_TOL - sh), "H&S-Bot"))
                    if notes is not None:
                        notes["H&S-Bot"] = ("左肩 %s %s → 头 %s %s → 右肩 %s %s → 颈线 %s → 现价已突破"
                                            % (_dlab(dates, a[0]), _f2(a[1]), _dlab(dates, b[0]), _f2(b[1]),
                                               _dlab(dates, c[0]), _f2(c[1]), _f2(neck)))

    # ---------- V底 (尖底快反弹) ----------
    v_hit = False
    if pl:
        iL, vL = min(pl, key=lambda x: x[1])
        pos = (iL - start) / max(1, lb - 1)
        since = (n - 1) - iL
        rebound = (cur - vL) / max(abs(vL), 1e-9)
        prior_ph = [p for p in ph if p[0] < iL]
        if prior_ph:
            iPH, phv = max(prior_ph, key=lambda x: x[1])
            drop = (phv - vL) / max(abs(phv), 1e-9)
            down_slope = (phv - vL) / max(1, iL - iPH)
            up_slope = (cur - vL) / max(1, (n - 1) - iL)
            if (pos >= 1 - V_RECENT and since <= V_FAST * lb
                    and rebound >= V_REBOUND and drop >= V_DROP
                    and up_slope >= down_slope * V_SLOPE):
                cands.append((2, rebound, "V-Bot"))
                if notes is not None:
                    notes["V-Bot"] = ("前高 %s %s → 低点 %s %s(跌 %.1f%%) → 反弹 %.1f%%"
                                      % (_dlab(dates, iPH), _f2(phv), _dlab(dates, iL), _f2(vL),
                                         drop * 100, rebound * 100))
                v_hit = True

    # ---------- U底 (圆底: 底部平缓 + 两臂均衡) ----------
    if pl and not v_hit:
        iL, vL = min(pl, key=lambda x: x[1])
        pos = (iL - start) / max(1, lb - 1)
        if U_POS[0] <= pos <= U_POS[1]:
            lo, hi = float(np.min(seg)), float(np.max(seg))
            band = lo + U_FLAT_BAND * (hi - lo)
            flat = float(np.sum(np.asarray(seg) <= band)) / len(seg)
            left = float(seg[0])
            rebound = (cur - vL) / max(abs(vL), 1e-9)
            lefth = (left - vL) / max(abs(vL), 1e-9)
            if rebound >= U_ARM and lefth >= U_ARM:
                ratio = max(rebound, lefth) / max(1e-9, min(rebound, lefth))
                m_recent = float(np.mean(seg[-10:]))
                m_prev = float(np.mean(seg[-20:-10])) if len(seg) >= 20 else m_recent
                if flat >= U_FLAT_MIN and ratio <= U_SYM and m_recent > m_prev:
                    cands.append((2, flat, "U-Bot"))
                    if notes is not None:
                        notes["U-Bot"] = ("底部带内 %.0f%% 的柱 → 低点 %s %s → 两臂 %+.1f%% / %+.1f%%"
                                          % (flat * 100, _dlab(dates, iL), _f2(vL),
                                             -lefth * 100, rebound * 100))

    # ---------- 箱体上破/下破 (2026-09-23 重写: 教科书口径 + 按周期门槛) ----------
    # ① 上下沿用【最高/最低价】(收盘口径会低估阻力、抬高支撑 → 假下破);
    # ② 振幅门槛按周期取 BOX_RANGE[period_key](固定 5.5% 在 1m 形同虚设、在日线永不触发);
    # ③ 要求上下沿反复交替(窄幅缓步趋势极差也小, 但只贴着一侧走 → 不是箱体);
    # ④ 要求末根收盘仍在箱外(排除"破而复返"仍报形态)。
    _box_lim = BOX_RANGE.get(period_key, BOX_RANGE_DEF) if isinstance(BOX_RANGE, dict) else BOX_RANGE
    if len(closes) >= 70:
        _bh, _bl, _bc = highs[-60:-10], lows[-60:-10], closes[-60:-10]
        _nb = len(_bh)
        if _nb >= 20:
            _top, _bot = max(_bh), min(_bl)
            _bmid = (_top + _bot) / 2.0
            _bamp = (_top - _bot) / max(_bmid, 1e-9)
            if _bamp < _box_lim:
                _tol = BOX_MID * (_top - _bot)
                _seq = []
                for _k in range(_nb):
                    _m = (_bh[_k] + _bl[_k]) / 2.0
                    _seq.append("B" if _m <= _bot + _tol else ("T" if _m >= _top - _tol else "M"))
                _seq = [x for x in _seq if x != "M"]
                _alt = sum(1 for _i in range(1, len(_seq)) if _seq[_i] != _seq[_i - 1])
                _trend = (sum(_bc[-10:]) / 10.0 - sum(_bc[:10]) / 10.0) / max(_bmid, 1e-9)
                if _alt >= BOX_ALT_MIN and abs(_trend) <= BOX_DRIFT_REL * _bamp:
                    _rh, _rl = highs[-10:], lows[-10:]
                    _bup = max(_rh) > _top * (1 + BOX_BREAK) and closes[-1] > _top
                    _bdn = min(_rl) < _bot * (1 - BOX_BREAK) and closes[-1] < _bot
                    if _bup:
                        _bup_pct = (max(_rh) - _top) / max(_top, 1e-9)
                        cands.append((1, _bup_pct, "Box-Up"))
                        if notes is not None:
                            notes["Box-Up"] = ("箱体 [%s ~ %s](振幅 %.1f%%, 上下沿交替 %d 次) → 上破 %.1f%% 且仍在箱外"
                                               % (_f2(_bot), _f2(_top), _bamp * 100, _alt, _bup_pct * 100))
                    elif _bdn:
                        _bdn_pct = (_bot - min(_rl)) / max(_bot, 1e-9)
                        cands.append((1, _bdn_pct, "Box-Down"))
                        if notes is not None:
                            notes["Box-Down"] = ("箱体 [%s ~ %s](振幅 %.1f%%, 上下沿交替 %d 次) → 下破 %.1f%% 且仍在箱外"
                                                 % (_f2(_bot), _f2(_top), _bamp * 100, _alt, _bdn_pct * 100))

    if not cands:
        return []
    # 反转形态(3) > V/U(2) > 箱体(1); 同类取置信分高者。只出一个, 结构上互斥。
    cands.sort(key=lambda x: (-x[0], -x[1]))
    return [cands[0][2]]


# ===================== 标的聚合 =====================
def compute_instrument(slot, symbol, name, user_ma_periods, neckline_levels, preset_points,
                       user_thresholds=None, custom_levels=None, ind_params=None):
    out = {"symbol": symbol, "name": name, "periods": {}, "errors": {}}
    ip = ind_params or DEFAULT_IND_PARAMS
    _thr_sig = ";".join("%s:%s,%s" % (k, v[0], v[1]) for k, v in sorted((user_thresholds or {}).items()))
    # 指标参数(MACD/KDJ/BOLL)也要进缓存键: 否则用户改了参数仍命中旧缓存, 页面看着"没生效"
    _ind_sig = ip.get("sig", "")
    # 自定义价位也要进缓存键: 用户改了自选价位/名称必须触发重算(否则命中旧缓存看不到变化)
    _cust_sig = ";".join("%s:%s" % ((it or {}).get("name", ""), (it or {}).get("value", ""))
                         for it in (custom_levels or []) if isinstance(it, dict))
    if _is_http_src(symbol):
        # HTTP 源: 先并发把 6 个周期的 K 线一次拉齐(单周期 HTTP ~120ms, 串行要 700ms+,
        # 拖慢整屏 0.5s 轮询); 之后各周期 compute_period 直接命中缓存。
        tx_kline.prefetch(symbol, {k: c for k, _, _, c in TECH_PERIODS})
    # 每轮请求只读一次【订阅推送】报价, 覆盖各周期结果里的价格字段 ——
    # 让"核心基本数据(最新价/前收/涨跌)"始终毫秒级新鲜, 而重指标(MA/BOLL/MACD/形态)继续走缓存,
    # 不因实时价而被迫重算(用户 2026-09-14 裁定: 复杂计算可缓存/异步, 基础数据要实时)。
    _tl0 = time.perf_counter()
    live_last, live_prev = _live_price(slot, symbol)
    if DEBUG_TIMING:
        print("[TECHDBG] %s live_price=%.1fms -> %s" % (
            symbol, (time.perf_counter() - _tl0) * 1000, live_last), flush=True)
    for key, label, dur, cnt in TECH_PERIODS:
        cache_key = "%s|%s|%s|%s|%s|%s|%s|%s" % (symbol, key, ",".join(map(str, user_ma_periods)),
                                                ",".join(map(str, neckline_levels[:6])),
                                                ",".join(map(str, preset_points)), _thr_sig, _cust_sig,
                                                _ind_sig)
        with _CACHE_LOCK:
            hit = _CACHE.get(cache_key)
        if hit is not None:
            res = hit[1]
            # 过期: 立即返回旧值(价格字段随后被实时价覆盖), 重算丢后台 —— 请求永不等计算
            if (time.time() - hit[0]) >= _CACHE_TTL:
                _refresh_period_bg(slot, symbol, key, label, dur, cnt,
                                   user_ma_periods, neckline_levels, preset_points,
                                   user_thresholds, cache_key, custom_levels, ip)
        else:
            # 冷启动(无缓存): 只能同步算一次, 之后即走缓存
            res = compute_period(slot, symbol, key, label, dur, cnt,
                                 user_ma_periods, neckline_levels, preset_points, user_thresholds,
                                 custom_levels, ip)
            if res.get("error"):
                out["errors"][key] = res["error"]
            else:
                # 仅缓存成功结果, 避免"暂无K线数据"等失败被缓存 30s 毒化后续请求
                _cache_put(cache_key, res)
        # 实时价覆盖(copy-on-write: dict(res) 生成新对象, 不污染缓存里的那份)
        if live_last is not None and isinstance(res, dict) and not res.get("error"):
            res = dict(res)
            res["last"] = live_last
            _p = live_prev if live_prev is not None else res.get("pre_close")
            if _p:
                res["pre_close"] = _p
                res["change"] = _round(live_last - _p)
                res["change_pct"] = round((live_last - _p) / _p * 100, 2)
        out["periods"][key] = res
    return out


# ===================== 指数旗下股指期货月份合约 (2026-09-12) =====================
# 指数 symbol -> (CFFEX 产品代码, 中文产品名)。未列出的指数 = 无股指期货。
# [2026-10-09 修复] 补 SSE.000903 中证100: 它在 EXTRA_INDEX_INSTRUMENTS(:2029) 里是可勾选标签,
# 但此前漏进本表 -> 前端 FUTMAP[sym] 取不到产品代码, 下拉回落展示通用 IM 合约,
# 且"取消子合约"时 clearView() 按 _active 打错标签 -> saveViewSel 删不到本指数的记录,
# 刷新后 restoreViewSel 又读回来, 表现为"取消了还有"。中证100 与中证1000 同用 IM 系列(正常)。
INDEX_FUT_PRODUCT = {
    "SSE.000016": ("CFFEX.IH", "上证50"),
    "SSE.000300": ("CFFEX.IF", "沪深"),
    "SSE.000852": ("CFFEX.IM", "中证1000"),
    "SSE.000903": ("CFFEX.IM", "中证100"),
    "SSE.000905": ("CFFEX.IC", "中证500"),
}
# 页面"＋"号可勾选的候选指数标签(按需加载, 不进预热, 不加重启负担)。
# 全部走腾讯 HTTP 源(src="tx"): 不占用天勤会话、不受账户权限/到期限制, 仅勾选后取数。
# 2026-09-14 用户要求"都加上" —— 已逐个实测腾讯日线+实时报价均有数据。
EXTRA_INDEX_INSTRUMENTS = [
    {"name": "中证500", "symbol": "SSE.000905"},
    {"name": "中证100", "symbol": "SSE.000903", "src": "tx"},
    {"name": "中证A500", "symbol": "SSE.000510", "src": "tx"},
    {"name": "上证180", "symbol": "SSE.000010", "src": "tx"},
    {"name": "上证380", "symbol": "SSE.000009", "src": "tx"},
    {"name": "上证红利", "symbol": "SSE.000015", "src": "tx"},
    {"name": "科创100", "symbol": "SSE.000698", "src": "tx"},
    {"name": "深证成指", "symbol": "SZSE.399001", "src": "tx"},
    {"name": "深证100", "symbol": "SZSE.399330", "src": "tx"},
    {"name": "中小100", "symbol": "SZSE.399005", "src": "tx"},
    {"name": "创业板50", "symbol": "SZSE.399673", "src": "tx"},
]
_EXTRA_INDEX_NAMES = {it["symbol"]: it["name"] for it in EXTRA_INDEX_INSTRUMENTS}
# 候选指数也必须进 标的->数据源 映射, 否则 _is_http_src() 会把它们误判成天勤源(会去要账户)
_SRC.update({it["symbol"]: (it.get("src") or "tq") for it in EXTRA_INDEX_INSTRUMENTS})

# 右栏标签候选池 = 除左栏固定的"上证指数"外的全部可展示指数(默认 5 个 + 候选)
TAB_POOL = [{"name": it["name"], "symbol": it["symbol"]}
            for it in (DEFAULT_INSTRUMENTS[1:] + EXTRA_INDEX_INSTRUMENTS)]

_FUT_MONTHS_CACHE = {}      # product -> {'ts': epoch, 'months': [id,...]} 或 {'ts':..,'err':..}(负缓存)
_FUT_MONTHS_TTL = 3600      # 合约列表一天最多变一次, 缓存 1h 足够
_FUT_MONTHS_NEG_TTL = 60    # 查询失败负缓存 60s, 避免前端重试打爆会话
_FUT_MONTHS_LOCK = threading.Lock()


def _list_fut_months(product):
    """列出某 CFFEX 产品(如 CFFEX.IF)旗下未到期的月份合约代码。

    用【独立短会话】查 —— Slot 的 TqApi 会话属其工作线程, 严禁跨线程调用;
    而合约列表变化极慢(一天最多一次), 开短会话查一次缓存 1h 最划算。
    凭据取当前页面在用的 Slot(同账号=同权限), 不硬编码任何账户。
    """
    cache = _FUT_MONTHS_CACHE.get(product)
    if cache:
        age = time.time() - cache["ts"]
        if "months" in cache and age < _FUT_MONTHS_TTL:
            return cache["months"]
        if "err" in cache and age < _FUT_MONTHS_NEG_TTL:
            raise RuntimeError(cache["err"])
    with _FUT_MONTHS_LOCK:
        cache = _FUT_MONTHS_CACHE.get(product)      # 双重检查: 并发请求只放一个进会话
        if cache:
            age = time.time() - cache["ts"]
            if "months" in cache and age < _FUT_MONTHS_TTL:
                return cache["months"]
            if "err" in cache and age < _FUT_MONTHS_NEG_TTL:
                raise RuntimeError(cache["err"])
        slot = _get_slot_for_scan(None)
        cred = (getattr(slot, "tq_user", ""), getattr(slot, "tq_pass", "")) if slot is not None else ("", "")
        if not cred[0] or not cred[1]:
            raise RuntimeError("未分配有效天勤账户")
        result = {}

        def _work():
            api = None
            try:
                import re as _re
                from tqsdk import TqApi, TqAuth
                api = TqApi(auth=TqAuth(cred[0], cred[1]))
                ins = api.query_quotes(ins_class="FUTURE", expired=False)
                # 注意: 合约 id 是 产品代码+月份【无分隔符】拼接 (CFFEX.IF + 2609 = CFFEX.IF2609),
                # 不能用 startswith(product+"."); 且 CFFEX.T 前缀会误匹配 TF/TL/TS, 必须锚定数字结尾。
                pat = _re.compile("^" + _re.escape(product) + r"\d+$")
                result["months"] = sorted(i for i in ins if pat.match(i))
            except Exception as e:      # 会话必须在同线程内 close
                result["err"] = str(e) or type(e).__name__
            finally:
                if api is not None:
                    try:
                        api.close()
                    except Exception:
                        pass

        t = threading.Thread(target=_work, daemon=True)
        t.start()
        t.join(timeout=25)
        if "months" not in result:
            err = result.get("err") or "查询超时"
            _FUT_MONTHS_CACHE[product] = {"ts": time.time(), "err": err}
            raise RuntimeError(err)
        _FUT_MONTHS_CACHE[product] = {"ts": time.time(), "months": result["months"]}
        return result["months"]


def api_fut_months():
    """GET /stocktech/api/fut_months?symbol=<指数symbol>
    返回该指数旗下股指期货月份合约: {ok, months:[{symbol,code,name,kind}], msg?}"""
    symbol = (request.args.get("symbol", "") or "").strip()
    info = INDEX_FUT_PRODUCT.get(symbol)
    if not info:
        return jsonify({"ok": True, "months": [], "msg": "该指数无股指期货合约"})
    product, pname = info
    try:
        ids = _list_fut_months(product)
    except Exception as e:
        return jsonify({"ok": False, "error": "查询月份合约失败: %s" % e})
    prod = product.split(".")[-1]       # IF
    out = []
    for i, cid in enumerate(ids):
        ym = cid.split(".")[-1][len(prod):]     # 2609
        kind = "当月" if i == 0 else ("次月" if i == 1 else "季月")
        out.append({"symbol": cid, "code": prod + ym, "ym": ym,
                    "name": pname + ym, "kind": kind})
    return jsonify({"ok": True, "months": out})


# ===================== 用户月份合约视图持久化 (2026-09-17) =====================
# 用户在 ▼ 下拉里给某指数选中的月份合约(指数symbol -> 合约symbol)按天勤账号落库:
#   - 键 = 天勤账号(网关给每个用户绑定独立账号, 等效用户身份; 页面从 api/data 响应的 acct 回读);
#   - 换浏览器/清 localStorage 后仍能恢复上次的合约视图;
#   - contract_symbol 为空 = 未选合约(回指数本身), 落库时直接删行。
_SEL_TABLE = "t_stocktech_view_sel"


def _sel_conn():
    import pymysql
    from services import futures_db
    cfg = futures_db.DB_CONFIG
    return pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                           password=cfg["password"], database=cfg["database"],
                           charset="utf8mb4", connect_timeout=5,
                           cursorclass=pymysql.cursors.DictCursor)


def _ensure_sel_table(cur):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS %s ("
        " id INT AUTO_INCREMENT PRIMARY KEY,"
        " acct VARCHAR(64) NOT NULL COMMENT '天勤账号(=网关用户绑定的行情账号)',"
        " index_symbol VARCHAR(64) NOT NULL COMMENT '指数symbol, 如 SSE.000300',"
        " contract_symbol VARCHAR(64) NULL COMMENT '选中的月份合约symbol, 如 CFFEX.IF2609',"
        " updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
        " UNIQUE KEY uk_acct_idx (acct, index_symbol)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='技术分析页 用户月份合约视图选择'" % _SEL_TABLE)


def api_view_sel():
    """GET  /stocktech/api/view_sel?acct=<账号>  -> {ok, sel:{指数: 合约}}
    POST /stocktech/api/view_sel  body {acct, index, contract(空=清除)}"""
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        acct = (body.get("acct") or "").strip()
        idx = (body.get("index") or "").strip()
        csym = (body.get("contract") or "").strip()
        if not acct or not idx:
            return jsonify({"ok": False, "error": "缺少 acct/index"})
        try:
            conn = _sel_conn()
            try:
                cur = conn.cursor()
                _ensure_sel_table(cur)
                if csym:
                    cur.execute("REPLACE INTO %s (acct,index_symbol,contract_symbol) VALUES (%%s,%%s,%%s)"
                                % _SEL_TABLE, (acct, idx, csym))
                else:
                    cur.execute("DELETE FROM %s WHERE acct=%%s AND index_symbol=%%s"
                                % _SEL_TABLE, (acct, idx))
            finally:
                conn.commit()
                conn.close()
            return jsonify({"ok": True})
        except Exception as e:
            print("[view_sel] 写库失败:", e)
            return jsonify({"ok": False, "error": str(e)})
    # GET
    acct = (request.args.get("acct") or "").strip()
    if not acct:
        return jsonify({"ok": True, "sel": {}})
    try:
        conn = _sel_conn()
        try:
            cur = conn.cursor()
            _ensure_sel_table(cur)
            cur.execute("SELECT index_symbol, contract_symbol FROM %s WHERE acct=%%s" % _SEL_TABLE, (acct,))
            rows = cur.fetchall()
        finally:
            conn.close()
        sel = {}
        for r in rows:
            if r.get("contract_symbol"):
                sel[r["index_symbol"]] = r["contract_symbol"]
        return jsonify({"ok": True, "sel": sel})
    except Exception as e:
        print("[view_sel] 读库失败:", e)
        return jsonify({"ok": True, "sel": {}})


# ===================== API =====================
def api_preload_priority():
    """POST /stocktech/api/preload_priority  body {"symbols":[...]}
    把用户(客户端 localStorage 缓存)关注的标的插到预加载队列队首, 使其优先被预热 → 打开即见。
    服务端并不知道每个用户存了哪几个(只存浏览器本地), 故由客户端在页面加载时上报。
    已在队中或已处理过的标忽略(不重复计算)。"""
    body = request.get_json(silent=True) or {}
    syms = body.get("symbols") or []
    if not isinstance(syms, list):
        syms = [syms]
    pushed = 0
    with _PRELOAD_LOCK:
        _known = {it["symbol"]: it for it in (DEFAULT_INSTRUMENTS + EXTRA_INDEX_INSTRUMENTS)}
        _front = []
        for s in syms:
            s = (s or "").strip()
            if not s or s in _PRELOAD_SEEN:
                continue
            # [2026-10-10 修复] 原来只认 DEFAULT+EXTRA 白名单, 月份合约(CFFEX.IF2611 等)一律被丢弃
            # => 子合约永不入预热队列, 首次点击必然冷算 6 周期(实测 13~16s)。
            # 现在对 CFFEX 月份合约按需放行(名称回查 _FUT_MONTHS_CACHE, 查不到就用代码本身)。
            if s in _known:
                _PRELOAD_SEEN.add(s)
                _front.append(_known[s])
                pushed += 1
                continue
            if s.startswith("CFFEX."):
                _nm = s
                for _lst in _FUT_MONTHS_CACHE.values():
                    for _m in (_lst.get("months") or []):
                        if _m.get("symbol") == s:
                            _nm = _m.get("name") or s
                            break
                _PRELOAD_SEEN.add(s)
                _front.append({"symbol": s, "name": _nm})
                pushed += 1
        if _front:
            # 把用户标插到队首, 其余保持原顺序(LEFT 优先于默认, 默认优先于候选)
            _front_syms = {it["symbol"] for it in _front}
            _rest = [it for it in list(_PRELOAD_QUEUE) if it["symbol"] not in _front_syms]
            _PRELOAD_QUEUE.clear()
            for it in _front:
                _PRELOAD_QUEUE.append(it)
            for it in _rest:
                _PRELOAD_QUEUE.append(it)
    return jsonify({"ok": True, "pushed": pushed})


def api_tech_data():
    start_ts = time.time()
    # 2026-10-08 修复"全盘无数据": 下面会给模块级计数器 _USER_REQ_ACTIVE 赋值(+=/-=),
    # 但本函数原先无 global 声明 => Python 视其为局部变量 => 首个请求即 UnboundLocalError
    # => /stocktech/api/data 一律 500(网关转报"下游服务返回 HTTP 500"), 前端整屏无数据。
    global _USER_REQ_ACTIVE
    # 2026-09-24 19:1x 用户需求: 「访问技术分析的时候, 优先技术分析调用天勤」—— 每拍请求
    # 打活跃标(12s 窗), futures_kline 的补扫/监控新起看到标就让路, 页面独占天勤带宽。
    try:
        futures_kline.mark_tech_active()
    except Exception:
        pass
    # 首次访问时后台串行预热 K 线订阅(避免并发订阅挤掉数据; 预热后页面直接命中缓存秒级返回)
    _ensure_warm()
    symbol = (request.args.get("symbol", "") or "").strip()
    ma_raw = (request.args.get("ma", "") or "").strip()
    neck_raw = (request.args.get("neckline") or request.args.get("neck") or "").strip()
    preset_raw = (request.args.get("preset", "") or "").strip()
    # 自定义价位(2026-09-14): 页面＋号面板添加的"名称:数值"条目, 用 ; 分隔不同条目(名称可含中文)
    custom_raw = (request.args.get("custom", "") or "").strip()

    if not symbol:
        symbol = DEFAULT_INSTRUMENTS[0]["symbol"]
        name = DEFAULT_INSTRUMENTS[0]["name"]
    else:
        name = _EXTRA_INDEX_NAMES.get(symbol, symbol)
        for it in DEFAULT_INSTRUMENTS:
            if it["symbol"] == symbol:
                name = it["name"]
                break

    user_ma_periods = []
    for x in ma_raw.split(","):
        x = x.strip()
        if x.isdigit():
            user_ma_periods.append(int(x))
    if not user_ma_periods:
        user_ma_periods = [5, 10, 20, 60]
    neckline_levels = []
    for x in neck_raw.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            neckline_levels.append(float(x))
        except Exception:
            pass
    neckline_levels = neckline_levels[:6]

    preset_points = []
    for x in preset_raw.split(","):
        x = x.strip()
        try:
            preset_points.append(float(x))
        except Exception:
            pass

    # 自定义价位: "名称:数值;名称:数值"
    custom_levels = []
    for seg in custom_raw.split(";"):
        seg = seg.strip()
        if not seg:
            continue
        nm, _sep, val = seg.partition(":")
        try:
            v = float(val.strip())
        except Exception:
            continue
        custom_levels.append({"name": nm.strip() or ("自选%d" % (len(custom_levels) + 1)), "value": v})
    custom_levels = custom_levels[:12]

    # 阈值: thr=kdj:80,20;rsi:70,30 (超过上限红=超买, 低于下限绿=超卖, 中间灰)
    user_thresholds = parse_thresholds(request.args.get("thr", "") or "")

    # 指标参数(2026-09-15 用户要求): macd=12,26,9 & kdj=9,3,3 & boll=20,20,2
    # (也接受合并写法 ind=12,26,9;9,3,3;20,20,2)。非法值静默回落默认值。
    ind_params = ind_params_from_args(request.args)

    slot = _get_slot_for_scan((request.args.get("acct", "") or "").strip())
    used_acct = getattr(slot, "tq_user", "") if slot is not None else ""
    # HTTP 兜底源标的(如科创50)不依赖天勤账号, 无 Slot 也照常出数
    if slot is None and not _is_http_src(symbol):
        return jsonify({"ok": False, "error": "行情账号未就绪"}), 503

    # 标记"用户请求活跃": 预加载 worker 看到 _USER_REQ_ACTIVE>0 就多让路, 打开页面期间绝不饿死前端
    with _USER_REQ_LOCK:
        _USER_REQ_ACTIVE += 1
    _cold_marked = False
    try:
        # 冷标的(尚未热)标记到 _USER_COLD, 让预加载 worker 跳过同标的双算抢 GIL(尤其天勤源 18s 冷算)
        if not _is_default_cached(symbol):
            with _USER_REQ_LOCK:
                if symbol not in _USER_COLD:
                    _USER_COLD.add(symbol)
                    _cold_marked = True
        # 服务端快速失败: 无权限/不存在的合约底层会卡 ~36s, 超时即返回清晰提示, 不卡前端
        # (2026-09-08 裁定: 不做"自动换账号重取" —— 没权限就没数据, 用户自己在页面切账号)
        try:
            # [2026-10-10 修复] 同标同参数的【同步冷算】去重: 原实现每次请求都 submit 一个新的
            # compute_instrument, 而客户端 abort 并不会取消服务端已在跑的任务 —— 冷合约(首次要串行
            # 算 6 周期, 实测 13~16s)时前端 500ms 节拍会不断重发, 2 秒就能在 6 线程池上堆出十几个
            # 重复冷算, 互相抢 GIL 拖垮整页(实测连热标的 SSE.000300 都要 18s)。这里按
            # (symbol+全部参数) 复用同一个在途 Future, 后来的请求等同一个结果。
            # 🔴 这里刻意用 str 拼接而不是 "%s|%s|..." % (...) —— 上一版用 %格式化时占位符
            # 8 个只传了 7 个值(TypeError: not enough arguments), 导致 /stocktech/api/data
            # 每个请求都 500、技术分析整页无数据。改拼接后不可能再出现占位符/参数个数错位。
            _thr_sig2 = ";".join("%s:%s,%s" % (k, v[0], v[1])
                                  for k, v in sorted((user_thresholds or {}).items()))
            _cust_sig2 = ";".join("%s:%s" % ((it or {}).get("name", ""), (it or {}).get("value", ""))
                                  for it in custom_levels if isinstance(it, dict))
            _ck = "|".join([
                symbol,
                ",".join(map(str, user_ma_periods)),
                ",".join(map(str, neckline_levels[:6])),
                ",".join(map(str, preset_points)),
                _thr_sig2, _cust_sig2,
                str(ind_params.get("sig", "")),
                str(used_acct or ""),          # 账号进key: 换账号=换权限, 不能复用旧任务结果
            ])
            with _INFLIGHT_LOCK:
                _fu = _INFLIGHT.get(_ck)
                if _fu is None or _fu.done():
                    _fu = _EXECUTOR.submit(
                        compute_instrument, slot, symbol, name, user_ma_periods, neckline_levels,
                        preset_points, user_thresholds, custom_levels, ind_params)
                    _INFLIGHT[_ck] = _fu
                    if len(_INFLIGHT) > _INFLIGHT_MAX:
                        for _k2 in [k for k, v in _INFLIGHT.items() if v.done()][:len(_INFLIGHT) // 4]:
                            _INFLIGHT.pop(_k2, None)
            data = _fu.result(timeout=_FETCH_TIMEOUT)
        except concurrent.futures.TimeoutError:
            data = {"ok": False, "error": "行情请求超时（%ds）：当前账号可能无该标的行情权限，或标的不存在" % _FETCH_TIMEOUT,
                    "symbol": symbol, "name": name}
    finally:
        if _cold_marked:
            with _USER_REQ_LOCK:
                _USER_COLD.discard(symbol)
        with _USER_REQ_LOCK:
            _USER_REQ_ACTIVE = max(0, _USER_REQ_ACTIVE - 1)
    # 以 compute_instrument 实际结果为准: 探测到无权限/未取到数据则 ok=False, 不再强行覆盖为 True
    _periods = data.get("periods") or {}
    _any_data = any(
        isinstance(p, dict) and p.get("last") not in (None, "", "—", "-")
        for p in _periods.values()
    )
    data["ok"] = bool(_any_data) and not data.get("error")
    data["ma_periods"] = user_ma_periods
    data["neckline_levels"] = neckline_levels
    data["custom_levels"] = custom_levels
    data["preset"] = [_round(x) for x in preset_points]
    # 回显本请求实际生效的指标参数(前端用它显示"当前参数", 也便于接口自查)
    data["ind_params"] = {"macd": list(ind_params["macd"]),
                          "kdj": list(ind_params["kdj"]),
                          "boll": list(ind_params["boll"]),
                          "rsi": list(ind_params["rsi"])}
    data["acct"] = used_acct
    data["_cost_ms"] = round((time.time() - start_ts) * 1000, 2)
    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


def api_tech_accounts():
    """账号切换下拉数据源: tq_accounts 全部账号 + 在线状态 + 版本/剩余天数 + 默认账号。

    2026-09-24 用户需求:
    - 页面进来默认选中【专业版(trial)且未过期】的账号(后端算好 default_acct 下发);
    - 普通版(free)账号剩余天数显示 0(free 的 expiry 是 2099 占位值, 不参与计算)。
    default_acct 回落链: 专业版未过期(在线优先) -> 专业版未过期 -> 在线任意 -> 库序第一个。
    """
    import datetime as _dt
    now = _dt.datetime.now()
    info, order = {}, []          # user -> {pro, rest} ; order = 库序(去重)
    try:
        import pymysql
        from services import futures_db
        cfg = futures_db.DB_CONFIG
        conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                               password=cfg["password"], database=cfg["database"], connect_timeout=5)
        try:
            cur = conn.cursor()
            cur.execute("SELECT tq_username, product_type, expiry FROM tq_accounts ORDER BY id")
            for u, ptype, exp in cur.fetchall():
                if not u or u in info:
                    continue
                pro = str(ptype or "").lower() == "trial" and exp is not None and exp > now
                rest = max(0, (exp.date() - now.date()).days) if pro else 0
                info[u] = {"pro": pro, "rest": rest}
                order.append(u)
        finally:
            conn.close()
    except Exception:
        pass
    online = set()
    if _SLOTS is not None:
        for s in _SLOTS.values():
            u = getattr(s, "tq_user", "")
            if not u:
                continue
            if getattr(s, "status", "") == "online":
                online.add(u)
            if u not in info:      # Slot 里有但库查询漏掉的兜底
                info[u] = {"pro": False, "rest": 0}
                order.append(u)
    accounts = [{"user": u, "online": u in online, "pro": info[u]["pro"], "rest": info[u]["rest"]}
                for u in order if u in info]
    default_acct = ""
    for pick in (lambda u: info[u]["pro"] and u in online,
                 lambda u: info[u]["pro"],
                 lambda u: u in online):
        for u in order:
            if pick(u):
                default_acct = u
                break
        if default_acct:
            break
    default_acct = default_acct or (order[0] if order else "")
    try:
        import services.stock_kline as sk
        scan = sk.get_scan_account()
    except Exception:
        scan = ""
    resp = jsonify({"ok": True, "accounts": accounts, "scan_account": scan,
                    "default_acct": default_acct})
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _get_slot_by_user(acct):
    """按天勤用户名找在线 Slot(页面账号切换用)。"""
    if not acct or _SLOTS is None:
        return None
    for s in _SLOTS.values():
        if getattr(s, "tq_user", "") == acct and getattr(s, "status", "") == "online":
            return s
    return None


def _get_slot_for_scan(acct=None):
    """行情 Slot 选择(2026-09-08 需求, 去掉"第一个在线 Slot"默认):
    1) 页面下拉 acct 参数指定 → 用该账户;
    2) 否则走网关注入的当前用户账户(_GET_SLOT = _resolve_slot, 登录后绑定的有效账户);
    3) 否则取扫描账号(动态从 tq_accounts 选的有效账户, 供后台任务);
    4) 都没有返回 None(付费功能无数据)。"""
    slot = _get_slot_by_user(acct)
    if slot is not None:
        return slot
    if _GET_SLOT is not None:
        try:
            s = _GET_SLOT()
            if s is not None:
                return s
        except Exception:
            pass
    try:
        import services.stock_kline as sk
        scan = sk.get_scan_account()
        if scan:
            slot = _get_slot_by_user(scan)
            if slot is not None:
                return slot
    except Exception:
        pass
    return None


# ===================== 页面 =====================
# 账号切换共用 JS(单标/多标页共用): 下拉选天勤账号, 记忆到 localStorage, 请求带 acct 参数。
# 默认选中扫描账号(需求裁定: 剔除 zhubajie 默认, 改为页面可切换)。
# 阈值着色共用 JS(2026-09-08 二次迭代): 点击表格里的指标名(VR/PSY/RSI/KDJ)单独填该指标阈值,
# 每指标存"上限,下限"(可单边留空), 汇总成 thr=kdj:80,20;rsi:70,30 传后端。
# 超上限=绿(超买), 低于下限=红(超卖), 中间=灰。阈值按指标记在 localStorage['techThrMap']。
# 🔴 2026-09-20 用户裁定翻转(原为超买红/超卖绿) —— 详见 THR_KEYS 上方注释。
_THR_JS = """
    var THR_HINT={kdj:{t:'KDJ（取 J 值）',ex:'80,20'},rsi:{t:'RSI',ex:'70,30'},psy:{t:'PSY（百分数 0~100）',ex:'75,25'},vr:{t:'VR（量比）',ex:'2,0.5'}};
    var THR=(function(){
      var o={}; try{ o=JSON.parse(localStorage.getItem('techThrMap')||'{}')||{}; }catch(e){ o={}; }
      // 清理已停用指标(如 MACD)的残留阈值: 这些指标已无行可点击, 阈值留在库里无法清除
      var ch=false; for(var k in o){ if(!THR_HINT[k]){ delete o[k]; ch=true; } }
      if(ch){ try{ localStorage.setItem('techThrMap', JSON.stringify(o)); }catch(e){} }
      return o;
    })();
    function thrVal(){ var s=[]; for(var k in THR){ var v=THR[k]; if(v&&v.replace(/,/g,'')!=='') s.push(k+':'+v); } return s.join(';'); }
    function thrEdit(key){
      var h=THR_HINT[key]||{t:key,ex:'80,20'}; var cur=THR[key]||'';
      var v=prompt(h.t+' 阈值\\n格式: 上限,下限（可单边留空，如 "80," 只判上限 ",20" 只判下限）\\n例: '+h.ex+'\\n留空并确定 = 清除该指标阈值', cur);
      if(v===null) return;
      v=v.trim();
      if(!v){ delete THR[key]; }
      else{
        var ps=v.split(','); var hi=(ps[0]||'').trim(); var lo=ps.length>1?(ps[1]||'').trim():'';
        if(hi!==''&&isNaN(Number(hi))){ alert('上限不是数字: '+hi); return; }
        if(lo!==''&&isNaN(Number(lo))){ alert('下限不是数字: '+lo); return; }
        if(hi===''&&lo===''){ delete THR[key]; } else { THR[key]=hi+','+lo; }
      }
      localStorage.setItem('techThrMap', JSON.stringify(THR));
      if(typeof loadAll==='function'){ loadAll(); } else if(typeof loadData==='function'){ loadData(); }
    }
    function thrLabel(label,key){
      var t=THR[key]; var badge=(t&&t.replace(/,/g,'')!=='')?'<span class="thrv">['+t+']</span>':'';
      return '<td class="rowlabel thrb" onclick="thrEdit(\\''+key+'\\')" title="点击填写该指标阈值：超上限标绿(超买)，低于下限标红(超卖)">'+label+badge+'</td>';
    }
    // 2026-09-20 用户裁定翻转: 超买(over)->绿(down类) / 超卖(under)->红(up类)。
    //   .up{color:var(--up)=红} .down{color:var(--down)=绿} —— 与全页"红=偏多"统一。
    function thrCls(pe,key){ var t=(pe&&pe.thr)||{}; var x=t[key]; if(!x) return ''; return x.st==='over'?'down':(x.st==='under'?'up':'muted'); }
    function thrW(pe,key,html){ var c=thrCls(pe,key); return c?('<span class="'+c+'">'+html+'</span>'):html; }
    // ===== [2026-10-10] RSI / PSY / VR / KDJ 只判日线 =====
    //   客户要求: 这四个指标只在【日线】做阈值判定(与指标位 rsi_kdj 已限 60m/日线对齐),
    //   非日线一律不着色(显示默认白)。原因: 分钟级 PSY/VR/RSI 噪声极大, 着色会误导方向。
    //   isDay: 指标区 onlyLast 模式下只渲染最后一列, 而 PKEYS 末位就是 'day' => 恒为 true;
    //   形态/价位区是全周期列, 靠调用方传入实际周期 key 判定。
    function isDay(k){ return (k==null||k==='')?true:(k==='day'); }
    // 非日线的【纯白】版本: 只显示数值, 不做任何着色(客户要求 RSI/PSY/VR/KDJ 只判日线)。
    function rsiPlain(pe){
      var s=(pe&&pe.rsi_sig)||{}; var vals=[s.s6,s.s12,s.s24]; var out=[];
      for(var i=0;i<3;i++){ var v=vals[i]; out.push('<span class="rsiv">'+(v==null||!isFinite(v)?'—':fmt(v,2))+'</span>'); }
      return '<div class="rsibar">'+out.join('/')+'</div>';
    }
    function kdjPlain(pe){
      var m=(pe&&pe.kdj)||{};
      return '<div class="kdjbar"><span>'+fmt(m.k,2)+'/'+fmt(m.d,2)+'/'+fmt(m.j,2)+'</span></div>';
    }
    // 阈值着色的日线限定版: 非日线直接返回原文(默认白, 不着色)。
    function thrWDay(pe,key,html,k){ if(!isDay(k)) return html; return thrW(pe,key,html); }
    // [2026-10-10] 做多红 / 做空绿 着色(客户指定): 中间态=默认白, 只在有明确方向时才着色。
    //   srv= 1 做多(红) / -1 做空(绿) / 0 或 null = 中间(白)。
    //   ⚠ 与 thrCls 的"超买绿/超卖红"是两套语义: 这里是【方向】着色(客户本轮要求),
    //     thrCls 那套是【位置】着色(指标位仍在用), 两者刻意不混用。
    function dirCls(srv){ return srv===1?'up':(srv===-1?'down':''); }
    function dirWrap(srv,html){ var c=dirCls(srv); return c?('<span class="'+c+'">'+html+'</span>'):html; }

"""

# 支撑/压力着色共用 JS(2026-09-11): BOLL 三轨与 KDJ 三值统一红绿标注, 与"支撑=红 / 压力=绿 / 命中=黄"同一套规则。
#  - 价位类(BOLL 上/中/下轨 · 与均线/颈线/黄金分割同规则): 现价>价位=支撑(红) / 现价<价位=压力(绿) / 恰在该位(±0.1%)=命中(黄)
#  - 摆动类(KDJ): K 与 D 的多空排列 —— K>D=多头(红, 金叉态) / K<D=空头(绿, 死叉态);
#    J 值优先用用户阈值(2026-09-20 翻转后: 超买绿/超卖红), 未触发阈值时跟随多空色。
_LVL_JS = """
    var LVL_TOL=0.001;
    // ⚠ 必须与 Python 侧 KDJ_CROSS_MIN_GAP 保持一致(交叉幅度的最少分离度, 只用于 title 说明)。
    var KDJ_MIN_GAP=1.5;
    function lvlRole(cur,lvl){
      if(cur==null||lvl==null||!isFinite(cur)||!isFinite(lvl)||lvl===0) return '';
      if(Math.abs(cur-lvl)/Math.abs(lvl)<=LVL_TOL) return 'hit';   // 现价正好落在该位
      return cur>lvl?'sup':'pres';                                  // 现价在上=支撑, 在下=压力
    }
    function lvlName(role){ return role==='hit'?'现价命中':(role==='sup'?'支撑':'压力'); }
    // BOLL 单条轨道: chip + 角色色 + 悬浮说明(如 "上轨 · 压力")
    function chipLvl(name,lvl,cur){
      var role=lvlRole(cur,lvl);
      var tt=name+(role?' · '+lvlName(role):'');
      return '<div><span class="chip'+(role?' '+role:'')+'" title="'+tt+'">'+fmt(lvl,2)+'</span></div>';
    }
    function kdjNat(m){ if(!m||m.k==null||m.d==null) return ''; return m.k>m.d?'up':(m.k<m.d?'down':''); }
    function kdjNatName(nat){ return nat==='up'?'多头(偏强)':(nat==='down'?'空头(偏弱)':'—'); }
    function kdjCell(pe){
      var m=(pe&&pe.kdj)||{};
      var kv=fmt(m.k,2), dv=fmt(m.d,2), jv=fmt(m.j,2);
      // [2026-10-10] 客户要求: 只判日线; 正常白色, 做多红 / 做空绿。
      //   方向取后端 kdj_dir(1 多 / -1 空 / 0 中性), 不再用 K/D 排列 + 阈值两套着色。
      //   日线限定的必要性: 分钟级 KDJ 抖动剧烈, 逐列着色会满屏红绿噪声。
      var dir=(pe&&pe.kdj_dir!=null)?pe.kdj_dir:0;
      var body=dirWrap(dir, kv+'/'+dv+'/'+jv);
      return '<div class="kdjbar" title="'+kv+' / '+dv+' / '+jv+(dir?'　→　'+(dir===1?'做多':'做空'):'　→　中间(不判定)')+'">'+body+'</div>';
    }
    // ===== RSI(6/12/24) 行 =====
    //   [2026-10-10 客户要求] 只判日线; 正常白色, 做多红 / 做空绿。
    //   不再按"每条线各自固定门槛"逐值着色 —— 那套是【位置】语义(超卖红/超买绿),
    //   且会给每一个数都上色; 客户要的是整行按【方向】给一个颜色, 中间态留白。
    //   方向取后端 rsi_dir(三条线中两条以上同处低位=>做多; 两条以上同处高位=>做空; 其余中性)。
    function rsiCell(pe){
      var s=(pe&&pe.rsi_sig)||{};
      var vals=[s.s6,s.s12,s.s24];
      var out=[], tips=[];
      for(var i=0;i<3;i++){
        var v=vals[i];
        if(v==null||!isFinite(v)){ out.push('<span class="rsiv">—</span>'); continue; }
        out.push('<span class="rsiv">'+fmt(v,2)+'</span>');
        tips.push([6,12,24][i]+'R='+fmt(v,2));
      }
      var dir=(pe&&pe.rsi_dir!=null)?pe.rsi_dir:0;
      var tip='RSI 三线：'+tips.join('，')+(dir?'　→　'+(dir===1?'做多(偏低)':'做空(偏高)'):'　→　中间(不判定)');
      return '<div class="rsibar" title="'+tip+'">'+dirWrap(dir, out.join('/'))+'</div>';
    }
"""

# ===================== 三模块布局共用 JS (2026-09-12) =====================
# 用户裁定: 技术分析表格拆成三个板块 ——
#   ① 压力(绿): 价位在当前价【上方】的 BOLL / 颈线 / MA / 预设点位
#   ② 支撑(红): 价位在当前价【下方】的同批指标
#   ③ 指标   : 跳空 / 形态 / 成交量 / VR / PSY / RSI / KDJ
# 规则要点:
#   - 只显示数值不显示"压力/支撑"字样; 板块内按数值【降序】排列;
#   - BOLL 有上/中/下三条轨, 只展示"有效那一个"(离现价最近): 压力取最小 / 支撑取最大。
#   - 命中(现价恰在某价位 ±0.1%)时, 看该价位角色【是否刚发生变化】:
#     刚进入压力 -> 在压力板块标黄; 下一拍仍是压力 -> 转绿(压力稳态色); 支撑同理转红。
#   - 黄金分割已按用户要求移除; 背离与金叉/死叉并入"形态"用文字显示。
# 状态跟踪放在前端内存(页面不整刷, 只在轮询后重渲染), key = 标的|周期|价位标识,
# 首次观测不标黄(没有"上一拍"可比), 只有真正发生角色翻转的那一拍才标黄。
_SEC_JS = """
    var _lvlPrev={};
    // 各"价位组"在该侧的显示策略(2026-09-15 用户裁定, 最终版):
    //   BOLL 每侧【只出最贴近现价的那一条】——
    //     压力侧取【最小】(下轨恒在支撑、上轨恒在压力, 中轨落在哪一侧就顶替那一侧的值);
    //     支撑侧取【最大】。见 lvlCell 的 BOLL 分支。
    //   MA 走整组竖排(见 maCell); 其余组(跳空/颈线/预设点位/自定义)本来就是该侧全列。
    // ===== 指标参数(MACD/KDJ/BOLL) (2026-09-15 用户要求) =====
    // 默认 = 主流看盘软件默认: MACD(12,26,9) / KDJ(9,3,3) / BOLL(20,20,2)。
    // BOLL 三个数 = (N 统计周期, P 中轨均线周期, M 标准差倍数); 后端 parse_ind_params 逐段校验。
    var INDC=(function(){
      var d={macd:'12,26,9',kdj:'9,3,3',boll:'20,20,2',rsi:'6,12,24'};
      try{
        var s=JSON.parse(localStorage.getItem('techIndParams')||'null');
        if(s&&typeof s==='object'){
          if(s.macd) d.macd=String(s.macd);
          if(s.kdj) d.kdj=String(s.kdj);
          if(s.boll) d.boll=String(s.boll);
          if(s.rsi) d.rsi=String(s.rsi);
        }
      }catch(e){}
      return d;
    })();
    var BOLL_SHORT={'boll_upper':'上轨','boll_mid':'中轨','boll_lower':'下轨'};
    function indSave(){ try{ localStorage.setItem('techIndParams', JSON.stringify(INDC)); }catch(e){} }
    // 指标参数下发(参数改动会让后端 cache_key 变化 -> 自动重算, 无需清缓存)
    function indQuery(){
      var p=[];
      if(INDC.macd) p.push('macd='+encodeURIComponent(INDC.macd));
      if(INDC.kdj) p.push('kdj='+encodeURIComponent(INDC.kdj));
      if(INDC.boll) p.push('boll='+encodeURIComponent(INDC.boll));
      if(INDC.rsi) p.push('rsi='+encodeURIComponent(INDC.rsi));
      return p.length?('&'+p.join('&')):'';
    }
    function indNums(k){ var a=String(INDC[k]||'').split(','); while(a.length<3) a.push(''); return a; }
    var _indDirty=false;   // 输入框改了但还没重算 -> 浮层关闭时自动补一次"应用"
    function indSet(k,i,v){ var a=indNums(k); a[i]=String(v==null?'':v).trim(); INDC[k]=a.join(','); indSave(); _indDirty=true; }
    function indReload(){ if(typeof loadAll==='function'){ loadAll(); } else if(typeof loadData==='function'){ loadData(); } }
    function indApply(){ _indDirty=false; indSave(); indRender(); indReload(); }
    function indReset(){
      _indDirty=false;
      INDC={macd:'12,26,9',kdj:'9,3,3',boll:'20,20,2',rsi:'6,12,24'};
      indSave(); indRender(); indReload();
    }
    // 浮层位置: 优先按钮正下方, 空间不足则翻到上方, 并夹在视口内
    function _panelPlace(el,r){
      if(!el||!r) return;
      var w=el.offsetWidth, h=el.offsetHeight;
      var left=Math.max(8, Math.min(r.left, window.innerWidth-w-10));
      var top=r.bottom+6;
      if(top+h>window.innerHeight-8) top=Math.max(8, r.top-h-6);
      el.style.left=left+'px'; el.style.top=top+'px';
    }
    var _ipOpen=false, _ipAnchor=null;
    function indToggle(ev){
      // 与"自选价位"浮层互斥, 两个面板不叠在一起
      if(_lpOpen){ _lpOpen=false; if(typeof lvlRender==='function') lvlRender(); }
      _ipOpen=!_ipOpen;
      if(_ipOpen&&ev&&ev.currentTarget){ _ipAnchor=ev.currentTarget.getBoundingClientRect(); }
      indRender();
    }
    function indRender(){
      var el=document.getElementById('indPanel'); if(!el) return;
      if(!_ipOpen){
        el.style.display='none'; el.innerHTML='';
        // 用户改了输入框却直接点空白关掉浮层 -> 自动补一次"应用", 避免"改了没反应"
        if(_indDirty){ _indDirty=false; indReload(); }
        return;
      }
      var esc=lvlEsc;
      function row3(k,label,hint){
        var a=indNums(k);
        return '<div class="lprow"><span class="iplb">'+label+'</span>'
          + '<input class="ipn" value="'+esc(a[0])+'" onchange="indSet(\\''+k+'\\',0,this.value)">'
          + '<input class="ipn" value="'+esc(a[1])+'" onchange="indSet(\\''+k+'\\',1,this.value)">'
          + '<input class="ipn" value="'+esc(a[2])+'" onchange="indSet(\\''+k+'\\',2,this.value)">'
          + '<span class="muted iph">'+hint+'</span></div>';
      }
      var h='<div class="lp" onclick="event.stopPropagation()">'
        + '<div class="lpt">指标参数 <span class="muted">（改完自动重算）</span></div>'
        + row3('macd','MACD','快, 慢, 信号')
        + row3('kdj','KDJ','N, M1, M2')
        + row3('boll','BOLL','周期, 中轨周期, 倍数')
        + row3('rsi','RSI','短, 中, 长')
        + '<div class="lprow lpf"><button onclick="indReset()">恢复默认</button>'
        + '<button onclick="indApply()">应用并刷新</button></div></div>';
      el.innerHTML=h; el.style.display='block'; _panelPlace(el,_ipAnchor);
    }
    document.addEventListener('click', function(ev){
      if(!_ipOpen) return;
      var t=ev.target;
      if(t&&t.closest&&(t.closest('#indPanel')||t.closest('.indbtn'))) return;
      _ipOpen=false; indRender();
    });
    window.addEventListener('scroll', function(){ if(_ipOpen){ _ipOpen=false; indRender(); } }, true);
    function lvlCalc(sym,pk,key,cur,lvl,strict){
      var r=lvlRole(cur,lvl); if(!r||lvl==null) return null;
      var sk=sym+'|'+pk+'|'+key; var pv=_lvlPrev[sk];
      if(r==='hit'){
        if(strict){ r=(cur>lvl)?'sup':'pres'; }         // 严格模式(MA区): 命中带内按实际数值方向分侧 -> 绝不混侧
        else { r=pv||(cur<=lvl?'pres':'sup'); }         // 防抖模式: 沿用上一拍(避免边界抖动反复翻转)
      }
      var fresh=(pv!=null && pv!==r);                   // 角色刚变化的那一拍 -> 标黄
      _lvlPrev[sk]=r;
      return {role:r,fresh:fresh};
    }
    function lvlCls(res){ return res.fresh?'fresh':(res.role==='sup'?'sup':'pres'); }
    // 取某组中"角色匹配当前侧"的价位, 降序(值大的在前)
    function lvlPick(pe,sym,pk,group,want,cur,strict){
      var arr=(pe.levels||[]).filter(function(x){ return x.group===group && x.value!=null; });
      var hit=[];
      arr.forEach(function(x){
        var res=lvlCalc(sym,pk,x.key,cur,x.value,strict);
        if(res&&res.role===want) hit.push({v:x.value,r:res,n:x.name,k:x.key,nt:x.note});
      });
      hit.sort(function(a,b){ return b.v-a.v; });
      return hit;
    }
    // BOLL 轨被穿越时的完整依据文案(2026-09-17 三改) —— 与已删除的 zoneChips 同款语义,
    //   只是从"形态格里的 chip"搬到"被穿越那条轨数值的 hover"上, 语义零丢失、格子零占用。
    function zoneBrkTip(pe,lvn){
      var z=pe.zone||{}, b=z.brk_at||{};
      var dsz=(z.dif==null?'':('，DIF '+z.dif)),
          zx=(z.zero_dir?('，零轴最近'+(z.zero_dir==='up'?'上穿':'下穿')+(z.zero_at?(' '+z.zero_at):'')):'');
      return (z.brk==='pres'?'突破压力：':'跌破支撑：')+lvn
           + (b.at?('，'+(z.brk==='pres'?'上穿 ':'下穿 ')+b.at):'')
           + (b.hold?('（'+b.hold+'）'):'')
           + (z.brk==='pres'?'，MACD 零轴上方确认':'，MACD 零轴下方确认')+dsz+zx;
    }
    function lvlCell(pe,sym,pk,group,want,cur,strict){
      var hit=lvlPick(pe,sym,pk,group,want,cur,strict);
      // BOLL(2026-09-15 用户裁定): 每侧【只出最贴近现价的那一条】, 不再有设置项 ——
      //   hit 是降序(值大的在前), 压力侧取最后一条(最小=最近的压力)、支撑侧取第一条(最大=最近的支撑)。
      //   效果: 下轨恒在支撑、上轨恒在压力, 中轨落在哪一侧就顶替那一侧(替代掉更远的那条)。
      if(group==='BOLL'){ hit=(want==='pres')?hit.slice(-1):hit.slice(0,1); }
      if(!hit.length) return '—';
      // 🔴 2026-09-17 三改(用户: "形态里面不需要 突破压力/跌破支撑/临近压力/临近支撑 了, 改为对
      //   BOLL 的处理, 如果是跌破支撑和突破压力, 就高亮显示该数值就好"):
      //   后端已把 zone 判定范围收敛到 BOLL 三条轨 -> zone.brk_at.key 必是 boll_upper/mid/lower。
      //   这里若某条轨正是【刚被有效穿越】的那条, 就给它的数值套 .hot(色块+描边+光晕),
      //   并且配色按【事件方向】给: 突破=绿(.pres)、跌破=红(.sup) —— 与用户 2026-09-17 已确认的
      //   "突破用绿字、跌破用红字"一致(而不是按该轨当前在价格的哪一侧上色)。
      var _zbrk=(group==='BOLL')?((pe.zone||{}).brk||null):null;
      var _zk=(group==='BOLL')?(((pe.zone||{}).brk_at)||{}).key:null;
      return hit.map(function(o){
        // BOLL 行必须标出"上轨/中轨/下轨"(用户 2026-09-15 要求), 数值前挂小徽标
        var tag=(group==='BOLL')?('<span class="bolltag">'+(BOLL_SHORT[o.k]||'')+'</span>'):'';
        var base=lvlCls(o.r);                       // 照常更新角色状态跟踪(_lvlPrev), 避免下一拍 fresh 误判
        var hot=(!!_zbrk && !!_zk && _zk===o.k);
        var tip=hot?zoneBrkTip(pe,o.n+' '+fmt(o.v,2))
                   :(o.n+(o.nt?(' · '+o.nt):'')+(o.r.fresh?' · 刚进入':''));
        return tag+'<span class="chip '+(hot?(_zbrk==='pres'?'pres':'sup'):base)+(hot?' hot':'')
             + '" title="'+lvlEsc(tip)+'">'+fmt(o.v,2)+'</span>';
      }).join('');
    }
    // ===== MA 区 (2026-09-14 建; 2026-09-17 三改: 单值 + 数字下面标 MA 几) =====
    // 用户原话(三改): "MA改一下, 不要五颜六色了, 改为只显示一个值, 压力位只显示最小的,
    //   支撑位显示最大的, 然后在数字下面写上是MA几"。
    //  -> 每侧【只出一条】: 压力侧取最小(最贴近现价的上方 MA)、支撑侧取最大(下方最近的 MA)。
    //     取舍规则与 BOLL 行逐字一致 —— hit 是降序, 压力 slice(-1) / 支撑 slice(0,1)。
    //  -> 颜色不再按 MA 序号轮转: 原有的 8 色调色板 + maColor() + maColorOf() 已【整块删除】,
    //     统一走全局语义色 压力=绿(.chip.pres) / 支撑=红(.chip.sup) —— 与 BOLL/颈线/预设点位
    //     同一套, 整行最多两色, 不再"五颜六色"。
    //  -> MA 名(如 MA20)以 11px muted 小字排在数值【下方】。原来跟在数字后的"距现价差"没有丢,
    //     移到 hover —— 与其它行"chip 只给结论、悬停给依据"同一取舍。
    //  -> ★"离现价最近"标记随之取消: 每侧只剩一条, 它必然就是最近的那条。
    function maCell(pe,sym,pk,want,cur){
      // strict=true: 落位严格按数值分侧, 命中带内不沿用上一拍 -> 绝不混入异侧 MA
      var hit=lvlPick(pe,sym,pk,'MA',want,cur,true);
      if(!hit.length) return '—';
      hit=(want==='pres')?hit.slice(-1):hit.slice(0,1);      // 压力取最小 / 支撑取最大
      var o=hit[0], d='', dv=null;
      if(cur!=null&&isFinite(cur)){ dv=(o.v-cur); d=' · 距现价 '+((dv>=0?'+':'')+dv.toFixed(2)); }
      var tt=o.n+' '+fmt(o.v,2)+d+(o.r.fresh?' · 刚进入该侧':'');
      // 2026-09-18 用户要求: 差值(距现价)恢复【可见】—— 上一版改版时被收进 hover title,
      //   用户找不到("我记得MA有显示差值是多少, 你去掉了吗? 要补上")。
      //   现以 10px 小字跟在 MA 名后, hover 仍给全文。
      return '<div class="maline" title="'+lvlEsc(tt)+'">'
           + '<span class="chip '+lvlCls(o.r)+'">'+fmt(o.v,2)+'</span>'
           + '<span class="matag">'+o.n+(dv!=null?'<span class="madiff">'+((dv>=0?'+':'')+dv.toFixed(2))+'</span>':'')
           + (o.r.fresh?'<span class="mafresh">●</span>':'')+'</span>'
           + '</div>';
    }
    // MA 周期编辑: 点表中 MA 行名弹出设置框 (2026-09-17 改版: 原生 prompt 换自定义弹窗,
    // 输入框下方按顺序显示每个数字对应的颜色 —— 与表格里带色数字一一对照)
    var MAP='';
    try{ MAP=localStorage.getItem('techMA')||'2,10,20,60'; }catch(e){ MAP='2,10,20,60'; }
    function maEdit(){
      var old=document.getElementById('maPop'); if(old) old.remove();
      var ov=document.createElement('div'); ov.id='maPop';
      ov.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:9999;display:flex;align-items:center;justify-content:center';
      ov.innerHTML='<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 16px;min-width:330px;font-size:13px;color:var(--text);box-shadow:0 8px 30px rgba(0,0,0,.35)">'
        +'<div style="font-weight:700;margin-bottom:8px">MA 周期设置 <span style="font-weight:400;color:var(--muted)">（逗号分隔，可增删；表里每侧只显示<b>最贴近现价</b>的那一条）</span></div>'
        +'<input id="maPopIn" spellcheck="false" style="width:100%;box-sizing:border-box;padding:6px 8px;border:1px solid var(--border);border-radius:6px;background:var(--bg);color:var(--text);font-size:14px">'
        +'<div id="maPopLg" style="margin-top:10px;display:flex;gap:12px;flex-wrap:wrap;align-items:baseline;min-height:20px"></div>'
        +'<div style="margin-top:12px;text-align:right"><button id="maPopOk">确定</button> <button id="maPopCancel">取消</button></div>'
        +'</div>';
      document.body.appendChild(ov);
      var inp=document.getElementById('maPopIn'); inp.value=MAP;
      function norm(){ return inp.value.replace(/[，、;；\\s]+/g,',').replace(/^,+|,+$/g,''); }
      // 2026-09-17 三改: 表里不再按序号给色(整行只剩 压力绿/支撑红 两色), 故这里也去掉色卡,
      //   改为列出【已配置的 MA 名】, 并提示"每侧只显示最贴近现价的那一条"。
      function renderLg(){
        var arr=norm().split(',').filter(Boolean);
        document.getElementById('maPopLg').innerHTML = arr.length
          ? '<span style="color:var(--muted)">已配置 '+arr.length+' 条：</span>'
            + arr.map(function(x){ return '<span style="font-weight:700;font-size:15px">MA'+x+'</span>'; }).join('')
          : '<span style="color:var(--muted)">填入周期后，矩阵里每侧只显示【最贴近现价】的那一条</span>';
      }
      inp.oninput=renderLg; renderLg();
      function ok(){
        var v=norm();
        if(!v || !/^\\d+(,\\d+)*$/.test(v)){ alert('格式不对，示例: 2,10,20,60'); return; }
        MAP=v;
        try{ localStorage.setItem('techMA',MAP); }catch(e){}
        var mi=document.getElementById('maInput'); if(mi) mi.value=MAP;
        ov.remove();
        if(typeof loadAll==='function'){ loadAll(); } else if(typeof loadData==='function'){ loadData(); }
      }
      document.getElementById('maPopOk').onclick=ok;
      document.getElementById('maPopCancel').onclick=function(){ ov.remove(); };
      ov.onclick=function(e){ if(e.target===ov) ov.remove(); };
      inp.focus(); inp.select();
    }
    // ===== 自选价位配置 (2026-09-14 用户要求: 预设点位改成＋号, 把颈线并进来, 可勾选, 可自定义) =====
    // 2026-09-18(三改): 颈线那一行已删(预设点位替代)
    // 未勾选的条目【不下发】给后端 -> 表格里那一行自然不存在(需求: 不勾选就不显示)。
    // 2026-09-18(三改) 用户要求: 去掉【颈线】复选框+输入框 —— "预设点位已经可以替代它了"
    //   (两者都是"用户手填若干价位", 功能重叠; 颈线历史默认 6 条上限也一样)。
    //   ⚠ 必须同步丢弃 localStorage 里的 neck 节点: 光删界面的话, lvlQuery() 仍会下发
    //     neckline= -> 压力/支撑表里还留着"颈线1..N"行, 看起来像没删掉。
    //   ⚠ 形态行里的"颈线"(M头/头肩顶两谷连线, 见 NECK_TOL)是算法算出来的, 与此无关, 未动。
    var LVLCFG=(function(){
      var d={preset:{on:false,vals:''},custom:[]};
      try{
        var s=JSON.parse(localStorage.getItem('techLevels')||'null');
        if(s&&typeof s==='object'){
          if(s.preset) d.preset={on:!!s.preset.on,vals:String(s.preset.vals||'')};
          if(s.custom&&s.custom.length) d.custom=s.custom.map(function(x){ return {on:!!x.on,name:String(x.name||''),val:String(x.val||'')}; });
          // 🔴 2026-09-18: 写回必须放在上面 preset/custom 赋值【之后】！
          //   第一版插在赋值之前 -> 序列化的是还是默认值的 d -> 连用户的预设点位/自定义价位一起清空(真机验收抓到)。
          if('neck' in s){ try{ localStorage.setItem('techLevels', JSON.stringify(d)); }catch(e2){} }
        }
      }catch(e){}
      return d;
    })();
    function lvlSave(){ try{ localStorage.setItem('techLevels', JSON.stringify(LVLCFG)); }catch(e){} }
    function lvlQuery(){
      var p=[];
      // 2026-09-18: 颈线已从界面移除(见 LVLCFG 处注释) -> 不再拼 neckline 参数。
      if(LVLCFG.preset.on && String(LVLCFG.preset.vals).trim()) p.push('preset='+encodeURIComponent(LVLCFG.preset.vals));
      var cs=[];
      (LVLCFG.custom||[]).forEach(function(c){
        if(c.on && String(c.val).trim()!=='' && !isNaN(Number(c.val))) cs.push(String(c.name||'').trim()+':'+String(c.val).trim());
      });
      if(cs.length) p.push('custom='+encodeURIComponent(cs.join(';')));
      return p.length?('&'+p.join('&')):'';
    }
    function lvlActiveNames(){
      var out=[];
      if(LVLCFG.preset.on&&String(LVLCFG.preset.vals).trim()) out.push('预设点位');
      (LVLCFG.custom||[]).forEach(function(c){ if(c.on&&String(c.val).trim()!=='') out.push(String(c.name||'').trim()||'自选'); });
      return out;
    }
    function lvlEsc(s){ return String(s==null?'':s).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
    var _lpOpen=false, _lpAnchor=null;
    function lvlToggle(ev){ _lpOpen=!_lpOpen; if(_lpOpen&&ev&&ev.currentTarget){ _lpAnchor=ev.currentTarget.getBoundingClientRect(); } lvlRender(); }
    function lvlPlace(){
      var el=document.getElementById('lvlPanel'); if(!el||el.style.display==='none'||!_lpAnchor) return;
      var r=_lpAnchor, w=el.offsetWidth, h=el.offsetHeight;
      var left=Math.max(8, Math.min(r.left, window.innerWidth-w-10));
      var top=r.bottom+6;
      if(top+h>window.innerHeight-8) top=Math.max(8, r.top-h-6);
      el.style.left=left+'px'; el.style.top=top+'px';
    }
    function lvlRender(){
      var el=document.getElementById('lvlPanel'); if(!el) return;
      if(!_lpOpen){ el.style.display='none'; el.innerHTML=''; return; }
      var h='<div class="lp" onclick="event.stopPropagation()">'
        + '<div class="lpt">自选价位 <span class="muted">（勾选后才在压力/支撑表中显示）</span></div>'
        + '<div class="lprow"><label class="ckr"><input type="checkbox" '+(LVLCFG.preset.on?'checked':'')+' onchange="lvlOn(\\'preset\\',this.checked)"> 预设点位</label>'
        + '<input class="lpv" value="'+lvlEsc(LVLCFG.preset.vals)+'" placeholder="逗号分隔，如 4000,4100" onchange="lvlVal(\\'preset\\',this.value)"></div>';
      (LVLCFG.custom||[]).forEach(function(c,i){
        h+='<div class="lprow"><label class="ckr"><input type="checkbox" '+(c.on?'checked':'')+' onchange="lvlOnC('+i+',this.checked)"></label>'
         + '<input class="lpn" value="'+lvlEsc(c.name)+'" placeholder="名称" onchange="lvlNameC('+i+',this.value)">'
         + '<input class="lpv" value="'+lvlEsc(c.val)+'" placeholder="数值" onchange="lvlValC('+i+',this.value)">'
         + '<span class="lpx" title="删除该条" onclick="lvlDelC('+i+')">×</span></div>';
      });
      h+='<div class="lprow lpf"><button onclick="lvlAddC()">＋ 自定义价位</button>'
        + '<button onclick="lvlApply()">应用并刷新</button></div></div>';
      el.innerHTML=h; el.style.display='block'; lvlPlace();
    }
    function lvlOn(k,on){ LVLCFG[k].on=!!on; lvlSave(); lvlRender(); lvlReload(); }
    function lvlVal(k,v){ LVLCFG[k].vals=v; lvlSave(); lvlReload(); }
    function lvlOnC(i,on){ if(!LVLCFG.custom[i]) return; LVLCFG.custom[i].on=!!on; lvlSave(); lvlReload(); }
    function lvlNameC(i,v){ if(!LVLCFG.custom[i]) return; LVLCFG.custom[i].name=v; lvlSave(); }
    function lvlValC(i,v){ if(!LVLCFG.custom[i]) return; LVLCFG.custom[i].val=v; lvlSave(); }
    function lvlDelC(i){ LVLCFG.custom.splice(i,1); lvlSave(); lvlRender(); lvlReload(); }
    function lvlAddC(){ LVLCFG.custom.push({on:true,name:'',val:''}); lvlSave(); lvlRender(); }
    function lvlApply(){ lvlSave(); lvlRender(); lvlReload(); }
    function lvlReload(){
      // 勾选/改值后立刻重取(缓存键含自定义签名, 后端会重算)
      if(typeof loadAll==='function'){ loadAll(); } else if(typeof loadData==='function'){ loadData(); }
    }
    document.addEventListener('click', function(ev){
      if(!_lpOpen) return;
      var t=ev.target;
      // .lvlzone(中间那行的空白双击热区): 其内的单击【不关】浮层 —— 否则双击的第一次单击
      //   会先把面板关掉, 双击就退化成"关了再开"。交给 dblclick 去 toggle。
      if(t&&t.closest&&(t.closest('#lvlPanel')||t.closest('.lvlbtn')||t.closest('.lvlzone'))) return;
      _lpOpen=false; lvlRender();
    });
    window.addEventListener('scroll', function(){ if(_lpOpen){ _lpOpen=false; lvlRender(); } }, true);
    // ===== 三模块表格渲染 =====
    var SHAPE_CN={'M-Top':'M头','H&S-Top':'头肩顶','W-Bot':'W底','H&S-Bot':'头肩底','V-Bot':'V底','U-Bot':'U底','Box-Up':'箱体上破','Box-Down':'箱体下破'};
    // MACD 零轴 x 关键位(2026-09-17 用户新增): 突破压力 / 临近压力 / 跌破支撑 / 临近支撑。
    //   chip 本体 4 个字, 长文字(哪条价位、距多少、零轴什么时候翻的)全进 title —— 用户原话
    //   "绿色显示突破即可, 后续的长文字你也可以自己做一个简化"。
    //   配色沿用压力=绿(pres) / 支撑=红(sup): 突破压力绿、跌破支撑红; "临近"两者加 .soft
    //   (虚线 + 降透明度)弱化, 一眼能分出"已确认"和"只是贴近"。
    // ===== zoneChips() 已于 2026-09-17(三改) 整体移除 =====
    // 原用途: 形态格里的 突破压力/临近压力/跌破支撑/临近支撑 四个 chip。
    // 现改为: BOLL 行里【被有效穿越的那条轨的数值】高亮 —— 见 lvlCell() 的 isHot 分支
    //   (突破上轨 -> 绿 .pres.hot; 跌破下轨 -> 红 .sup.hot), 完整依据(上穿时间/驻留度/
    //   零轴确认/DIF)全部搬进那个数值的 title(hover), 语义零丢失、格子零占用。
    // 判定口径未变: 穿越须在最近 ZONE_BREAK_BARS 根内 + 穿越前驻留度达标 + DIF 与方向同侧。
    // 回退: 恢复本函数 + 在 patternChips() 里 concat 回来即可(后端把 zone 候选还原成全部
    //   levels 才会重新出现非 BOLL 的穿越事件)。
    // 形态格 chip 数超过这个值 -> 转两列(见 CSS .patbox.cols2)。
    // 用户原话(2026-09-17): "一个形态格子里, 你预留两列, 如果超过四行, 你就分两列显示在一格里"。
    var PAT_COL_MIN=4;
    function patternChips(pe){
      var out=[]; var _sn=pe.shape_notes||{};
      // 形态 chip 带 title: 悬停显示【关键点位 + 日期】(左峰/右峰/颈线/破位日), 避免把
      // "曾经出现过双顶" 误读成 "现在这一段是双顶" —— 形态是事件, 不是位置。
      (pe.shapes||[]).forEach(function(s){
        var nm=(SHAPE_CN[s]||s), tip=_sn[s]||nm;
        out.push('<span class="chip" title="'+lvlEsc(nm+'：'+tip)+'">'+nm+'</span>');
      });
      // 🔴 2026-09-17(三改) 用户要求: 形态行不再要"突破压力/跌破支撑/临近压力/临近支撑"四个
      //   chip —— "改为对 BOLL 的处理, 如果是跌破支撑和突破压力, 就高亮显示该数值就好"。
      //   故此处不再 concat zoneChips(): 这四个信号改由【BOLL 行的那条轨数值高亮】表达,
      //   判定结果仍来自 pe.zone(后端已把判定范围收敛到 BOLL 三条轨, 见 detect_zone_signals 调用处)。
      if(pe.divergence){
        // 背离同样带 title: 两次枢轴的日期/点位/指标值(2026-09-17 修"背离从不显示"的顺带改进,
        //   chip 只有 3 个字时, 用户无法判断这是哪一段的背离)。
        // 颜色(2026-09-21 用户要求): 【顶背离=绿(看空) / 底背离=红(看多)】—— 与全页"红=偏多"统一,
        //   与阈值/RSI-KDJ 同族(超买·做空=绿, 超卖·做多=红)。翻转前的写法是 top?'up':'down'(顶红底绿), 已废弃。
        // 独立类名 div-up/div-dn: 【不复用交叉的 .up/.down】—— 交叉(MACD/KDJ 金叉死叉)仍用 up/down,
        //   两类解耦后, 以后单独调交叉配色不会连带改到背离(这正是复用类名的隐患, 见 sig-up 的同类处理)。
        var dnm=(pe.divergence==='top'?'顶背离':'底背离');
        out.push('<span class="chip '+(pe.divergence==='top'?'div-dn':'div-up')+'" title="'
                 +lvlEsc(pe.divergence_note||dnm)+'">'+dnm+'</span>');
      }
      // 交叉 chip 也补 title(2026-09-17): 与形态/背离一致 —— chip 只给结论, hover 交代数值依据。
      //   _num 自备(不依赖外部 fmt): patternChips 与 fmt 的定义块未必同作用域, 少一个耦合点。
      var _num=function(v,d){ if(v==null||v==='') return '—'; var x=Number(v); return isNaN(x)?String(v):x.toFixed(d==null?2:d); };
      var md=pe.macd||{};
      if(md.cross) out.push('<span class="chip '+(md.cross==='golden'?'up':'down')+'" title="'
        +lvlEsc((md.cross==='golden'?'MACD 金叉（DIF 上穿 DEA）':'MACD 死叉（DIF 下穿 DEA）')
                +'：DIF '+_num(md.dif,3)+' / DEA '+_num(md.dea,3))+'">'+(md.cross==='golden'?'MACD金叉':'MACD死叉')+'</span>');
      var kd=pe.kdj||{};
      if(kd.cross){
        // title 里带上交叉幅度 |K-D|: 用户拿参考端对照时, "幅度 0.8 的擦边交叉"与
        //   "幅度 8 的明确交叉"必须一眼可分(2026-09-17 上证 60m 死叉幅度仅 1.06 引出的改进)。
        var kg=(kd.gap==null?'':('，交叉幅度 '+Math.abs(kd.gap).toFixed(2)));
        out.push('<span class="chip '+(kd.cross==='golden'?'up':'down')+'" title="'
          +lvlEsc((kd.cross==='golden'?'KDJ 金叉（K 上穿 D）':'KDJ 死叉（K 下穿 D）')
                  +'：K '+_num(kd.k,2)+' / D '+_num(kd.d,2)+' / J '+_num(kd.j,2)+kg
                  +(KDJ_MIN_GAP>0?('（幅度 ≥ '+KDJ_MIN_GAP+' 才判定为交叉）'):''))+'">'+(kd.cross==='golden'?'KDJ金叉':'KDJ死叉')+'</span>');
      }
      // RSI(6/12/24) 三线合一 · 差值做多做空 · KDJ 超买超卖 (2026-09-20 用户规格)
      //   后端 pe.rsi_kdj = [{code,label,dir,reason}] —— 只在 60m/日线有值, 其余周期为空数组。
      //   dir=up 红(偏多) / dir=down 绿(偏空), 用【独立类名】sig-up/sig-dn 而不是复用 up/down:
      //     以后若调整交叉类配色不会连带改到这一族。
      //   六类各自独立判定(用户裁定"按需求人的来"), 所以同一格【可能出现红绿并存】, 照实全渲染。
      //   chip 只给结论, 完整依据(实际 RSI 值/差值/满足哪几条)全在 title。
      var _rs=pe.rsi_kdj||[];
      for(var _ri=0;_ri<_rs.length;_ri++){
        var _r=_rs[_ri]; if(!_r||!_r.label) continue;
        out.push('<span class="chip sig ' + (_r.dir==='down'?'sig-dn':'sig-up') + '" title="'
                 + lvlEsc(_r.label + '：' + (_r.reason||'')) + '\">' + _r.label + '</span>');
      }
      if(!out.length) return '—';
      // 形态格两列(2026-09-17 用户要求): <=4 个 chip 保持原样(inline-block 自然换行, 对现状零影响);
      //   超过 4 个才包一层 .patbox.cols2, 列优先两列填充 —— 左列 ceil(n/2) 个、余下进右列,
      //   格高从 n 行降到 ceil(n/2) 行。
      //   ⚠ 行数必须用 inline style 钉死: grid-auto-flow:column 在只给列数时, 行数由容器高度反推,
      //     而容器高度又由内容行数决定 —— 循环依赖会让浏览器摆成一行铺开(真机踩过)。
      if(out.length<=PAT_COL_MIN) return out.join('');
      return '<div class="patbox cols2" style="grid-template-rows:repeat('+Math.ceil(out.length/2)+',auto)">'
           + out.join('') + '</div>';
    }
    // 行序(2026-09-14 数据驱动): 只渲染【本页实际出现的价位分组】—— 未勾选的自选价位不下发, 行自然消失。
    // 固定优先序保证多标的/多标的下行序稳定; 用户自定义组按出现顺序追加在末尾。
    var LVL_ROW_ORDER=['BOLL','MA','颈线','预设点位'];
    // LVL_BASE_ROWS(2026-09-15 左右对齐): 恒定的 3 行 —— 即使某标的当期无跳空/无 MA 值也占位显示 "—"。
    //   原因: 左栏/右栏是两套独立 DOM, 每拍各按【自己的数据】决定行集合; 只要有一侧多/少一行,
    //   后续所有行就整体错开(用户反馈"左右两边高度没对齐")。恒定的行集合 + alignColumns() 逐行等高,
    //   才能让两栏横线严格对齐。(颈线/预设点位/自定义由全局配置驱动, 两侧天然同步, 仍按需渲染。)
    var LVL_BASE_ROWS=['BOLL','MA'];
    // dropLab(2026-09-17 用户要求·二版，取代上一版"只置空表头"的做法):
    //   右栏(屏幕中间那栏)【整个首列不渲染】—— 首列(指标/BOLL/跳空/MA/形态/成交量/VR/PSY/RSI/KDJ)
    //   与左栏逐行一模一样, 用户要"省出一列空间": 去掉后该栏 6 个周期列各宽 +约14px(72→86px),
    //   MA 这种塞 4 个 chip 的格子明显好读。行集合/行序不变, 只是每行少一格 -> 左右"逐行等高"继续成立。
    //   ⚠ 行键不能再用首列文字(右栏已无首列): 每个 <tr> 一律挂 data-row="<行名>", _rowMap 以它为键。
    //   ⚠ 左栏照旧渲染首列 —— 语义、点击改 MA 周期(maEdit)、点击改阈值(thrEdit) 都留在左栏。
    function buildLevelSection(d,pkeys,plabels,want,noHead,dropLab){
      var seen={}, rows=[];
      pkeys.forEach(function(k){
        var pe=d.periods[k]||{};
        (pe.levels||[]).forEach(function(x){ if(x.value!=null && !seen[x.group]){ seen[x.group]=1; rows.push(x.group); } });
      });
      var pref=LVL_ROW_ORDER.filter(function(g){ return LVL_BASE_ROWS.indexOf(g)>=0 || seen[g]; });
      rows.forEach(function(g){ if(LVL_ROW_ORDER.indexOf(g)<0) pref.push(g); });
      if(!pref.length) pref=['BOLL'];
      var html='<table>';
      if(!noHead){
        html+='<thead><tr data-row="指标">';
        if(!dropLab) html+='<th class="rowlabel">指标</th>';
        plabels.forEach(function(lb){ html+='<th>'+lb+'</th>'; });
        html+='</tr></thead>';
      }
      html+='<tbody>';
      pref.forEach(function(g){
        var isMA=(g==='MA');
        html+='<tr data-row="'+g+'"'+(isMA?' class="maRow"':'')+'>';
        if(!dropLab) html+='<td class="rowlabel'+(isMA?' maedit':'')+'"'+(isMA?' title="点击修改 MA 周期(可增删)" onclick="maEdit()"':'')+'>'+g+'</td>';
        pkeys.forEach(function(k){
          var pe=d.periods[k]||{};
          html+='<td>'+(pe.error?'<span class=err>—</span>':(isMA?maCell(pe,d.symbol,k,want,pe.last):lvlCell(pe,d.symbol,k,g,want,pe.last)))+'</td>';
        });
        html+='</tr>';
      });
      return html+'</tbody></table>';
    }
    function buildIndicatorSection(d,pkeys,plabels,noHead,dropLab,onlyLast){
      // 2026-09-23 用户要求: 指标区【只保留最后一列】—— 其它周期列合并进最后一列,
      //   每行只展示最后一个周期(日线)的数值; onlyLast 由 buildSections 恒传 true。
      // 🔴 2026-09-23 二改(用户原话: "成交量和形态不要改, 只是下面指标要改"):
      //   形态 / 成交量 两行【不参与合并】, 保留全部周期列; 只有 VR/PSY/RSI/KDJ 合并。
      //   合并行用 colspan=<周期数> 把"其它列"并进最后一列 —— 不能直接少渲染 td:
      //     table-layout:fixed 只认【第一行】的格子宽度(见 CSS .sec td.rowlabel+td),
      //     第一行就是 形态 行 -> 它保留 7 格才能让本表列边界与上方价位区两张表逐列重合;
      //     若整表都只剩 2 格, 首列会被按比例撑到 ~499px(实测), 与价位区错位。
      var NPK=pkeys.length;
      function row(label,fn,thrKey,keepAll){
        var merge=((!keepAll)&&onlyLast&&NPK>1);
        var ks=merge?[pkeys[NPK-1]]:pkeys;
        var span=merge?(' colspan="'+NPK+'"'):'';
        var html='<tr data-row="'+label+'">';
        if(!dropLab) html+=(thrKey?thrLabel(label,thrKey):('<td class="rowlabel">'+label+'</td>'));
        // 传入周期 key k: 供 fn 内部做"只判日线"判定(merge 时k恒为 'day', 见上方 merge 取法)。
        ks.forEach(function(k){ var pe=d.periods[k]||{}; html+='<td'+span+'>'+(pe.error?'<span class=err>—</span>':fn(pe,k))+'</td>'; });
        return html+'</tr>';
      }
      var html='<table>';
      if(!noHead){
        html+='<thead><tr data-row="指标">';
        if(!dropLab) html+='<th class="rowlabel">指标</th>';
        plabels.forEach(function(lb){ html+='<th>'+lb+'</th>'; });
        html+='</tr></thead>';
      }
      html+='<tbody>';
      // 跳空已移入价位模块(支撑/压力), 此处不再重复
      // 形态/成交量 保留全部周期列(2026-09-23 二改, 用户要求"不要改")
      html+=row('形态',patternChips,null,true);
      html+=row('成交量',function(pe){ return fmt(pe.volume,0); },null,true);
      // [2026-10-10] VR 行按客户要求【整行隐藏】(量比在本页无方向含义, 且与成交量行信息重叠)。
      //   如需恢复, 把下面这行改成 html+=row('VR',function(pe){ return thrWDay(pe,'vr',fmt(pe.vol_ratio,2),null); },'vr');
      // PSY: 只判日线; 正常白, 做多红/做空绿(方向取后端 psy_dir, 与用户可配阈值无关)。
      html+=row('PSY',function(pe,k){ if(!isDay(k)) return fmt(pe.psy,2)+'%'; return dirWrap((pe&&pe.psy_dir!=null)?pe.psy_dir:0, fmt(pe.psy,2)+'%'); },null);
      // 2026-09-23 用户要求: RSI 行显示 6/12/24 三个数; [2026-10-10] 改为整行按方向着色(只判日线)。
      html+=row('RSI',function(pe,k){ return isDay(k)?rsiCell(pe):rsiPlain(pe); });
      html+=row('KDJ',function(pe,k){ return isDay(k)?kdjCell(pe):kdjPlain(pe); },null);
      return html+'</tbody></table>';
    }
    function lvlBar(){
      // 2026-09-15: 按用户要求去掉 ＋ 后面全部汉字提示, 只保留按钮本体;
      // 同排加一个 ⚙ = 指标参数浮层(MACD/KDJ/BOLL 参数 + BOLL 显示轨道), 两页统一入口。
      return '<div class="lvlbar">'
           + '<span class="lvlbtn" onclick="lvlToggle(event)" title="勾选/取消 预设点位，或添加自定义价位">＋</span>'
           + '<span class="lvlbtn indbtn" onclick="indToggle(event)" title="MACD / KDJ / BOLL 参数，以及 BOLL 显示哪条轨">⚙</span>'
           + '</div>';
    }
    function lvlZone(){
      // 2026-09-18(二改) 用户要求: 顶部把 ＋/⚙ 挪走后, 中间那一行【保留成空白双击热区】——
      //   "空白行比较大，方便双击，用户体验好，而且是在屏幕中间"。
      //   且明确要【隐藏的惊喜】: 零文案、零边框、零 hover 变化, 平时完全看不出能点。
      //   所以这里不放任何 span/title, 只留 ondblclick; 热区尺寸与手型由 .lvlzone 的 CSS 给。
      return '<div class="lvlzone" ondblclick="lvlToggle(event)"></div>';
    }
    function buildSections(d,pkeys,plabels,dropLab,blankBar){
      // blankBar(true=多标的页): 中间那行【不放 ＋/⚙】, 换成空白双击热区 lvlZone()
      //   —— 热区大且在屏幕中间, 双击同样开自选价位浮层(2026-09-18 二改)。
      //   单标的页不传该参 -> 仍旧渲染 lvlBar(), 中间那组按钮行为完全不变。
      return '<div class="sec sec-pres">'+buildLevelSection(d,pkeys,plabels,'pres',false,dropLab)+(blankBar?lvlZone():lvlBar())+'</div>'
           + '<div class="sec sec-sup">'+buildLevelSection(d,pkeys,plabels,'sup',true,dropLab)+'</div>'
           + '<div class="sec sec-ind">'+buildIndicatorSection(d,pkeys,plabels,true,dropLab,true)+'</div>';
    }
"""

_ACCT_JS = """
    var ACCT=localStorage.getItem('techAcct')||'';
    function loadAccts(){
      fetch(apiBase()+'api/accounts',{cache:'no-store'}).then(function(r){return r.json();}).then(function(d){
        var sel=document.getElementById('acctSel'); if(!sel) return;
        var accs=d.accounts||[];
        var onl=accs.filter(function(a){return a.online;});
        // 2026-09-24 用户需求: 进页面默认选【专业版未过期】账号(后端 default_acct 已算好回落链)。
        // 记忆只在无默认可选时兜底 -> 每次进来都回到专业版未过期账号, 手动切换仅当前页面会话有效。
        var cur=d.default_acct||'';
        if(!cur) cur=ACCT;
        if(cur && !accs.some(function(a){return a.user===cur;})) cur='';
        if(!cur) cur=(onl[0]||accs[0]||{}).user||'';
        sel.innerHTML='';
        accs.forEach(function(a){
          var o=document.createElement('option'); o.value=a.user;
          var tag=a.pro?('专业版·剩'+a.rest+'天'):'普通版·剩0天';
          o.textContent=a.user+'（'+tag+(a.online?'':'·离线')+'）';
          if(a.user===cur) o.selected=true;
          sel.appendChild(o);
        });
        ACCT=sel.value||'';
        localStorage.setItem('techAcct',ACCT);
      }).catch(function(){});
    }
    function onAcctChange(){
      ACCT=document.getElementById('acctSel').value;
      localStorage.setItem('techAcct',ACCT);
      if(typeof loadAll==='function'){ loadAll(); } else if(typeof loadData==='function'){ loadData(); }
    }
"""


def tech_page():
    inst_json = json.dumps(DEFAULT_INSTRUMENTS, ensure_ascii=False)
    doc = """<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><title>多周期技术分析</title>"""
    doc += '<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">'
    doc += """<style>
    :root{--bg:#0f1115;--surface:#171a21;--surface2:#1e222b;--text:#e6e9ef;--muted:#8b93a7;--border:#2a2f3a;
      --accent:#4f8cff;--up:#ef4d4d;--down:#21ba72;--violet:#a78bfa;--amber:#fbbf24;}
    *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,'Segoe UI',Roboto,'Microsoft YaHei',sans-serif;font-size:13px}
    .wrap{padding:14px}
    .topbar{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px}
    .topbar h2{margin:0;font-size:17px}
    select,input{background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:6px 8px;font-size:13px}
    button{background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:6px 12px;cursor:pointer}
    button:hover{border-color:var(--accent)}
    .badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:12px}
    .up{color:var(--up)} .down{color:var(--down)} .muted{color:var(--muted)}
    .settings{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:12px;display:flex;gap:14px;flex-wrap:wrap;align-items:center}
    .settings label{color:var(--muted);margin-right:4px}
    table{border-collapse:collapse;width:100%;background:var(--surface);border-radius:8px;overflow:hidden}
    th,td{border:1px solid var(--border);padding:9px 9px;text-align:center;vertical-align:middle;font-size:14px}
    thead th{background:var(--surface2);position:sticky;top:0;z-index:2}
    .rowlabel{background:var(--surface2);text-align:left;font-weight:600;white-space:nowrap}
    .rowlabel.thrb{cursor:pointer} .rowlabel.thrb:hover{color:var(--accent);border-color:var(--accent)}
    .thrv{font-weight:400;font-size:11px;color:var(--muted);margin-left:4px}
    .thrhit{font-weight:700}
    .sup{color:var(--up)} .pres{color:var(--down)}
    .pos{color:var(--up)} .neg{color:var(--down)}
    .chip{display:inline-block;padding:2px 7px;border-radius:4px;background:var(--surface2);margin:1px;border:1px solid var(--border)}
    .chip[title]{cursor:help}
    .golden{color:var(--violet)} .gap-up{color:var(--up)} .gap-down{color:var(--down)}
    .chip.sup{color:var(--up)} .chip.pres{color:var(--down)}
    /* RSI/KDJ 提示 chip (2026-09-20): 红=偏多(sig-up) / 绿=偏空(sig-dn)。
       独立类名, 不复用 .up/.down —— 与交叉类解耦, 以后改一处不会连带影响另一处。 */
    .chip.sig{border-color:currentColor}
    .chip.sig-up{color:var(--up)} .chip.sig-dn{color:var(--down)}
    /* 背离 chip (2026-09-21 用户要求): 顶背离=绿(div-dn) / 底背离=红(div-up)。
       独立类名, 不复用交叉的 .up/.down(金叉红/死叉绿)。 */
    .chip.div-up{color:var(--up)} .chip.div-dn{color:var(--down)}
    .chip.hit{color:var(--amber);font-weight:700;border-color:var(--amber);background:rgba(251,191,36,.13)}
    /* 三模块布局(2026-09-12): 压力(绿)/支撑(红)/指标; fresh=角色刚发生变化的那一拍(标黄) */
    .chip.fresh{color:var(--amber);font-weight:700;border-color:var(--amber);background:rgba(251,191,36,.13)}
    /* 临近压力/临近支撑(2026-09-17): 位置贴近但尚未确认 -> 虚线 + 降透明度, 与"突破/跌破"一眼可分 */
    .chip.soft{opacity:.72;border-style:dashed}
    /* 形态格两列(2026-09-17 用户要求 "超过四行就分两列显示在一格里"):
       patternChips 在 chip 数 > PAT_COL_MIN(4) 时包一层 .patbox.cols2。
       grid-auto-flow:column = 列优先填充(左列排满再排右列, 阅读顺序仍是列表顺序),
       行数由 inline style 的 grid-template-rows 钉死(见 patternChips 注释)。
       宽度: 形态列已加宽(见下方 "形态列预留两列"), 扣 td padding 10px 与 gap 4px 后每列 >= 68px
       —— 足够放下 "箱体突破"(4 字) 与 "MACD金叉"(英文+2 字); 仍给 max-width 兜底防极端溢出。 */
    .patbox{display:grid;justify-items:center;gap:1px 4px}
    .patbox.cols2{grid-auto-flow:column;grid-template-columns:repeat(2,minmax(0,1fr))}
    .patbox.cols2 .chip{font-size:12px;padding:2px 5px;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .fresh{color:var(--amber);font-weight:700}
    .sec{margin-bottom:12px;padding-left:6px;border-left:3px solid transparent}
    .sec:last-child{margin-bottom:0}
    .sec-pres{border-left-color:var(--down)}
    .sec-sup{border-left-color:var(--up)}
    .sec-ind{border-left-color:var(--border)}
    /* 三模块列对齐(2026-09-12): 三张表独立成表时会各算各的列宽 -> 必须强制固定列宽:
       首列(指标/rowlabel)钉死 84px, 其余周期列平分剩余宽度(table-layout:fixed 下无宽列等分)。
       三个 .sec 的盒模型完全一致, 故三张表总宽相同 -> 列边界逐列重合。 */
    .sec table{table-layout:fixed}
    .sec th.rowlabel,.sec td.rowlabel{width:84px;overflow:hidden;text-overflow:ellipsis}
    /* 形态列预留两列宽度(2026-09-17 用户要求 "一个形态格子里, 你预留两列"):
       最长的 chip 是 "MACD金叉"/"KDJ死叉"(英文前缀 + 2 汉字), 12px 实测 68.5px
       -> 两列各需 >= 70px 才不被省略号截断 -> 形态列 = 70*2 + gap4 + padding10 = 154, 取 160
       ⚠ 2026-09-20 调到 176: 新增的 "RSI二合一"(3 英文+3 汉字) 比 "MACD金叉" 还宽约 3px,
         160 下只剩 1px 余量(cur/2=73 vs 71.9), 故上调 16px 换取 9px 安全余量;
         代价 = 同卡片其余周期列各让出约 3px。改这里必须【两份 style 块一起改】。
       (实测 150 时 "MACD金叉" 被截断, 由 tmp/cdp_patbox.py 合成格 5/7 chips 抓出)。
       定位必须按"首个周期列", 因为右栏已 dropLab(无 .rowlabel 首列):
         左栏 = .rowlabel 后面那格; 右栏 = 首格(不带 .rowlabel)。
       table-layout:fixed 只认【表格第一行】的宽度 -> th/td 都写, 覆盖有/无 thead 的三张表;
       三张表命中同一套选择器 -> 同卡片内列边界依旧逐列重合(左右两栏的周期列宽本就不同, 与本次无关)。 */
    .sec th.rowlabel+th,.sec td.rowlabel+td{width:176px}
    .sec th:first-child:not(.rowlabel),.sec td:first-child:not(.rowlabel){width:176px}
    .sec th,.sec td{padding:9px 5px}
    .sec td.rowlabel.maedit{cursor:pointer;color:var(--accent)}
    .sec td.rowlabel.maedit:hover{text-decoration:underline}
    /* MA 区(2026-09-14 建; 2026-09-17 三改 = 单值): 一格只显示【该侧最贴近现价的那一条 MA】——
       压力侧取最小、支撑侧取最大, 与 BOLL 行同一取舍规则; 数值在上、MA 名在下(11px muted)。
       颜色 = 压力绿(.pres) / 支撑红(.sup), 与 BOLL/颈线/预设点位同一套语义色;
       不再有 ★"离现价最近"标记、不再有"距现价差"、不再有按设置序号轮转的色卡。
       宽度约束: 1600px 视窗下每周期列实测仅 109px —— 单值后占用比原来 4 条小得多, 不会横向溢出。 */
    /* 2026-09-17 用户要求: MA 行内容【顶对齐】。该行行高由 MA 条数最多的那一列(4 条)决定,
       而 td 默认 vertical-align:middle -> 标签列与少值列会在【行中段】留出一整块空白
       (用户截图对质"中间这块不要了")。顶对齐后各列第一条 MA 落在同一基线上(真正的横向对齐),
       空白只留在行底部, 与 fillHeight 拉升出来的行高融为一体。 */
    .sec tr.maRow td,.sec tr.maRow th{vertical-align:top}
    /* 2026-09-17 三改(用户: "只显示一个值...然后在数字下面写上是MA几"): 竖排单值 ——
       上行=数值, 下行=11px muted 的 MA 名; 去掉 距现价差 / ★ / 按序号轮转的身份色(色卡已删)。 */
    .maline{display:flex;flex-direction:column;align-items:center;justify-content:center;line-height:1.25}
    .maline .chip{padding:0 4px;margin:0;font-size:13px;font-weight:700}
    .maline .matag{font-size:11px;line-height:1.2;color:var(--muted);white-space:nowrap}
    .maline .madiff{font-size:10px;line-height:1.2;margin-left:3px;white-space:nowrap;color:var(--text);opacity:.72}
    .maline .mafresh{color:var(--amber);font-size:9px;line-height:1;margin-left:2px}
    /* BOLL 数值高亮(2026-09-17 三改): 该轨被【有效穿越】时套 .hot —— 突破=绿(.pres)、
       跌破=红(.sup); 高亮 = 同色描边 + 柔光晕 + 加粗, 与普通 chip(纯文字)拉开层次。
       完整依据(穿越时刻/驻留度/零轴确认/DIF)全部在 title(hover)里。 */
    .chip.hot{font-weight:700;border-color:currentColor;box-shadow:0 0 7px -1px currentColor}
    .chip.pres.hot{background:rgba(33,186,114,.20)}
    .chip.sup.hot{background:rgba(239,77,77,.20)}
    .kdjbar{white-space:nowrap;font-size:12px;margin:0 -4px;overflow:hidden;text-overflow:ellipsis}
    .rsibar{white-space:nowrap;font-size:12px;margin:0 -4px;overflow:hidden;text-overflow:ellipsis}
    .rsiv.under{color:var(--up);font-weight:700}
    .rsiv.over{color:var(--down);font-weight:700}
    /* 自选价位 ＋ 号面板(2026-09-14): 未勾选的条目【不下发】-> 表中那一行自然不出现 */
    .lvlbar{display:flex;align-items:center;gap:10px;margin-top:8px;font-size:13px;flex-wrap:wrap}
    .lvlbtn{border:1px dashed var(--border);border-radius:6px;padding:4px 12px;color:var(--muted);cursor:pointer;white-space:nowrap}
    .lvlbtn:hover{color:var(--accent);border-color:var(--accent)}
    #lvlPanel,#indPanel{position:fixed;z-index:1200;display:none;box-shadow:0 10px 28px rgba(0,0,0,.35)}
    .iplb{color:var(--muted);display:inline-block;width:62px}
    .ipn{width:46px;text-align:center;padding:3px 4px;font-size:12px}
    .iph{font-size:11px;margin-left:6px}
.bolltag{display:inline-block;font-size:12px;line-height:16px;padding:0 2px;margin-right:2px;border:1px solid var(--border);border-radius:3px;color:var(--muted);vertical-align:middle}
    .lp{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:10px 12px;font-size:12px;min-width:340px;max-width:min(560px,92vw)}
    .lpt{margin-bottom:8px;font-weight:600}
    .lprow{display:flex;align-items:center;gap:8px;margin-bottom:6px}
    .lprow .ckr{display:flex;align-items:center;gap:5px;white-space:nowrap;cursor:pointer}
    .lprow .ckr input{width:13px;height:13px;accent-color:var(--accent);margin:0;cursor:pointer}
    .lprow .lpn{width:96px;flex:0 0 auto}
    .lprow .lpv{flex:1 1 auto;min-width:130px}
    .lpx{color:var(--muted);cursor:pointer;padding:0 4px;font-size:15px;line-height:1}
    .lpx:hover{color:var(--up)}
    .lpf{justify-content:flex-end;margin-top:8px;margin-bottom:0}
    .lpf button{border:1px solid var(--border);background:var(--surface2);color:var(--text);border-radius:6px;padding:4px 10px;cursor:pointer;font-size:12px}
    .lpf button:hover{border-color:var(--accent)}
    .err{color:var(--muted);font-size:12px}
    .nodata{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:22px 16px;text-align:center;color:var(--muted);font-size:13px;line-height:1.8}
    .legend{margin-top:10px;color:var(--muted);font-size:13px;line-height:1.7}
    </style></head><body><div class="wrap">"""
    doc += '<div class="topbar">'
    doc += '<h2>📊 多周期技术分析</h2>'
    doc += '<select id="instSel"></select>'
    doc += '<label class="muted">账号</label><select id="acctSel" onchange="onAcctChange()" style="min-width:110px"></select>'
    doc += '<span class="badge" id="instInfo"></span>'
    doc += '<button onclick="loadData()">刷新</button>'
    doc += '<span class="muted" id="updTime"></span>'
    doc += '<a href="multi" style="text-decoration:none"><button type="button">多标的</button></a>'
    doc += '</div>'

    doc += '<div class="settings">'
    doc += '<span><label>MA</label><input id="maInput" style="width:150px" value="5,10,20,60" title="逗号分隔，如 5,10,20,60"></span>'
    doc += '<span class="lvlbtn" onclick="lvlToggle(event)" title="勾选/取消 预设点位，或添加自定义价位（勾选后才在压力/支撑表中显示）">＋</span>'
    doc += '<span class="muted" title="点击表格中的 VR / PSY / RSI / KDJ 指标名可单独填写阈值">阈值：点击指标名填写</span>'
    doc += '<button onclick="loadData()">应用</button>'
    doc += '<span class="muted">三模块：上=压力(绿) / 中=支撑(红) / 下=指标 · <span style="color:var(--amber)">●</span>刚进入压力或支撑的那一拍标黄</span>'
    doc += '<span class="muted" title="价位类指标(BOLL/MA/预设点位/自定义)按现价在其上或下，分入压力(绿)与支撑(红)两个模块，模块内按数值降序显示，只显示数值不显示名称；BOLL 三轨只展示离现价最近的那一条(压力取最小/支撑取最大)；刚由支撑转为压力(或反之)的那一拍标黄，下一拍转回稳态色。KDJ：K>D=偏多(红)，K<D=偏空(绿)，J 值优先按阈值(超买红/超卖绿)">KDJ：<span class="sup">红</span>=偏多(K&gt;D) / <span class="pres">绿</span>=偏空(K&lt;D)</span>'
    doc += '</div>'

    doc += '<div id="tbl"></div>'
    doc += '<div id="lvlPanel" style="display:none"></div>'
    doc += '<div id="indPanel" style="display:none"></div>'
    doc += "</div>"

    # JS
    doc += "<script>"
    doc += "var INSTS=" + inst_json + ";"
    doc += """
    function apiBase(){ var p=location.pathname; var i=p.indexOf('/stocktech'); return p.substring(0,i+10)+'/'; }
""" + _ACCT_JS + _THR_JS + _LVL_JS + _SEC_JS + """
    function fmt(v,d){ if(v==null||v==='') return '—'; var n=Number(v); if(isNaN(n)) return v; return n.toFixed(d==null?2:d); }
    function loadInst(){ var sel=document.getElementById('instSel'); INSTS.forEach(function(it){ var o=document.createElement('option'); o.value=it.symbol; o.textContent=it.name+' ('+it.symbol+')'; sel.appendChild(o); });
      // [2026-10-17] 恢复上次选中的标的(与多标页 techActiveSym 同思路): 原来刷新/从别的页面
      // 切回本页, 下拉重置为第一个(上证指数), 用户想看的标的要重选。缓存值不在 INSTS 内则回落默认。
      try{ var _si=localStorage.getItem('techInstSel'); if(_si && INSTS.some(function(it){ return it.symbol===_si; })) sel.value=_si; }catch(e){}
      sel.onchange=function(){ try{ localStorage.setItem('techInstSel', sel.value); }catch(_){} loadData(); }; }
    function roleClass(role){ return role==='support'?'sup':'pres'; }
    var _noData=false;
    function render(d){
      // 2026-09-12 修 BUG: 原来用 Object.keys(d.periods) 当作列顺序 —— 但 Flask 的 jsonify 默认
      // sort_keys=True, 下发的键序是【字母序】"15m,1m,30m,5m,60m,day", 而表头写死
      // ['1m','5m','15m','30m','60m','D'] -> 6 列里有 4 列表头与数据错位!
      // 现改为按固定时间序取列, 与多标的页(PKEYS)保持一致。
      var PKEYS=['1m','5m','15m','30m','60m','day'], PLABELS=['1m','5m','15m','30m','60m','D'];
      var cols=[], labels=[];
      PKEYS.forEach(function(k,i){ if(d.periods[k]){ cols.push(k); labels.push(PLABELS[i]); } });
      // 全部周期均无数据(多为当前账号无该标的行情权限): 显示单行提示, 不铺满"暂无K线数据"
      var hasData = cols.some(function(k){ return !(d.periods[k]||{}).error; });
      _noData = !hasData;
      if(!hasData){
        document.getElementById('tbl').innerHTML='<div class="nodata">⚠ <b>'+d.name+'</b> — 暂无行情数据</div>';
        var info=document.getElementById('instInfo');
        if(info) info.innerHTML=d.name+' <span class="muted">—</span>';
        var ut=document.getElementById('updTime'); if(ut) ut.textContent='更新 '+new Date().toLocaleTimeString()+(d._cost_ms?' · '+d._cost_ms+'ms':'');
        return;
      }
      // 三模块布局(2026-09-12): 压力(绿) / 支撑(红) / 指标 —— 统一由 buildSections 生成
      document.getElementById('tbl').innerHTML=buildSections(d,cols,labels);
      // 顶部信息: 取第一个有数据的周期(1m 可能因权限/停牌为空)
      var first=null;
      for(var _i=0;_i<cols.length;_i++){ var _pe=d.periods[cols[_i]]; if(_pe&&!_pe.error){ first=_pe; break; } }
      if(!first) first={};
      var info=document.getElementById('instInfo');
      info.innerHTML=d.name+' '+fmt(first.last,2)+' <span class="'+(first.change_pct>=0?'up':'down')+'">'+(first.change_pct>=0?'+':'')+fmt(first.change_pct,2)+'%</span>';
      document.getElementById('updTime').textContent='更新 '+new Date().toLocaleTimeString()+(d._cost_ms?' · '+d._cost_ms+'ms':'');
    }
    // 刷新节奏: 实测行情源本身 ~2~3s 才出一笔(两次实测: 13 次变更/36.8s、10 次/22.3s),
    // 间隔只需把"显示延迟"压到半拍以内; 再快无意义(数据未变 + 后端有缓存)。
    // 2026-09-11 补时调度: 原 setInterval 在上一轮未归时会整拍跳过(1s 悄悄退化成 2s)。
    // 现改为"回包后再排下一拍, 且只补差额" => 实际节奏 = max(AUTO_MS, 本轮耗时), 不丢节拍也不叠加请求。
    var AUTO_MS=500;
    var _busy=false, _wd=null, _timer=null, _t0=0;
    function schedule(){ _timer=setTimeout(loadData, Math.max(0, AUTO_MS-(Date.now()-_t0))); }
    function loadData(){
      if(_busy) return;                 // 防堆积: 上一轮未归则跳过本轮(正常路径由 schedule 保证不叠加)
      if(_noData){ _t0=Date.now(); schedule(); return; }   // 无标的时保持空转探测(与原 setInterval 行为一致)
      _busy=true; _t0=Date.now();
      if(_wd) clearTimeout(_wd);
      _wd=setTimeout(function(){ _busy=false; _wd=null; schedule(); }, 10000);   // 兜底解锁: 悬挂后自动接回节奏, 不停摆
      var sym=document.getElementById('instSel').value;
      var ma=document.getElementById('maInput').value;
      var url=apiBase()+'api/data?symbol='+encodeURIComponent(sym)+'&ma='+encodeURIComponent(ma)+(ACCT?'&acct='+encodeURIComponent(ACCT):'')+(thrVal()?'&thr='+encodeURIComponent(thrVal()):'')+lvlQuery()+indQuery();
      var _ctrl=null,_to=null; if(typeof AbortController!=='undefined'){ _ctrl=new AbortController(); _to=setTimeout(function(){try{_ctrl.abort();}catch(_){}},8000); }   // 防后端某次挂起导致 fetch 永不 settle
      fetch(url+'&_t='+Date.now(), {cache:'no-store', signal:_ctrl?_ctrl.signal:undefined}).then(function(r){ if(_to)clearTimeout(_to); return r.json(); }).then(function(d){
        if(!d.ok){ document.getElementById('tbl').innerHTML='<div class=err>'+(d.error||'请求失败')+'</div>'; return; }
        render(d);
      }).catch(function(e){ try{document.getElementById('tbl').innerHTML='<div class=err>'+(e&&e.message||'请求失败')+'</div>';}catch(_){} })
        .then(function(){
          var was=_busy;
          if(_wd){ clearTimeout(_wd); _wd=null; }
          _busy=false;
          if(was) schedule();            // 补时排下一拍(watchdog 已接管时 was=false, 不重复排)
        })
        .catch(function(){ _busy=false; _wd=null; schedule(); });   // 极端兜底: 释放链任何异常都不卡 _busy
    }
    try{ var _mi=document.getElementById('maInput'); if(_mi&&MAP) _mi.value=MAP; }catch(e){}   // MA 周期与表中 MA 行共用一份(MAP)
    loadInst(); loadAccts(); loadData();
    </script></body></html>"""
    return doc


def tech_multi_page():
    """多标的同屏 (2x1): 左栏固定上证指数; 右栏为其余 5 个标的以标签切换, 标签上直接显示实时最新价/涨跌幅/涨跌额, 点击标签显示对应标的完整矩阵。

    路由注册在 /stocktech/multi (不能注册成 /stocktech_multi: 网关会按模块 route_path 前缀剥离,
    把 stocktech_multi 误当成 stocktech 前缀而拼成 /stocktech/_multi)。
    """
    inst_json = json.dumps(DEFAULT_INSTRUMENTS, ensure_ascii=False)
    futmap_json = json.dumps({k: v[0] for k, v in INDEX_FUT_PRODUCT.items()}, ensure_ascii=False)
    pool_json = json.dumps(TAB_POOL, ensure_ascii=False)
    doc = """<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><title>多标的同屏技术分析</title>"""
    doc += '<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">'
    doc += """<style>
    :root{--bg:#0f1115;--surface:#171a21;--surface2:#1e222b;--text:#e6e9ef;--muted:#8b93a7;--border:#2a2f3a;
      --accent:#4f8cff;--up:#ef4d4d;--down:#21ba72;--violet:#a78bfa;--amber:#fbbf24;}
    *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,'Segoe UI',Roboto,'Microsoft YaHei',sans-serif;font-size:13px}
    .wrap{padding:14px}
    .topbar{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px}
    .topbar h2{margin:0;font-size:17px}
    /* 中间那行的"空白双击热区"(2026-09-18 二改): 按用户要求做成【隐藏的惊喜】——
       零文案、零边框、零 hover 变化, 就是一行 40px 高的空白(cursor:pointer 便于瞄准),
       双击 = 自选价位设置。中性色: 它不承载涨跌/突破跌破方向, 故不用 --up/--down。
       user-select:none: 防止双击时把上方表格的文字选蓝。 */
    .lvlzone{min-height:40px;margin-top:8px;cursor:pointer;user-select:none;-webkit-user-select:none}
    input{background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:6px 8px;font-size:13px}
    button{background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:6px 12px;cursor:pointer;font-size:13px}
    button:hover{border-color:var(--accent)}
    a{text-decoration:none}
    .up{color:var(--up)} .down{color:var(--down)} .muted{color:var(--muted)}
    .layout{display:grid;grid-template-columns:1fr 1fr;gap:14px;align-items:start}
    .left,.right{min-width:0}
    /* 两栏共用一个页面级滚动条(最右侧): 卡片自身不滚动, 整页滚动带动两栏同时移动 */
    .left .card,.right .card{overflow:visible;max-height:none}
    .card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px}
    /* 左栏标签行: 结构与右栏 .tabs 一致(在 .card 之外), 只有一个标签占满整行 */
    .ltabs{display:flex;gap:4px;margin-bottom:10px;flex-wrap:nowrap;overflow-x:auto}
    .ltab{background:var(--surface2);border:1px solid var(--accent);color:var(--accent);border-radius:6px;padding:7px 9px;font-size:12px;display:flex;flex-direction:column;align-items:flex-start;gap:2px;white-space:nowrap;min-width:0;flex:1 1 0}
    .ltab .tnm{font-weight:700;font-size:13px;line-height:1.2}
    .ltab .tnum{font-size:12px;line-height:1.25;color:var(--muted);display:flex;gap:4px;align-items:center}
    .ltab .tnum b{font-size:13px;margin-right:2px;color:var(--text)}
    .ltab .tnum .pct{font-weight:600}
    .tabs{display:flex;gap:4px;margin-bottom:10px;flex-wrap:nowrap;overflow-x:auto}
    .tab{background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:7px 9px;cursor:pointer;font-size:12px;display:flex;flex-direction:column;align-items:flex-start;gap:2px;white-space:nowrap;min-width:0;flex:1 1 0}
    .tab:hover{border-color:var(--accent)}
    .tab.active{border-color:var(--accent);color:var(--accent);background:#1b2330;box-shadow:inset 0 -2px 0 0 var(--accent)}
    .tab .tnm{font-weight:700;font-size:13px;line-height:1.2}
    .tab .tnum{font-size:12px;line-height:1.25;color:var(--muted);display:flex;gap:4px;align-items:center}
    .tab .tnum b{font-size:13px;margin-right:2px;color:var(--text)}
    .tab .tnum .pct{font-weight:600}
    table{border-collapse:collapse;width:100%;background:var(--surface);border-radius:8px;overflow:hidden}
    th,td{border:1px solid var(--border);padding:9px 8px;text-align:center;vertical-align:middle;font-size:14px}
    thead th{background:var(--surface2);position:sticky;top:0;z-index:2}
    .rowlabel{background:var(--surface2);text-align:left;font-weight:600;white-space:nowrap}
    .rowlabel.thrb{cursor:pointer} .rowlabel.thrb:hover{color:var(--accent);border-color:var(--accent)}
    .thrv{font-weight:400;font-size:11px;color:var(--muted);margin-left:4px}
    .thrhit{font-weight:700}
    .sup{color:var(--up)} .pres{color:var(--down)}
    .chip{display:inline-block;padding:2px 7px;border-radius:4px;background:var(--surface2);margin:1px;border:1px solid var(--border)}
    .chip[title]{cursor:help}
    .golden{color:var(--violet)} .gap-up{color:var(--up)} .gap-down{color:var(--down)}
    .chip.sup{color:var(--up)} .chip.pres{color:var(--down)}
    /* RSI/KDJ 提示 chip (2026-09-20): 红=偏多(sig-up) / 绿=偏空(sig-dn)。
       独立类名, 不复用 .up/.down —— 与交叉类解耦, 以后改一处不会连带影响另一处。 */
    .chip.sig{border-color:currentColor}
    .chip.sig-up{color:var(--up)} .chip.sig-dn{color:var(--down)}
    /* 背离 chip (2026-09-21 用户要求): 顶背离=绿(div-dn) / 底背离=红(div-up)。
       独立类名, 不复用交叉的 .up/.down(金叉红/死叉绿)。 */
    .chip.div-up{color:var(--up)} .chip.div-dn{color:var(--down)}
    .chip.hit{color:var(--amber);font-weight:700;border-color:var(--amber);background:rgba(251,191,36,.13)}
    /* 三模块布局(2026-09-12): 压力(绿)/支撑(红)/指标; fresh=角色刚发生变化的那一拍(标黄) */
    .chip.fresh{color:var(--amber);font-weight:700;border-color:var(--amber);background:rgba(251,191,36,.13)}
    /* 临近压力/临近支撑(2026-09-17): 位置贴近但尚未确认 -> 虚线 + 降透明度, 与"突破/跌破"一眼可分 */
    .chip.soft{opacity:.72;border-style:dashed}
    /* 形态格两列(2026-09-17 用户要求 "超过四行就分两列显示在一格里"):
       patternChips 在 chip 数 > PAT_COL_MIN(4) 时包一层 .patbox.cols2。
       grid-auto-flow:column = 列优先填充(左列排满再排右列, 阅读顺序仍是列表顺序),
       行数由 inline style 的 grid-template-rows 钉死(见 patternChips 注释)。
       宽度: 形态列已加宽(见下方 "形态列预留两列"), 扣 td padding 10px 与 gap 4px 后每列 >= 68px
       —— 足够放下 "箱体突破"(4 字) 与 "MACD金叉"(英文+2 字); 仍给 max-width 兜底防极端溢出。 */
    .patbox{display:grid;justify-items:center;gap:1px 4px}
    .patbox.cols2{grid-auto-flow:column;grid-template-columns:repeat(2,minmax(0,1fr))}
    .patbox.cols2 .chip{font-size:12px;padding:2px 5px;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .fresh{color:var(--amber);font-weight:700}
    .sec{margin-bottom:12px;padding-left:6px;border-left:3px solid transparent}
    .sec:last-child{margin-bottom:0}
    .sec-pres{border-left-color:var(--down)}
    .sec-sup{border-left-color:var(--up)}
    .sec-ind{border-left-color:var(--border)}
    /* 三模块列对齐(2026-09-12): 三张表独立成表时会各算各的列宽 -> 必须强制固定列宽:
       首列(指标/rowlabel)钉死 84px, 其余周期列平分剩余宽度(table-layout:fixed 下无宽列等分)。
       三个 .sec 的盒模型完全一致, 故三张表总宽相同 -> 列边界逐列重合。 */
    .sec table{table-layout:fixed}
    .sec th.rowlabel,.sec td.rowlabel{width:84px;overflow:hidden;text-overflow:ellipsis}
    /* 形态列预留两列宽度(2026-09-17 用户要求 "一个形态格子里, 你预留两列"):
       最长的 chip 是 "MACD金叉"/"KDJ死叉"(英文前缀 + 2 汉字), 12px 实测 68.5px
       -> 两列各需 >= 70px 才不被省略号截断 -> 形态列 = 70*2 + gap4 + padding10 = 154, 取 160
       ⚠ 2026-09-20 调到 176: 新增的 "RSI二合一"(3 英文+3 汉字) 比 "MACD金叉" 还宽约 3px,
         160 下只剩 1px 余量(cur/2=73 vs 71.9), 故上调 16px 换取 9px 安全余量;
         代价 = 同卡片其余周期列各让出约 3px。改这里必须【两份 style 块一起改】。
       (实测 150 时 "MACD金叉" 被截断, 由 tmp/cdp_patbox.py 合成格 5/7 chips 抓出)。
       定位必须按"首个周期列", 因为右栏已 dropLab(无 .rowlabel 首列):
         左栏 = .rowlabel 后面那格; 右栏 = 首格(不带 .rowlabel)。
       table-layout:fixed 只认【表格第一行】的宽度 -> th/td 都写, 覆盖有/无 thead 的三张表;
       三张表命中同一套选择器 -> 同卡片内列边界依旧逐列重合(左右两栏的周期列宽本就不同, 与本次无关)。 */
    .sec th.rowlabel+th,.sec td.rowlabel+td{width:176px}
    .sec th:first-child:not(.rowlabel),.sec td:first-child:not(.rowlabel){width:176px}
    .sec th,.sec td{padding:9px 5px}
    .sec td.rowlabel.maedit{cursor:pointer;color:var(--accent)}
    .sec td.rowlabel.maedit:hover{text-decoration:underline}
    /* MA 区(2026-09-14 建; 2026-09-17 三改 = 单值): 一格只显示【该侧最贴近现价的那一条 MA】——
       压力侧取最小、支撑侧取最大, 与 BOLL 行同一取舍规则; 数值在上、MA 名在下(11px muted)。
       颜色 = 压力绿(.pres) / 支撑红(.sup), 与 BOLL/颈线/预设点位同一套语义色;
       不再有 ★"离现价最近"标记、不再有"距现价差"、不再有按设置序号轮转的色卡。
       宽度约束: 1600px 视窗下每周期列实测仅 109px —— 单值后占用比原来 4 条小得多, 不会横向溢出。 */
    /* 2026-09-17 用户要求: MA 行内容【顶对齐】。该行行高由 MA 条数最多的那一列(4 条)决定,
       而 td 默认 vertical-align:middle -> 标签列与少值列会在【行中段】留出一整块空白
       (用户截图对质"中间这块不要了")。顶对齐后各列第一条 MA 落在同一基线上(真正的横向对齐),
       空白只留在行底部, 与 fillHeight 拉升出来的行高融为一体。 */
    .sec tr.maRow td,.sec tr.maRow th{vertical-align:top}
    /* 2026-09-17 三改(用户: "只显示一个值...然后在数字下面写上是MA几"): 竖排单值 ——
       上行=数值, 下行=11px muted 的 MA 名; 去掉 距现价差 / ★ / 按序号轮转的身份色(色卡已删)。 */
    .maline{display:flex;flex-direction:column;align-items:center;justify-content:center;line-height:1.25}
    .maline .chip{padding:0 4px;margin:0;font-size:13px;font-weight:700}
    .maline .matag{font-size:11px;line-height:1.2;color:var(--muted);white-space:nowrap}
    .maline .madiff{font-size:10px;line-height:1.2;margin-left:3px;white-space:nowrap;color:var(--text);opacity:.72}
    .maline .mafresh{color:var(--amber);font-size:9px;line-height:1;margin-left:2px}
    /* BOLL 数值高亮(2026-09-17 三改): 该轨被【有效穿越】时套 .hot —— 突破=绿(.pres)、
       跌破=红(.sup); 高亮 = 同色描边 + 柔光晕 + 加粗, 与普通 chip(纯文字)拉开层次。
       完整依据(穿越时刻/驻留度/零轴确认/DIF)全部在 title(hover)里。 */
    .chip.hot{font-weight:700;border-color:currentColor;box-shadow:0 0 7px -1px currentColor}
    .chip.pres.hot{background:rgba(33,186,114,.20)}
    .chip.sup.hot{background:rgba(239,77,77,.20)}
    .kdjbar{white-space:nowrap;font-size:12px;margin:0 -4px;overflow:hidden;text-overflow:ellipsis}
    .rsibar{white-space:nowrap;font-size:12px;margin:0 -4px;overflow:hidden;text-overflow:ellipsis}
    .rsiv.under{color:var(--up);font-weight:700}
    .rsiv.over{color:var(--down);font-weight:700}
    /* 自选价位 ＋ 号面板(2026-09-14): 未勾选的条目【不下发】-> 表中那一行自然不出现 */
    .lvlbar{display:flex;align-items:center;gap:10px;margin-top:8px;font-size:13px;flex-wrap:wrap}
    .lvlbtn{border:1px dashed var(--border);border-radius:6px;padding:4px 12px;color:var(--muted);cursor:pointer;white-space:nowrap}
    .lvlbtn:hover{color:var(--accent);border-color:var(--accent)}
    #lvlPanel,#indPanel{position:fixed;z-index:1200;display:none;box-shadow:0 10px 28px rgba(0,0,0,.35)}
    .iplb{color:var(--muted);display:inline-block;width:62px}
    .ipn{width:46px;text-align:center;padding:3px 4px;font-size:12px}
    .iph{font-size:11px;margin-left:6px}
.bolltag{display:inline-block;font-size:12px;line-height:16px;padding:0 2px;margin-right:2px;border:1px solid var(--border);border-radius:3px;color:var(--muted);vertical-align:middle}
    .lp{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:10px 12px;font-size:12px;min-width:340px;max-width:min(560px,92vw)}
    .lpt{margin-bottom:8px;font-weight:600}
    .lprow{display:flex;align-items:center;gap:8px;margin-bottom:6px}
    .lprow .ckr{display:flex;align-items:center;gap:5px;white-space:nowrap;cursor:pointer}
    .lprow .ckr input{width:13px;height:13px;accent-color:var(--accent);margin:0;cursor:pointer}
    .lprow .lpn{width:96px;flex:0 0 auto}
    .lprow .lpv{flex:1 1 auto;min-width:130px}
    .lpx{color:var(--muted);cursor:pointer;padding:0 4px;font-size:15px;line-height:1}
    .lpx:hover{color:var(--up)}
    .lpf{justify-content:flex-end;margin-top:8px;margin-bottom:0}
    .lpf button{border:1px solid var(--border);background:var(--surface2);color:var(--text);border-radius:6px;padding:4px 10px;cursor:pointer;font-size:12px}
    .lpf button:hover{border-color:var(--accent)}
    .err{color:var(--muted);font-size:12px}
    .nodata{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:20px 16px;text-align:center;color:var(--muted);font-size:13px;line-height:1.8}
    .legend{margin-top:10px;color:var(--muted);font-size:13px;line-height:1.7}
    /* 月份合约下拉 (2026-09-12): 标签 ▼ 展开该指数旗下股指期货月份; + 号添加指数标签 */
    .tabs .tab{position:relative}
    .tarr{position:absolute;top:1px;right:2px;font-size:9px;color:var(--muted);cursor:pointer;padding:3px 5px;line-height:1}
    .tarr:before{content:'';position:absolute;inset:-5px}   /* 扩大点击热区, 小三角不再难点中 */
    .tarr:hover{color:var(--accent)}
    /* 2026-09-17 修: 「有合约视图」(.inview) 与「当前选中」(.active) 原来都是 accent 蓝边 ——
       恢复多个合约视图后多个标签同时发亮, 用户误以为"两个都选中"(截图对质)。
       现在: 选中 = 唯一的 accent 边框 + accent 文字 + 底色 + 底部 2px 压条;
       有合约视图 = 只用名字后面的 .vtag 合约徽标表示, 边框与 ▼ 一律回到常态色(不再改 .inview 任何样式)。 */
    .tabadd{border:1px dashed var(--border);background:transparent;color:var(--muted);border-radius:8px;padding:6px 11px;font-size:14px;cursor:pointer}
    .tabadd:hover{color:var(--accent);border-color:var(--accent)}
    /* 真下拉框: fixed 浮层按触发按钮坐标定位, 不受 .tabs overflow-x 剪裁 */
    #tabdd{position:fixed;z-index:1000;max-width:440px;min-width:190px;display:none;box-shadow:0 10px 28px rgba(0,0,0,.3)}
    #tabdd:before{content:'';position:absolute;top:-5px;width:8px;height:8px;background:var(--surface);border-left:1px solid var(--border);border-top:1px solid var(--border);transform:rotate(45deg);left:var(--caret,14px)}
    .dd{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:8px 10px;font-size:12px;color:var(--muted)}
    .ddt{margin-bottom:6px}
    .ddempty{color:var(--muted);padding:2px 0}
    .mchip{display:inline-flex;align-items:center;gap:5px;border:1px solid var(--border);border-radius:8px;padding:5px 10px;margin:0 6px 4px 0;cursor:pointer;font-size:13px;color:var(--text)}
    .mchip i{font-style:normal;color:var(--muted);font-size:11px}
    .mchip em{font-style:normal;font-size:10px;color:var(--accent);border:1px solid var(--accent);border-radius:4px;padding:0 3px}
    .mchip:hover{border-color:var(--accent)}
    .mchip.on{border-color:var(--accent);color:var(--accent);background:rgba(79,140,255,.12)}
    /* 下拉内 chips 竖排一列 (2026-09-12 用户要求) */
    #tabdd .mchip{display:flex;width:100%;box-sizing:border-box;margin:0 0 6px;justify-content:space-between}
    .mchip b.mcnt{font-style:normal;font-weight:700;color:#fff;background:var(--amber);border-radius:9px;padding:0 7px;font-size:11px;line-height:17px}
    .mchip b.mcnt.zero,.mchip b.mcnt.pend{color:var(--muted);background:transparent;font-weight:400}
    .ddt .ddtot{color:var(--amber);font-weight:700;font-size:15px;margin-left:6px}
    .vtag{font-size:10px;color:var(--accent);border:1px solid var(--accent);border-radius:4px;padding:0 3px;margin-left:4px}
    /* 标签上的形态数徽标 (2026-09-20): 与下拉 .mcnt 同款琥珀圆角, 计数=该标签正在监控标的(合约视图=子合约) */
    .tab .tnm b.tabcnt,.ltab .tnm b.tabcnt{font-style:normal;font-weight:700;color:#fff;background:var(--amber);border-radius:9px;padding:0 6px;font-size:10px;line-height:15px;margin-left:4px;vertical-align:1px}
    /* 标签勾选面板 (2026-09-14): ＋ 打开复选框列表, 右栏最多勾选 5 个; 取消一个才能勾选新的 */
    #tabdd .dd{max-height:64vh;overflow:auto}
    .ckrow{display:flex;align-items:center;gap:7px;padding:5px 6px;border-radius:6px;cursor:pointer;color:var(--text);white-space:nowrap}
    .ckrow:hover{background:var(--surface2)}
    .ckrow input{width:13px;height:13px;accent-color:var(--accent);cursor:pointer;flex:0 0 auto;margin:0}
    .ckrow .cknm{font-size:13px}
    .ckrow i{font-style:normal;color:var(--muted);font-size:11px;margin-left:auto}
    .ckrow.on .cknm{color:var(--accent);font-weight:700}
    .ckhint{color:var(--amber);font-size:12px;padding:2px 0 6px}
    .tabadd.on{color:var(--accent);border-color:var(--accent)}
    /* 形态预警 (2026-09-18): 右下角小圆球 + 下拉面板 + 顶栏徽标 */
    #patBall{position:fixed;right:18px;bottom:18px;width:46px;height:46px;border-radius:50%;background:var(--amber);color:#fff;display:none;align-items:center;justify-content:center;font-size:18px;font-weight:700;cursor:pointer;z-index:1200;box-shadow:0 6px 18px rgba(0,0,0,.35)}
    #patBall:hover{filter:brightness(1.1)}
    #patBallX{position:absolute;top:-7px;right:-7px;width:18px;height:18px;border-radius:50%;background:#666;color:#fff;font-size:13px;line-height:17px;text-align:center;cursor:pointer;box-shadow:0 2px 6px rgba(0,0,0,.35)}
    /* 收拢态(× = 收拢, 不是关闭) + 未读呼吸脉冲 + 顶栏未读数 (2026-09-20) */
    #patBall.fold{width:30px;height:30px;right:14px;bottom:14px;font-size:13px;opacity:.72}
    #patBall.fold:hover{opacity:1}
    #patBall.fold #patBallX{width:15px;height:15px;font-size:11px;line-height:14px;top:-5px;right:-5px}
    #patBall.unread{animation:patPulse 1.6s ease-in-out infinite}
    @keyframes patPulse{0%,100%{box-shadow:0 6px 18px rgba(0,0,0,.35),0 0 0 0 rgba(251,191,36,.55)}50%{box-shadow:0 6px 18px rgba(0,0,0,.35),0 0 0 8px rgba(251,191,36,0)}}
    #patBall.fold.unread{animation:patPulseSm 1.6s ease-in-out infinite}
    @keyframes patPulseSm{0%,100%{box-shadow:0 3px 10px rgba(0,0,0,.3),0 0 0 0 rgba(251,191,36,.5)}50%{box-shadow:0 3px 10px rgba(0,0,0,.3),0 0 0 6px rgba(251,191,36,0)}}
    .patunread{color:var(--amber);font-size:12px;font-weight:700;margin-left:2px}
    #patDD{position:fixed;right:18px;bottom:74px;min-width:190px;max-width:320px;background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:10px 12px;font-size:13px;color:var(--text);display:none;z-index:1200;box-shadow:0 10px 28px rgba(0,0,0,.3)}
    #patDD .pddt{font-weight:700;margin-bottom:6px;color:var(--muted);font-size:12px;display:flex;justify-content:space-between;align-items:center;gap:12px}
    #patDD .pdtst{font-weight:400;font-size:11px;color:var(--accent);border:1px solid var(--border);border-radius:5px;padding:0 6px;line-height:16px;cursor:pointer;white-space:nowrap}
    #patDD .pdtst:hover{border-color:var(--accent)}
    #patDD .pdrow{display:flex;justify-content:space-between;gap:16px;padding:3px 0}
    #patDD .pdrow b{color:var(--amber)}
    #patDD .pdempty{color:var(--muted)}
    #patBadge{color:var(--amber);border-color:var(--amber);font-weight:700;cursor:pointer}
    .patmute{cursor:pointer;user-select:none}
    </style></head><body><div class="wrap">"""
    # 顶栏整行移除 (2026-09-12 用户裁定): 0.5s 固定自动刷新, MA 周期点表中 MA 行名修改
    # 但保留一个轻量更新时间指示(updTime), 否则无任何视觉反馈 -> 用户误判"刷新卡住/慢"
    # 2026-09-18 用户要求: 自选价位入口从"左/右栏中间那组 ＋"挪到顶栏(加号去掉)。
    # 2026-09-18(二改) 用户改主意: 顶栏【不再】做双击区(提示 chip 一并去掉), 自选价位入口
    #   回到"中间那一行的空白双击热区"(lvlZone/.lvlzone, 定义见 _SEC_JS); 顶栏只留 ⚙。
    doc += ('<div class="topbar" style="margin-bottom:8px">'
            '<h2 style="font-size:15px">📊 多标的同屏技术分析</h2>'
            '<span class="lvlbtn indbtn" onclick="indToggle(event)" '
            'title="MACD / KDJ / BOLL / RSI 参数，以及 BOLL 显示哪条轨" aria-label="指标参数">'
            '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:-2px" aria-hidden="true"><circle cx="12" cy="12" r="3"></circle><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0 -1.51 1z"></path></svg></span>'
            '<span class="lvlbtn patmute" id="patMute" onclick="patMuteToggle(event)" title="预警声音开关">🔔</span>'
            '<span class="lvlbtn" id="patBadge" onclick="patBadgeClick(event)" title="形态预警：点击查看各合约形态数量(视为已读；再有新增会重新提醒)" style="display:none"></span>'
            '<span class="muted" id="updTime"></span>'
            '</div>')
    doc += '<div class="layout">'
    doc += '<div class="left"><div class="ltabs" id="ltabs"></div><div class="card" id="leftCard"><div class="muted">…</div></div></div>'
    doc += '<div class="right"><div class="tabs" id="tabs"></div><div id="tabdd" style="display:none"></div><div class="card" id="rightCard"><div class="muted">…</div></div></div>'
    doc += '<div id="patBall" onclick="patBallClick(event)" title="形态预警：点击=已读并查看各合约数量；右上角 × 为收拢(不关闭提醒)"></div>'
    doc += '<div id="patDD"></div>'
    doc += '<div id="lvlPanel" style="display:none"></div>'
    doc += '<div id="indPanel" style="display:none"></div>'
    doc += '</div>'
    doc += "</div>"

    doc += "<script>"
    doc += "var INSTS=" + inst_json + ";"
    doc += "var FUTMAP=" + futmap_json + ";"
    doc += "var POOL=" + pool_json + ";"
    doc += """
    var NAMES={}; INSTS.forEach(function(x){ NAMES[x.symbol]=x.name; });
    POOL.forEach(function(x){ if(!NAMES[x.symbol]) NAMES[x.symbol]=x.name; });
    var LEFT=INSTS[0].symbol;
    var RIGHT_MAX=5;                                                   // 右栏最多同时展示 5 个标签
    var RIGHT=INSTS.slice(1).map(function(x){ return x.symbol; });     // 默认 = 原有 5 个
    restoreSel();                                                      // localStorage 保存的勾选覆盖默认
    // 把用户(本地缓存)关注的标的插到服务端预加载队首: LEFT 固定标 + 右栏已勾选标, 打开即见、不干等
    try{
      var _ps=[LEFT].concat(RIGHT);
      fetch(apiBase()+'api/preload_priority',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbols:_ps}),cache:'no-store'}).catch(function(){});
    }catch(e){}
    var PKEYS=['1m','5m','15m','30m','60m','day'];
    var PLABELS=['1m','5m','15m','30m','60m','D'];
    var _data={}; var _ready={}; var _active=RIGHT.length?RIGHT[0]:LEFT; var _timer=null;
    // [2026-10-17] 恢复上次【选中】的标签: 此前只缓存了"显示哪些标签"(techTabsSel), 没缓存
    // "选中哪个" -> 用户切到中证1000后去别的页面再回来, 又重置成第一个(沪深300), 需重选。
    // 与 saveSel 同一 localStorage 命名空间/同一生命周期; 缓存值不在当前 RIGHT 内则回落默认。
    try{ var _sa=localStorage.getItem('techActiveSym'); if(_sa && RIGHT.indexOf(_sa)>=0) _active=_sa; }catch(e){}
    // 月份合约视图 (2026-09-12): _view[指数symbol] = 选中的合约symbol(空=显示指数本身)
    var _view={}; var _ddSym=null; var _ddMonths={}; var _ddErr={}; var _ddBusy={};
    function apiBase(){ var p=location.pathname; var i=p.indexOf('/stocktech'); return p.substring(0,i+10)+'/'; }
""" + _ACCT_JS + _THR_JS + _LVL_JS + _SEC_JS + """
    function fmt(v,d){ if(v==null||v==='') return '—'; var n=Number(v); if(isNaN(n)) return v; return n.toFixed(d==null?2:d); }
    function roleClass(role){ return role==='support'?'sup':'pres'; }
    function hasData(d){ if(!d||!d.periods) return false; return PKEYS.some(function(k){ var pe=d.periods[k]; return pe && !pe.error; }); }
    function dayPE(d){
      if(!d||!d.periods) return {};
      var pe=d.periods['day']; if(pe&&!pe.error) return pe;
      for(var i=0;i<PKEYS.length;i++){ var x=d.periods[PKEYS[i]]; if(x&&!x.error) return x; }
      return {};
    }
    function nums(d){
      var pe=dayPE(d); var last=pe.last, pct=pe.change_pct;
      // 涨跌额优先级: 后端原始 前收/涨跌额 > 旧路径(由两位小数 pct 反推, 有舍入误差, 仅兜底)
      var abs=null;
      if(last!=null){
        if(pe.pre_close!=null){ abs=last-pe.pre_close; }
        else if(pe.change!=null){ abs=pe.change; }
        else if(pct!=null){ abs=last-last/(1+pct/100); }
      }
      var sign=(pct!=null&&pct>=0)?'+':''; var asign=(abs!=null&&abs>=0)?'+':'';
      var cls = (pct==null)?'muted':(pct>0?'up':(pct<0?'down':'muted'));
      return { cls:cls, last:fmt(last,2), pct:sign+fmt(pct,2), abs:asign+fmt(abs,2) };
    }
    function headHTML(sym){
      // 左栏只有一个标的: 渲染成"单个标签卡"(外观与右栏 .tab 一致).
      // 与右栏结构保持一致 —— 标签行在 .card 之外, 表格在 .card 之内, 因此两边首行高度天然相同.
      return '<div class="ltab">'+tabInner(sym)+'</div>';
    }
    function tabInner(sym){
      var vs=_view[sym];
      // 2026-09-20 用户要求: 标签上也显示形态数(合约视图=该合约的计数, 与头部行情同一标的口径)
      var pc=patCountOf(vs||sym);
      var nm='<div class="tnm">'+NAMES[sym]+(vs?' <span class="vtag">'+(NAMES[vs]||vs)+'</span>':'')+(pc>0?'<b class="tabcnt" title="该标的当前形态数">'+pc+'</b>':'')+'</div>';
      // 2026-09-17: 合约视图下头部行情(最新价/涨跌幅/涨跌额)切到【子合约自身】的数据,
      //   不再永远显示指数的; 合约数据未就绪时显示 …, 就绪(含预载)后立即切换。
      var dsym=(vs && _ready[vs] && _data[vs] && hasData(_data[vs]))?vs:sym;
      if(!_ready[dsym]) return nm+'<div class="tnum muted">…</div>';
      if(!hasData(_data[dsym])) return nm+'<div class="tnum muted">—</div>';
      var n=nums(_data[dsym]);
      return nm+'<div class="tnum"><b class="'+n.cls+'">'+n.last+'</b> <span class="pct '+n.cls+'">'+n.pct+'%</span> <span class="'+n.cls+'">'+n.abs+'</span></div>';
    }
    function renderTabs(){
      var bar=document.getElementById('tabs'); if(!bar) return;
      bar.innerHTML='';
      RIGHT.forEach(function(sym){
        var b=document.createElement('button');
        b.className='tab'+(sym===_active?' active':'')+(_view[sym]?' inview':'');
        b.innerHTML=tabInner(sym)+'<span class="tarr" title="月份合约">▼</span>';
        (function(sym){
          b.onmousedown=function(ev){
            ev.stopPropagation(); ev.preventDefault();
            // 标签栏 0.5s 会整条重建; 改用 mousedown 响应, 避免"按下->抬起"跨重建瞬间被吞(见 ▼ 同款修法)
            // ▼ 用 onmousedown 开合且已 stopPropagation, 不会冒泡到本体
            if(ev.target && ev.target.closest && ev.target.closest('.tarr')) return;
            // 再点一下已激活(且正显示合约)的标签本体 = 退出合约视图, 回到指数
            // [2026-10-09 修复] 原只清内存 _view[sym], 没调 saveViewSel -> 库里记录还在,
            // 刷新后 restoreViewSel() 又读回来, 表现为"退出了又出现"(且这条是最顺手的入口)。
            // 任何把 _view[sym] 置空的地方都必须同时落库清除。
            if(_active===sym && _view[sym]){ _view[sym]=null; saveViewSel(sym,''); }
            _active=sym; saveActive(); _ddSym=null; renderDD(); renderTabs(); renderRight();
          };
          // ▼ 用 mousedown 触发: 标签栏 0.5s 重建, click 的"按下->抬起"若跨重建瞬间会整次丢失
          b.querySelector('.tarr').onmousedown=function(ev){ ev.stopPropagation(); ev.preventDefault(); toggleDD(sym, ev); };
        })(sym);
        bar.appendChild(b);
      });
      // "＋" 打开指数标签勾选面板 (2026-09-14 用户裁定: 右栏最多展示 5 个, 取消勾选后可勾选新的)
      var add=document.createElement('button');
      add.className='tabadd'+(_ddSym==='__add__'?' on':''); add.textContent='＋'; add.title='选择显示的指数标签（最多 5 个）';
      add.onmousedown=function(ev){ ev.stopPropagation(); ev.preventDefault(); _hint=''; _ddSym=(_ddSym==='__add__')?null:'__add__'; if(_ddSym==='__add__') _ddAnchor=ev.currentTarget.getBoundingClientRect(); renderDD(); };
      bar.appendChild(add);
    }
    // ---- 月份合约下拉 (真下拉框: 浮层按触发按钮定位) ----
    var _ddAnchor=null;
    var _hint='';                       // 勾选面板的临时提示(超上限/低于下限)
    function placeDD(){
      var el=document.getElementById('tabdd'); if(!el||el.style.display==='none'||!_ddAnchor) return;
      var r=_ddAnchor;   // 点击瞬间捕获的矩形数值(标签栏 0.5s 重建会脱离元素引用, 不能存元素)
      var w=el.offsetWidth, h=el.offsetHeight;
      var left=Math.max(8, Math.min(r.left+r.width/2-w/2, window.innerWidth-w-10));
      var top=r.bottom+6;
      if(top+h>window.innerHeight-8) top=Math.max(8, r.top-h-6);   // 底部放不下则往上弹
      el.style.left=left+'px'; el.style.top=top+'px';
      var cx=r.left+r.width/2-left;                                  // 小箭头对准按钮中心
      el.style.setProperty('--caret', Math.max(8, Math.min(w-16, cx-4))+'px');
    }
    // 预取月份列表 (2026-09-12 用户建议: 进入页面空闲时就加载, 点击 ▼ 秒出选项, 不再现场等)
    function prefetchDD(sym){
      if(!FUTMAP[sym]||_ddMonths[sym]||_ddErr[sym]||_ddBusy[sym]) return;
      _ddBusy[sym]=true;
      fetch(apiBase()+'api/fut_months?symbol='+encodeURIComponent(sym), {cache:'no-store'})
        .then(function(r){ return r.json(); })
        .then(function(d){
          if(d && d.ok){ _ddMonths[sym]=d.months||[]; enqueuePreload(sym,_ddMonths[sym]); } else { _ddErr[sym]=(d&&d.error)||'查询失败'; }
          if(_ddSym===sym) renderDD();
        })
        .catch(function(){ _ddErr[sym]='网络错误'; if(_ddSym===sym) renderDD(); });
    }
    // 串行预取 + 间隔, 避免同时开多个天勤独立会话压垮行情源
    function preloadDD(){
      var p=Promise.resolve();
      [LEFT].concat(RIGHT).forEach(function(sym){
        if(!FUTMAP[sym]) return;
        p=p.then(function(){ prefetchDD(sym); return new Promise(function(r){ setTimeout(r,1500); }); });
      });
    }
    function toggleDD(sym,ev){
      _ddSym=(_ddSym===sym)?null:sym;
      if(_ddSym && ev && ev.currentTarget){
        var tb=ev.currentTarget.closest('.tab')||ev.currentTarget;   // 锚定整个标签按钮, 而非 ▼ 小三角
        _ddAnchor=tb.getBoundingClientRect();
      }
      renderDD();
      if(_ddSym) prefetchDD(sym);   // 没预取到就现场补取
    }
    function ddShell(title,extra,body){
      // 标题行只在有内容时渲染; 2026-09-18 用户要求月份列表带系列形态合计
      // (patDdTitle 在无形态时返回 '', 仍不渲染标题行)
      var head=(title||extra)?('<div class="ddt">'+title+(extra||'')+'</div>'):'';
      return '<div class="dd" onclick="event.stopPropagation()">'+head+body+'</div>';
    }
    function renderDD(){
      var el=document.getElementById('tabdd'); if(!el) return;
      if(!_ddSym){ el.style.display='none'; el.innerHTML=''; return; }
      var html='';
      if(_ddSym==='__add__'){                                  // "＋" 指数标签勾选面板
        html='<div class="dd" onclick="event.stopPropagation()">'
            +'<div class="ddt">选择显示的指数标签 <span class="muted">（最多 '+RIGHT_MAX+' 个，已选 '+RIGHT.length+'）</span></div>'
            +(_hint?'<div class="ckhint">'+_hint+'</div>':'')
            + POOL.map(function(x){
                var on=RIGHT.indexOf(x.symbol)>=0;
                return '<label class="ckrow'+(on?' on':'')+'">'
                     +'<input type="checkbox" data-sym="'+x.symbol+'"'+(on?' checked':'')+' onchange="toggleTab(this.dataset.sym,this.checked)">'
                     +'<span class="cknm">'+x.name+'</span><i>'+x.symbol+'</i></label>';
              }).join('')
            +'</div>';
      } else {
        var sym=_ddSym, nm=NAMES[sym]||sym;
        var rm=(RIGHT.length>1)? '<span style="float:right;cursor:pointer" data-sym="'+sym+'" onclick="toggleTab(this.dataset.sym,false)">×取消勾选</span>' : '';
        if(!FUTMAP[sym]){                                      // 无股指期货的指数
          html=ddShell(nm, rm, '<div class="ddempty">该指数无股指期货合约</div>');
        } else {
          var ms=_ddMonths[sym];
          if(_ddErr[sym]){ html=ddShell(nm, rm, '<div class="ddempty">'+_ddErr[sym]+'</div>'); }
          else if(!ms){ html=ddShell('', rm, '<div class="ddempty">加载中…</div>'); }
          else if(!ms.length){ html=ddShell(nm, rm, '<div class="ddempty">该指数无股指期货合约</div>'); }
          else {
            var cur=_view[sym];
            // [2026-10-09 修复] clearView 改为接收被点标签 sym, 不再依赖 _active:
            // 原实现取 _active, 在多标签场景下"取消 A 标签的合约视图"会误清 _active 指向的那个标签。
            var back=cur? '<span style="float:right;cursor:pointer;color:var(--accent);margin-left:12px" onclick="clearView(\\''+sym+'\\')">↩ 返回指数</span>' : '';
            var idxRow='<span class="mchip idxrow'+(cur?'':' on')+'" data-msym="'+sym+'"'
                      +(cur?' onclick="clearView(\\''+sym+'\\')"':'')+'>'+nm+' <em>指数</em>'+mchipCnt(sym)+'</span>';
            html=ddShell(patDdTitle(sym), rm+back, idxRow+ms.map(function(m){
              return '<span class="mchip'+(cur===m.symbol?' on':'')+'" data-msym="'+m.symbol+'" onclick="pickMonth(\\''+sym+'\\',\\''+m.symbol+'\\',\\''+m.name+'\\')">'+m.name+' <i>'+m.code+'</i><em>'+m.kind+'</em>'+mchipCnt(m.symbol)+'</span>';
            }).join(''));
          }
        }
      }
      el.innerHTML=html; el.style.display='block'; placeDD();
    }
    function pickMonth(sym,csym,cname){
      NAMES[csym]=cname; _view[sym]=csym; _ddSym=null;
      _active=sym; saveActive();         // 选中月份=要看这个合约, 顺带切到该标签(选中态落缓存)
      // 2026-09-17: 先渲染样式(头部切合约名/骨架卡), 数据就绪(或预载命中)后自动填入, 不再白屏干等
      renderDD(); renderTabs(); renderRight();
      if(!_ready[csym]){
        // [2026-10-10 修复] 点击月份合约改走【预载专用通道】(preFetch), 不再用 fetchSym。
        // 原因: fetchSym 会把 ctrl 放进 _ABORTS, 且回调校验 _tickId 拍号 —— 500ms 节拍的
        // loadAll 每拍先 abort 上一拍未回包请求, 冷合约(首次要串行算 6 周期, 实测 13~16s)
        // 必然跨拍被掐, 且迟到回调因 myTick!==_tickId 被整条丢弃 => 永远等不到, 表现为
        // "点开等十多秒"。preFetch 不进 _ABORTS、不校验拍号, 请求一定能跑完。
        _loading[csym]=true;
        preFetch(csym,function(){ _loading[csym]=false; if(_view[sym]===csym){ renderTabs(); renderRight(); } });
      }
      saveViewSel(sym,csym);             // 用户选择落库(按天勤账号, 换浏览器也能恢复)
      prioPreload(sym);                  // 该指数其余月份合约插队预载
      // [2026-10-10] 把选中/已恢复的月份合约上报后端预热队列(原来只报指数, 子合约永远冷算)
      reportViewPreload();
    }
    function clearView(sym){
      // [2026-10-09 修复] 改为接收被点击的标签 sym; 不传时回落 _active(兼容旧调用)。
      // 原实现只取 _active, 多标签下"取消 A 的合约视图"会误清 _active 那个标签的记录,
      // 且被误清的那个刷新后又被 restoreViewSel 读回来 -> "取消了还有"。
      var idx = sym || _active;
      if(!idx) return;
      // [2026-10-10] 切走前清掉旧合约的 loading 标记: 否则该合约若仍在预载中, _loading[old]
      // 会永久停在 true -> loadAll 的 !_loading[vsym] 门控让它再也不发请求(数据永远不来)。
      var _old=_view[idx]; if(_old){ try{ _loading[_old]=false; }catch(e){} }
      _view[idx]=null; renderTabs(); renderRight();
      saveViewSel(idx,'');               // 清除落库记录(必须落库, 否则刷新会复活)
    }
    // ===== 月份合约数据预载 (2026-09-17) =====
    // 背景: 点月份合约才现场构建 6 周期 K 线, 冷数据要等数秒, 且 500ms 轮询每拍会 abort 上拍
    // 未回包的请求(fetchSym 进 _ABORTS + _tickId 拍号门), 冷合约反复被掐, 感觉"点开等半天"。
    // 方案: 每个指数旗下全部月份合约在空闲时【串行+2s 间隔】预载 —— 专用通道不进 _ABORTS、
    // 不校验拍号, 请求一定跑完(后端顺势把 K 线缓存建热); 之后点击 _ready 命中即秒切。
    var _loading={}; var _EF_ACCT='';
    var _preQ=[]; var _preBusy=false; var _preMark={}; var _selLoaded=false;
    function preUrl(sym){
      var ma=MAP||'2,10,20,60';
      var acct=ACCT||_EF_ACCT;
      return apiBase()+'api/data?symbol='+encodeURIComponent(sym)+'&ma='+encodeURIComponent(ma)
        +(acct?'&acct='+encodeURIComponent(acct):'')
        +(thrVal()?'&thr='+encodeURIComponent(thrVal()):'')+lvlQuery()+indQuery();
    }
    function preFetch(csym,cb){
      if(_ready[csym]&&_data[csym]){ if(cb) cb(); return; }
      _loading[csym]=true;
      fetch(preUrl(csym)+'&_t='+Date.now(), {cache:'no-store'})
        .then(function(r){ return r.json(); })
        .then(function(d){
          _loading[csym]=false;
          if(d&&d.ok){
            _data[csym]=d; _ready[csym]=true;
            if(d.acct) _EF_ACCT=d.acct;
            var need=false;
            for(var k in _view){ if(_view[k]===csym){ need=true; break; } }
            if(need){ renderTabs(); if(_view[_active]===csym) renderRight(); }
          }
        })
        .catch(function(){ _loading[csym]=false; })
        .then(function(){ if(cb) cb(); });
    }
    function enqueuePreload(sym,months){
      if(!FUTMAP[sym]||!months) return;
      months.forEach(function(m){
        if(!m||!m.symbol||_preMark[m.symbol]) return;
        _preMark[m.symbol]=1;
        _preQ.push({idx:sym,cs:m.symbol});
      });
      runPreQ();
    }
    function runPreQ(){
      if(_preBusy||!_preQ.length) return;
      _preBusy=true;
      var job=_preQ.shift();
      preFetch(job.cs,function(){ setTimeout(function(){ _preBusy=false; runPreQ(); },2000); });
    }
    function prioPreload(sym){             // 用户打开某指数下拉/选中合约: 该指数的月份插到队首
      var head=[], rest=[];
      _preQ.forEach(function(j){ (j.idx===sym?head:rest).push(j); });
      _preQ=head.concat(rest);
    }
    // ===== 用户合约视图落库/恢复 (2026-09-17) =====
    // 身份 = 天勤账号(优先页面下拉的 ACCT, 否则从行情响应的 acct 回读 —— 网关给每个用户绑定独立账号)。
    function selAcct(){ return ACCT||_EF_ACCT; }
    function saveViewSel(idx,csym){
      var a=selAcct(); if(!a||!idx) return;
      try{
        fetch(apiBase()+'api/view_sel',{method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({acct:a,index:idx,contract:csym||''})}).catch(function(){});
      }catch(e){}
    }
    function restoreViewSel(){
      if(_selLoaded) return; var a=selAcct(); if(!a) return;
      _selLoaded=true;
      fetch(apiBase()+'api/view_sel?acct='+encodeURIComponent(a),{cache:'no-store'})
        .then(function(r){ return r.json(); })
        .then(function(d){
          if(!d||!d.ok) return;
          var sel=d.sel||{}; var jobs=[];
          Object.keys(sel).forEach(function(k){
            if(RIGHT.indexOf(k)<0) return;               // 只恢复当前勾选的标签
            var cs=sel[k]; if(!cs) return;
            NAMES[cs]=NAMES[cs]||cs;
            _view[k]=cs;
            if(!_ready[cs]) jobs.push(k);
          });
          if(!jobs.length){ renderTabs(); reportViewPreload(); return; }
          renderTabs();
          jobs.forEach(function(k){                      // 恢复出的合约插队预载, 就绪后轮询自动接管刷新
            enqueuePreload(k,[{symbol:_view[k]}]);
            prioPreload(k);
          });
          reportViewPreload();                          // [2026-10-10] 恢复出的合约也上报后端预热
          renderRight();
        }).catch(function(){});
    }
    // [2026-10-10] 把 _view 里所有选中的月份合约上报后端预热队列(月份合约不在原白名单内,
    // 不上报则永远冷算 6 周期 => 切换子合约要等 13~16s)。
    function reportViewPreload(){
      try{
        var vs=[]; for(var k in _view){ if(_view[k]) vs.push(_view[k]); }
        if(!vs.length) return;
        fetch(apiBase()+'api/preload_priority',{method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({symbols:vs}),cache:'no-store'}).catch(function(){});
      }catch(e){}
    }
    function toggleTab(sym,on){
      // 勾选/取消勾选指数标签: 右栏最多 RIGHT_MAX 个, 取消勾选腾出位置后再勾选新的。
      if(on){
        if(RIGHT.indexOf(sym)>=0) return;
        if(RIGHT.length>=RIGHT_MAX){ _hint='最多只能显示 '+RIGHT_MAX+' 个标签，请先取消勾选一个'; keepPanel(); return; }
        RIGHT.push(sym);
        if(!_ready[sym]) fetchSym(sym,function(){ renderTabs(); if(sym===_active) renderRight(); });
        setTimeout(function(){ preloadDD(); }, 1500);   // 新勾选的标签也进预取队列
      } else {
        if(RIGHT.length<=1){ _hint='至少保留 1 个标签'; keepPanel(); return; }
        var i=RIGHT.indexOf(sym); if(i>=0) RIGHT.splice(i,1);
        if(_active===sym) _active=RIGHT[0];
        delete _view[sym];
        saveViewSel(sym,'');                            // 取消勾选标签: 该指数的合约视图记录一并清除
        if(_ddSym===sym) _ddSym=null;                   // 从 ▼ 面板取消勾选 -> 关掉该面板
      }
      _hint=''; saveSel(); saveActive(); renderTabs(); renderRight();
      if(_ddSym==='__add__'){ var ab=document.querySelector('.tabadd'); if(ab) _ddAnchor=ab.getBoundingClientRect(); }
      renderDD();
    }
    function keepPanel(){                                 // 触达上/下限: 面板保持打开并显示提示
      if(_ddSym==='__add__'){ var ab=document.querySelector('.tabadd'); if(ab) _ddAnchor=ab.getBoundingClientRect(); }
      renderTabs(); renderDD();
    }
    function saveSel(){
      try{ localStorage.setItem('techTabsSel', JSON.stringify(RIGHT)); }catch(e){}
    }
    function saveActive(){               // [2026-10-17] 选中态持久化(init 处按 techActiveSym 恢复)
      try{ localStorage.setItem('techActiveSym', _active); }catch(e){}
    }
    function restoreSel(){
      try{
        var s=JSON.parse(localStorage.getItem('techTabsSel')||'null');
        if(s && s.length){
          var out=[];
          s.forEach(function(sym){ if(POOL.some(function(x){ return x.symbol===sym; }) && out.indexOf(sym)<0) out.push(sym); });
          if(out.length) RIGHT=out.slice(0, RIGHT_MAX);
        }
      }catch(e){}
    }
    function buildMatrix(sym,noHead,dropLab){
      var d=_data[sym]; var html=noHead?'':headHTML(sym);
      if(!hasData(d)){
        // 2026-09-17: 正在预载/首次拉取的合约先渲染"加载中"骨架(样式先出), 拉到数据后本函数会被重跑填入
        var ld=_loading[sym]?'⏳ <b>'+NAMES[sym]+'</b> 数据加载中…（首次构建 6 周期 K 线, 已在后台预载, 下次秒开）'
                            :'⚠ <b>'+NAMES[sym]+'</b> — 暂无行情数据（服务刚启动时数据预热约 1-2 分钟, 页面会自动重试; 若长时间如此请刷新页面）';
        return html+'<div class="nodata">'+ld+'</div>';
      }
      // 三模块布局(2026-09-12): 压力(绿) / 支撑(红) / 指标 —— 统一由 buildSections 生成
      // 2026-09-18 用户要求: ＋/⚙ 那组按钮不再出现在表格区;
      //   中间原位置【保留一行空白双击热区】(blankBar=true -> lvlZone()) —— 热区大、
      //   位于屏幕中间, 双击同样弹出自选价位浮层。
      html+=buildSections(d,PKEYS,PLABELS,dropLab,true);
      return html;
    }
    function _renderLeft(){
      // 标签行在 .card 外(与右栏一致); 表格在 .card 内
      var tb=document.getElementById('ltabs'); if(tb) tb.innerHTML=headHTML(LEFT);
      var el=document.getElementById('leftCard'); if(el) el.innerHTML=buildMatrix(LEFT,true);
    }
    function _renderRight(){
      var el=document.getElementById('rightCard'); if(!el) return;
      // 2026-09-17: 去掉顶部「📌 合约名 (代码) + 返回指数」横条(用户要求);
      // 「返回指数」入口移到 ▼ 月份下拉里(renderDD), 当前视图是合约时下拉标题行会显示。
      var vsym=_view[_active];
      // 2026-09-17(二版): 右栏(中间栏)【整个首列不渲染】—— 与左栏逐行一模一样, 去掉可给 6 个周期列
      //   各腾出约 14px。行键改由 <tr data-row> 承载, "逐行等高" 不变式不受影响。
      el.innerHTML=buildMatrix(vsym||_active,true,true);
    }
    // 对齐必须【紧接着每一次渲染同步执行】(2026-09-15): 渲染会重建表格, 写在上面的 inline 行高会一并丢失。
    //   若改成 rAF/定时器延后对齐, 就会出现"渲染任务已结束、对齐还没跑"的一帧 -> 浏览器正好把错位的那一帧画出来。
    //   同步执行的代价只有一次强制布局(十几行), 换来"任何任务结束时 DOM 都是对齐的"这一不变式。
    function renderLeft(){ _renderLeft(); try{ alignColumns(); }catch(e){} }
    function renderRight(){ _renderRight(); try{ alignColumns(); }catch(e){} }
    // ===== 左右两栏"逐行等高"(2026-09-15 用户要求: 左右两边的高度要对齐) =====
    // 背景: 两栏是独立 DOM, 表格高度由内容撑开 —— 历史最典型的是 MA 行(竖排 .maline):
    //       三改前左栏这条轨道下挂 4 条 MA、右栏只挂 1 条, 行高 146px vs 46px 就差了 100px。
    //       三改后 MA 行每侧恒为 1 条(高度问题基本消失), 但 BOLL/形态/成交量等行仍会因
    //       两侧内容不同而高低不一, 故本机制照旧保留。
    // 做法: 每拍全部渲染完成后, 把【同名行】(行标签为键)的高度统一取两栏较大值;
    //       行集合已由 LVL_BASE_ROWS 保证两侧一致。只增不减 —— 不裁切任何数值, 只在较短的一侧留白。
    // 先清空上一拍写入的 inline height, 让行按内容自然回缩后再重新取 max, 否则高度只增不减会累积。
    function _clearRowH(tb){
      var cs=tb.querySelectorAll('td,th');
      for(var i=0;i<cs.length;i++){ if(cs[i].style.height) cs[i].style.height=''; }
    }
    function _setRowH(tr,h){
      if(!tr||h<=0) return;
      var cs=tr.querySelectorAll('td,th');
      for(var i=0;i<cs.length;i++){ cs[i].style.height=h+'px'; }
    }
    // 行键 -> tr。2026-09-17 二版: 右栏整个首列已不渲染, 行键不能再靠首列文字,
    //   改为每行挂 data-row="<行名>"(表头行 = "指标")。两边键名因此完全一致 -> 逐行等高照旧。
    function _rowMap(tb){
      var m={}, trs=tb.querySelectorAll('tr');
      for(var i=0;i<trs.length;i++){
        var nm=trs[i].getAttribute('data-row');
        if(nm==null){
          var lab=trs[i].querySelector('th.rowlabel,td.rowlabel');
          nm=lab?((lab.getAttribute('data-lab')||lab.innerText||'').trim()):'';
        }
        m[nm?('L:'+nm):('I:'+i)]=trs[i];
      }
      return m;
    }
    // ===== 卡片高度填充 (2026-09-17 用户要求) =====
    // 现象: 卡片内容只占上半屏, 下面空一大块("可以适当向下拉升, 字体扩大点")。
    // 做法: 行高对齐(alignColumns)之后, 把【卡片底部 -> 视窗底部】的剩余高度均摊到每一行上。
    //   只增不减; 两栏应用【同一个 delta】且行集合一致 -> "逐行等高" 不变式继续成立(不会把两侧撑歪)。
    //   每拍都从 _clearRowH 清空后的自然高度重算, 因此不会逐拍累积; 窗口变矮/surplus<8 时自动不动。
    function _addRowH(tr,d){
      // ⚠ 基准高度必须在循环【外】量一次: 行内任一单元格设高都会立刻抬高整行,
      //   若在循环里逐个 getBoundingClientRect(), 后面几个格子读到的就是"已被撑高的行高",
      //   同一行会变成 自然+d / +2d / +3d …(实测 7 格 -> 行高被放大 7 倍)。
      var cs=tr.querySelectorAll('td,th');
      if(!cs.length) return;
      var base=parseFloat(cs[0].style.height)||tr.getBoundingClientRect().height;
      if(!(base>0)) return;
      var h=(Math.round((base+d)*10)/10)+'px';
      for(var i=0;i<cs.length;i++) cs[i].style.height=h;
    }
    function fillHeight(){
      var L=document.getElementById('leftCard'), R=document.getElementById('rightCard');
      var host=R||L||document.getElementById('tbl');       // 多标页=右卡片, 单页=#tbl
      if(!host) return;
      var r=host.getBoundingClientRect();
      if(!r.height) return;
      var top=r.top+(window.pageYOffset||document.documentElement.scrollTop||0);   // 文档绝对 y, 与滚动无关
      var avail=window.innerHeight-top-16;                 // 视窗底部留 16px 呼吸
      var cur=Math.max(r.height, L?L.getBoundingClientRect().height:0);
      var surplus=Math.min(avail-cur, cur*0.9);            // 最多把卡片撑高 90%(防超大屏上行距稀得离谱)
      if(surplus<8) return;
      var pairs=[], i, k;
      var lt=L?L.querySelectorAll('.sec table'):null, rt=R?R.querySelectorAll('.sec table'):null;
      if(L&&R&&lt&&rt&&lt.length&&lt.length===rt.length){  // 两栏结构一致 -> 只动【两侧都有】的同名行
        for(i=0;i<rt.length;i++){
          var a=_rowMap(lt[i]), b=_rowMap(rt[i]);
          for(k in b){ if(a[k]) pairs.push([a[k], b[k]]); }
        }
      }
      if(!pairs.length){                                   // 单页或结构不同 -> 本容器内全部行一起加
        var trs=host.querySelectorAll('.sec table tr');
        for(i=0;i<trs.length;i++) pairs.push([trs[i]]);
      }
      if(!pairs.length) return;
      var delta=Math.floor(surplus/pairs.length*10)/10;
      if(delta<1) return;
      pairs.forEach(function(pr){ for(var j=0;j<pr.length;j++) _addRowH(pr[j],delta); });
    }
    function alignColumns(){
      var L=document.getElementById('leftCard'), R=document.getElementById('rightCard');
      if(!L||!R) return;
      var lt=L.querySelectorAll('.sec table'), rt=R.querySelectorAll('.sec table');
      if(!lt.length||lt.length!==rt.length) return;    // 右栏无数据(⚠ no quote data)等结构不同 -> 不强行对齐
      for(var i=0;i<lt.length;i++){
        _clearRowH(lt[i]); _clearRowH(rt[i]);
        var a=_rowMap(lt[i]), b=_rowMap(rt[i]);
        for(var k in a){
          if(!b[k]) continue;                          // 该行只在单侧存在(自定义组) -> 跳过
          // 用分数像素: 两栏宽度是 1fr+1fr 的分数分配, 同一段文字换行后的自然高度会差 0.3~0.4px;
          // offsetHeight 取整会把这个差留在版面上(Δ0.4px 虽肉眼难辨, 但会让"严格对齐"断言失败)。
          var ha=a[k].getBoundingClientRect().height, hb=b[k].getBoundingClientRect().height;
          var h=Math.ceil(Math.max(ha,hb)*100)/100;
          if(h<=0) continue;
          if(Math.abs(ha-h)>0.01) _setRowH(a[k], h);
          if(Math.abs(hb-h)>0.01) _setRowH(b[k], h);
        }
      }
      fillHeight();   // 行高对齐完 -> 再把卡片下方剩余高度均摊到各行(2026-09-17)
    }
    // 对齐必须赶在浏览器绘制之前完成 —— 否则每拍都会出现"先错开一帧、再对齐"的抖动。
    // 用 rAF 把同一帧内的多次请求合并: 无论哪个 fetch 先回包, 都在本帧绘制前统一量一次。
    var _alignPend=false;
    function _scheduleAlign(){
      if(_alignPend) return;
      _alignPend=true;
      var run=function(){ _alignPend=false; try{ alignColumns(); }catch(e){} };
      if(typeof requestAnimationFrame==='function') requestAnimationFrame(run); else setTimeout(run,0);
    }
    // 跨拍 abort 集合: 每拍 loadAll 开头取消上一拍尚未返回的请求。
    //   ⚠ abort 只让 promise 走 catch, 【并不会阻止回调执行】—— 所以真正起作用的是下面的 _tickId 拍号:
    //   本拍开始即自增, 上一拍遗留请求带着旧拍号回来时被 fetchSym.fin 整条丢弃。
    var _ABORTS=[];
    var _tickId=0;
    function fetchSym(sym,cb){
      var ma=MAP||'2,10,20,60';
      var url=apiBase()+'api/data?symbol='+encodeURIComponent(sym)+'&ma='+encodeURIComponent(ma)+(ACCT?'&acct='+encodeURIComponent(ACCT):'')+(thrVal()?'&thr='+encodeURIComponent(thrVal()):'')+lvlQuery()+indQuery();
      var ctrl=null, to=null;
      var myTick=_tickId;                       // 本请求所属拍号
      if(typeof AbortController!=='undefined'){
        ctrl=new AbortController();
        _ABORTS.push(ctrl);
        to=setTimeout(function(){ try{ ctrl.abort(); }catch(e){} }, 8000);   // 防后端某次挂起导致 fetch 永不 settle
      }
      // 迟到回调必须整条丢弃(2026-09-15): 上一拍被 abort 的请求会在【本拍中途】走 catch 回到这里,
      //   若不拦, 它会: ① 用 null 覆盖 _data 把卡片刷成"no quote data" ② 重跑一次 render 把已对齐的行高冲掉
      //   ③ 扣掉本拍的 pending 计数 -> 提前触发 done()/更新时间。判定: 拍号已过期就 return。
      var fin=function(d){
        if(to) clearTimeout(to);
        if(myTick!==_tickId) return;
        _ready[sym]=true; _data[sym]=(d&&d.ok)?d:null;
        // 2026-09-17: 回读后端实际使用的天勤账号(网关按用户绑定注入)作为落库身份; 身份就绪后恢复该账号上次的合约视图(仅一次)
        if(d&&d.ok&&d.acct){ _EF_ACCT=d.acct; restoreViewSel(); }
        if(cb) cb();
      };
      fetch(url+'&_t='+Date.now(), {cache:'no-store', signal: ctrl?ctrl.signal:undefined})
        .then(function(r){ return r.json(); })
        .then(function(d){ fin(d); })
        .catch(function(){ fin(null); });   // 网络/解析/超时均回调, 保证 done() 一定被调用
    }
    // 刷新节奏: 固定 AUTO_MS 节拍。每拍 loadAll 先取消上一拍未完成的请求(见 _ABORTS),
    // 再以【本拍独立的 pending 计数】驱动 —— 计数完全闭包隔离, 旧拍回调只影响它自己的计数器,
    // 不会污染新拍; schedule 用【单一 _timer】+= clearTimeout 覆盖, 杜绝多拍定时器叠加导致请求风暴。
    var AUTO_MS=500;
    var _timer=null, _autoOn=false, _tickT0=0;
    function schedule(){
      if(!_autoOn) return;
      if(_timer) clearTimeout(_timer);
      // 补时: 以本拍【起始时刻】为基准, 令下一拍恒定在 AUTO_MS 后启动(而不是 AUTO_MS+本拍耗时),
      // 从而节拍锁定 500ms; 至少留 30ms 防 0ms 忙循环。单 _timer+clearTimeout 保证不会叠加定时器。
      var el=Date.now()-_tickT0;
      _timer=setTimeout(autoLoop, Math.max(30, AUTO_MS-el));
    }
    function loadAll(){
      _tickT0=Date.now();
      _tickId++;                                   // 本拍号: 先自增, 再 abort 上一拍请求 -> 它们的回调带旧号, 会被丢弃
      // 取消上一拍未回包请求, 防止旧回调干扰本拍
      _ABORTS.splice(0).forEach(function(a){ try{ a.abort(); }catch(_){} });
      // 本拍任务数: 左栏 + 每个右栏标签(处于合约视图且本拍确实会发请求时额外 +1)。
      // 🔴 [2026-10-10 修复] 计数必须与【实际发出的请求数】严格一致。原先 pending 按 _view[sym]
      // 无条件 +2, 但下面加了 !_loading[vsym] 门控后该请求可能不发 -> pending 永不归零 ->
      // done() 不触发 -> 不再 schedule() -> 500ms 节拍整体停摆(现象: "更新"时间冻结、
      // 所有标签数据卡在最后一帧, 等多久都不再刷新)。
      var pending=1;
      RIGHT.forEach(function(sym){ pending += (_view[sym] && !_loading[_view[sym]])?2:1; });
      function done(){
        if(--pending>0) return;
        // 两侧都已渲染完 -> 逐行取两栏较大高度(绘制前生效), 使后续 section 起始位置一致
        _scheduleAlign();
        try{ patCheck(); }catch(e){}
        try{ var ut=document.getElementById('updTime'); if(ut) ut.textContent='更新 '+new Date().toLocaleTimeString()+(ACCT?' · '+ACCT:''); }catch(e){}
        schedule();
      }
      // 渲染可能抛错的防御: 无论成功失败都 done(), 否则 pending 永不归零 → 本拍更新时间被冻死
      function safe(fn){ try{ fn(); }catch(e){} finally{ done(); } }
      // 兜底: 本拍结束时若仍有未归零的计数(例如某个请求因门控未发出), 强制补齐,
      // 否则 pending 永不归零 -> done() 不触发 -> schedule() 不再排下一拍 -> 节拍永久停摆。
      setTimeout(function(){ try{ while(pending>0){ done(); } }catch(e){} }, 3000);
      fetchSym(LEFT,function(){ safe(renderLeft); });
      RIGHT.forEach(function(sym){
        // 下拉展开期间暂停标签栏重建: 防止点击热区随 DOM 重建移动/丢失, 选完自动恢复
        fetchSym(sym,function(){ safe(function(){ if(!_ddSym) renderTabs(); if(sym===_active) renderRight(); }); });
        var vsym=_view[sym];
        // [2026-10-10 修复] 专用预载通道(preFetch)正在为该合约冷算时, 本拍不要再发 fetchSym:
        // 客户端 abort 不会取消服务端已在跑的 compute_instrument, 每 500ms 叠一个 13~16s 的
        // 冷算任务, 2 秒就能把 6 线程池堆满 -> 拖垮整页(含所有指数)。等专用通道跑完自动接管。
        if(vsym && !_loading[vsym]) fetchSym(vsym,function(){ safe(function(){ if(sym===_active) renderRight(); }); });
      });
    }
    function autoLoop(){
      if(!_autoOn) return;
      loadAll();
    }
    // 0.5s 固定自动刷新 (2026-09-12 用户裁定: 顶栏删除, 不再提供开关; 首轮回全后由 done() 接上节奏)
    _autoOn=true;
    renderTabs(); loadAccts(); loadAll();
    if(ACCT) restoreViewSel();     // localStorage 已有账号: 立即恢复上次合约视图; 否则等首个行情响应回读 acct
    setTimeout(preloadDD, 3000);   // 首轮数据加载完, 空闲时预取各标签月份合约(月份列表 -> 数据串行预载)
    // 点空白处关闭月份合约下拉
    document.addEventListener('click', function(ev){
      if(!_ddSym) return;
      var t=ev.target;
      if(t.closest && (t.closest('.tarr')||t.closest('#tabdd')||t.closest('.tabadd'))) return;
      _ddSym=null; renderDD();
    });
    // 滚动/缩放: 收起下拉 (标签栏 0.5s 重建, 元素引用已失效, 不尝试跟随)
    window.addEventListener('scroll', function(){ if(_ddSym){ _ddSym=null; renderDD(); } }, true);
    window.addEventListener('resize', function(){ if(_ddSym){ _ddSym=null; renderDD(); } });
    // ===== 形态预警 (2026-09-18 用户需求) =====
    // "如果有出现形态, 需要发出警报声, 然后右下角弹出小圆球, 告知有多少个形态,
    //  并且在顶栏提示数量, 点击下拉显示每个合约有多少形态(只需要数字)"。
    // 每拍 done() 后统计【当前页全部标的 = 各指数 + 旗下子合约(月份合约)】当前显示的形态行 chip 集合
    //   (形态/背离/MACD·KDJ 交叉) —— 总数 = 悬浮球/顶栏徽标 = 各指数及其子合约逐行相加,
    //   与上一拍比对 —— 只有【新增】才响铃(8s 冷却防刷屏), 首拍不响; 形态消失后再次出现会重新计为新增。
    // 声音走 WebAudio 振荡器(无音频文件); 浏览器自动播放策略要求先有一次
    //   用户手势, 故挂 pointerdown 解锁 —— 未解锁前徽标/圆球照常, 只是暂时静音。
    var _patPrev={}, _patRows=[], _patTotal=0, _patDDOpen=false, _lastBeep=0, _actx=null;
    // 2026-09-20 提醒升级: ①点球/点徽标 = 已读(此后只在【再有新增】时响) ②× = 收拢(不关闭)
    //   ③降噪: 预载静默 + 首段静默 + 冷却合并补响 + flap 抑制 + 后台不响(切回补响) ④未读脉冲 + 顶栏 +N
    //   ⑤试听: 下拉标题行右侧「试听」-> patTest() -> patPlay(true) 绕过全部静默门禁(静音开关不拦主动试听)。
    //     ⚠️ 加它的原因(2026-09-20): 用户报"没听到声音"时, 没有任何办法自证音效是否可用 ——
    //     实际上一整天不响通常是因为 休市无新K线 / 页面打开后 PAT_LOAD_SILENCE_MS(10min) 预载静默。
    var _patFold=false; try{ _patFold=localStorage.getItem('techPatFold')==='1'; }catch(e){}
    var _patUnread=0, _patBorn=Date.now(), _patSeen={}, _patPendT=null, _patBackPend=false;
    var PAT_COOLDOWN_MS=8000;        // 两次提示音最小间隔; 期间的新增合并成"一次补响", 不再丢弃
    var PAT_FLAP_MS=120000;          // 同一标的同一形态"消失后 2 分钟内再现"不重复提醒(防抖动刷屏)
    var PAT_BOOT_MS=20000;           // 页面打开后的首段静默(首屏/预热产物不算信号)
    var PAT_LOAD_SILENCE_MS=600000;  // 预载期静默的上限; 超时后不再因预载队列未清空而静默
    var _patMute=false; try{ _patMute=localStorage.getItem('techPatMute')==='1'; }catch(e){}
    function patList(d){
      var out=[];
      if(!d||!d.periods) return out;
      PKEYS.forEach(function(k){
        var pe=d.periods[k]; if(!pe||pe.error) return;
        (pe.shapes||[]).forEach(function(s){ out.push(SHAPE_CN[s]||s); });
        if(pe.divergence) out.push(pe.divergence==='top'?'顶背离':'底背离');
        if(pe.macd&&pe.macd.cross) out.push(pe.macd.cross==='golden'?'MACD金叉':'MACD死叉');
        if(pe.kdj&&pe.kdj.cross) out.push(pe.kdj.cross==='golden'?'KDJ金叉':'KDJ死叉');
        // RSI 合一/做多做空 + KDJ 超买超卖 (2026-09-20) 也计入形态数 —— 它们已经渲染在形态格里,
        //   若不计数会出现"格子里的 chip 数对不上悬浮球/徽标"(用户对该口径有强诉求)。
        (pe.rsi_kdj||[]).forEach(function(r){ if(r&&r.label) out.push(r.label); });
      });
      return out;
    }
    // 合约下拉形态计数 (2026-09-20): 列表首行=指数本身, 其后=各月合约; 标题合计 = 逐行相加
    function patCountOf(sym){
      var d=_data[sym];
      if(!d||!_ready[sym]||!hasData(d)) return 0;
      return patList(d).length;
    }
    function mchipCnt(msym){          // 月份/指数行尾形态数: 有形态=琥珀 / 无形态=灰 0 / 未就绪=灰 …
      var st=patMchipState(msym);
      return '<b class="mcnt'+st.cls+'" data-msym="'+msym+'">'+st.txt+'</b>';
    }
    function patMchipState(msym){
      if(!_ready[msym]||!_data[msym]||!hasData(_data[msym])) return {cls:' pend', txt:_ready[msym]?'–':'…'};
      var n=patCountOf(msym);
      return n>0? {cls:'', txt:String(n)} : {cls:' zero', txt:'0'};
    }
    function patDdTotal(sym){
      var t=patCountOf(sym);
      (_ddMonths[sym]||[]).forEach(function(m){ t+=patCountOf(m.symbol); });
      return t;
    }
    function patDdTitle(sym){
      // 2026-09-20: 始终出标题(含 0), 因为列表每行都显示数量, 合计必须可对账
      return (NAMES[sym]||sym)+' <b class="ddtot">'+patDdTotal(sym)+'</b>';
    }
    function patDDRefresh(){          // 下拉打开期间随预载进度就地补数字(只改徽标节点, 不重建 DOM, 免丢点击)
      if(!_ddSym||_ddSym==='__add__') return;
      var el=document.getElementById('tabdd'); if(!el||el.style.display==='none') return;
      var bs=el.querySelectorAll('b.mcnt[data-msym]');
      for(var i=0;i<bs.length;i++){
        var st=patMchipState(bs[i].getAttribute('data-msym'));
        if(bs[i].className!==('mcnt'+st.cls)||bs[i].innerHTML!==st.txt){ bs[i].className='mcnt'+st.cls; bs[i].innerHTML=st.txt; }
      }
      var tt=el.querySelector('b.ddtot');
      if(tt){ var n=patDdTotal(_ddSym); if(tt.innerHTML!==String(n)) tt.innerHTML=String(n); }
    }
    function patLoading(){                 // 月份合约串行预载中: 此时的"新增"多为加载产物 -> 只计未读不响铃
      if(Date.now()-_patBorn>=PAT_LOAD_SILENCE_MS) return false;
      if(typeof _preQ!=='undefined'&&_preQ&&_preQ.length) return true;
      return (typeof _preBusy!=='undefined')&&!!_preBusy;
    }
    function patFresh(sym,cn){             // flap 抑制: 同一标的同一形态 2 分钟内"消失后再现"不算新增
      return (Date.now()-(_patSeen[sym+'|'+cn]||0))>PAT_FLAP_MS;
    }
    function patCheck(){
      // 2026-09-20 refined: 数量球/顶栏徽标总数 = 当前页【全部标的】之和, 含各指数旗下的子合约(月份合约)。
      //   - 每个标签(左栏+右栏): 统计 指数本身 + 旗下全部月份合约(_ddMonths) 的形态数(=patDdTitle 口径)。
      //   - 悬浮球=全页监控覆盖总量(含子合约); 下拉明细逐行相加与球一致。
      //   - 标签上的 .tabcnt 仍为"该标签当前正在看的标的"(合约视图=那张合约), 二者口径不同, 属预期。
      var syms=[LEFT]; RIGHT.forEach(function(s){ syms.push(s); });
      var rows=[], total=0, newN=0;      // newN = 本拍【真正的新增】条数(已读过 flap/首次门槛)
      syms.forEach(function(sym){
        // 指数本身
        var d=_data[sym];
        if(d&&_ready[sym]&&hasData(d)){
          var names=patList(d);
          if(names.length){
            rows.push({sym:sym,n:names.length}); total+=names.length;
            var cur=names.slice().sort().join('|'), prev=_patPrev[sym]||'';
            if(prev){ var pp={}; prev.split('|').forEach(function(x){ if(x) pp[x]=1; });
              for(var i=0;i<names.length;i++){ if(!pp[names[i]]&&patFresh(sym,names[i])) newN++; } }
            names.forEach(function(nm){ _patSeen[sym+'|'+nm]=Date.now(); });
            _patPrev[sym]=cur;
          } else { delete _patPrev[sym]; }
        }
        // 取数抖动(_ready/hasData 暂时为假)不再清基线 —— 否则"抖一下"就被当成新形态提醒
        // 子合约(月份合约): 已就绪才计, 渐进加载期间未就绪的不算(避免初始误报)
        (_ddMonths[sym]||[]).forEach(function(m){
          var ms=m.symbol, md=_data[ms];
          if(!md||!_ready[ms]||!hasData(md)) return;      // 取数抖动: 保留基线, 避免误判新增
          var mn=patList(md);
          if(!mn.length){ delete _patPrev[ms]; return; }
          rows.push({sym:ms,n:mn.length}); total+=mn.length;
          var mcur=mn.slice().sort().join('|'), mprev=_patPrev[ms]||'';
          if(mprev){ var mpp={}; mprev.split('|').forEach(function(x){ if(x) mpp[x]=1; });
            for(var j=0;j<mn.length;j++){ if(!mpp[mn[j]]&&patFresh(ms,mn[j])) newN++; } }
          mn.forEach(function(nm){ _patSeen[ms+'|'+nm]=Date.now(); });
          _patPrev[ms]=mcur;
        });
      });
      _patRows=rows; _patTotal=total;
      if(newN>0) _patUnread+=newN;   // 已读模型: 累计自上次点球(已读)以来的新增条数
      patRender();
      try{ patReport(); }catch(e){}
      if(_ddSym) patDDRefresh();     // 月份下开着: 就地刷新各行数字与标题合计
      if(newN>0) patBeep();          // 只有【新增】才走提示音(响不响由 patPlay 的门禁决定)
    }
    // 形态总数上报(2026-10-17 用户诉求"左侧模块与页面顶部没同步"):
    //   导航徽标原口径=全局(DEFAULT 6+EXTRA 11+月份合约), 与页面顶部([LEFT]+RIGHT+子合约)
    //   对不上(实测 45 vs 12)。这里每拍把页面口径 POST 给服务端, 徽标优先取页面值 ->
    //   两边严格同源; 页面关闭后徽标保留最后值(age 增大, 网关侧自动提示"数据陈旧")。
    //   值没变就跳过(500ms 节拍不刷请求); total=0 也上报, 保证徽标与页面同步归零。
    var _patLastReport='';
    function patReport(){
      try{
        var syms=[LEFT].concat(RIGHT), scope=syms.length;
        syms.forEach(function(s){ scope += (_ddMonths[s]||[]).length; });
        var payload=JSON.stringify({total:_patTotal, rows:_patRows, scope:scope});
        if(payload===_patLastReport) return;
        _patLastReport=payload;
        fetch(apiBase()+'api/pattern_total',{method:'POST',headers:{'Content-Type':'application/json'},body:payload,cache:'no-store'}).catch(function(){});
      }catch(e){}
    }
    function patRender(){
      var b=document.getElementById('patBadge'), ball=document.getElementById('patBall');
      if(_patTotal<=0){
        _patUnread=0;
        if(b) b.style.display='none';
        if(ball) ball.style.display='none';
        if(_patDDOpen){ _patDDOpen=false; patDDRender(); }
        return;
      }
      if(b){ b.style.display=''; b.innerHTML='🔔 形态 <b>'+_patTotal+'</b>'
        +(_patUnread>0? ' <span class="patunread">+'+_patUnread+'</span>':''); }
      if(ball){
        ball.style.display='flex';
        ball.className=(_patFold?'fold':'')+(_patUnread>0?' unread':'');
        ball.innerHTML=String(_patTotal)
          +'<span id="patBallX" onclick="patBallFold(event)" title="'
          +(_patFold?'展开数量球':'收拢数量球(不是关闭, 新形态仍会提醒)')+'">×</span>';
      }
      if(_patDDOpen) patDDRender();
    }
    function patAck(){                                          // 已读: 清未读(停脉冲/清顶栏 +N)
      _patUnread=0; _patBackPend=false;
      if(_patPendT){ clearTimeout(_patPendT); _patPendT=null; }  // 取消已排队的补响(点了就不响)
      patRender();
    }
    function patBallFold(ev){ if(ev) ev.stopPropagation(); _patFold=!_patFold;
      try{ localStorage.setItem('techPatFold',_patFold?'1':'0'); }catch(e){} patRender(); }
    function patDDToggle(ev){ if(ev) ev.stopPropagation(); _patDDOpen=!_patDDOpen; patDDRender(); }
    function patBallClick(ev){ patAck(); patDDToggle(ev); }     // 点球 = 已读 + 展开各合约数量(明细未改)
    function patBadgeClick(ev){ patAck(); patDDToggle(ev); }    // 点顶栏徽标 = 同上
    function patDDRender(){
      var dd=document.getElementById('patDD'); if(!dd) return;
      if(!_patDDOpen){ dd.style.display='none'; return; }
      var html='<div class="pddt"><span>形态预警 · 各合约数量'+(_patUnread>0?'（新增 '+_patUnread+'）':'')+'</span>'
        +'<span class="pdtst" onclick="patTest(event)" title="试听提醒音（三声短响，不受静默门禁影响）">试听</span></div>';
      if(!_patRows.length) html+='<div class="pdempty">当前无形态</div>';
      else html+=_patRows.map(function(r){
        var nm=NAMES[r.sym]||r.sym;
        return '<div class="pdrow"><span>'+lvlEsc(nm)+'</span><b>'+r.n+'</b></div>';
      }).join('');
      dd.innerHTML=html; dd.style.display='block';
    }
    function patTest(ev){                    // 试听: 绕过全部静默门禁; 静音开关只影响"自动提醒", 不拦主动试听
      if(ev){ ev.stopPropagation(); ev.preventDefault(); }
      if(_patPendT){ clearTimeout(_patPendT); _patPendT=null; }   // 刚听过就不必再补响
      var m0=_patMute; _patMute=false;
      patPlay(true);
      _patMute=m0;
    }
    function patMuteToggle(ev){ if(ev) ev.stopPropagation(); _patMute=!_patMute; try{ localStorage.setItem('techPatMute',_patMute?'1':'0'); }catch(e){} patMuteRender(); }
    function patMuteRender(){ var m=document.getElementById('patMute'); if(m){ m.textContent=_patMute?'🔕':'🔔'; m.title=_patMute?'预警声音已关(点击开启)':'预警声音开启中(点击静音)'; } }
    function patAudio(){
      if(_patMute) return null;
      try{
        if(!_actx){ var AC=window.AudioContext||window.webkitAudioContext; if(!AC) return null; _actx=new AC(); }
        if(_actx.state==='suspended') _actx.resume();
        return _actx;
      }catch(e){ return null; }
    }
    function patBeep(){                                 // 新增形态 -> 提示音; 冷却期内的新增【合并】为一次补响
      if(_patMute) return;
      if(_patPendT) return;                             // 已排定一次播报, 本次合并进去(不丢也不连响)
      var wait=PAT_COOLDOWN_MS-(Date.now()-_lastBeep);
      if(wait<=0) patPlay();
      else _patPendT=setTimeout(patPlay, wait);
    }
    function patPlay(force){                     // force=true = 用户点"试听": 跳过静默门禁
      _patPendT=null;
      if(!force){
        if(document.hidden){ _patBackPend=true; return; }         // 页面在后台: 不响, 切回前台补响一次
        if(_patMute) return;
        if(Date.now()-_patBorn<PAT_BOOT_MS||patLoading()) return;  // 首段/预载静默: 只计未读不发声
      }
      _lastBeep=Date.now();
      var ctx=patAudio(); if(!ctx||ctx.state!=='running') return;
      [0,0.22,0.44].forEach(function(t){                // 2026-09-18 用户要求: 三声
        var o=ctx.createOscillator(), g=ctx.createGain();
        o.type='sine'; o.frequency.value=880;
        var st=ctx.currentTime+t;
        g.gain.setValueAtTime(0.0001,st);
        g.gain.exponentialRampToValueAtTime(0.25,st+0.02);
        g.gain.exponentialRampToValueAtTime(0.0001,st+0.18);
        o.connect(g); g.connect(ctx.destination);
        o.start(st); o.stop(st+0.2);
      });
    }
    patMuteRender();
    document.addEventListener('pointerdown', function(){   // 首次手势即解锁音频(浏览器 autoplay 策略)
      try{
        if(!_actx&&!_patMute){ var AC=window.AudioContext||window.webkitAudioContext; if(AC) _actx=new AC(); }
        if(_actx&&_actx.state==='suspended') _actx.resume();
      }catch(e){}
    });
    document.addEventListener('visibilitychange', function(){          // 切回前台: 有未读则补响一次
      if(document.hidden) return;
      if(_patBackPend){ _patBackPend=false; if(_patUnread>0) patBeep(); }
    });
    document.addEventListener('click', function(ev){
      if(!_patDDOpen) return;
      var t=ev.target;
      if(t.closest&&(t.closest('#patDD')||t.closest('#patBall')||t.closest('#patBadge'))) return;
      _patDDOpen=false; patDDRender();
    });
    </script></body></html>"""
    return doc


def stock_serve_static(filename):
    return send_from_directory(_STATIC_DIR, filename)


def register_routes(app, slots, default_key, get_slot=None, make_slot=None):
    global _SLOTS, _DEFAULT_KEY, _GET_SLOT, _MAKE_SLOT
    _SLOTS = slots
    _DEFAULT_KEY = default_key
    _GET_SLOT = get_slot
    _MAKE_SLOT = make_slot
    app.add_url_rule("/stocktech", view_func=tech_multi_page, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stocktech/", view_func=tech_multi_page, methods=["GET"], strict_slashes=False)
    # 单标页面仍保留, 供需要细看单个标的的使用(默认入口为上方多标同屏)
    app.add_url_rule("/stocktech/single", view_func=tech_page, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stocktech/single/", view_func=tech_page, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stocktech/api/data", view_func=api_tech_data, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stocktech/api/fut_months", view_func=api_fut_months, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stocktech/api/view_sel", view_func=api_view_sel, methods=["GET", "POST"], strict_slashes=False)
    app.add_url_rule("/stocktech/api/preload_priority", view_func=api_preload_priority, methods=["POST"], strict_slashes=False)
    app.add_url_rule("/stocktech/api/accounts", view_func=api_tech_accounts, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stocktech/api/pattern_total", view_func=api_pattern_total, methods=["GET"], strict_slashes=False)
    # 页面上报通道(2026-10-17): 多标页 patCheck 每拍把当前页形态总数 POST 上来, 导航徽标优先取它
    app.add_url_rule("/stocktech/api/pattern_total", view_func=api_pattern_total_report,
                     endpoint="pattern_total_report", methods=["POST"], strict_slashes=False)
    app.add_url_rule("/stocktech/multi", view_func=tech_multi_page, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stocktech/multi/", view_func=tech_multi_page, methods=["GET"], strict_slashes=False)
    app.add_url_rule("/stocktech/static/<path:filename>", view_func=stock_serve_static, endpoint="stocktech_serve_static", methods=["GET"], strict_slashes=False)
    print("[技术分析模块] 路由已注册: /stocktech, /stocktech/multi, /stocktech/api/data, /stocktech/api/fut_months, /stocktech/api/pattern_total")
    # 启动即后台预热 K 线订阅(不等首个请求才触发), 使页面打开时数据已就绪、秒级返回
    _ensure_warm()
    # 形态总数聚合(网关导航徽标数据源): 15s 一拍读缓存, 零新增天勤负载
    start_pat_total_worker()
