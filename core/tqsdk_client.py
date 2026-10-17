"""
天勤 TqSdk 行情客户端封装
文档: https://doc.shinnytech.com/tqsdk/latest/
"""
import os
import time
import threading
from tqsdk import TqApi, TqAuth


class TqSdkClient:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        # 绕过系统代理，直连天勤服务器（避免本机代理软件未启动导致连接被拒绝）
        for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
                   "ALL_PROXY", "all_proxy"):
            os.environ.pop(_k, None)
        os.environ["NO_PROXY"] = "*"
        os.environ["no_proxy"] = "*"
        
        # 重试连接（网络不稳定时自动重试）
        last_error = None
        for attempt in range(5):
            try:
                self.api = TqApi(auth=TqAuth(username, password))
                last_error = None
                break
            except Exception as e:
                last_error = e
                print(f"[连接] 第{attempt+1}次连接失败: {str(e)[:80]}")
                if attempt < 4:
                    print(f"[连接] 等待5秒后重试...")
                    time.sleep(5)
        
        if last_error:
            raise RuntimeError(f"无法连接到天勤服务器，请检查网络。错误: {last_error}")
        print(f"[连接] 天勤 TqSdk 连接成功")
        self._quotes = {}  # symbol -> quote 对象
        self._quotes_lock = threading.Lock()
        # worker 线程登记"待主线程批量注册"的合约(绝不从请求线程调 api.get_quote,
        # 其内部会 wait_update 等首包, 与主线程泵并发跑同一事件循环会毁坏 loop)
        self._pending_quotes = set()
        self._pending_quotes_lock = threading.Lock()

    def get_quote_cached(self, symbol: str):
        """只读获取已注册的 quote 引用; 未注册返回 None, 绝不触发阻塞注册。

        供主线程泵的快照路径使用: 在泵线程里做任何阻塞式注册(单个合约可能烧满
        TqSdk 固定 30s 超时), 都会把整个轮询冻死 => 全站行情/K线停更。"""
        return self._quotes.get(symbol)

    def register_quotes_async(self, symbols):
        """主线程专用: 非阻塞批量注册 quote 订阅(创建订阅任务立即返回, 泵 wait_update 驱动)。

        组合策略(2026-09-08 实测收敛):
        - 【锚定】startup_poll 里官方 get_quote_main_thread 同步注册一个合约并等到首包,
          作用是激活本会话的 quote 推送通道(此时订阅 100% 发出)。
        - 【批量】本函数用 TqSdk 内部 QuoteList 一次性承载整批 symbol, 只发一次
          subscribe_quote, 创建即返回【不阻塞主线程泵】; 订阅任务留在 TqApi 事件循环里,
          由泵每轮 wait_update 自然推进并送达数据。
        验证: 锚定后 QuoteList 批量订阅可靠送达(最小对照实验); 无锚定的纯 QuoteList
        作为会话首批订阅不可靠(任务未被驱动 => 服务器零推送), 有锚定的官方逐个同步注册
        又会把泵卡死在等首包 —— 两者结合才是正确形态。
        返回 (ok, fail): 非阻塞模式下全部登记为 ok(失败由运行期静默, 数据不到缓存即空)。
        """
        from tqsdk.diff import _get_obj
        from tqsdk.objs_not_entity import QuoteList
        ok, fail = [], []
        todo = []
        with self._quotes_lock:
            for s in symbols:
                if self._quotes.get(s) is not None:  # None=占位(请求线程正在建), 由它收尾
                    ok.append(s)
                    continue
                todo.append(s)
        if not todo:
            return ok, fail
        try:
            entities = [_get_obj(self.api._data, ["quotes", s],
                                 self.api._prototype["quotes"]["#"]) for s in todo]
            ql = QuoteList(self.api, entities)
            with self._quotes_lock:
                for s, q in zip(todo, ql):
                    self._quotes[s] = q
            ok.extend(todo)
        except Exception as e:
            fail.append(("<batch>", str(e)[:100]))
            with self._quotes_lock:
                for s in todo:
                    self._quotes.pop(s, None)
        return ok, fail

    def get_quote(self, symbol: str):
        """
        获取合约行情(线程安全、非阻塞), symbol 格式如:
          期货: SHFE.cu2409, CFFEX.IF2409, DCE.i2409, CZCE.SR409
          股票: SSE.600519, SZSE.000001 (需企业版)

        重要：quote 对象必须在【TqApi 创建线程(主线程)】创建才会真正发出订阅请求。
        本方法可能被 Flask 请求线程调用——若缓存未命中, 绝不从请求线程直接调
        api.get_quote(其内部 get_quote_list 在 loop 未运行时从【当前线程】wait_update
        等首包, 与主线程泵并发跑同一 asyncio 事件循环 => 毁坏 loop => 之后所有
        get_kline_serial/set_chart 拿不到响应而 25s 超时, 全站 K线/行情冻死)。
        缓存未命中仅登记到待注册集合, 由主线程 _register_pending 批量注册, 返回 None。
        """
        with self._quotes_lock:
            q = self._quotes.get(symbol)
            if q is not None:
                return q
        with self._pending_quotes_lock:
            self._pending_quotes.add(symbol)
        return None

    def drain_pending_quotes(self):
        """主线程专用: 取出并清空 worker 线程登记的待注册合约。"""
        with self._pending_quotes_lock:
            syms = list(self._pending_quotes)
            self._pending_quotes.clear()
        return syms

    def get_quote_main_thread(self, symbol: str, timeout: float = None):
        """主线程( TqApi 创建线程)专用：官方 get_quote 同步注册订阅并等首包，写回缓存。

        timeout 参数保留仅作兼容(不再限时): 2026-09-08 实锤 QuoteList 限时注册路径
        (旧 timeout 分支)作为首批订阅不可靠(订阅任务未被驱动 => 服务器零推送)。
        官方 api.get_quote 同步等首包(收盘后亦立即返回快照), 无权限合约的 30s 烧泵
        风险由调用方本地权限预检(_has_md_grants)与订阅上限兜底。"""
        q = self.api.get_quote(symbol)
        with self._quotes_lock:
            self._quotes[symbol] = q
        return q

    def wait_update(self, timeout: float = 3.0):
        """等待行情数据更新"""
        import time
        return self.api.wait_update(deadline=time.time() + timeout)

    def get_kline(self, symbol: str, duration_seconds: int = 60, data_length: int = 200, end_dt=None):
        """获取K线数据, duration: 60=1分钟, 86400=日线。
        end_dt: 翻页用, 传入 datetime 时取该时间【之前】的 data_length 根(不含 end_dt 本身),
        用于历史往回翻页(对齐 WH6 能拉到去年)。"""
        if end_dt is not None:
            return self.api.get_kline_serial(symbol, duration_seconds, data_length, end_dt=end_dt)
        return self.api.get_kline_serial(symbol, duration_seconds, data_length)

    def get_kline_nonblocking(self, symbol: str, duration_seconds: int = 60, data_length: int = 200):
        """非阻塞创建 K 线序列对象(主线程专用, 与 get_kline_serial 等价但不阻塞等首包)。

        复刻 tqsdk get_kline_serial 的"创建 serial + 发 set_chart 任务"部分, 去掉末尾
        `while not serial["init"]: wait_update(deadline=25s)` 同步等首包循环 —— 该循环在
        运行期会长时间占用主线程泵(两 slot 交错下 25s deadline 甚至不触发 => 全站行情/K线
        冻死)。改为创建即返回 df(初始为空), 数据由泵的 wait_update 自然驱动送达;
        调用方用 _kline_ready 有界等待, 拿不到数据则优雅降级, 绝不冻泵。
        注: end_dt 在 tqsdk get_kline_serial 里本就经 **kwargs 被忽略(分页不生效), 此处
        同样不做分页, 语义与原 get_kline(end_dt=None) 完全一致。"""
        api = self.api
        symbol_list = [symbol]
        dur_id = int(duration_seconds) * 1000000000
        request = (tuple(symbol_list), int(duration_seconds), int(data_length), None)
        serial = api._requests.get("klines", {}).get(request)
        if serial is not None:
            return serial["df"]
        from tqsdk.utils import _generate_uuid
        from tqsdk.diff import _get_obj
        pack = {
            "aid": "set_chart",
            "chart_id": _generate_uuid("PYSDK_realtime"),
            "ins_list": ",".join(symbol_list),
            "duration": dur_id,
            "view_width": int(data_length),
        }
        api.create_task(api._get_serial_async(symbol_list, pack.copy()), _caller_api=True)
        serial = api._init_serial(
            [_get_obj(api._data, ["klines", s, str(dur_id)]) for s in symbol_list],
            int(data_length), api._prototype["klines"]["*"]["*"]["data"]["@"], None,
        )
        serial["chart"] = _get_obj(api._data, ["charts", pack["chart_id"]])
        serial["chart"].update(pack)
        api._requests["klines"][request] = serial
        api._serials[id(serial["df"])] = serial
        return serial["df"]

    def get_kline_cached(self, symbol: str, duration_seconds: int = 60, data_length: int = 200):
        """只读获取已创建的 K 线序列 df; 未创建返回 None, 绝不触发阻塞创建。
        供 worker 线程读已建好的 chart(避免 get_kline 阻塞式 get_kline_serial 从请求线程
        等首包, 与主线程泵并发跑同一事件循环 => 毁坏 loop)。"""
        try:
            request = (tuple([symbol]), int(duration_seconds), int(data_length), None)
            serial = self.api._requests.get("klines", {}).get(request)
            if serial is None:
                return None
            return serial["df"]
        except Exception:
            return None

    def get_tick_serial(self, symbol: str, data_length: int = 40):
        """获取 tick 序列, 用于计算现手 / 外盘 / 内盘。
        注意: TqSdk tick 序列不含主动买卖方向字段, 内外盘只能估算。"""
        return self.api.get_tick_serial(symbol, data_length)

    def close(self):
        self.api.close()