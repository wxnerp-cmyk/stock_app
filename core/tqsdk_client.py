"""
天勤 TqSdk 行情客户端封装
文档: https://doc.shinnytech.com/tqsdk/latest/
"""
import os
import time
import threading
from tqsdk import TqApi, TqAuth


class TqSdkClient:
    def __init__(self, username: str = "zhubajie", password: str = "jinhu303"):
        self.username = username
        self.password = password
        # 绕过公司代理，直连天勤服务器
        os.environ.setdefault("NO_PROXY", "shinnytech.com,.shinnytech.com")
        os.environ.setdefault("no_proxy", "shinnytech.com,.shinnytech.com")
        
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

    def get_quote(self, symbol: str):
        """
        获取合约行情, symbol 格式如:
          期货: SHFE.cu2409, CFFEX.IF2409, DCE.i2409, CZCE.SR409
          股票: SSE.600519, SZSE.000001 (需企业版)

        重要：quote 对象必须在【TqApi 创建线程(主线程)】创建，TqSdk 才会真正发送订阅请求。
        其他线程调用时若缓存未命中，也只登记返回（订阅请求由主线程注册后，数据按 symbol
        分发更新所有引用）。加锁保证“检查-创建”原子性。
        """
        with self._quotes_lock:
            if symbol not in self._quotes:
                self._quotes[symbol] = self.api.get_quote(symbol)
        return self._quotes[symbol]

    def get_quote_main_thread(self, symbol: str):
        """主线程( TqApi 创建线程)专用：直接向 TqApi 注册订阅，并同步缓存引用。
        必须在 TqApi 创建线程调用才会真正发送订阅请求；同时把返回的生效引用写回
        _quotes 缓存，保证请求线程后续 get_quote 命中同一对象并读到更新数据。"""
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

    def get_tick_serial(self, symbol: str, data_length: int = 40):
        """获取 tick 序列, 用于计算现手 / 外盘 / 内盘。
        注意: TqSdk tick 序列不含主动买卖方向字段, 内外盘只能估算。"""
        return self.api.get_tick_serial(symbol, data_length)

    def close(self):
        self.api.close()