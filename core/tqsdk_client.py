"""
天勤 TqSdk 行情客户端封装
文档: https://doc.shinnytech.com/tqsdk/latest/
"""
import os
import time
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

    def get_quote(self, symbol: str):
        """
        获取合约行情, symbol 格式如:
          期货: SHFE.cu2409, CFFEX.IF2409, DCE.i2409, CZCE.SR409
          股票: SSE.600519, SZSE.000001 (需企业版)
        """
        if symbol not in self._quotes:
            self._quotes[symbol] = self.api.get_quote(symbol)
        return self._quotes[symbol]

    def wait_update(self, timeout: float = 3.0):
        """等待行情数据更新"""
        import time
        return self.api.wait_update(deadline=time.time() + timeout)

    def get_kline(self, symbol: str, duration_seconds: int = 60, data_length: int = 200):
        """获取K线数据, duration: 60=1分钟, 86400=日线"""
        kline = self.api.get_kline_serial(symbol, duration_seconds, data_length)
        return kline

    def close(self):
        self.api.close()