"""
行情数据服务 - 基于 TqSdk 的实时行情和历史数据
"""
import pandas as pd
import math
from datetime import datetime
from typing import Optional, List, Dict


class MarketDataService:
    def __init__(self, client):
        self.client = client

    def get_realtime_quotes(self, symbols: List[str]) -> List[Dict]:
        """获取多个合约的实时行情（调用前需先 wait_update）"""
        results = []
        for symbol in symbols:
            try:
                quote = self.client.get_quote(symbol)
                data = {
                    "symbol": symbol,
                    "name": quote.instrument_name if hasattr(quote, 'instrument_name') else symbol,
                    "last_price": self._safe_float(quote.last_price),
                    "open": self._safe_float(quote.open),
                    "high": self._safe_float(quote.highest),
                    "low": self._safe_float(quote.lowest),
                    "pre_close": self._safe_float(quote.pre_close),
                    "volume": self._safe_int(quote.volume),
                    "amount": self._safe_float(quote.amount),
                    "pct_change": self._calc_pct_change(quote),
                    "bid_price1": self._safe_float(quote.bid_price1),
                    "bid_volume1": self._safe_int(quote.bid_volume1),
                    "ask_price1": self._safe_float(quote.ask_price1),
                    "ask_volume1": self._safe_int(quote.ask_volume1),
                    "update_time": datetime.now().strftime("%H:%M:%S"),
                }
                results.append(data)
            except Exception as e:
                results.append({"symbol": symbol, "error": str(e)})
        return results

    def get_kline_data(self, symbol: str, duration_seconds: int = 86400, 
                       data_length: int = 200) -> pd.DataFrame:
        """获取K线数据并转为DataFrame（读取后台线程已拉取的历史数据）"""
        try:
            import traceback
            # 1. 获取K线序列（不调用 wait_update，直接读已推送过来的数据）
            kline = self.client.get_kline(symbol, duration_seconds, data_length)
            
            # 2. 手动提取 OHLCV 数据
            closes, opens, highs, lows, volumes = [], [], [], [], []
            try:
                length = min(len(kline.close), data_length)
                for i in range(length):
                    c = self._safe_float(kline.close[i])
                    o = self._safe_float(kline.open[i])
                    h = self._safe_float(kline.high[i])
                    ll = self._safe_float(kline.low[i])
                    v = self._safe_int(kline.volume[i])
                    if c > 0:
                        closes.append(c)
                        opens.append(o if o > 0 else c)
                        highs.append(h if h > 0 else max(o, c))
                        lows.append(ll if ll > 0 else min(o, c))
                        volumes.append(v)
            except Exception as e:
                print(f"手动解析K线异常: {e}")
            
            if not closes:
                # 3. 兜底：尝试 to_dataframe
                try:
                    data = kline.to_dataframe()
                    if not data.empty:
                        rename_map = {}
                        for col in data.columns:
                            if "开" in col: rename_map[col] = "open"
                            elif "收" in col: rename_map[col] = "close"
                            elif "高" in col: rename_map[col] = "high"
                            elif "低" in col: rename_map[col] = "low"
                            elif "量" in col: rename_map[col] = "volume"
                        if rename_map:
                            data = data.rename(columns=rename_map)
                        cols = [c for c in ["close","open","high","low","volume"] if c in data.columns]
                        if cols:
                            return data[cols].tail(50)
                except Exception:
                    pass
                print(f"K线 {symbol}: 暂无数据")
                return pd.DataFrame()
            
            return pd.DataFrame({
                "close": closes, "open": opens, "high": highs,
                "low": lows, "volume": volumes,
            }).tail(50)
        except Exception as e:
            print(f"获取K线数据失败: {e}")
            return pd.DataFrame()

    def preload_kline(self, symbols: List[str], duration_seconds: int = 86400, data_length: int = 200):
        """后台线程预加载K线数据到内存（非阻塞调用）"""
        for symbol in symbols:
            try:
                self.client.get_kline(symbol, duration_seconds, data_length)
            except Exception as e:
                print(f"预加载K线失败 {symbol}: {e}")

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            v = float(value) if value is not None else default
            if math.isnan(v) or math.isinf(v):
                return default
            return v
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_int(value, default=0):
        try:
            v = int(value) if value is not None else default
            return v
        except (ValueError, TypeError, OverflowError):
            return default

    @staticmethod
    def _calc_pct_change(quote):
        """计算涨跌幅"""
        try:
            if float(quote.pre_close) > 0:
                return round((float(quote.last_price) - float(quote.pre_close)) 
                           / float(quote.pre_close) * 100, 2)
        except Exception:
            pass
        return 0.0