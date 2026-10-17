"""
行情数据服务 - 基于 TqSdk 的实时行情和历史数据
"""
import pandas as pd
import math
from datetime import datetime
from typing import Optional, List, Dict

# 交易所默认保证金率 (估算杠杆用, 实际以期货公司调整为准)
# 杠杆 = 1 / 保证金率
EXCHANGE_MARGIN_RATE = {
    "CFFEX": 0.12,   # 中金所 (股指/国债)
    "SHFE": 0.10,    # 上海期货交易所
    "DCE": 0.10,     # 大连商品交易所
    "CZCE": 0.10,    # 郑州商品交易所
    "INE": 0.10,     # 上海国际能源交易中心
    "SSE": 0.12,     # 上交所 (股票类, 需企业版)
    "SZSE": 0.12,    # 深交所
}


class MarketDataService:
    def __init__(self, client):
        self.client = client
        # tick 缓存: symbol -> {serial, prev_volume, last_volume, outer, inner, last_price}
        # 用于计算现手 / 外盘 / 内盘
        self.tick_cache = {}

    def get_realtime_quotes(self, symbols: List[str]) -> List[Dict]:
        """获取多个合约的实时行情（调用前需先 wait_update）

        泵保护(2026-09-08): 只读已注册的 quote 缓存, 未注册的合约返回 error 条目。
        绝不在此路径触发阻塞式注册 —— 原实现会对缓存未命中的合约直接调 api.get_quote
        (同步等待首包, 单个最多 30s), 主泵快照 5568 个订阅时被逐个卡死 => 全站停更。"""
        results = []
        for symbol in symbols:
            try:
                quote = self.client.get_quote_cached(symbol)
                if quote is None:
                    results.append({"symbol": symbol, "error": "订阅未就绪"})
                    continue
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
                    "open_interest": self._safe_int(quote.open_interest),
                    "update_time": datetime.now().strftime("%H:%M:%S"),
                }
                # 5 档盘口（期货/期权通常提供 1-5 档）
                for i in range(2, 6):
                    try:
                        data[f"bid_price{i}"] = self._safe_float(getattr(quote, f"bid_price{i}", None))
                        data[f"bid_volume{i}"] = self._safe_int(getattr(quote, f"bid_volume{i}", None))
                        data[f"ask_price{i}"] = self._safe_float(getattr(quote, f"ask_price{i}", None))
                        data[f"ask_volume{i}"] = self._safe_int(getattr(quote, f"ask_volume{i}", None))
                    except Exception:
                        data[f"bid_price{i}"] = 0.0
                        data[f"bid_volume{i}"] = 0
                        data[f"ask_price{i}"] = 0.0
                        data[f"ask_volume{i}"] = 0
                # ---- 由 tick 累积的现手 / 外盘 / 内盘 ----
                tick = self.tick_cache.get(symbol)
                if tick:
                    data["last_volume"] = tick.get("last_volume", 0)    # 现手
                    data["outer_volume"] = tick.get("outer", 0)          # 外盘 (主动买)
                    data["inner_volume"] = tick.get("inner", 0)          # 内盘 (主动卖)
                else:
                    data["last_volume"] = 0
                    data["outer_volume"] = 0
                    data["inner_volume"] = 0
                # ---- 衍生字段 ----
                oi = self._safe_int(getattr(quote, "open_interest", 0))
                pre_oi = self._safe_int(getattr(quote, "pre_open_interest", 0))
                data["day_increase"] = oi - pre_oi                       # 日增 (持仓增减)
                data["average"] = self._safe_float(getattr(quote, "average", 0))            # 当日均价
                data["settlement"] = self._safe_float(getattr(quote, "settlement", 0))      # 结算价
                data["pre_settlement"] = self._safe_float(getattr(quote, "pre_settlement", 0))  # 昨结算
                data["upper_limit"] = self._safe_float(getattr(quote, "upper_limit", 0))    # 涨停
                data["lower_limit"] = self._safe_float(getattr(quote, "lower_limit", 0))    # 跌停
                # ---- 杠杆 (由保证金率估算) ----
                exch = symbol.split(".")[0] if "." in symbol else ""
                margin = EXCHANGE_MARGIN_RATE.get(exch, 0.10)
                data["margin_rate"] = margin
                data["leverage"] = round(1.0 / margin, 1) if margin > 0 else 0
                results.append(data)
            except Exception as e:
                results.append({"symbol": symbol, "error": str(e)})
        return results

    def get_kline_data(self, symbol: str, duration_seconds: int = 86400, 
                       data_length: int = 200) -> pd.DataFrame:
        """获取K线数据并转为DataFrame（读取后台线程已拉取的历史数据）"""
        try:
            import traceback
            # 1. 只读获取已建好的 K 线序列（绝不触发阻塞创建——本方法可能被请求线程调用,
            #    阻塞式 get_kline_serial 会从请求线程等首包、与主线程泵并发跑同一事件循环 => 毁坏 loop）
            kline = self.client.get_kline_cached(symbol, duration_seconds, data_length)
            if kline is None:
                return pd.DataFrame()
            
            # 2. 手动提取 OHLCV + datetime 数据
            closes, opens, highs, lows, volumes, datetimes = [], [], [], [], [], []
            try:
                length = min(len(kline.close), data_length)
                for i in range(length):
                    c = self._safe_float(kline.close[i])
                    o = self._safe_float(kline.open[i])
                    h = self._safe_float(kline.high[i])
                    ll = self._safe_float(kline.low[i])
                    v = self._safe_int(kline.volume[i])
                    # 天勤 Kline 时间戳: datetime 优先, id 次之(毫秒/纳秒视版本)
                    dt = None
                    try:
                        if hasattr(kline, 'datetime'):
                            dt = int(kline.datetime[i])
                        elif hasattr(kline, 'id'):
                            dt = int(kline.id[i])
                    except Exception:
                        dt = None
                    # 保留：① 已收盘的完整 K 线(close>0)；② 有有效时间戳且至少有一个有效价格的行
                    # （避免把整段历史数据因为最后一根未收盘而过滤为空）
                    has_price = c > 0 or o > 0 or h > 0 or ll > 0
                    if c > 0 or (dt is not None and dt > 0 and has_price):
                        closes.append(c if c > 0 else 0)
                        opens.append(o if o > 0 else c)
                        highs.append(h if h > 0 else max(o, c, 0.0001))
                        lows.append(ll if ll > 0 else min(o, c, 0.0001))
                        volumes.append(v)
                        datetimes.append(dt if dt is not None else 0)
            except Exception as e:
                print(f"手动解析K线异常: {e}")

            if not closes:
                # 3. 兜底：尝试 to_dataframe
                try:
                    data = kline.to_dataframe()
                    if not data.empty:
                        rename_map = {}
                        dt_col = None
                        for col in data.columns:
                            if "开" in col: rename_map[col] = "open"
                            elif "收" in col: rename_map[col] = "close"
                            elif "高" in col: rename_map[col] = "high"
                            elif "低" in col: rename_map[col] = "low"
                            elif "量" in col: rename_map[col] = "volume"
                            elif col in ("datetime", "id") or "时间" in col:
                                dt_col = col
                        if rename_map:
                            data = data.rename(columns=rename_map)
                        cols = [c for c in ["close","open","high","low","volume"] if c in data.columns]
                        if dt_col:
                            data["datetime"] = data[dt_col]
                        if cols:
                            return data[cols + (["datetime"] if "datetime" in data.columns else [])].tail(50).reset_index(drop=True)
                except Exception:
                    pass
                print(f"K线 {symbol}: 暂无数据")
                return pd.DataFrame()

            return pd.DataFrame({
                "close": closes, "open": opens, "high": highs,
                "low": lows, "volume": volumes, "datetime": datetimes,
            }).tail(50).reset_index(drop=True)
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

    def subscribe_ticks(self, symbols: List[str]):
        """订阅 tick 序列, 用于计算现手 / 外盘 / 内盘。

        天勤 Quote 对象本身不含这些字段, 但 get_tick_serial 返回的 tick 序列
        含每笔成交量(volume, 当日累计), 据此可算:
          - 现手   = 相邻 tick 的 volume 差值
          - 外盘/内盘 = 用最新价与买卖一档比较估算主动买卖量 (天勤 tick 无买卖方向字段, 故为估算值)
        """
        for sym in symbols:
            if sym in self.tick_cache:
                continue
            try:
                serial = self.client.get_tick_serial(sym, 40)
                self.tick_cache[sym] = {
                    "serial": serial,
                    "prev_volume": 0,
                    "last_volume": 0,
                    "outer": 0,
                    "inner": 0,
                    "last_price": 0.0,
                }
            except Exception as e:
                print(f"[tick] 订阅失败 {sym}: {e}")

    def update_tick_metrics(self):
        """每次 wait_update 后调用, 累积现手 / 外盘 / 内盘 (估算)。"""
        for sym, c in self.tick_cache.items():
            try:
                s = c.get("serial")
                if s is None or len(s) == 0:
                    continue
                last = s.iloc[-1]
                vol = self._safe_int(last["volume"], 0)
                lp = self._safe_float(last["last_price"], 0.0)
                ask1 = self._safe_float(last["ask_price1"], 0.0)
                bid1 = self._safe_float(last["bid_price1"], 0.0)
                if c["prev_volume"] > 0 and vol >= c["prev_volume"]:
                    delta = vol - c["prev_volume"]
                    c["last_volume"] = delta
                    # 估算主动买卖: 最新价 >= 卖一价 视为主动买入(外盘),
                    #             最新价 <= 买一价 视为主动卖出(内盘)
                    if lp >= ask1 and ask1 > 0:
                        c["outer"] += delta
                    elif lp <= bid1 and bid1 > 0:
                        c["inner"] += delta
                c["prev_volume"] = vol
                c["last_price"] = lp
            except Exception:
                pass

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
        """计算涨跌幅。

        期货/期权按【昨结算价 pre_settlement】为基准（国内行业惯例，与文华 WH6/
        博易/快期一致，交易所涨跌停板亦按昨结算）；股票等无昨结算时回退昨收。
        """
        try:
            base = MarketDataService._safe_float(getattr(quote, "pre_settlement", 0))
            if base <= 0:
                base = MarketDataService._safe_float(getattr(quote, "pre_close", 0))
            if base > 0:
                return round((float(quote.last_price) - base) / base * 100, 2)
        except Exception:
            pass
        return 0.0