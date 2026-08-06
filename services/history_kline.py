"""
历史K线服务 - 基于 akshare (东方财富免费数据)
盘后 / 周末 / 任意时间都能拉
"""
import akshare as ak
import pandas as pd
import math
from datetime import datetime


class HistoryKlineService:
    """东方财富历史日线数据"""

    @classmethod
    def get_daily_kline(cls, symbol: str, days: int = 200) -> pd.DataFrame:
        """
        获取期货品种历史日线
        symbol: 天勤格式如 SHFE.cu2501, DCE.i2501
        """
        try:
            parts = symbol.split(".")
            if len(parts) != 2:
                return pd.DataFrame()

            exchange, contract = parts
            # 提取品种代码（去掉数字），如 cu2501 -> cu
            variety = "".join(c for c in contract if not c.isdigit())

            # 用 akshare 的期货历史日线接口
            df = ak.futures_main_sina(symbol=variety)

            if df is None or df.empty:
                return pd.DataFrame()

            # 探测列名并重命名
            rename_map = {}
            for col in df.columns:
                col_str = str(col)
                if "日期" in col_str or "date" in col_str.lower():
                    rename_map[col] = "date"
                elif "开盘" in col_str or "open" in col_str.lower():
                    rename_map[col] = "open"
                elif "收盘" in col_str or "close" in col_str.lower():
                    rename_map[col] = "close"
                elif "最高" in col_str or "high" in col_str.lower():
                    rename_map[col] = "high"
                elif "最低" in col_str or "low" in col_str.lower():
                    rename_map[col] = "low"
                elif "成交量" in col_str or "volume" in col_str.lower():
                    rename_map[col] = "volume"

            if rename_map:
                df = df.rename(columns=rename_map)

            # 确保有需要的列
            needed = ["date", "open", "close", "high", "low", "volume"]
            available = [c for c in needed if c in df.columns]
            if not available:
                return pd.DataFrame()

            result = df[available].tail(days).copy()

            # 数值列清理 NaN/Inf
            for c in ["open", "close", "high", "low", "volume"]:
                if c in result.columns:
                    result[c] = pd.to_numeric(
                        result[c], errors="coerce"
                    ).fillna(0.0)

            return result

        except Exception as e:
            print(f"东方财富历史K线获取失败 {symbol}: {e}")
            return pd.DataFrame()