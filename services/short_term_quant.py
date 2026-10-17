# -*- coding: utf-8 -*-
"""
短线量化模块

实现两张表:
1. 短线行情识别量化条件（大周期/小周期/MACD/量价/支撑阻力/ATR）
2. 短线入场点量化选择（突破/回调/动量/开盘区间）

数据:
- 日线（86400）判断大周期方向 MA20
- 30 分钟（1800）判断小周期趋势、MACD 金叉
- 5 分钟（300）判断量价、支撑阻力、ATR、动量、开盘区间
"""
import time
import html
import math
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Any

import pandas as pd
import numpy as np
from flask import request, jsonify
from services import futures_db

try:
    from services.opt_special_pages import _OPT_SPECIAL_STYLE, _MINI_SCRIPT
except Exception:
    _OPT_SPECIAL_STYLE = ''
    _MINI_SCRIPT = ''

_SLOTS = None
_DEFAULT_KEY = None
_GET_SLOT = None  # 请求感知的账号解析器（来自 main.py 的 _resolve_slot）


def _get_slot():
    """解析当前请求应选用的账户 Slot（优先网关注入的选中账号，回落默认）。"""
    if _GET_SLOT is not None:
        try:
            s = _GET_SLOT()
            if s is not None:
                return s
        except Exception:
            pass
    if _SLOTS and _DEFAULT_KEY:
        return _SLOTS.get(_DEFAULT_KEY)
    return None


# ==================== 工具函数 ====================

def _safe_float(v, default=None):
    try:
        if v is None:
            return default
        v = float(v)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


def _safe_int(v, default=0):
    try:
        if v is None:
            return default
        v = int(float(v))
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


# 单笔可承受亏损占权益比例（风险预算），用于反推仓位
RISK_PER_TRADE = 0.01


def _risk_position_pct(entry: float, stop: float) -> int:
    """按止损距离与风险预算反推仓位百分比（取整到 5%，区间 5%~100%）。"""
    entry = _safe_float(entry)
    stop = _safe_float(stop)
    if not entry or entry <= 0 or not stop:
        return 50
    stop_pct = abs(entry - stop) / entry
    if stop_pct <= 0:
        return 100
    pct = RISK_PER_TRADE / stop_pct
    return max(5, min(100, int(round(pct * 100 / 5) * 5)))


def _fix_stop(direction: str, entry: float, raw_stop: float) -> float:
    """确保止损在入场价正确一侧：多头止损在 entry 下方，空头在上方。
    突破信号的 '突破位±1ATR' 在 entry 略低于/高于突破位时可能落到错误一侧，这里兜底修正。"""
    entry = _safe_float(entry)
    raw = _safe_float(raw_stop)
    pad = (abs(entry) * 0.002) if entry else 1.0
    if direction == "bull":
        if raw is None or raw >= entry:
            return entry - pad
        return raw
    else:
        if raw is None or raw <= entry:
            return entry + pad
        return raw


def _current_price_from_quote(quote):
    """从天勤 quote 对象取最新价，依次尝试 last_price、close、pre_close。"""
    if quote is None:
        return None
    for attr in ("last_price", "close"):
        try:
            v = getattr(quote, attr, None)
            v = _safe_float(v)
            if v:
                return v
        except Exception:
            pass
    try:
        v = getattr(quote, "pre_close", None)
        return _safe_float(v)
    except Exception:
        return None


# ==================== 技术指标 ====================

def _ema(s: pd.Series, n: int) -> pd.Series:
    """指数移动平均，兼容长度不足 n。"""
    return s.ewm(span=n, adjust=False, min_periods=1).mean()


def _macd(s: pd.Series, fast=12, slow=26, signal=9):
    """返回 DIF, DEA, MACD_HIST。"""
    dif = _ema(s, fast) - _ema(s, slow)
    dea = _ema(dif, signal)
    hist = (dif - dea) * 2.0
    return dif, dea, hist


def _atr(df: pd.DataFrame, n=14) -> pd.Series:
    """平均真实波幅。"""
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=n, min_periods=1).mean()


def _ma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(window=n, min_periods=1).mean()


# ==================== 行情识别条件 ====================

def _condition_major_direction(df_daily: pd.DataFrame):
    """
    大周期方向：日线 MA20 向上/向下。
    需要至少 21 根日线。
    """
    if df_daily is None or df_daily.empty or len(df_daily) < 21:
        return "neutral", "日线数据不足（需>=21根）"
    ma20 = _ma(df_daily["close"], 20)
    if len(ma20) < 3:
        return "neutral", "MA20 尚未稳定"
    prev, cur = ma20.iloc[-2], ma20.iloc[-1]
    if cur > prev:
        return "bull", f"日线MA20向上 ({cur:.2f} > {prev:.2f})"
    if cur < prev:
        return "bear", f"日线MA20向下 ({cur:.2f} < {prev:.2f})"
    return "neutral", "日线MA20走平"


def _condition_minor_trend(df_30m: pd.DataFrame):
    """
    小周期趋势：30 分钟 MA5>MA10>MA20 多头 / MA5<MA10<MA20 空头。
    """
    if df_30m is None or df_30m.empty or len(df_30m) < 20:
        return "neutral", "30分钟数据不足（需>=20根）"
    close = df_30m["close"]
    ma5 = _ma(close, 5).iloc[-1]
    ma10 = _ma(close, 10).iloc[-1]
    ma20 = _ma(close, 20).iloc[-1]
    if ma5 > ma10 > ma20:
        return "bull", f"MA5({ma5:.2f})>MA10({ma10:.2f})>MA20({ma20:.2f})"
    if ma5 < ma10 < ma20:
        return "bear", f"MA5({ma5:.2f})<MA10({ma10:.2f})<MA20({ma20:.2f})"
    return "neutral", f"均线排列杂乱（MA5={ma5:.2f}, MA10={ma10:.2f}, MA20={ma20:.2f})"


def _condition_macd_cross(df_30m: pd.DataFrame):
    """
    30 分钟 MACD 金叉/死叉。
    金叉：DIF 上穿 DEA；死叉：DIF 下穿 DEA。
    """
    if df_30m is None or df_30m.empty or len(df_30m) < 35:
        return "neutral", "30分钟数据不足（MACD需>=35根）"
    dif, dea, _ = _macd(df_30m["close"])
    if len(dif) < 3:
        return "neutral", "MACD 尚未稳定"
    prev_dif, cur_dif = dif.iloc[-2], dif.iloc[-1]
    prev_dea, cur_dea = dea.iloc[-2], dea.iloc[-1]
    if prev_dif <= prev_dea and cur_dif > cur_dea:
        return "bull", f"DIF({cur_dif:.3f})上穿DEA({cur_dea:.3f})"
    if prev_dif >= prev_dea and cur_dif < cur_dea:
        return "bear", f"DIF({cur_dif:.3f})下穿DEA({cur_dea:.3f})"
    return "neutral", f"DIF({cur_dif:.3f})与DEA({cur_dea:.3f})未交叉"


def _condition_volume_price(df_30m: pd.DataFrame):
    """
    量价配合：
    多头：上涨趋势中，上涨 K 线平均成交量 > 回调 K 线平均成交量 * 1.2
    空头：下跌趋势中，下跌 K 线平均成交量 > 反弹 K 线平均成交量 * 1.2
    取最近 12 根 30 分钟 K 线。
    """
    if df_30m is None or df_30m.empty or len(df_30m) < 12:
        return "neutral", "30分钟数据不足（需>=12根）"
    recent = df_30m.tail(12).copy()
    recent["delta"] = recent["close"].diff()
    up_vol = recent.loc[recent["delta"] > 0, "volume"].mean()
    down_vol = recent.loc[recent["delta"] < 0, "volume"].mean()
    up_vol = _safe_float(up_vol, 0) or 0
    down_vol = _safe_float(down_vol, 0) or 0
    price_trend = recent["close"].iloc[-1] - recent["close"].iloc[0]

    if price_trend > 0:
        if up_vol > down_vol * 1.2 and up_vol > 0:
            return "bull", f"上涨放量({up_vol:.0f})>回调缩量({down_vol:.0f})"
        return "neutral", f"上涨趋势但量能未配合（涨量={up_vol:.0f}, 跌量={down_vol:.0f})"
    else:
        if down_vol > up_vol * 1.2 and down_vol > 0:
            return "bear", f"下跌放量({down_vol:.0f})>反弹缩量({up_vol:.0f})"
        return "neutral", f"下跌趋势但量能未配合（跌量={down_vol:.0f}, 涨量={up_vol:.0f})"


def _condition_support_resistance(df_5m: pd.DataFrame, current_price: float, direction_hint: str):
    """
    5 分钟支撑/阻力：价格回踩关键支撑位（多头）/反弹关键阻力位（空头）。
    用最近 20 根 5 分钟 K 线的高低点形成区间。
    """
    if df_5m is None or df_5m.empty or len(df_5m) < 20 or current_price is None:
        return "neutral", "5分钟数据不足或无最新价"
    recent = df_5m.tail(20)
    high = recent["high"].max()
    low = recent["low"].min()
    rng = high - low
    if rng <= 0:
        return "neutral", "5分钟高低点区间无效"
    if direction_hint == "bull":
        # 回踩支撑位：价格接近近期低点
        dist = (current_price - low) / rng
        if dist < 0.15:
            return "bull", f"价格{current_price:.2f}接近支撑位{low:.2f}（区间{dist:.1%}）"
        return "neutral", f"未明显回踩支撑（距支撑位{(current_price-low)/rng:.1%}）"
    elif direction_hint == "bear":
        dist = (high - current_price) / rng
        if dist < 0.15:
            return "bear", f"价格{current_price:.2f}接近阻力位{high:.2f}（区间{dist:.1%}）"
        return "neutral", f"未明显反弹阻力（距阻力位{(high-current_price)/rng:.1%}）"
    return "neutral", "方向不明，不判断支撑阻力"


def _condition_atr_filter(df_5m: pd.DataFrame, df_daily: pd.DataFrame):
    """
    ATR 过滤：5 分钟 ATR(14) > 日线 ATR(14) * 0.3。
    """
    if df_5m is None or df_daily is None or len(df_5m) < 14 or len(df_daily) < 14:
        return "neutral", "数据不足（需>=14根）"
    atr5 = _atr(df_5m, 14).iloc[-1]
    atr_d = _atr(df_daily, 14).iloc[-1]
    if atr_d <= 0:
        return "neutral", "日线ATR无效"
    ratio = atr5 / atr_d
    # ATR 只表征波动强度，不指示方向；改为 neutral，避免系统性给多头灌票
    if ratio > 0.3:
        return "neutral", f"波动充足（5分ATR/日线ATR={ratio:.2f}>0.30），可参与"
    return "neutral", f"5分钟ATR/日线ATR={ratio:.2f}（<=0.30，波动不足，观望）"


# ==================== 入场信号 ====================

def _signal_breakout(symbol: str, df_30m: pd.DataFrame, current_price: float, direction: str, atr5: float):
    """
    突破入场：30 分钟级别突破前高/前低，回踩确认。
    仓位 100%，止损 突破位 ± 1 ATR。
    """
    if df_30m is None or len(df_30m) < 5 or current_price is None:
        return None
    recent = df_30m.tail(20)
    prev_high = recent["high"].max()
    prev_low = recent["low"].min()
    atr_val = atr5 if atr5 and atr5 > 0 else (prev_high - prev_low) * 0.02

    sig = None
    if direction == "bull" and current_price > prev_high * 0.998:
        # 回踩确认：突破后回撤不超过 0.5%
        stop = _fix_stop("bull", current_price, prev_high - atr_val)
        sig = {
            "type": "突破入场",
            "direction": "bull",
            "triggered": True,
            "position_ratio": f"{_risk_position_pct(current_price, stop)}%",
            "entry_price": current_price,
            "stop_loss": round(stop, 2),
            "stop_desc": f"突破位{prev_high:.2f}下方1个ATR({atr_val:.2f})",
            "reason": f"30分钟突破前高{prev_high:.2f}，回踩确认",
        }
    elif direction == "bear" and current_price < prev_low * 1.002:
        stop = _fix_stop("bear", current_price, prev_low + atr_val)
        sig = {
            "type": "突破入场",
            "direction": "bear",
            "triggered": True,
            "position_ratio": f"{_risk_position_pct(current_price, stop)}%",
            "entry_price": current_price,
            "stop_loss": round(stop, 2),
            "stop_desc": f"突破位{prev_low:.2f}上方1个ATR({atr_val:.2f})",
            "reason": f"30分钟突破前低{prev_low:.2f}，回踩确认",
        }
    return sig


def _signal_pullback(symbol: str, df_30m: pd.DataFrame, current_price: float, direction: str):
    """
    回调入场：趋势中回调至 MA10 或 38.2% 回撤，止跌信号。
    仓位 70%，止损 回调低点/高点。
    """
    if df_30m is None or len(df_30m) < 12 or current_price is None:
        return None
    recent = df_30m.tail(20)
    close = recent["close"]
    ma10 = _ma(close, 10).iloc[-1]
    high = recent["high"].max()
    low = recent["low"].min()
    rng = high - low

    # 38.2% 回撤位
    fib382 = high - rng * 0.382 if direction == "bull" else low + rng * 0.382

    sig = None
    if direction == "bull":
        # 价格接近 MA10 或 38.2% 位；止跌：当前收阳或下影线
        near_ma10 = abs(current_price - ma10) / current_price < 0.005
        near_fib = abs(current_price - fib382) / current_price < 0.005
        last_k = recent.iloc[-1]
        bullish_stop = (last_k["close"] > last_k["open"]) or (last_k["low"] < ma10 and last_k["close"] > last_k["open"])
        if (near_ma10 or near_fib) and bullish_stop:
            stop = _fix_stop("bull", current_price, recent["low"].min())
            sig = {
                "type": "回调入场",
                "direction": "bull",
                "triggered": True,
                "position_ratio": f"{_risk_position_pct(current_price, stop)}%",
                "entry_price": current_price,
                "stop_loss": round(stop, 2),
                "stop_desc": f"回调低点{stop:.2f}",
                "reason": f"多头趋势回调至MA10({ma10:.2f})/38.2%({fib382:.2f})并现止跌阳线",
            }
    else:
        near_ma10 = abs(current_price - ma10) / current_price < 0.005
        near_fib = abs(current_price - fib382) / current_price < 0.005
        last_k = recent.iloc[-1]
        bearish_stop = (last_k["close"] < last_k["open"]) or (last_k["high"] > ma10 and last_k["close"] < last_k["open"])
        if (near_ma10 or near_fib) and bearish_stop:
            stop = _fix_stop("bear", current_price, recent["high"].max())
            sig = {
                "type": "回调入场",
                "direction": "bear",
                "triggered": True,
                "position_ratio": f"{_risk_position_pct(current_price, stop)}%",
                "entry_price": current_price,
                "stop_loss": round(stop, 2),
                "stop_desc": f"回调高点{stop:.2f}",
                "reason": f"空头趋势回调至MA10({ma10:.2f})/38.2%({fib382:.2f})并现止跌阴线",
            }
    return sig


def _signal_momentum(symbol: str, df_5m: pd.DataFrame, current_price: float, direction: str):
    """
    动量入场：5 分钟 MACD 金叉/死叉 + 放量。
    仓位 50%，止损 前低/前高。
    """
    if df_5m is None or len(df_5m) < 20 or current_price is None:
        return None
    close = df_5m["close"]
    volume = df_5m["volume"]
    dif, dea, hist = _macd(close)
    if len(dif) < 3:
        return None
    prev_dif, cur_dif = dif.iloc[-2], dif.iloc[-1]
    prev_dea, cur_dea = dea.iloc[-2], dea.iloc[-1]

    # 放量：最近一根成交量 > 前 5 根均量 * 1.2
    vol_ma5 = volume.rolling(5, min_periods=1).mean().iloc[-2]
    last_vol = volume.iloc[-1]
    expanded = last_vol > vol_ma5 * 1.2 if vol_ma5 and vol_ma5 > 0 else False

    sig = None
    if direction == "bull" and prev_dif <= prev_dea and cur_dif > cur_dea and expanded:
        stop = _fix_stop("bull", current_price, df_5m.tail(10)["low"].min())
        sig = {
            "type": "动量入场",
            "direction": "bull",
            "triggered": True,
            "position_ratio": f"{_risk_position_pct(current_price, stop)}%",
            "entry_price": current_price,
            "stop_loss": round(stop, 2),
            "stop_desc": f"前低{stop:.2f}",
            "reason": "5分钟MACD金叉+放量",
        }
    elif direction == "bear" and prev_dif >= prev_dea and cur_dif < cur_dea and expanded:
        stop = _fix_stop("bear", current_price, df_5m.tail(10)["high"].max())
        sig = {
            "type": "动量入场",
            "direction": "bear",
            "triggered": True,
            "position_ratio": f"{_risk_position_pct(current_price, stop)}%",
            "entry_price": current_price,
            "stop_loss": round(stop, 2),
            "stop_desc": f"前高{stop:.2f}",
            "reason": "5分钟MACD死叉+放量",
        }
    return sig


def _session_start_pos(df_5m: pd.DataFrame) -> int:
    """
    定位当前交易时段起点：5 分钟 K 线正常间隔 300s，
    找最后一个大间隔（>1800s，隔夜/午休/周末）之后的第一根作为时段起点。
    """
    if df_5m is None or len(df_5m) < 8 or not isinstance(df_5m.index, pd.DatetimeIndex):
        return 0
    diffs = df_5m.index.to_series().diff().dt.total_seconds().values
    big = [i for i, d in enumerate(diffs) if d is not None and d > 1800]
    if not big:
        return 0
    return big[-1] + 1  # diff[i] 属于 i-1 与 i 之间，故起点为 i


def _signal_opening_range(symbol: str, df_5m: pd.DataFrame, current_price: float, direction: str):
    """
    开盘区间突破：开盘后 30 分钟（6 根 5 分钟 K 线）形成的高低点区间，随后突破。
    仓位 60%，止损 区间另一侧。
    """
    if df_5m is None or len(df_5m) < 8 or current_price is None:
        return None
    # 取当前交易时段开盘后前 6 根 K 线（30 分钟），锚定时段起点而非窗口头部
    start = _session_start_pos(df_5m)
    opening = df_5m.iloc[start:start + 6]
    if len(opening) < 2:
        return None
    range_high = opening["high"].max()
    range_low = opening["low"].min()
    if range_high <= range_low:
        return None

    sig = None
    if direction == "bull" and current_price > range_high:
        stop = _fix_stop("bull", current_price, range_low)
        sig = {
            "type": "开盘区间突破",
            "direction": "bull",
            "triggered": True,
            "position_ratio": f"{_risk_position_pct(current_price, stop)}%",
            "entry_price": current_price,
            "stop_loss": round(stop, 2),
            "stop_desc": f"开盘区间低点{range_low:.2f}",
            "reason": f"开盘30分钟后突破区间高{range_high:.2f}",
        }
    elif direction == "bear" and current_price < range_low:
        stop = _fix_stop("bear", current_price, range_high)
        sig = {
            "type": "开盘区间突破",
            "direction": "bear",
            "triggered": True,
            "position_ratio": f"{_risk_position_pct(current_price, stop)}%",
            "entry_price": current_price,
            "stop_loss": round(stop, 2),
            "stop_desc": f"开盘区间高点{range_high:.2f}",
            "reason": f"开盘30分钟后突破区间低{range_low:.2f}",
        }
    return sig


# ==================== 主评估入口 ====================

def evaluate_short_term(symbol: str, slot, max_wait: float = 8.0):
    """
    对单个合约执行短线行情识别与入场信号生成。
    返回 dict，包含条件识别、综合方向、入场信号。
    """
    start = time.time()
    result = {
        "ok": True,
        "symbol": symbol,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "current_price": None,
        "overall": "neutral",
        "overall_label": "无明确方向",
        "score": {"bull": 0, "bear": 0, "neutral": 0},
        "conditions": [],
        "signals": [],
        "elapsed_ms": 0,
        "note": "",
    }

    if slot is None or slot.status != "online" or slot.client is None:
        result["ok"] = False
        result["error"] = "默认账户行情未就绪"
        return result

    try:
        # 订阅 + 最新价
        try:
            slot.ensure_subscribed([symbol])
        except Exception:
            pass
        quote = slot.client.get_quote(symbol)
        current_price = _current_price_from_quote(quote)
        result["current_price"] = current_price

        # 并行获取三个周期 K 线会冲突 wait_update，这里串行获取
        # 每个周期单独 get_kline 内部只等待对象就绪（轮询线程驱动），不主动 wait_update
        df_daily = slot.get_kline(symbol, 86400, 30)
        df_30m = slot.get_kline(symbol, 1800, 40)
        df_5m = slot.get_kline(symbol, 300, 30)

        # 如果日线数据不足，尝试用主力连续合约
        if (df_daily is None or df_daily.empty or len(df_daily) < 21) and symbol.startswith("KQ.m@"):
            # 已经是连续合约，无能为力
            pass

        # 1) 行情识别
        major_dir, major_detail = _condition_major_direction(df_daily)
        minor_dir, minor_detail = _condition_minor_trend(df_30m)
        macd_dir, macd_detail = _condition_macd_cross(df_30m)
        vol_dir, vol_detail = _condition_volume_price(df_30m)
        sr_dir, sr_detail = _condition_support_resistance(df_5m, current_price, "bull")
        # 如果大周期偏空，重新用空头视角判断支撑阻力
        if major_dir == "bear" or minor_dir == "bear":
            sr_dir, sr_detail = _condition_support_resistance(df_5m, current_price, "bear")
        atr_dir, atr_detail = _condition_atr_filter(df_5m, df_daily)

        conds = [
            {"name": "大周期方向", "timeframe": "日线", "direction": major_dir, "detail": major_detail},
            {"name": "小周期趋势", "timeframe": "30分钟", "direction": minor_dir, "detail": minor_detail},
            {"name": "MACD金叉", "timeframe": "30分钟", "direction": macd_dir, "detail": macd_detail},
            {"name": "量价配合", "timeframe": "5-30分钟", "direction": vol_dir, "detail": vol_detail},
            {"name": "支撑/阻力", "timeframe": "5-15分钟", "direction": sr_dir, "detail": sr_detail},
            {"name": "ATR过滤", "timeframe": "5分钟", "direction": atr_dir, "detail": atr_detail},
        ]
        result["conditions"] = conds

        # 综合方向：按条件投票
        for c in conds:
            result["score"][c["direction"]] += 1

        bullish = result["score"]["bull"]
        bearish = result["score"]["bear"]
        if bullish >= 4 and bearish <= 1:
            result["overall"] = "bull"
            result["overall_label"] = "偏多（条件共振）"
        elif bearish >= 4 and bullish <= 1:
            result["overall"] = "bear"
            result["overall_label"] = "偏空（条件共振）"
        elif bullish > bearish:
            result["overall"] = "bull"
            result["overall_label"] = "略偏多"
        elif bearish > bullish:
            result["overall"] = "bear"
            result["overall_label"] = "略偏空"
        else:
            result["overall"] = "neutral"
            result["overall_label"] = "方向不明"

        # 2) 入场信号生成（仅在非 neutral 时尝试）
        if result["overall"] != "neutral" and current_price is not None:
            atr5 = _safe_float(_atr(df_5m, 14).iloc[-1]) if df_5m is not None and len(df_5m) >= 14 else None
            direction = result["overall"]
            sigs = [
                _signal_breakout(symbol, df_30m, current_price, direction, atr5),
                _signal_pullback(symbol, df_30m, current_price, direction),
                _signal_momentum(symbol, df_5m, current_price, direction),
                _signal_opening_range(symbol, df_5m, current_price, direction),
            ]
            result["signals"] = [s for s in sigs if s is not None]

        result["elapsed_ms"] = round((time.time() - start) * 1000, 1)
    except Exception as e:
        result["ok"] = False
        result["error"] = f"评估异常: {e}"
        result["trace"] = traceback.format_exc()
        result["elapsed_ms"] = round((time.time() - start) * 1000, 1)

    return result


# ==================== HTML / CSS ====================
# 复用期权模块深色主题（_OPT_SPECIAL_STYLE 提供 :root 变量与 .opt-page/.opt-table/.btn/.opt-tabbar 等），
# 这里只补充短线量化页面特有的类：卡片摘要、徽章、列表网格、底部回到列表导航。

_SQ_STYLE = '''
<style>
.card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px}
.card h2{margin:0 0 12px;font-size:15px;font-weight:700;color:var(--accent)}
.summary{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:12px}
.summary .item{flex:1;min-width:130px;background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:12px}
.summary .label{font-size:12px;color:var(--muted);margin-bottom:4px}
.summary .value{font-size:18px;font-weight:700}
.bull{color:var(--up)} .bear{color:var(--down)} .neutral{color:var(--muted)}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:12px;font-weight:600;background:var(--surface2);color:var(--muted)}
.badge.bull{background:rgba(255,77,79,.15);color:var(--up)}
.badge.bear{background:rgba(25,195,125,.15);color:var(--down)}
.note{font-size:12px;color:var(--muted);margin-top:6px}
.empty{padding:30px;text-align:center;color:var(--muted);background:var(--surface);border:1px dashed var(--border);border-radius:8px}
.signal-yes{color:var(--accent);font-weight:700}
.signal-no{color:var(--muted)}
/* 期货列表 */
.sq-search{display:flex;gap:10px;margin-bottom:14px}
.sq-search input{flex:1;min-width:200px;padding:8px 12px;background:#0d1424;border:1px solid var(--border);color:var(--text);border-radius:6px;font-size:14px}
.sq-ex-tabs{display:flex;gap:6px;margin-bottom:14px;flex-wrap:wrap}
.sq-ex-tabs button{padding:5px 12px;background:var(--surface);border:1px solid var(--border);color:var(--muted);border-radius:14px;cursor:pointer;font-size:13px}
.sq-ex-tabs button.active{background:var(--accent);color:#05101c;border-color:var(--accent);font-weight:700}
.sq-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px}
.sq-card{display:block;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px;color:var(--text);text-decoration:none;transition:.15s}
.sq-card:hover{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent)}
.sq-card .nm{font-size:15px;font-weight:700}
.sq-card .code{font-size:12px;color:var(--muted);margin-top:4px;word-break:break-all}
.sq-group{margin-bottom:18px}
.sq-group h3{color:var(--accent);font-size:15px;margin:0 0 8px;font-weight:700}
.back-link{display:inline-block;margin-bottom:12px;color:var(--accent);text-decoration:none;font-size:13px}
.back-link:hover{text-decoration:underline}
</style>
'''

_SQ_LIST_SCRIPT = '''
<script>
function sqTextFilter(){
  var q=document.getElementById('sqSearch').value.trim().toLowerCase();
  var groups=document.querySelectorAll('.sq-group');
  for(var i=0;i<groups.length;i++){
    var g=groups[i]; var cards=g.querySelectorAll('.sq-card'); var any=false;
    for(var j=0;j<cards.length;j++){
      var c=cards[j]; var t=c.textContent.toLowerCase();
      var show=(!q||t.indexOf(q)>=0);
      c.style.display=show?'':'none'; if(show) any=true;
    }
    g.style.display=any?'':'none';
  }
}
function sqEx(ex){
  var groups=document.querySelectorAll('.sq-group');
  for(var i=0;i<groups.length;i++){
    groups[i].style.display=(!ex||groups[i].getAttribute('data-group')===ex)?'':'none';
  }
  var tabs=document.querySelectorAll('#sqExTabs button');
  for(var k=0;k<tabs.length;k++){ tabs[k].className=(tabs[k].getAttribute('data-ex')===ex)?'active':''; }
  if(ex){ document.getElementById('sqSearch').value=''; sqTextFilter(); }
}
</script>
'''


def _fmt(v, nd=2):
    if v is None:
        return "—"
    try:
        return f"{v:.{nd}f}"
    except Exception:
        return str(v)


# ==================== 期货列表 ====================

_CFFEX_FUT = {
    "IC": "中证500", "IF": "沪深300", "IH": "上证50", "IM": "中证1000",
    "T": "10年国债", "TF": "5年国债", "TL": "30年国债", "TS": "2年国债",
}


def _clean_prod_name(exchange, pc, raw):
    """品种中文名：优先内置 PROD_NAME，其次中金所期货映射，最后去掉 product_name 尾随月份数字。"""
    key = f"{exchange}.{pc}"
    if key in futures_db.PROD_NAME:
        return futures_db.PROD_NAME[key]
    if exchange == "CFFEX" and pc in _CFFEX_FUT:
        return _CFFEX_FUT[pc]
    import re
    cleaned = re.sub(r"\d+$", "", raw or "")
    return cleaned or pc


def list_futures_products():
    """从 t_futures_expiry 取 distinct 品种，按交易所分组，生成连续主力合约符号 KQ.m@<exchange>.<product_code>。"""
    try:
        conn = futures_db.get_connection()
    except Exception as e:
        print(f"[短线量化-列表] 连接失败: {e}")
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
        g = grouped.setdefault(exchange, {"name": ex_name or exchange, "items": {}})
        if pc not in g["items"]:
            g["items"][pc] = _clean_prod_name(exchange, pc, pname)

    out = []
    for ex in order:
        if ex in grouped:
            items = sorted(grouped[ex]["items"].items(), key=lambda kv: kv[1])
            out.append((ex, grouped[ex]["name"], [
                {"code": pc, "name": nm, "symbol": f"KQ.m@{ex}.{pc}"} for pc, nm in items
            ]))
    for ex in grouped:
        if ex not in order:
            items = sorted(grouped[ex]["items"].items(), key=lambda kv: kv[1])
            out.append((ex, grouped[ex]["name"], [
                {"code": pc, "name": nm, "symbol": f"KQ.m@{ex}.{pc}"} for pc, nm in items
            ]))
    return out


def shortquant_tabbar(active):
    """底部导航：期货列表（点击回到列表页）。"""
    cls = "active" if active == "list" else ""
    return f'<div class="opt-tabbar"><a class="{cls}" href="?">期货列表</a></div>'


def _build_list_html():
    """期货品种列表页：按交易所分组 + 搜索 + 点击进入短线量化详情。"""
    groups = list_futures_products()
    ex_tabs = '<button data-ex="" onclick="sqEx(\'\')" class="active">全部</button>'
    for ex, name, _ in groups:
        ex_tabs += f'<button data-ex="{html.escape(ex)}" onclick="sqEx(\'{ex}\')">{html.escape(name)}</button>'

    cards_html = ""
    for ex, name, items in groups:
        card_rows = ""
        for it in items:
            card_rows += (
                f'<a class="sq-card" href="?symbol={html.escape(it["symbol"])}">'
                f'<div class="nm">{html.escape(it["name"])}</div>'
                f'<div class="code">{html.escape(it["symbol"])}</div></a>'
            )
        cards_html += f'<div class="sq-group" data-group="{html.escape(ex)}"><h3>{html.escape(name)}</h3><div class="sq-grid">{card_rows}</div></div>'

    doc = _OPT_SPECIAL_STYLE + _SQ_STYLE + _MINI_SCRIPT
    doc += '<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><title>短线量化 - 期货列表</title></head><body>'
    doc += '<div class="opt-page">'
    doc += '<div class="page-title">短线量化</div>'
    doc += '<div class="page-sub">选择期货品种，查看其连续主力合约的短线行情识别与入场信号</div>'
    doc += '<div class="sq-search"><input id="sqSearch" placeholder="搜索品种名称或代码，如 沪铜 / cu" oninput="sqTextFilter()"></div>'
    doc += f'<div class="sq-ex-tabs" id="sqExTabs">{ex_tabs}</div>'
    doc += f'<div id="sqList">{cards_html}</div>'
    doc += '</div>'
    doc += shortquant_tabbar("list")
    doc += _SQ_LIST_SCRIPT
    doc += '</body></html>'
    return doc


def _build_detail_html(symbol, result, error):
    """短线量化详情页：深色主题，复用期权模块视觉。"""
    back = '<a class="back-link" href="?">← 返回期货列表</a>'
    if error:
        body = f'<div class="card"><div class="empty">{html.escape(str(error))}</div></div>'
    elif result is None:
        body = '<div class="card"><div class="empty">输入合约代码后点击查询</div></div>'
    elif not result.get("ok"):
        body = f'<div class="card"><div class="empty">{html.escape(result.get("error", "未知错误"))}</div></div>'
    else:
        # 综合摘要
        overall = result.get("overall", "neutral")
        overall_label = result.get("overall_label", "无明确方向")
        score = result.get("score", {})
        price = result.get("current_price")
        conditions = result.get("conditions", [])
        signals = result.get("signals", [])
        elapsed = result.get("elapsed_ms", 0)

        summary_html = f'''
        <div class="card">
          <div class="summary">
            <div class="item"><div class="label">合约</div><div class="value">{html.escape(symbol)}</div></div>
            <div class="item"><div class="label">最新价</div><div class="value">{_fmt(price)}</div></div>
            <div class="item"><div class="label">综合方向</div><div class="value {overall}">{html.escape(overall_label)}</div></div>
            <div class="item"><div class="label">条件投票</div><div class="value">多{score.get('bull',0)} / 空{score.get('bear',0)} / 中{score.get('neutral',0)}</div></div>
            <div class="item"><div class="label">耗时</div><div class="value">{elapsed:.1f}ms</div></div>
          </div>
        </div>
        '''

        # 条件识别表
        cond_rows = []
        for c in conditions:
            d = c.get("direction", "neutral")
            badge = f'<span class="badge {d}">{"多头" if d=="bull" else ("空头" if d=="bear" else "中性")}</span>'
            cond_rows.append(
                f'<tr><td class="left">{html.escape(c.get("name",""))}</td>'
                f'<td>{html.escape(c.get("timeframe",""))}</td>'
                f'<td>{badge}</td>'
                f'<td class="left">{html.escape(c.get("detail",""))}</td></tr>'
            )
        conditions_html = f'''
        <div class="card">
          <h2>一、短线行情识别量化条件</h2>
          <table class="opt-table"><thead><tr><th class="left">条件维度</th><th>时间级别</th><th>方向</th><th class="left">说明</th></tr></thead><tbody>{''.join(cond_rows)}</tbody></table>
        </div>
        '''

        # 入场信号表：始终展示 4 行，触发给数据，未触发显示未触发及原因
        sig_rows = []
        all_types = ["突破入场", "回调入场", "动量入场", "开盘区间突破"]
        triggered_types = {s["type"] for s in signals}
        no_signal_reason = "综合方向不明或当前条件未满足" if overall == "neutral" else f"当前整体{overall_label}，暂无匹配该类型的入场条件"
        for t in all_types:
            if t in triggered_types:
                s = next(x for x in signals if x["type"] == t)
                d = s.get("direction", "bull")
                sig_rows.append(
                    f'<tr><td class="left">{html.escape(t)}</td>'
                    f'<td><span class="badge {d}">{"多" if d=="bull" else "空"}</span></td>'
                    f'<td class="signal-yes">已触发</td>'
                    f'<td>{html.escape(s.get("position_ratio",""))}</td>'
                    f'<td>{_fmt(s.get("entry_price"))}</td>'
                    f'<td>{_fmt(s.get("stop_loss"))}</td>'
                    f'<td class="left">{html.escape(s.get("reason",""))}<br><span class="note">止损：{html.escape(s.get("stop_desc",""))}</span></td></tr>'
                )
            else:
                sig_rows.append(
                    f'<tr><td class="left">{html.escape(t)}</td><td>—</td><td class="signal-no">未触发</td>'
                    f'<td>—</td><td>—</td><td>—</td><td class="left note">{html.escape(no_signal_reason)}</td></tr>'
                )
        signals_html = f'''
        <div class="card">
          <h2>二、短线入场点量化选择</h2>
          <table class="opt-table"><thead><tr><th class="left">入场类型</th><th>方向</th><th>状态</th><th>仓位</th><th>入场价</th><th>止损</th><th class="left">说明</th></tr></thead><tbody>{''.join(sig_rows)}</tbody></table>
        </div>
        '''

        body = summary_html + conditions_html + signals_html

    doc = _OPT_SPECIAL_STYLE + _SQ_STYLE + _MINI_SCRIPT
    doc += '<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><title>短线量化 - ' + html.escape(symbol) + '</title></head><body>'
    doc += '<div class="opt-page">'
    doc += '<div class="page-title">短线量化</div>'
    doc += '<div class="page-sub">基于日线 / 30分钟 / 5分钟 K 线的短线行情识别与入场信号</div>'
    doc += back
    doc += '<form method="get" action="?" style="display:flex;gap:8px;margin-bottom:14px">'
    doc += f'<input type="text" name="symbol" value="{html.escape(symbol)}" placeholder="输入合约代码，如 KQ.m@SHFE.cu" style="flex:1;padding:8px 12px;background:#0d1424;border:1px solid var(--border);color:var(--text);border-radius:6px">'
    doc += '<button type="submit" class="btn">查询</button>'
    doc += '</form>'
    doc += body
    doc += '</div>'
    doc += shortquant_tabbar("detail")
    doc += '</body></html>'
    return doc


# ==================== Flask 路由 ====================

def short_quant_page():
    """HTML 页面入口：无 symbol 显示期货列表，有 symbol 显示短线量化详情。"""
    symbol = request.args.get("symbol", "").strip()
    if not symbol:
        return _build_list_html()

    slot = _get_slot()
    result = None
    error = ""
    if slot is None:
        error = "默认账户行情未就绪"
    else:
        try:
            result = evaluate_short_term(symbol, slot)
        except Exception as e:
            error = f"评估异常: {e}"

    return _build_detail_html(symbol, result, error)


def api_short_quant_signal():
    """JSON API 入口。"""
    symbol = request.args.get("symbol", "").strip()
    if not symbol:
        return jsonify({"ok": False, "error": "缺少 symbol 参数"}), 400

    slot = _get_slot()
    if slot is None:
        return jsonify({"ok": False, "error": "默认账户行情未就绪"}), 503

    result = evaluate_short_term(symbol, slot)
    if not result.get("ok"):
        return jsonify(result), 503
    return jsonify(result)


def register_routes(app, slots, default_key, get_slot=None):
    """由 main.py 在启动时调用，注入 Slot 与默认账户。"""
    global _SLOTS, _DEFAULT_KEY, _GET_SLOT
    _SLOTS = slots
    _DEFAULT_KEY = default_key
    _GET_SLOT = get_slot

    app.add_url_rule("/shortquant", view_func=short_quant_page, methods=["GET"])
    app.add_url_rule("/shortquant/api/signal", view_func=api_short_quant_signal, methods=["GET"])
    print("[短线量化] 路由已注册: /shortquant, /shortquant/api/signal")
