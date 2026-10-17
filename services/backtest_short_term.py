# -*- coding: utf-8 -*-
"""
短线量化信号历史回放（最小回测）

设计原则：
- 直接复用 short_term_quant 里的条件/信号函数，避免"实盘一套、回测一套"的逻辑漂移。
- 对每根 5 分钟 K 线，用"截至该时刻"的日线/30分/5分切片重算条件与信号，
  再用之后的 N 根 K 线前向模拟：先触止损= -1R，先触 2R 止盈= +2R，否则到窗口末按市价平。
- 输出：信号数、胜率、平均 R、期望 R/笔、盈利因子、样本区间。

数据来源：
  ① --selftest        ：内置合成随机游走数据，验证回放框架本身（无需网络/账户）
  ② --csv5/--csv30/--csvdaily ：离线 CSV（列需含 open/high/low/close/volume + datetime 列）
  ③ 默认              ：用 TqSdk 免费账户实时拉取历史 K 线（需网络与行情账户）

用法：
  python services/backtest_short_term.py --selftest
  python services/backtest_short_term.py --symbol KQ.m@SHFE.cu --bars 2000
  python services/backtest_short_term.py --symbol KQ.m@SHFE.cu --csv5 cu_5m.csv --csv30 cu_30m.csv --csvdaily cu_d.csv
"""
import sys
import os
import argparse
import time
import math
import numpy as np
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)  # stock_app 目录
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from services.short_term_quant import (
    _condition_major_direction,
    _condition_minor_trend,
    _condition_macd_cross,
    _condition_volume_price,
    _condition_support_resistance,
    _condition_atr_filter,
    _signal_breakout,
    _signal_pullback,
    _signal_momentum,
    _signal_opening_range,
    _atr,
)

TP_R_MULT = 2.0        # 止盈目标 = 2R
MAX_HOLD_BARS = 60     # 最多持仓 60 根 5 分钟（约 5 小时）


# ==================== 回放核心 ====================

def _vote_overall(dd, d30, d5, cur):
    """复刻 evaluate_short_term 的 6 条件投票逻辑，返回综合方向。"""
    major, _ = _condition_major_direction(dd)
    minor, _ = _condition_minor_trend(d30)
    macd, _ = _condition_macd_cross(d30)
    vol, _ = _condition_volume_price(d30)
    sr, _ = _condition_support_resistance(d5, cur, "bull")
    if major == "bear" or minor == "bear":
        sr, _ = _condition_support_resistance(d5, cur, "bear")
    atr, _ = _condition_atr_filter(d5, dd)

    score = {"bull": 0, "bear": 0, "neutral": 0}
    for d in (major, minor, macd, vol, sr, atr):
        score[d] += 1
    bull, bear = score["bull"], score["bear"]
    if bull >= 4 and bear <= 1:
        return "bull"
    if bear >= 4 and bull <= 1:
        return "bear"
    if bull > bear:
        return "bull"
    if bear > bull:
        return "bear"
    return "neutral"


def replay(df_daily, df_30m, df_5m, tp_r=TP_R_MULT, max_hold=MAX_HOLD_BARS):
    """对 5 分钟序列逐根回放，返回每笔触发信号的 R 收益列表。"""
    results = []
    n = len(df_5m)
    if n < 60:
        return results
    times = df_5m.index

    for i in range(50, n - max_hold - 2):
        t_i = times[i]
        d5 = df_5m.iloc[: i + 1]
        d30 = df_30m[df_30m.index <= t_i]
        dd = df_daily[df_daily.index <= t_i]
        if len(d5) < 14 or len(d30) < 14 or len(dd) < 21:
            continue

        cur = float(d5["close"].iloc[-1])
        overall = _vote_overall(dd, d30, d5, cur)
        if overall == "neutral":
            continue

        atr5 = float(_atr(d5, 14).iloc[-1])
        if not math.isfinite(atr5) or atr5 <= 0:
            continue

        sigs = [
            _signal_breakout(None, d30, cur, overall, atr5),
            _signal_pullback(None, d30, cur, overall),
            _signal_momentum(None, d5, cur, overall),
            _signal_opening_range(None, d5, cur, overall),
        ]
        sigs = [s for s in sigs if s]
        if not sigs:
            continue

        sig = sigs[0]  # 取首个触发信号
        direction = sig["direction"]
        entry = float(sig["entry_price"])
        stop = float(sig["stop_loss"])
        stop_dist = abs(entry - stop)
        if stop_dist <= 0:
            continue

        tp = entry + tp_r * atr5 if direction == "bull" else entry - tp_r * atr5

        outcome_r = None
        for j in range(i + 1, min(i + 1 + max_hold, n)):
            lo = float(df_5m["low"].iloc[j])
            hi = float(df_5m["high"].iloc[j])
            if direction == "bull":
                if lo <= stop:
                    outcome_r = -stop_dist / atr5
                    break
                if hi >= tp:
                    outcome_r = tp_r
                    break
            else:
                if hi >= stop:
                    outcome_r = -stop_dist / atr5
                    break
                if lo <= tp:
                    outcome_r = tp_r
                    break
        if outcome_r is None:
            last = float(df_5m["close"].iloc[min(i + max_hold, n - 1)])
            outcome_r = (last - entry) / atr5 if direction == "bull" else (entry - last) / atr5

        results.append({"direction": direction, "R": outcome_r, "entry": entry, "stop": stop})

    return results


def summarize(results):
    if not results:
        return "无触发信号（样本内 = 当前参数无交易）"
    rs = [r["R"] for r in results]
    wins = [r for r in rs if r > 0]
    losses = [r for r in rs if r <= 0]
    win_rate = len(wins) / len(rs)
    avg_r = float(np.mean(rs))
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    bull = sum(1 for r in results if r["direction"] == "bull")
    return (
        f"信号数={len(rs)} (多{bull}/空{len(rs)-bull}) | "
        f"胜率={win_rate:.1%} | 平均R={avg_r:+.2f} | "
        f"期望R/笔={avg_r:+.2f} | 盈利因子={pf:.2f}"
    )


# ==================== 数据加载 ====================

def _to_frame(df, freq):
    df = pd.DataFrame(df)
    if "datetime" in df.columns:
        df = df.set_index("datetime")
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    need = ["open", "high", "low", "close", "volume"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"CSV 缺少列 {c}（需 {need}）")
    df = df[need].astype(float)
    return df


def load_data(symbol, bars, csv5, csv30, csvdaily):
    if csv5:
        d5 = _to_frame(pd.read_csv(csv5), 300)
        d30 = _to_frame(pd.read_csv(csv30), 1800) if csv30 else None
        dd = _to_frame(pd.read_csv(csvdaily), 86400) if csvdaily else None
        return dd, d30, d5

    # TqSdk 实时拉取历史 K 线
    from tqsdk import TqApi, TqAuth
    from tqsdk.tafunc import get_kline_serial

    from services.futures_db import get_sync_account
    _su, _sp = get_sync_account()
    if not _su:
        print("[回测] 无有效账户, 无法拉取历史K线")
        return None, None, None
    api = TqApi(auth=TqAuth(_su, _sp))
    print(f"[回测] 正在从 TqSdk 拉取 {symbol} 历史 K 线 ...")
    k5 = get_kline_serial(symbol, 300, bars)
    k30 = get_kline_serial(symbol, 1800, max(bars // 6 + 60, 120))
    kd = get_kline_serial(symbol, 86400, 250)
    while not (api.is_serial_ready(k5) and api.is_serial_ready(k30) and api.is_serial_ready(kd)):
        api.wait_update()

    d5 = _to_frame(k5, 300)
    d30 = _to_frame(k30, 1800)
    dd = _to_frame(kd, 86400)
    api.close()
    return dd, d30, d5


def _selftest():
    """内置合成数据，验证回放框架本身可跑通（不依赖网络/账户）。"""
    np.random.seed(7)
    # 单向上行趋势，足够长度以产生 >=21 根日线（MA20 需要）
    n = 6000
    idx = pd.date_range("2026-01-01 09:00", periods=n, freq="5min")
    drift = 0.0022
    ret = np.random.normal(0, 0.5, n) * 0.0006 + drift
    close = pd.Series(5000 + np.cumsum(ret) * 300, index=idx)
    high = (close + np.abs(np.random.normal(0, 1.5, n))).values
    low = (close - np.abs(np.random.normal(0, 1.5, n))).values
    open_ = close.shift(1).fillna(close.iloc[0]).values
    vol = np.random.randint(1000, 5000, n).astype(float)
    d5 = pd.DataFrame({"open": open_, "high": high, "low": low, "close": close.values, "volume": vol}, index=idx)
    d30 = d5.resample("30min").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
    dd = d5.resample("1D").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
    print(f"[selftest] 5m={len(d5)} 30m={len(d30)} daily={len(dd)}")
    res = replay(dd, d30, d5)
    print("[selftest]", summarize(res))
    # 抽样打印前 3 笔
    for r in res[:3]:
        print("   ", r)


def main():
    ap = argparse.ArgumentParser(description="短线量化历史回放")
    ap.add_argument("--symbol", default="KQ.m@SHFE.cu")
    ap.add_argument("--bars", type=int, default=2000)
    ap.add_argument("--csv5")
    ap.add_argument("--csv30")
    ap.add_argument("--csvdaily")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        _selftest()
        return

    t0 = time.time()
    dd, d30, d5 = load_data(args.symbol, args.bars, args.csv5, args.csv30, args.csvdaily)
    print(f"数据: 5m={len(d5)} 30m={len(d30) if d30 is not None else 0} daily={len(dd) if dd is not None else 0}")
    res = replay(dd, d30, d5)
    print("结果:", summarize(res))
    print(f"耗时 {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
