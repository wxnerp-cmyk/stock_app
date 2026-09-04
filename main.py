"""
stock_app - 行情子服务 (多账户数据隔离版)
端口: 5088
"""
import threading, json, sys, traceback, math, time
import html
from urllib.parse import quote as _urlquote
from datetime import datetime
from flask import Flask, render_template_string, request, Response, jsonify
from services import sync_service
from services import option_greeks
from core.tqsdk_client import TqSdkClient
from services.market_data import MarketDataService
from services.option_expiry import fetch_expiry_data, build_calendar_data, build_weekly_summary
from services import futures_db
# 历史K线改为走天勤 TqSdk (get_kline_serial), 与实时行情共用同一账号/连接
# (不再依赖 akshare/东方财富外网)

app = Flask(__name__)

def safe_json(data, status=200):
    def sanitize(obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return 0.0
            return obj
        if isinstance(obj, dict):
            return {k: sanitize(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [sanitize(v) for v in obj]
        return obj
    return Response(json.dumps(sanitize(data), ensure_ascii=False),
                    status=status, mimetype='application/json')


# ===== 历史K线辅助 (基于天勤 TqSdk) =====
def _fmt_tq_dt(v):
    """天勤K线 datetime 为纳秒时间戳(int), 转为 YYYY-MM-DD"""
    try:
        v = float(v)
        if v <= 0:
            return ""
        return datetime.fromtimestamp(v / 1e9).strftime("%Y-%m-%d")
    except Exception:
        return str(v)


def _kf(x):
    try:
        x = float(x)
        if math.isnan(x) or math.isinf(x):
            return 0.0
        return x
    except Exception:
        return 0.0


def _ki(x):
    try:
        return int(x)
    except Exception:
        return 0


def _base_price(q):
    """涨跌幅基准价：昨结算优先(期货/期权行业惯例,与文华WH6一致), 无昨结算(股票)回退昨收。"""
    try:
        ps = float(getattr(q, "pre_settlement", 0) or 0)
        if ps > 0:
            return ps
    except Exception:
        pass
    try:
        pc = float(getattr(q, "pre_close", 0) or 0)
        if pc > 0:
            return pc
    except Exception:
        pass
    return None


def _history_to_records(df):
    """TqSdk K线对象 -> [{'datetime','open','high','low','close','volume'}]"""
    try:
        n = len(df)
    except Exception:
        return []
    recs = []
    # 兼容不同列名: datetime / id / 索引
    if "datetime" in df.columns:
        dt = df["datetime"]
    elif "id" in df.columns:
        dt = df["id"]
    else:
        dt = list(range(n))
    for i in range(n):
        recs.append({
            "datetime": _fmt_tq_dt(dt[i]),
            "open": _kf(df["open"][i]),
            "high": _kf(df["high"][i]),
            "low": _kf(df["low"][i]),
            "close": _kf(df["close"][i]),
            "volume": _ki(df["volume"][i]),
        })
    return recs


def _gv(series, i):
    """安全取 Series 第 i 个值: NaN/None -> None。"""
    try:
        v = series[i]
        if v is None:
            return None
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except Exception:
        return None


def _compute_all_indicators(df):
    """基于 pandas DataFrame (open/high/low/close/volume) 计算全套副图指标。
    返回 dict: {indicator_name: {series_name: [values]}} 与 records 索引对齐。
    对非标准/文华特有指标使用常用简化公式; 公式复杂的会标注。
    """
    import pandas as _pd
    import numpy as _np
    n = len(df)
    out = {}
    if n == 0:
        return out

    def _s(col):
        s = df[col].astype(float) if col in df else _pd.Series([0.0] * n)
        return s.fillna(0.0).replace([_np.inf, -_np.inf], 0.0)

    O = _s('open'); H = _s('high'); L = _s('low'); C = _s('close'); V = _s('volume')
    nan_list = [None] * n

    def _to_list(s):
        try:
            return [None if (_np.isnan(x) or _np.isinf(x)) else float(x) for x in s.values]
        except Exception:
            return nan_list

    def _ema(s, span):
        return s.ewm(span=span, adjust=False, min_periods=1).mean()

    def _sma(s, window):
        return s.rolling(window=window, min_periods=1).mean()

    def _std(s, window):
        return s.rolling(window=window, min_periods=1).std(ddof=0)

    # --- MACD ---
    try:
        ema12 = _ema(C, 12); ema26 = _ema(C, 26)
        dif = ema12 - ema26
        dea = _ema(dif, 9)
        out['macd'] = {'dif': _to_list(dif), 'dea': _to_list(dea), 'bar': _to_list((dif - dea) * 2)}
    except Exception:
        out['macd'] = {'dif': nan_list, 'dea': nan_list, 'bar': nan_list}

    # --- KDJ ---
    try:
        low_min = L.rolling(window=9, min_periods=1).min()
        high_max = H.rolling(window=9, min_periods=1).max()
        rsv = (C - low_min) / (high_max - low_min) * 100
        rsv = rsv.fillna(50.0).replace([_np.inf, -_np.inf], 50.0)
        k = _pd.Series(index=C.index, dtype=float); d = _pd.Series(index=C.index, dtype=float)
        k.iloc[0] = 50.0; d.iloc[0] = 50.0
        for i in range(1, n):
            k.iloc[i] = 2.0 / 3.0 * k.iloc[i - 1] + 1.0 / 3.0 * rsv.iloc[i]
            d.iloc[i] = 2.0 / 3.0 * d.iloc[i - 1] + 1.0 / 3.0 * k.iloc[i]
        j = 3 * k - 2 * d
        out['kdj'] = {'k': _to_list(k), 'd': _to_list(d), 'j': _to_list(j)}
    except Exception:
        out['kdj'] = {'k': nan_list, 'd': nan_list, 'j': nan_list}

    # --- RSI(14) ---
    try:
        delta = C.diff().fillna(0)
        gain = delta.where(delta > 0, 0.0); loss = -delta.where(delta < 0, 0.0)
        avg_gain = gain.ewm(alpha=1.0 / 14, adjust=False, min_periods=1).mean()
        avg_loss = loss.ewm(alpha=1.0 / 14, adjust=False, min_periods=1).mean()
        rs = avg_gain / avg_loss.replace(0, _np.nan)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        out['rsi'] = {'rsi': _to_list(rsi.fillna(50.0))}
    except Exception:
        out['rsi'] = {'rsi': nan_list}

    # --- WR(14) Williams %R ---
    try:
        hh = H.rolling(window=14, min_periods=1).max()
        ll = L.rolling(window=14, min_periods=1).min()
        wr = (hh - C) / (hh - ll) * -100
        out['wr'] = {'wr': _to_list(wr.fillna(-50.0))}
    except Exception:
        out['wr'] = {'wr': nan_list}

    # --- MTM(12) Momentum ---
    try:
        mtm = C - C.shift(12).fillna(C)
        mtm_ma = _sma(mtm, 6)
        out['mtm'] = {'mtm': _to_list(mtm), 'mtmma': _to_list(mtm_ma)}
    except Exception:
        out['mtm'] = {'mtm': nan_list, 'mtmma': nan_list}

    # --- ROC(12) Rate of Change ---
    try:
        roc = (C - C.shift(12)) / C.shift(12).replace(0, _np.nan) * 100
        out['roc'] = {'roc': _to_list(roc.fillna(0.0))}
    except Exception:
        out['roc'] = {'roc': nan_list}

    # --- BIAS(6,12,24) ---
    try:
        out['bias'] = {
            'bias6': _to_list(((C - _sma(C, 6)) / _sma(C, 6).replace(0, _np.nan) * 100).fillna(0.0)),
            'bias12': _to_list(((C - _sma(C, 12)) / _sma(C, 12).replace(0, _np.nan) * 100).fillna(0.0)),
            'bias24': _to_list(((C - _sma(C, 24)) / _sma(C, 24).replace(0, _np.nan) * 100).fillna(0.0)),
        }
    except Exception:
        out['bias'] = {'bias6': nan_list, 'bias12': nan_list, 'bias24': nan_list}

    # --- PSY(12) Psychological Line ---
    try:
        up_days = (C > C.shift(1)).rolling(window=12, min_periods=1).sum()
        psy = up_days / 12.0 * 100.0
        out['psy'] = {'psy': _to_list(psy.fillna(50.0))}
    except Exception:
        out['psy'] = {'psy': nan_list}

    # --- ARBR(26) ---
    try:
        ar = (H - O).rolling(window=26, min_periods=1).sum() / (O - L).replace(0, _np.nan).rolling(window=26, min_periods=1).sum()
        pc = C.shift(1)
        br = (H - pc).rolling(window=26, min_periods=1).sum() / (pc - L).replace(0, _np.nan).rolling(window=26, min_periods=1).sum()
        out['arbr'] = {'ar': _to_list(ar.fillna(100.0) * 100), 'br': _to_list(br.fillna(100.0) * 100)}
    except Exception:
        out['arbr'] = {'ar': nan_list, 'br': nan_list}

    # --- DMA(10,50) ---
    try:
        dma = _sma(C, 10) - _sma(C, 50)
        ama = _sma(dma, 10)
        out['dma'] = {'dma': _to_list(dma.fillna(0.0)), 'ama': _to_list(ama.fillna(0.0))}
    except Exception:
        out['dma'] = {'dma': nan_list, 'ama': nan_list}

    # --- DMI(14) / ADX ---
    try:
        tr1 = H - L
        tr2 = (H - C.shift(1)).abs()
        tr3 = (L - C.shift(1)).abs()
        tr = _pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        plus_dm = (H - H.shift(1)).where((H - H.shift(1)) > (L.shift(1) - L), 0.0)
        plus_dm = plus_dm.where(plus_dm > 0, 0.0)
        minus_dm = (L.shift(1) - L).where((L.shift(1) - L) > (H - H.shift(1)), 0.0)
        minus_dm = minus_dm.where(minus_dm > 0, 0.0)
        atr14 = _ema(tr, 14)
        plus_di = plus_dm.ewm(alpha=1.0 / 14, adjust=False, min_periods=1).mean() / atr14.replace(0, _np.nan) * 100
        minus_di = minus_dm.ewm(alpha=1.0 / 14, adjust=False, min_periods=1).mean() / atr14.replace(0, _np.nan) * 100
        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, _np.nan) * 100).fillna(0.0)
        adx = _ema(dx, 14)
        out['dmi'] = {'pdi': _to_list(plus_di.fillna(0.0)), 'mdi': _to_list(minus_di.fillna(0.0)), 'adx': _to_list(adx.fillna(0.0))}
    except Exception:
        out['dmi'] = {'pdi': nan_list, 'mdi': nan_list, 'adx': nan_list}

    # --- ATR(14) ---
    try:
        tr = _pd.concat([H - L, (H - C.shift(1)).abs(), (L - C.shift(1)).abs()], axis=1).max(axis=1)
        atr = _ema(tr, 14)
        out['atr'] = {'atr': _to_list(atr.fillna(0.0))}
    except Exception:
        out['atr'] = {'atr': nan_list}

    # --- CCI(20) ---
    try:
        tp = (H + L + C) / 3.0
        ma_tp = _sma(tp, 20)
        md = tp.rolling(window=20, min_periods=1).apply(lambda x: _np.abs(x - x.mean()).mean(), raw=False)
        cci = (tp - ma_tp) / (0.015 * md.replace(0, _np.nan))
        out['cci'] = {'cci': _to_list(cci.fillna(0.0))}
    except Exception:
        out['cci'] = {'cci': nan_list}

    # --- MI Mass Index(25,9) ---
    try:
        hl = H - L
        ema_hl = _ema(hl, 9)
        ema_ema_hl = _ema(ema_hl, 9)
        mi = (ema_hl / ema_ema_hl.replace(0, _np.nan)).rolling(window=25, min_periods=1).sum()
        out['mi'] = {'mi': _to_list(mi.fillna(0.0))}
    except Exception:
        out['mi'] = {'mi': nan_list}

    # --- ADTM(23,8) Dynamic Trading Move ---
    try:
        dtm = (H - O).where(H > O.shift(1), 0.0)
        dbm = (O - L).where(L < O.shift(1), 0.0)
        stm = dtm.rolling(window=23, min_periods=1).sum()
        sbm = dbm.rolling(window=23, min_periods=1).sum()
        adtm = (stm - sbm) / _pd.concat([stm, sbm], axis=1).max(axis=1).replace(0, _np.nan)
        out['adtm'] = {'adtm': _to_list(adtm.fillna(0.0))}
    except Exception:
        out['adtm'] = {'adtm': nan_list}

    # --- ASI simplified (Accumulation Swing Index) ---
    try:
        k = _np.maximum(H - C.shift(1), L - C.shift(1))
        r = H - C.shift(1) - 0.5 * (H - C.shift(1)) + 0.25 * (C.shift(1) - O.shift(1))
        si = 50 * ((C - C.shift(1)) + 0.5 * (C - O) + 0.25 * (C.shift(1) - O.shift(1))) / k.replace(0, _np.nan) * r / 3
        asi = si.cumsum()
        out['asi'] = {'asi': _to_list(asi.fillna(0.0))}
    except Exception:
        out['asi'] = {'asi': nan_list}

    # --- Volume indicators ---
    # CJL (volume bars) - just volume
    out['cjl'] = {'vol': _to_list(V)}
    # MV (均量 MA5/MA10)
    try:
        out['mv'] = {'mv5': _to_list(_sma(V, 5)), 'mv10': _to_list(_sma(V, 10))}
    except Exception:
        out['mv'] = {'mv5': nan_list, 'mv10': nan_list}
    # OBV
    try:
        obv = (V * (C.diff() > 0).astype(int) - V * (C.diff() < 0).astype(int)).cumsum()
        out['obv'] = {'obv': _to_list(obv.fillna(0.0))}
    except Exception:
        out['obv'] = {'obv': nan_list}
    # VR(26)
    try:
        up_v = V.where(C > C.shift(1), 0.0).rolling(window=26, min_periods=1).sum()
        down_v = V.where(C < C.shift(1), 0.0).rolling(window=26, min_periods=1).sum()
        same_v = V.where(C == C.shift(1), 0.0).rolling(window=26, min_periods=1).sum()
        vr = (up_v + same_v / 2.0) / (down_v + same_v / 2.0).replace(0, _np.nan) * 100.0
        out['vr'] = {'vr': _to_list(vr.fillna(100.0))}
    except Exception:
        out['vr'] = {'vr': nan_list}
    # VROC(12)
    try:
        vroc = (V - V.shift(12)) / V.shift(12).replace(0, _np.nan) * 100.0
        out['vroc'] = {'vroc': _to_list(vroc.fillna(0.0))}
    except Exception:
        out['vroc'] = {'vroc': nan_list}
    # VRSI(14) RSI on volume
    try:
        dv = V.diff().fillna(0)
        vgain = dv.where(dv > 0, 0.0); vloss = -dv.where(dv < 0, 0.0)
        avg_vg = vgain.ewm(alpha=1.0 / 14, adjust=False, min_periods=1).mean()
        avg_vl = vloss.ewm(alpha=1.0 / 14, adjust=False, min_periods=1).mean()
        vrs = avg_vg / avg_vl.replace(0, _np.nan)
        vrsi = 100.0 - (100.0 / (1.0 + vrs))
        out['vrsi'] = {'vrsi': _to_list(vrsi.fillna(50.0))}
    except Exception:
        out['vrsi'] = {'vrsi': nan_list}
    # WVAD(24)
    try:
        wvad = ((C - O) / (H - L).replace(0, _np.nan) * V).rolling(window=24, min_periods=1).sum()
        out['wvad'] = {'wvad': _to_list(wvad.fillna(0.0))}
    except Exception:
        out['wvad'] = {'wvad': nan_list}

    # --- CR(26) CR能量指标 ---
    try:
        m = (H + L + C) / 3.0
        cr = (H - m.shift(1)).rolling(window=26, min_periods=1).sum() / (m.shift(1) - L).replace(0, _np.nan).rolling(window=26, min_periods=1).sum()
        out['cr'] = {'cr': _to_list(cr.fillna(100.0) * 100)}
    except Exception:
        out['cr'] = {'cr': nan_list}

    # --- CCL 持仓量 (天勤 K线无持仓量字段, 占位) ---
    out['ccl'] = {'oi': nan_list}

    # --- 非标准/文华特有指标: 用简化公式占位, 后续可替换 ---
    # B3612, DBCD, DDI, MICD, QHLSR, RC, RCCD, SRDM, ZDZB, LON, SHORT, PSY? already
    try:
        out['b3612'] = {'b36': _to_list(_sma(C, 3) - _sma(C, 6)), 'b12': _to_list(_sma(C, 12) - _sma(C, 24))}
    except Exception:
        out['b3612'] = {'b36': nan_list, 'b12': nan_list}
    try:
        dbcd = (_sma(C, 5) - _sma(C, 10)) / _sma(C, 10).replace(0, _np.nan) * 100
        out['dbcd'] = {'dbcd': _to_list(dbcd.fillna(0.0))}
    except Exception:
        out['dbcd'] = {'dbcd': nan_list}
    try:
        ddi = (_sma(H, 5) - _sma(L, 5)) / (_sma(H, 5) + _sma(L, 5)).replace(0, _np.nan) * 100
        out['ddi'] = {'ddi': _to_list(ddi.fillna(0.0))}
    except Exception:
        out['ddi'] = {'ddi': nan_list}
    try:
        micd = _ema(C, 12) - _ema(C, 26)
        out['micd'] = {'micd': _to_list(micd.fillna(0.0))}
    except Exception:
        out['micd'] = {'micd': nan_list}
    try:
        qhlsr = (C - L.rolling(window=10, min_periods=1).min()) / (H.rolling(window=10, min_periods=1).max() - L.rolling(window=10, min_periods=1).min()).replace(0, _np.nan) * 100
        out['qhlsr'] = {'qhlsr': _to_list(qhlsr.fillna(50.0))}
    except Exception:
        out['qhlsr'] = {'qhlsr': nan_list}
    try:
        rc = (C - C.shift(12)) / C.shift(12).replace(0, _np.nan) * 100
        out['rc'] = {'rc': _to_list(rc.fillna(0.0))}
    except Exception:
        out['rc'] = {'rc': nan_list}
    try:
        rccd = _ema(C, 12) - _ema(C, 26) - _ema(_ema(C, 12) - _ema(C, 26), 9)
        out['rccd'] = {'rccd': _to_list(rccd.fillna(0.0))}
    except Exception:
        out['rccd'] = {'rccd': nan_list}
    try:
        srdm = (H - L.shift(1)) / (H.shift(1) - L).replace(0, _np.nan) * 100
        out['srdm'] = {'srdm': _to_list(srdm.fillna(0.0))}
    except Exception:
        out['srdm'] = {'srdm': nan_list}
    try:
        zdzb = (C - _sma(C, 26)) / _sma(C, 26).replace(0, _np.nan) * 100
        out['zdzb'] = {'zdzb': _to_list(zdzb.fillna(0.0))}
    except Exception:
        out['zdzb'] = {'zdzb': nan_list}
    try:
        lon = _ema(C, 26) - _ema(C, 12)
        out['lon'] = {'lon': _to_list(lon.fillna(0.0))}
    except Exception:
        out['lon'] = {'lon': nan_list}
    try:
        short = C - _sma(C, 5)
        out['short'] = {'short': _to_list(short.fillna(0.0))}
    except Exception:
        out['short'] = {'short': nan_list}

    return out


def _build_kline_response(df):
    """TqSdk K线对象 -> (records 含指标, indicators 副图序列)。

    records 每条含: datetime/open/high/low/close/volume/ma5/10/60、
                     boll_top/boll_bottom。
    indicators: dict, 包含 macd/kdj/rsi/wr/... 等全套副图指标序列。
    供前端 drawKline 在主图叠加均线与布林带、副图切换显示指标。
    """
    if df is None or len(df) == 0:
        return [], {}
    n = len(df)
    # --- 识别无成交占位 bar + 构建 df_calc 副本 ---
    # 实时 K 线中, TqSdk 对未成交时段用昨收填充 (OHLC 全相等 + V=0).
    # 这类 bar 不该参与技术指标计算, 否则 BOLL/MA 会被"假跳价"扭曲出奇异曲线.
    # 策略: 构造 df_calc 副本, 占位 bar 的 OHLC 设为 NaN 喂给 tqsdk.ta,
    # pandas rolling/ewm 默认 skipna=True, std/mean/EMA 只基于真实成交 bar 计算,
    # 结果: BOLL 通道干净, 不再有"椭圆形"奇异曲线.
    placeholder_idx = set()
    try:
        for i in range(n):
            o = _kf(df["open"][i]); h = _kf(df["high"][i])
            l = _kf(df["low"][i]);  c = _kf(df["close"][i])
            v = _ki(df["volume"][i])
            if v == 0 and o == h == l == c:
                placeholder_idx.add(i)
    except Exception:
        pass
    df_calc = df
    if placeholder_idx:
        try:
            import pandas as _pd
            nan = float('nan')
            df_calc = _pd.DataFrame({
                col: [(nan if i in placeholder_idx else _kf(df[col][i])) for i in range(n)]
                for col in ('open', 'high', 'low', 'close')
            })
        except Exception:
            df_calc = df
    # --- 技术指标 (主图均线/布林带继续用 tqsdk.ta; 副图全套指标用纯 pandas 实现) ---
    ma5 = ma10 = ma60 = boll = None
    try:
        from tqsdk import ta
        ma5 = ta.MA(df_calc, 5)
        ma10 = ta.MA(df_calc, 10)
        ma60 = ta.MA(df_calc, 60)
        boll = ta.BOLL(df_calc, 20, 2)
    except Exception as e:
        print(f"[K线主图指标] 计算失败: {e}")
    try:
        all_indicators = _compute_all_indicators(df_calc)
    except Exception as e:
        print(f"[K线副图指标] 全量计算失败: {e}")
        all_indicators = {}
    # --- 组装 records (OHLC 用原 df 保留昨收填充价; 指标用 df_calc 输出 pandas 自动 NaN) ---
    if "datetime" in df.columns:
        dt = df["datetime"]
    elif "id" in df.columns:
        dt = df["id"]
    else:
        dt = list(range(n))
    recs = []
    for i in range(n):
        r = {
            "datetime": _fmt_tq_dt(dt[i]),
            "open": _kf(df["open"][i]), "high": _kf(df["high"][i]),
            "low": _kf(df["low"][i]), "close": _kf(df["close"][i]),
            "volume": _ki(df["volume"][i]),
        }
        if ma5 is not None:
            r["ma5"] = _gv(ma5["ma"], i)
            r["ma10"] = _gv(ma10["ma"], i)
            r["ma60"] = _gv(ma60["ma"], i)
            r["boll_top"] = _gv(boll["top"], i)
            r["boll_mid"] = _gv(boll["mid"], i)
            r["boll_bottom"] = _gv(boll["bottom"], i)
        recs.append(r)
    # --- 裁剪前后零占位 bar ---
    # TqSdk 对期权合约的历史 K 线会包含"未上市日期"的占位 bar (datetime 空 + O=H=L=C=0+V=0),
    # 若不过滤则前/后留大片空白。裁剪到 [first_valid, last_valid] 连续有效区间。
    def _is_valid(r):
        try:
            return bool(r.get("datetime")) and float(r.get("close") or 0) > 0
        except Exception:
            return False
    first_valid = None
    for i, r in enumerate(recs):
        if _is_valid(r):
            first_valid = i; break
    if first_valid is None:
        return [], {}
    last_valid = len(recs) - 1
    for i in range(len(recs) - 1, first_valid - 1, -1):
        if _is_valid(recs[i]):
            last_valid = i; break
    if last_valid < first_valid:
        return [], {}
    if first_valid > 0 or last_valid < len(recs) - 1:
        recs = recs[first_valid:last_valid + 1]
        for k, v in all_indicators.items():
            if isinstance(v, dict):
                for kk in list(v.keys()):
                    if isinstance(v[kk], list) and len(v[kk]) >= last_valid + 1:
                        v[kk] = v[kk][first_valid:last_valid + 1]
    return recs, all_indicators


DEFAULT_SYMBOLS = [
    "KQ.m@SHFE.cu", "KQ.m@SHFE.rb", "KQ.m@DCE.i", "KQ.m@DCE.m",
]

# 顶部状态栏所需的指数篮子（上证指数 + 文华商品指数成分）。
# 在轮询线程启动时【永久订阅】，保证期权页状态栏/标的最新价毫秒级可读，
# 不再在每次请求里为取指数硬等。
INDEX_SYMBOLS = ["SSE.000001"]
WH_INDEX_SYMBOLS = ["KQ.m@SHFE.cu", "KQ.m@SHFE.rb", "KQ.m@DCE.i", "KQ.m@INE.sc",
                    "KQ.m@DCE.m", "KQ.m@CZCE.TA", "KQ.m@SHFE.au", "KQ.m@DCE.p",
                    "KQ.m@CZCE.MA", "KQ.m@SHFE.fu"]


# ===== 账户Slot: 每个账户独立客户端 + 数据 + 轮询线程 =====
class AccountSlot:
    def __init__(self, tq_user: str, tq_pass: str):
        self.tq_user = tq_user
        self.tq_pass = tq_pass
        self.client = None
        self.market = None
        self.latest_data = {}
        self.data_lock = threading.Lock()
        self.status = "init"       # init | online | error
        self.error_msg = ""
        self.poll_thread = None
        self._stop = False
        self._access_count = 0     # 活跃引用计数，为0时可释放
        self._cont_to_real = {}    # KQ.m@连续合约 -> 当前真实合约 缓存
        self.subscribed = set()    # 常驻订阅集合(由轮询线程持续 wait_update 驱动)
        self._pending_sub = set()  # 待主线程 get_quote 的合约(请求线程只登记, 不直接 get_quote)
        self._sub_lock = threading.Lock()

    def ensure_subscribed(self, symbols):
        """请求线程调用：仅登记待订阅合约（不直接 get_quote）。
        关键：TqSdk 的 get_quote 必须在 TqApi 创建线程(主线程)调用才会真正发送订阅请求，
        否则行情不推送（实测请求线程 get_quote 仅 3/76 合约有行情, 主线程 75/76）。
        真正 get_quote 由主线程订阅注册循环执行。"""
        if self.client is None or self.client.api is None:
            return
        with self._sub_lock:
            for s in symbols:
                if s not in self.subscribed:
                    self._pending_sub.add(s)
                    self.subscribed.add(s)

    def connect(self):
        """连接天勤(仅调用一次)。TqApi 必须在创建它的同一线程里调用 wait_update，
        因此轮询不再单独开线程，而是由主线程的订阅注册循环驱动。"""
        if self.status != "init":
            return
        self.status = "connecting"
        try:
            self.client = TqSdkClient(self.tq_user, self.tq_pass)
            self.market = MarketDataService(self.client)
            self.status = "online"
            print(f"[账户:{self.tq_user}] ✓ 连接成功")
        except Exception as e:
            self.status = "error"
            self.error_msg = str(e)
            print(f"[账户:{self.tq_user}] ✗ 连接失败: {e}")

    def _register_pending(self):
        """在主线程(TqApi 创建线程)里把请求线程登记的待订阅合约真正注册到天勤。"""
        if self.client is None or self.client.api is None:
            return
        with self._sub_lock:
            pending = list(self._pending_sub)
            self._pending_sub.clear()
        for sym in pending:
            try:
                self.client.get_quote_main_thread(sym)
            except Exception:
                pass

    def startup_poll(self):
        """主线程调用：启动时一次性预加载 K 线、注册默认订阅、订阅 tick 序列。"""
        import time
        if self.client is None or self.client.api is None:
            return
        # 预加载K线
        retry = 0
        while retry < 3 and not self._stop:
            print(f"[账户:{self.tq_user}] 预加载K线... (尝试{retry+1}/3)")
            try:
                self.market.preload_kline(DEFAULT_SYMBOLS, 86400, 200)
                self.client.api.wait_update(deadline=time.time() + 5)
                sample = self.market.get_kline_data(DEFAULT_SYMBOLS[0], 86400, 200)
                if not sample.empty and len(sample) > 0:
                    print(f"[账户:{self.tq_user}] ✓ K线预加载完成 {len(sample)} 条")
                    break
            except Exception as e:
                print(f"[账户:{self.tq_user}] K线预加载失败: {e}")
            retry += 1
            time.sleep(3)

        print(f"[账户:{self.tq_user}] 订阅实时行情...")
        self.ensure_subscribed(list(DEFAULT_SYMBOLS) + list(INDEX_SYMBOLS) + list(WH_INDEX_SYMBOLS))
        # 订阅 tick 序列, 用于现手 / 外盘 / 内盘
        try:
            self.market.subscribe_ticks(DEFAULT_SYMBOLS)
            print(f"[账户:{self.tq_user}] ✓ 已订阅 {len(DEFAULT_SYMBOLS)} 个合约的 tick 序列", flush=True)
        except Exception as e:
            print(f"[账户:{self.tq_user}] tick 订阅失败: {e}", flush=True)
        print(f"[账户:{self.tq_user}] 已订阅 {len(self.subscribed)} 个合约, 开始轮询...", flush=True)

    def poll_step(self):
        """主线程调用：执行一次 wait_update + 注册待订阅合约 + 快照缓存。
        这样 wait_update 始终运行在 TqApi 创建线程，避免'不能在任意线程中调用 wait_update'。"""
        import time
        if self.client is None or self.client.api is None:
            time.sleep(0.5)
            return
        # 先把请求线程登记的合约注册掉（TqSdk 要求创建线程调用）
        self._register_pending()
        try:
            got = self.client.api.wait_update(deadline=time.time() + 0.8)
            self._poll_cnt = getattr(self, '_poll_cnt', 0) + 1
            if self._poll_cnt % 60 == 1:
                print(f"[轮询] #{self._poll_cnt} got={got} subscribed={len(self.subscribed)}", flush=True)
            # 累积 tick 指标 (现手/外盘/内盘)
            try:
                self.market.update_tick_metrics()
            except Exception:
                pass
            # 保活：默认标的若掉订阅则补回
            self.ensure_subscribed(DEFAULT_SYMBOLS)
            # 对已订阅合约做快照写入缓存
            all_syms = list(self.subscribed)
            try:
                quotes = self.market.get_realtime_quotes(all_syms)
                with self.data_lock:
                    for q in quotes:
                        if "error" not in q and q.get("last_price", 0) > 0:
                            self.latest_data[q["symbol"]] = q
                        elif "error" in q:
                            s = q.get("symbol", "")
                            if s and s not in self.subscribed:
                                self.ensure_subscribed([s])
            except Exception as e:
                print(f"[账户:{self.tq_user}] 批量读取快照失败: {e}")
        except Exception as e:
            # 注意: 这里原先无条件 sleep(2)。扫描 A 股时若账号无权限, 天勤会持续抛
            # "您的账户不支持查看 XXX 的行情数据" -> 每次轮询卡 2 秒, 把整个主轮询线程
            # 拖垮(连期货实时行情也跟着变慢)。改为: 权限类报错不 sleep(属于数据缺失,
            # wait_update 本身已返回, 无需退避); 其他异常才退避, 且日志去重避免刷屏。
            msg = str(e)
            self._poll_err_cnt = getattr(self, '_poll_err_cnt', 0) + 1
            self._poll_err_last = getattr(self, '_poll_err_last', '')
            if msg != self._poll_err_last or self._poll_err_cnt % 50 == 1:
                print(f"[账户:{self.tq_user}] poll err: {msg}"
                      + (f" (重复 {self._poll_err_cnt} 次)" if self._poll_err_cnt > 1 else ""), flush=True)
            self._poll_err_last = msg
            if "不支持查看" in msg or "需要购买" in msg:
                pass          # 权限问题: 退避无意义, 立即继续轮询
            else:
                time.sleep(0.5)

    def _resolve_symbol(self, symbol: str) -> str:
        """把 KQ.m@ 连续合约解析为当前真实合约，再请求 K 线。

        天勤 get_kline_serial 不支持连续合约直接拉 K 线，但 Quote.underlying_symbol
        会给出当前对应的真实合约（如 KQ.m@DCE.m -> DCE.m2501）。
        """
        if not symbol or not symbol.startswith("KQ.m@"):
            return symbol
        real = self._cont_to_real.get(symbol)
        if real:
            return real
        if self.client is None or self.client.api is None:
            return symbol
        try:
            q = self.client.api.get_quote(symbol)
            real = getattr(q, "underlying_symbol", "") or symbol
            if real and real != symbol:
                self._cont_to_real[symbol] = real
                print(f"[连续合约映射] {symbol} -> {real}")
                return real
            print(f"[连续合约映射] {symbol} underlying_symbol 为空，使用原合约")
        except Exception as e:
            print(f"[连续合约映射] {symbol} 解析失败: {e}")
        return symbol

    def _kline_ready(self, k):
        """K线对象是否已有可展示数据(末尾K线时间戳>0 或 收盘价>0)"""
        try:
            return len(k) > 0 and float(k["datetime"].iloc[-1]) > 0
        except Exception:
            try:
                return len(k) > 0 and float(k["close"].iloc[-1]) > 0
            except Exception:
                return len(k) > 0

    def _get_kline_obj(self, symbol, duration, count, end_dt=None):
        """安全获取 Kline 对象：连续合约先解析真实合约；失败回退连续合约本身；
        任何异常都捕获返回 (None, real_symbol) 防止 500。end_dt 用于历史翻页。"""
        real = self._resolve_symbol(symbol)
        # 先试解析出的真实合约
        try:
            return self.client.get_kline(real, duration, count, end_dt=end_dt), real
        except Exception as e:
            print(f"[K线] 真实合约 {real} 获取失败: {e}")
        # 回退到连续合约本身(天勤支持对 KQ.m@ 直接拉 K 线)
        try:
            return self.client.get_kline(symbol, duration, count, end_dt=end_dt), symbol
        except Exception as e:
            print(f"[K线] 连续合约 {symbol} 也获取失败: {e}")
        return None, real

    def get_quotes(self):
        with self.data_lock:
            return list(self.latest_data.values())

    def get_kline(self, symbol, duration=86400, count=80):
        """实时K线: 仅注册订阅 + 轮询 Kline 对象就绪, 绝不自己调 wait_update
        (轮询线程已独占 wait_update, 请求线程再调会抛 'Another wait_update is running' 被吞,
        导致返回空 DataFrame → K 线无数据)。数据由轮询线程的 wait_update 驱动下载。"""
        import time
        if self.market is None or self.client is None or self.client.api is None:
            return None
        # 连续合约解析 + 安全获取 Kline 对象(失败回退连续合约本身, 不抛 500)
        k, real_symbol = self._get_kline_obj(symbol, duration, count)
        if k is None:
            print(f"[实时K线] {symbol}: 无法创建 Kline 对象(已回退仍失败)")
            return None

        # 关键：订阅与读取必须用同一 (symbol, duration, data_length)，否则天勤会创建不同
        # Kline 对象，实时那个对象可能还没被 wait_update 填充，导致返回空。
        deadline = time.time() + 6
        while time.time() < deadline:
            if self._kline_ready(k):
                break
            time.sleep(0.3)

        df = self.market.get_kline_data(real_symbol, duration, count)
        if df is not None and not df.empty:
            print(f"[实时K线] {symbol}({real_symbol}) 返回 {len(df)} 条")
            return df
        print(f"[实时K线] {symbol}({real_symbol}) 无数据(对象就绪={self._kline_ready(k)})")
        return df

    def get_history_kline(self, symbol, duration=86400, count=800):
        """历史K线: 走天勤TqSdk, 按真实合约返回; 无数据则回退主力连续.
        仅在请求线程注册订阅, 由轮询线程的 wait_update 驱动下载, 避免并发 wait_update 冲突."""
        import time
        if self.client is None or self.client.api is None:
            return None
        api = self.client.api
        # 连续合约需映射到真实合约
        real_symbol = self._resolve_symbol(symbol)

        def _fetch(sym):
            k, real = self._get_kline_obj(sym, duration, count)
            if k is None:
                return None
            deadline = time.time() + 6
            while time.time() < deadline:
                if self._kline_ready(k):
                    break
                time.sleep(0.3)
            return k

        try:
            df = _fetch(real_symbol)
            if df is None or not self._kline_ready(df):
                # 真实合约无数据 -> 回退该品种主力连续合约
                parts = symbol.split(".")
                if len(parts) >= 2:
                    exchange = parts[0]
                    contract = parts[-1]
                    variety = "".join(c for c in contract if not c.isdigit())
                    if variety:
                        cont = f"KQ.m@{exchange}.{variety}"
                        print(f"[历史K线] {symbol} 无数据, 回退主力连续 {cont}")
                        df = _fetch(cont)
            return df
        except Exception as e:
            print(f"[历史K线] TqSdk 获取失败 {symbol}: {e}")
            return None

    def add_access(self):
        self._access_count += 1

    def release_access(self):
        self._access_count = max(0, self._access_count - 1)

    def shutdown(self):
        self._stop = True


# ===== 全局账户Slot管理器 =====
_slots: dict = {}        # key:"user:pass" -> AccountSlot
_slots_lock = threading.Lock()

# 默认账户(启动时创建, 供无参数访问使用)
DEFAULT_ACCT_KEY = "zhubajie:jinhu303"

def get_slot(tq_user: str = "", tq_pass: str = ""):
    """获取或创建账户Slot(激活+轮询)"""
    if not tq_user or not tq_pass:
        key = DEFAULT_ACCT_KEY
    else:
        key = f"{tq_user}:{tq_pass}"

    with _slots_lock:
        if key not in _slots:
            user, pwd = key.split(":", 1)
            # 仅建对象(status="init")；真正连接 / 创建 TqApi 由主线程轮询循环统一驱动，
            # 避免在任何 Flask 请求线程里创建 TqApi 触发 TqSdk "wait_update 必须在创建线程调用" 的约束报错。
            slot = AccountSlot(user, pwd)
            _slots[key] = slot
        slot = _slots[key]
        slot.add_access()
    return slot


def _resolve_slot():
    """请求感知的账号解析：优先使用【网关注入的当前选中账号】(tq_user/tq_pass 查询参数)，
    回落到 DEFAULT_ACCT_KEY。用于替换各处写死的 DEFAULT_ACCT_KEY 取 slot 逻辑，
    使"页面右上角账户选择器"选中的账号真正生效。

    注意：网关(main_v4.py)在代理每个请求时会把 session 里 current_account_id 对应的
    tq_username/tq_password 以查询参数 tq_user/tq_pass 注入转发，所以此处读 request.args 即可拿到
    用户选中的账号，而非写死的 zhubajie。
    """
    tu = tp = ""
    try:
        if request and request.args.get('tq_user') and request.args.get('tq_pass'):
            tu = request.args.get('tq_user')
            tp = request.args.get('tq_pass')
    except Exception:
        pass
    if tu and tp:
        try:
            # 记住网关注入的凭据: 后台定时任务(全市场遍历)没有 request 上下文,
            # 只能靠这份记忆按用户名重建 Slot, 否则会回落 zhubajie(无 A 股权限) -> 遍历全空。
            try:
                import services.stock_kline as _sk
                _sk.remember_account(tu, tp)
            except Exception:
                pass
            return get_slot(tu, tp)
        except Exception as _e:
            print(f"[账户] 选中账号 {tu} 连接失败，回落默认: {_e}")
    try:
        return get_slot()
    except Exception:
        return None


# ===== HTML页面(通过网关代理时, h1/nav会被CSS隐藏) =====
HTML = r"""<!DOCTYPE html>
<html lang="zh"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>行情看板</title>
<style>
:root{
  --bg:#0b1120; --bg2:#0d1526; --surface:#111827; --surface2:#1b2538;
  --border:#27344b; --border-soft:#1f2a3d;
  --text:#e6edf7; --muted:#94a3b8; --accent:#38bdf8; --accent2:#f472b6; --violet:#c084fc;
  --up:#ff5b5b; --down:#22c55e; --gold:#fbbf24; --teal:#34d399;
}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Microsoft YaHei','PingFang SC',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;font-size:18px;line-height:1.55;overflow-x:hidden}
/* ===== 顶部状态条 ===== */
.topbar{position:sticky;top:0;z-index:30;display:flex;align-items:center;justify-content:space-between;gap:14px;padding:12px 18px;background:linear-gradient(135deg,rgba(17,24,39,.98),rgba(13,21,38,.98));backdrop-filter:blur(8px);border-bottom:1px solid var(--border)}
.brand{display:flex;align-items:center;gap:10px;font-size:20px;font-weight:800}
.brand .logo{width:32px;height:32px;border-radius:8px;background:linear-gradient(135deg,var(--accent),var(--violet));display:flex;align-items:center;justify-content:center;font-size:18px}
.brand b{background:linear-gradient(135deg,#7dd3fc,#c084fc);-webkit-background-clip:text;background-clip:text;color:transparent}
.topbar-right{display:flex;align-items:center;gap:12px;font-size:15px;color:var(--muted)}
.clock{font-variant-numeric:tabular-nums;font-weight:700;color:var(--text);font-size:17px}
.chip{display:inline-flex;align-items:center;gap:6px;padding:5px 11px;border-radius:20px;background:rgba(34,197,94,.12);border:1px solid rgba(34,197,94,.32);color:#4ade80;font-size:13px;font-weight:600}
.chip .dot{width:8px;height:8px;border-radius:50%;background:#4ade80;box-shadow:0 0 6px #4ade80;animation:pulse 2s infinite}
.chip.warn{background:rgba(251,191,36,.12);border-color:rgba(251,191,36,.32);color:var(--gold)}
.chip.warn .dot{background:var(--gold);box-shadow:0 0 6px var(--gold)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
/* ===== 导航 ===== */
.nav{display:flex;gap:8px;margin:12px 18px 0;flex-wrap:wrap}
.nav a{padding:10px 20px;border-radius:30px;background:var(--surface2);border:1px solid var(--border);color:var(--muted);text-decoration:none;font-size:16px;font-weight:600;transition:.2s}
.nav a:hover{color:var(--text);border-color:var(--accent)}
.nav a.active{background:linear-gradient(135deg,var(--accent),var(--violet));color:#08111f;border-color:transparent}
/* ===== 自选合约工具条 ===== */
.toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:14px 18px 12px;background:var(--surface2);border:1px solid var(--border);border-radius:12px;padding:12px 16px}
.toolbar input{flex:1;min-width:180px;max-width:260px;padding:11px 14px;border-radius:8px;border:1px solid var(--border);background:#0d1626;color:var(--text);font-size:16px}
.toolbar input:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px rgba(56,189,248,.15)}
.btn{padding:11px 18px;border-radius:8px;border:1px solid var(--border);background:#1b2538;color:var(--text);cursor:pointer;font-size:16px;font-weight:600;transition:.2s}
.btn:hover{background:#243049;border-color:var(--accent)}
.btn-primary{background:linear-gradient(135deg,var(--accent),var(--violet));color:#08111f;border:none;font-weight:700}
.btn-primary:hover{filter:brightness(1.08)}
.toolbar .lbl{font-size:14px;color:var(--muted);margin-left:4px;white-space:nowrap}
.toolbar select{background:#0d1626;color:var(--text);border:1px solid var(--border);border-radius:8px;padding:10px 12px;font-size:15px;cursor:pointer}
.toolbar select:focus{outline:none;border-color:var(--accent)}
/* ===== 终端主区域 ===== */
.terminal{display:flex;gap:10px;margin:0 18px 16px;min-height:620px;height:calc(100vh - 200px)}
.panel{background:var(--surface2);border:1px solid var(--border);border-radius:12px;overflow:hidden;display:flex;flex-direction:column}
/* 左侧自选 */
.watchlist{width:300px;min-width:280px}
.wl-title{padding:12px 14px;font-size:18px;font-weight:700;border-bottom:1px solid var(--border);background:linear-gradient(90deg,rgba(56,189,248,.1),transparent)}
.wl-toolbar{display:flex;gap:6px;padding:10px 12px;border-bottom:1px solid var(--border)}
.wl-toolbar input{flex:1;min-width:0;padding:9px 10px;border-radius:6px;border:1px solid var(--border);background:#0d1626;color:var(--text);font-size:15px}
.wl-toolbar button{padding:8px 12px;font-size:16px;line-height:1}
.wl-body{flex:1;overflow-y:auto;padding:6px}
.wl-empty{padding:20px;text-align:center;color:var(--muted);font-size:16px}
.wl-item{display:flex;flex-direction:column;gap:6px;padding:12px 14px;border-bottom:1px solid var(--border-soft);cursor:pointer;transition:.15s}
.wl-item:hover{background:#16213a}
.wl-item.active{background:rgba(56,189,248,.15);border-left:4px solid var(--accent)}
.wl-row1{display:flex;align-items:center;justify-content:space-between;gap:10px}
.wl-row2{display:flex;align-items:center;justify-content:space-between;gap:8px}
.wl-sym{font-size:17px;font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;letter-spacing:.3px}
.wl-name{font-size:14px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1}
.wl-right{display:flex;align-items:center;gap:10px;flex-shrink:0}
.wl-price{font-size:17px;font-weight:800;white-space:nowrap}
.wl-pct{font-size:15px;font-weight:700;white-space:nowrap;min-width:68px;text-align:right}
.wl-kline{padding:5px 10px;font-size:13px;line-height:1;border-radius:6px;margin:0}
/* 中间K线 */
.chart-area{flex:1;min-width:0}
.chart-toolbar{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 14px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.tabs{display:flex;gap:4px}
.tab{padding:8px 18px;border:none;background:#0d1626;color:var(--muted);cursor:pointer;font-size:15px;font-weight:600;border-radius:8px;transition:.2s}
.tab:hover{color:var(--text)}
.tab.active{background:linear-gradient(135deg,var(--accent),var(--violet));color:#08111f}
.chart-info{display:flex;gap:12px;font-size:14px;color:var(--muted);flex-wrap:wrap}
.chart-period{padding:7px 10px;border:1px solid var(--border);border-radius:8px;background:#0d1626;color:var(--muted);font-size:14px;cursor:pointer;outline:none;transition:.2s}
.chart-period:hover{color:#fff;border-color:var(--accent)}
.chart-period:disabled{opacity:.4;cursor:not-allowed}
.chart-info b{color:var(--text);font-variant-numeric:tabular-nums;margin-left:3px}
.chart-header{padding:10px 14px;display:flex;align-items:center;gap:12px;border-bottom:1px solid var(--border-soft)}
.symbol-name{color:var(--accent);font-size:20px;font-weight:800}
.chart-source{color:var(--muted);font-size:13px;margin-left:auto}
.chart-wrap{flex:1;position:relative;min-height:0;padding:8px}
#kc{display:block;width:100%;height:100%;border-radius:8px;background:#0b1120;border:1px solid var(--border)}
.ind-title{position:absolute;left:74px;bottom:4px;font-size:12px;color:#7c8aa3;cursor:pointer;background:rgba(13,22,38,.8);padding:2px 8px;border-radius:4px;border:1px solid var(--border);z-index:5}
.ind-title:hover{color:#fff;border-color:var(--accent)}
.ind-values{position:absolute;left:74px;bottom:24px;font-size:12px;color:#e6edf7;background:rgba(13,22,38,.85);padding:3px 10px;border-radius:4px;border:1px solid var(--border);z-index:5;pointer-events:none;white-space:nowrap}
.ind-menu{position:absolute;left:10px;bottom:30px;width:520px;max-width:90vw;background:#0d1626;border:1px solid var(--border);border-radius:10px;box-shadow:0 10px 30px rgba(0,0,0,.5);padding:14px;z-index:20;display:none;grid-template-columns:repeat(4,1fr);gap:8px}
.ind-menu.show{display:grid}
.ind-menu.vol-menu{grid-template-columns:repeat(3,1fr);width:360px}
.ind-menu h4{grid-column:1/-1;margin:0 0 6px;font-size:13px;color:var(--accent)}
.ind-menu .ind-item{padding:5px 8px;border-radius:5px;font-size:13px;color:var(--text);cursor:pointer;text-align:center;border:1px solid transparent;white-space:nowrap}
.ind-menu .ind-item:hover{background:#1a2744;border-color:var(--accent)}
.ind-menu .ind-item.active{background:linear-gradient(135deg,var(--accent),var(--violet));color:#08111f;font-weight:700}
.ind-overlay{position:fixed;inset:0;z-index:15;display:none}
.ind-overlay.show{display:block}
/* ===== 划线分析工具栏 ===== */
.draw-btn{padding:7px 12px;border-radius:6px;border:1px solid var(--border);background:#1b2538;color:var(--muted);cursor:pointer;font-size:14px;font-weight:600;transition:.2s}
.draw-btn:hover,.draw-btn.active{color:#fff;border-color:var(--accent);background:#243049}
.draw-panel{position:absolute;right:10px;top:10px;width:240px;background:rgba(13,22,38,.96);border:1px solid var(--border);border-radius:10px;box-shadow:0 10px 30px rgba(0,0,0,.5);padding:10px;z-index:20;display:none;grid-template-columns:repeat(4,1fr);gap:6px}
.draw-panel.show{display:grid}
.draw-panel h4{grid-column:1/-1;margin:0 0 4px;font-size:13px;color:var(--accent)}
.draw-tool{padding:8px 4px;border-radius:6px;border:1px solid var(--border-soft);background:#111827;color:var(--text);cursor:pointer;font-size:18px;text-align:center;transition:.15s}
.draw-tool:hover{border-color:var(--accent);background:#1a2744}
.draw-tool.active{border-color:var(--accent);background:rgba(56,189,248,.15);color:var(--accent)}
.draw-colors{grid-column:1/-1;display:flex;gap:6px;align-items:center;justify-content:center;padding-top:4px}
.draw-colors span{width:18px;height:18px;border-radius:50%;cursor:pointer;border:2px solid transparent}
.draw-colors span.active{border-color:#fff}
.draw-overlay{position:fixed;inset:0;z-index:19;display:none}
.draw-overlay.show{display:block}
/* 右侧盘口 (WH6 风格) */
.orderbook{width:320px;min-width:300px}
.ob-block{flex-shrink:0;background:#0f1729}
.ob-title{padding:7px 14px;font-size:14px;font-weight:700;background:#0d1626;border-bottom:1px solid var(--border-soft);color:var(--accent)}
.ob-head{display:grid;grid-template-columns:48px 1fr 1fr;padding:5px 14px;font-size:12px;color:var(--muted);border-bottom:1px solid var(--border-soft)}
.ob-row{display:grid;grid-template-columns:48px 1fr 1fr;padding:6px 14px;font-size:15px;border-bottom:1px solid var(--border-soft);align-items:center}
.ob-row .rank{font-size:12px;color:var(--muted)}
.ob-row .price{font-weight:700;text-align:right;padding-right:6px;font-variant-numeric:tabular-nums}
.ob-row .vol{text-align:right;color:var(--muted);font-variant-numeric:tabular-nums}
.ob-ask .price{color:var(--up)}
.ob-bid .price{color:var(--down)}
.ob-divider{height:4px;background:var(--border);flex-shrink:0}
/* 实时概览条 */
.ob-quote{flex-shrink:0;display:flex;align-items:baseline;gap:14px;padding:8px 14px;background:linear-gradient(90deg,rgba(56,189,248,.08),transparent);border-bottom:1px solid var(--border-soft)}
.ob-quote .q-last{font-size:26px;font-weight:800;font-variant-numeric:tabular-nums;line-height:1}
.ob-quote .q-chg{font-size:16px;font-weight:700}
.ob-quote .q-pct{font-size:15px;font-weight:700}
/* 明细网格 */
.ob-grid{flex:1 1 0;overflow-y:auto;padding:8px 10px;display:grid;grid-template-columns:1fr 1fr;gap:4px 14px;align-content:start}
.ob-cell{display:flex;justify-content:space-between;align-items:baseline;gap:8px;padding:4px 4px;border-bottom:1px dashed var(--border-soft)}
.ob-cell .k{font-size:13px;color:var(--muted);white-space:nowrap}
.ob-cell .v{font-size:15px;font-weight:700;font-variant-numeric:tabular-nums;text-align:right}
.ob-cell .v.up{color:var(--up)}
.ob-cell .v.down{color:var(--down)}
.ob-cell .v.hl{color:var(--gold)}
/* 占位提示 */
.ob-placeholder{padding:14px;font-size:13px;color:var(--muted);background:rgba(251,191,36,.06);border:1px dashed rgba(251,191,36,.3);border-radius:8px;margin:8px 10px;line-height:1.5}
/* ===== 表格 ===== */
.up{color:var(--up);font-weight:700}
.down{color:var(--down);font-weight:700}
/* ===== 状态栏 ===== */
#status{margin:0 18px 18px;font-size:14px;color:var(--muted)}
/* ===== 移动端 ===== */
@media(max-width:1024px){
  .terminal{flex-wrap:wrap;height:auto;min-height:auto}
  .watchlist{width:100%;height:220px;margin-bottom:10px}
  .wl-item{padding:10px 12px}
  .chart-area{min-width:100%;height:520px;margin-bottom:10px}
  .orderbook{width:100%;height:360px;flex-direction:row}
  .ob-block{flex:1;height:100%}
  .ob-divider{display:none}
}
@media(max-width:480px){
  body{font-size:19px}
  .topbar{padding:10px 12px}
  .nav a{font-size:15px;padding:9px 14px}
  .toolbar{margin:10px 12px;padding:10px}
  .toolbar input{max-width:none}
  .terminal{margin:0 12px 12px}
  .chart-area{height:420px}
  .orderbook{height:auto;flex-direction:column}
}
</style></head><body>
<div class="topbar">
  <div class="brand"><span class="logo">📈</span><b>行情看板</b></div>
  <div class="topbar-right">
    <span class="chip" id="conn-chip"><span class="dot"></span><span id="conn-text">连接中</span></span>
    <span id="cur-sym" style="color:#9fd4ff;font-weight:600">未选标的</span>
    <span class="clock" id="clock">--:--:--</span>
  </div>
</div>
<nav class="nav">
  <a href="/" class="active">📊 行情看板</a>
  <a href="/option-expiry">📅 期货到期表</a>
</nav>

<div class="toolbar">
  <input type="text" id="si" placeholder="输入合约代码，如 SHFE.cu2501">
  <button class="btn btn-primary" onclick="add()">➕ 添加</button>
  <button class="btn" onclick="rem()">➖ 移除</button>
  <span class="lbl">刷新:</span>
  <select id="refresh-interval" onchange="changeInterval()">
    <option value="500">0.5秒</option><option value="1000">1秒</option><option value="2000">2秒</option>
    <option value="3000" selected>3秒</option><option value="5000">5秒</option><option value="10000">10秒</option>
  </select>
</div>

<div class="terminal">
  <!-- 左侧自选 -->
  <div class="panel watchlist">
    <div class="wl-title">📋 自选合约</div>
    <div class="wl-toolbar">
      <input type="text" id="si2" placeholder="代码">
      <button class="btn btn-primary" onclick="add2()">+</button>
      <button class="btn" onclick="rem2()">−</button>
    </div>
    <div class="wl-body" id="wl-body"><div class="wl-empty">加载中...</div></div>
  </div>
  <!-- 中间K线 -->
  <div class="panel chart-area">
    <div class="chart-toolbar">
      <div class="tabs">
        <button class="tab active" id="tab-realtime" onclick="switchTab('realtime')">实时K线</button>
        <button class="tab" id="tab-history" onclick="switchTab('history')">历史K线</button>
      </div>
      <select class="chart-period" id="kline-duration" onchange="changePeriod()">
        <option value="60">1分钟</option>
        <option value="300">5分钟</option>
        <option value="900">15分钟</option>
        <option value="1800">30分钟</option>
        <option value="3600">1小时</option>
        <option value="86400" selected>日线</option>
        <option value="604800">周线</option>
        <option value="2592000">月线</option>
      </select>
      <button class="draw-btn" id="drawBtn" onclick="toggleDrawPanel(event)">✏ 划线分析</button>
      <div class="chart-info" id="ohlc-info">
        <span>开 <b id="ohlc-o">--</b></span><span>高 <b id="ohlc-h">--</b></span>
        <span>低 <b id="ohlc-l">--</b></span><span>收 <b id="ohlc-c">--</b></span><span>量 <b id="ohlc-v">--</b></span>
      </div>
    </div>
    <div class="chart-header">
      <span class="symbol-name" id="ohlc-symbol">--</span>
      <span class="chart-source" id="kline-source"></span>
    </div>
    <div class="chart-wrap">
      <canvas id="kc"></canvas>
      <div class="ind-values" id="indValues"></div>
      <div class="ind-title" id="indTitle" onclick="toggleIndMenu(event)">MACD(12,26,9) ▾</div>
      <div class="ind-overlay" id="indOverlay" onclick="closeIndMenu()"></div>
      <div class="draw-overlay" id="drawOverlay" onclick="closeDrawPanel()"></div>
      <div class="draw-panel" id="drawPanel">
        <h4>划线分析</h4>
        <div class="draw-tool" data-tool="line" title="直线" onclick="selectDrawTool('line')">↗</div>
        <div class="draw-tool" data-tool="ray" title="射线" onclick="selectDrawTool('ray')">⇗</div>
        <div class="draw-tool" data-tool="segment" title="线段" onclick="selectDrawTool('segment')">╱</div>
        <div class="draw-tool" data-tool="hline" title="水平线" onclick="selectDrawTool('hline')">━</div>
        <div class="draw-tool" data-tool="vline" title="垂直线" onclick="selectDrawTool('vline')">┃</div>
        <div class="draw-tool" data-tool="rect" title="矩形" onclick="selectDrawTool('rect')">▭</div>
        <div class="draw-tool" data-tool="text" title="文字" onclick="selectDrawTool('text')">T</div>
        <div class="draw-tool" data-tool="erase" title="清空" onclick="clearDrawings()">✕</div>
        <div class="draw-colors">
          <span style="background:#fbbf24" data-color="#fbbf24" onclick="selectDrawColor('#fbbf24')" class="active"></span>
          <span style="background:#38bdf8" data-color="#38bdf8" onclick="selectDrawColor('#38bdf8')"></span>
          <span style="background:#ff5b5b" data-color="#ff5b5b" onclick="selectDrawColor('#ff5b5b')"></span>
          <span style="background:#22c55e" data-color="#22c55e" onclick="selectDrawColor('#22c55e')"></span>
          <span style="background:#c084fc" data-color="#c084fc" onclick="selectDrawColor('#c084fc')"></span>
          <span style="background:#e6edf7" data-color="#e6edf7" onclick="selectDrawColor('#e6edf7')"></span>
        </div>
      </div>
      <div class="ind-menu" id="indMenu">
        <h4>价格指标</h4>
        <div class="ind-item" data-key="macd" onclick="selectInd('macd')">MACD</div>
        <div class="ind-item" data-key="rsi" onclick="selectInd('rsi')">RSI</div>
        <div class="ind-item" data-key="kdj" onclick="selectInd('kdj')">KDJ</div>
        <div class="ind-item" data-key="wr" onclick="selectInd('wr')">WR</div>
        <div class="ind-item" data-key="mtm" onclick="selectInd('mtm')">MTM</div>
        <div class="ind-item" data-key="dmi" onclick="selectInd('dmi')">DMI</div>
        <div class="ind-item" data-key="bias" onclick="selectInd('bias')">BIAS</div>
        <div class="ind-item" data-key="psy" onclick="selectInd('psy')">PSY</div>
        <div class="ind-item" data-key="arbr" onclick="selectInd('arbr')">ARBR</div>
        <div class="ind-item" data-key="roc" onclick="selectInd('roc')">ROC</div>
        <div class="ind-item" data-key="adtm" onclick="selectInd('adtm')">ADTM</div>
        <div class="ind-item" data-key="asi" onclick="selectInd('asi')">ASI</div>
        <div class="ind-item" data-key="atr" onclick="selectInd('atr')">ATR</div>
        <div class="ind-item" data-key="b3612" onclick="selectInd('b3612')">B3612</div>
        <div class="ind-item" data-key="cr" onclick="selectInd('cr')">CR</div>
        <div class="ind-item" data-key="dbcd" onclick="selectInd('dbcd')">DBCD</div>
        <div class="ind-item" data-key="ddi" onclick="selectInd('ddi')">DDI</div>
        <div class="ind-item" data-key="dma" onclick="selectInd('dma')">DMA</div>
        <div class="ind-item" data-key="mi" onclick="selectInd('mi')">MI</div>
        <div class="ind-item" data-key="micd" onclick="selectInd('micd')">MICD</div>
        <div class="ind-item" data-key="qhlsr" onclick="selectInd('qhlsr')">QHLSR</div>
        <div class="ind-item" data-key="rc" onclick="selectInd('rc')">RC</div>
        <div class="ind-item" data-key="rccd" onclick="selectInd('rccd')">RCCD</div>
        <div class="ind-item" data-key="srdm" onclick="selectInd('srdm')">SRDM</div>
        <div class="ind-item" data-key="zdzb" onclick="selectInd('zdzb')">ZDZB</div>
        <div class="ind-item" data-key="cci" onclick="selectInd('cci')">CCI</div>
        <div class="ind-item" data-key="lon" onclick="selectInd('lon')">LON</div>
        <div class="ind-item" data-key="short" onclick="selectInd('short')">SHORT</div>
        <h4>量能指标</h4>
        <div class="ind-item" data-key="cjl" onclick="selectInd('cjl')">CJL</div>
        <div class="ind-item" data-key="ccl" onclick="selectInd('ccl')">CCL</div>
        <div class="ind-item" data-key="mv" onclick="selectInd('mv')">MV</div>
        <div class="ind-item" data-key="obv" onclick="selectInd('obv')">OBV</div>
        <div class="ind-item" data-key="vr" onclick="selectInd('vr')">VR</div>
        <div class="ind-item" data-key="vroc" onclick="selectInd('vroc')">VROC</div>
        <div class="ind-item" data-key="vrsi" onclick="selectInd('vrsi')">VRSI</div>
        <div class="ind-item" data-key="wvad" onclick="selectInd('wvad')">WVAD</div>
      </div>
    </div>
  </div>
  <!-- 右侧盘口 (WH6 风格) -->
  <div class="panel orderbook">
    <!-- 卖盘 5 档 -->
    <div class="ob-block">
      <div class="ob-head"><span>卖档</span><span>价格</span><span>数量</span></div>
      <div id="ask-list"></div>
    </div>
    <!-- 实时概览 -->
    <div class="ob-quote">
      <span class="q-last" id="ob-last">--</span>
      <span class="q-chg" id="ob-chg">--</span>
      <span class="q-pct" id="ob-pct">--</span>
    </div>
    <!-- 买盘 5 档 -->
    <div class="ob-block">
      <div class="ob-head"><span>买档</span><span>价格</span><span>数量</span></div>
      <div id="bid-list"></div>
    </div>
    <!-- 明细网格 -->
    <div class="ob-grid" id="ob-grid"></div>
    <!-- 股票/行业板块占位 (免费版天勤不支持, 后续付费升级) -->
    <div class="ob-placeholder" id="ob-placeholder">
      ⚠ 股票 / 行业板块 / 概念板块：当前天勤免费版不提供，已预留入口，后续开通付费账户后统一接入。
    </div>
  </div>
</div>
<div id="status">初始化中...</div>
<script>
var curSym='',curTab='realtime',curDuration=86400,klineData=[],klinePollTimer=null,currentPollMs=3000,lastQuotes=[];
window.klineHoverIndex=-1;window.klineIndicators=null;window.klineLayout=null;
var INIT_SYM=new URLSearchParams(location.search).get('symbol')||'';
// ===== 划线分析状态 =====
var drawTool='',drawColor='#fbbf24',drawings=[],drawState={active:false,tool:'',start:null,end:null,temp:null};
var DRAW_TOOLS={line:'直线',ray:'射线',segment:'线段',hline:'水平线',vline:'垂直线',rect:'矩形',text:'文字'};
function pad2(n){return n<10?'0'+n:''+n;}
function tickClock(){var d=new Date();document.getElementById('clock').textContent=pad2(d.getHours())+':'+pad2(d.getMinutes())+':'+pad2(d.getSeconds());}
setInterval(tickClock,1000);tickClock();
function fmtPrice(v){return isFinite(v)?v.toFixed(2):'--';}
function fmtVol(v){return (v!=null&&isFinite(v))?Number(v).toLocaleString():'--';}
function r(){
  fetch('/api/quotes').then(function(x){return x.json();}).then(function(d){
    if(d.code==-1){document.getElementById('status').textContent='错误: '+d.msg;setConn(false,d.msg);return;}
    lastQuotes=d.data||[];
    var h='';
    d.data.forEach(function(q){
      var u=q.pct_change>=0?'up':'down',sig=q.pct_change>0?'+':'';
      var lp=isFinite(q.last_price)?q.last_price.toFixed(2):'--';
      var active=(q.symbol===curSym)?' active':'';
      h+='<div class="wl-item'+active+'" data-sym="'+q.symbol+'" onclick="loadKline(\''+q.symbol+'\',\'realtime\')">'+
        '<div class="wl-row1"><span class="wl-sym">'+q.symbol+'</span><div class="wl-right"><span class="wl-price '+u+'">'+lp+'</span><span class="wl-pct '+u+'">'+sig+(q.pct_change).toFixed(2)+'%</span></div></div>'+
        '<div class="wl-row2"><span class="wl-name">'+(q.name||'')+'</span><button class="btn btn-primary wl-kline" onclick="event.stopPropagation();loadKline(\''+q.symbol+'\',\'realtime\')">K线</button></div></div>';
    });
    document.getElementById('wl-body').innerHTML=h||'<div class="wl-empty">无数据</div>';
    document.getElementById('status').textContent='更新: '+new Date().toLocaleTimeString()+' | TqSdk';
    setConn(true,'已连接');
    if(curSym){var q=d.data.find(function(x){return x.symbol===curSym});if(q)renderOrderbook(q);}
    else if(d.data&&d.data.length){loadKline(d.data[0].symbol,'realtime');}
    if(INIT_SYM){loadKline(INIT_SYM,'realtime');}
    syncPeriodOption();
  }).catch(function(e){document.getElementById('status').textContent='连接中...';setConn(false,'连接中');});
}
function setConn(ok,text){var c=document.getElementById('conn-chip');document.getElementById('conn-text').textContent=text;if(ok){c.className='chip';}else{c.className='chip warn';}}
function renderOrderbook(q){
  var last=q.last_price||0,pre=q.pre_close||last,u=q.pct_change>=0?'up':'down';
  var askHtml='',bidHtml='';
  for(var i=5;i>=1;i--){
    var ap=q['ask_price'+i],av=q['ask_volume'+i];
    askHtml+='<div class="ob-row ob-ask"><span class="rank">卖'+i+'</span><span class="price">'+fmtPrice(ap)+'</span><span class="vol">'+fmtVol(av)+'</span></div>';
  }
  for(var i=1;i<=5;i++){
    var bp=q['bid_price'+i],bv=q['bid_volume'+i];
    bidHtml+='<div class="ob-row ob-bid"><span class="rank">买'+i+'</span><span class="price">'+fmtPrice(bp)+'</span><span class="vol">'+fmtVol(bv)+'</span></div>';
  }
  document.getElementById('ask-list').innerHTML=askHtml;
  document.getElementById('bid-list').innerHTML=bidHtml;
  // 实时概览条
  var el=document.getElementById('ob-last');el.textContent=fmtPrice(last);el.className='q-last '+u;
  var chg=(last&&pre)?(last-pre):null;
  var ec=document.getElementById('ob-chg');ec.textContent=(chg!=null?(chg>=0?'+':'')+chg.toFixed(2):'--');ec.className='q-chg '+u;
  var pc=document.getElementById('ob-pct');pc.textContent=(q.pct_change!=null?(q.pct_change>=0?'+':'')+q.pct_change.toFixed(2)+'%':'--');pc.className='q-pct '+u;
  // 明细网格
  function cell(k,v,cls){return '<div class="ob-cell"><span class="k">'+k+'</span><span class="v '+(cls||'')+'">'+v+'</span></div>';}
  var todayChg=(last&&pre)?(last-pre):0;
  var dirc=todayChg>=0?'up':'down';
  var cells='';
  cells+=cell('现手',fmtVol(q.last_volume));
  cells+=cell('总手',fmtVol(q.volume));
  cells+=cell('持仓',fmtVol(q.open_interest));
  cells+=cell('日增',fmtVol(q.day_increase));
  cells+=cell('最高',fmtPrice(q.high),'up');
  cells+=cell('最低',fmtPrice(q.low),'down');
  cells+=cell('开盘',fmtPrice(q.open));
  cells+=cell('均价',fmtPrice(q.average));
  cells+=cell('结算价',fmtPrice(q.settlement),'hl');
  cells+=cell('昨收',fmtPrice(q.pre_close));
  cells+=cell('外盘',fmtVol(q.outer_volume));
  cells+=cell('内盘',fmtVol(q.inner_volume));
  cells+=cell('昨结',fmtPrice(q.pre_settlement),'hl');
  cells+=cell('涨停',fmtPrice(q.upper_limit),'up');
  cells+=cell('跌停',fmtPrice(q.lower_limit),'down');
  cells+=cell('杠杆',(q.leverage!=null?q.leverage+'x':'--'),'hl');
  cells+=cell('时间',(q.update_time||'--'));
  document.getElementById('ob-grid').innerHTML=cells;
  // 股票/行业板块占位: 仅当标的为股票类(SSE/SZSE)时显示
  var place=document.getElementById('ob-placeholder');
  if(place){place.style.display=(/^SSE\.|^SZSE\./.test(q.symbol||''))?'block':'none';}
}
function renderTrades(){
  var list=document.getElementById('trade-list');
  if(!list)return;  // 元素已移除(WH6 风格盘口不再单独展示成交明细)
  if(!klineData||!klineData.length){list.innerHTML='<div class="trade-row"><span class="t-time">--</span><span class="t-price">--</span><span class="t-vol">--</span></div>';return;}
  var h='';
  for(var i=Math.max(0,klineData.length-8);i<klineData.length;i++){
    var d=klineData[i],dt=String(d.datetime||d.date||'');
    if(dt.length>6&&dt.indexOf('-')<0)dt=dt.substring(4,6)+'-'+dt.substring(6,8);
    h+='<div class="trade-row"><span class="t-time">'+dt+'</span><span class="t-price">'+(isFinite(d.close)?d.close.toFixed(2):'--')+'</span><span class="t-vol">'+fmtVol(d.volume)+'</span></div>';
  }
  list.innerHTML=h;
}
function switchTab(tab){curTab=tab;document.getElementById('tab-realtime').classList.toggle('active',tab==='realtime');document.getElementById('tab-history').classList.toggle('active',tab==='history');syncPeriodOption();if(curSym)loadKline(curSym);}
function syncPeriodOption(){var sel=document.getElementById('kline-duration');if(!sel)return;var mo=sel.querySelector('option[value="2592000"]');if(mo)mo.disabled=(curTab==='realtime');}
function changePeriod(){var sel=document.getElementById('kline-duration');if(!sel)return;curDuration=parseInt(sel.value,10)||86400;if(curSym)loadKline(curSym);}
function loadKline(sym,tab){if(tab)curTab=tab;if(sym!==curSym){if(klinePollTimer)clearInterval(klinePollTimer);curSym=sym;klinePollTimer=setInterval(fetchKlineData,currentPollMs);}document.querySelectorAll('.wl-item').forEach(function(el){el.classList.toggle('active',el.getAttribute('data-sym')===sym);});document.getElementById('tab-realtime').classList.toggle('active',curTab==='realtime');document.getElementById('tab-history').classList.toggle('active',curTab==='history');document.getElementById('ohlc-symbol').textContent=sym;document.getElementById('cur-sym').textContent=sym;var q=lastQuotes.find(function(x){return x.symbol===sym});if(q)renderOrderbook(q);fetchKlineData();}
function fetchKlineData(){if(!curSym)return;var url=curTab==='realtime'?'/api/kline?symbol='+encodeURIComponent(curSym)+'&count=100&duration='+curDuration:'/api/kline/history?symbol='+encodeURIComponent(curSym)+'&duration='+curDuration;fetch(url).then(function(x){return x.json();}).then(function(d){klineData=d.data||[];window.klineIndicators=d.indicators||null;var src=document.getElementById('kline-source');src.textContent=(d.source||'')+(klineData.length?(' · '+klineData.length+'根'):'')+(d.msg?(' · '+d.msg):'');if(d.code===-1){src.textContent='错误: '+(d.msg||'未知');}drawKline(klineData,curSym);renderTrades();}).catch(function(e){klineData=[];window.klineIndicators=null;drawKline([],curSym);renderTrades();document.getElementById('kline-source').textContent='加载失败';});}
// ===== 副图指标配置 (与后端 _compute_all_indicators 返回的键对齐) =====
var curInd='macd';
var IND_CONFIG={
  macd:{name:'MACD(12,26,9)',zeroLine:true,series:[{key:'bar',type:'bar',up:'#ff5b5b',down:'#22c55e'},{key:'dif',type:'line',color:'#fbbf24'},{key:'dea',type:'line',color:'#38bdf8'}]},
  rsi:{name:'RSI(14)',min:0,max:100,levels:[30,50,70],series:[{key:'rsi',type:'line',color:'#fbbf24'}]},
  kdj:{name:'KDJ(9,3,3)',min:0,max:100,levels:[20,50,80],series:[{key:'k',type:'line',color:'#fbbf24'},{key:'d',type:'line',color:'#38bdf8'},{key:'j',type:'line',color:'#f472b6'}]},
  wr:{name:'WR(14)',min:-100,max:0,levels:[-80,-50,-20],series:[{key:'wr',type:'line',color:'#fbbf24'}]},
  mtm:{name:'MTM(12)',zeroLine:true,series:[{key:'mtm',type:'line',color:'#fbbf24'},{key:'mtmma',type:'line',color:'#38bdf8'}]},
  roc:{name:'ROC(12)',zeroLine:true,series:[{key:'roc',type:'line',color:'#fbbf24'}]},
  bias:{name:'BIAS(6,12,24)',zeroLine:true,series:[{key:'bias6',type:'line',color:'#fbbf24'},{key:'bias12',type:'line',color:'#38bdf8'},{key:'bias24',type:'line',color:'#f472b6'}]},
  psy:{name:'PSY(12)',min:0,max:100,levels:[25,50,75],series:[{key:'psy',type:'line',color:'#fbbf24'}]},
  arbr:{name:'ARBR(26)',series:[{key:'ar',type:'line',color:'#fbbf24'},{key:'br',type:'line',color:'#38bdf8'}]},
  dmi:{name:'DMI(14)',min:0,max:100,levels:[20,50],series:[{key:'pdi',type:'line',color:'#fbbf24'},{key:'mdi',type:'line',color:'#38bdf8'},{key:'adx',type:'line',color:'#f472b6'}]},
  cci:{name:'CCI(20)',zeroLine:true,levels:[-100,100],series:[{key:'cci',type:'line',color:'#fbbf24'}]},
  dma:{name:'DMA(10,50)',zeroLine:true,series:[{key:'dma',type:'line',color:'#fbbf24'},{key:'ama',type:'line',color:'#38bdf8'}]},
  atr:{name:'ATR(14)',series:[{key:'atr',type:'line',color:'#fbbf24'}]},
  mi:{name:'MI(25,9)',series:[{key:'mi',type:'line',color:'#fbbf24'}]},
  adtm:{name:'ADTM(23,8)',zeroLine:true,series:[{key:'adtm',type:'line',color:'#fbbf24'}]},
  asi:{name:'ASI',zeroLine:true,series:[{key:'asi',type:'line',color:'#fbbf24'}]},
  b3612:{name:'B3612',zeroLine:true,series:[{key:'b36',type:'line',color:'#fbbf24'},{key:'b12',type:'line',color:'#38bdf8'}]},
  cr:{name:'CR(26)',series:[{key:'cr',type:'line',color:'#fbbf24'}]},
  dbcd:{name:'DBCD',zeroLine:true,series:[{key:'dbcd',type:'line',color:'#fbbf24'}]},
  ddi:{name:'DDI',zeroLine:true,series:[{key:'ddi',type:'line',color:'#fbbf24'}]},
  micd:{name:'MICD',zeroLine:true,series:[{key:'micd',type:'line',color:'#fbbf24'}]},
  qhlsr:{name:'QHLSR(10)',min:0,max:100,levels:[20,50,80],series:[{key:'qhlsr',type:'line',color:'#fbbf24'}]},
  rc:{name:'RC(12)',zeroLine:true,series:[{key:'rc',type:'line',color:'#fbbf24'}]},
  rccd:{name:'RCCD',zeroLine:true,series:[{key:'rccd',type:'line',color:'#fbbf24'}]},
  srdm:{name:'SRDM',zeroLine:true,series:[{key:'srdm',type:'line',color:'#fbbf24'}]},
  zdzb:{name:'ZDZB',zeroLine:true,series:[{key:'zdzb',type:'line',color:'#fbbf24'}]},
  lon:{name:'LON',zeroLine:true,series:[{key:'lon',type:'line',color:'#fbbf24'}]},
  short:{name:'SHORT',zeroLine:true,series:[{key:'short',type:'line',color:'#fbbf24'}]},
  cjl:{name:'CJL(成交量)',barFromZero:true,series:[{key:'vol',type:'bar',up:'#ff5b5b',down:'#22c55e'}]},
  ccl:{name:'CCL(持仓量)',barFromZero:true,series:[{key:'oi',type:'bar',up:'#ff5b5b',down:'#22c55e'}]},
  mv:{name:'MV(均量)',barFromZero:true,series:[{key:'mv5',type:'line',color:'#fbbf24'},{key:'mv10',type:'line',color:'#38bdf8'}]},
  obv:{name:'OBV',series:[{key:'obv',type:'line',color:'#fbbf24'}]},
  vr:{name:'VR(26)',min:0,max:200,levels:[70,100,150],series:[{key:'vr',type:'line',color:'#fbbf24'}]},
  vroc:{name:'VROC(12)',zeroLine:true,series:[{key:'vroc',type:'line',color:'#fbbf24'}]},
  vrsi:{name:'VRSI(14)',min:0,max:100,levels:[30,50,70],series:[{key:'vrsi',type:'line',color:'#fbbf24'}]},
  wvad:{name:'WVAD(24)',zeroLine:true,series:[{key:'wvad',type:'line',color:'#fbbf24'}]}
};
function selectInd(key){
  curInd=key;
  var cfg=IND_CONFIG[key];
  var t=document.getElementById('indTitle'); if(t)t.textContent=(cfg?cfg.name:key)+' ▾';
  document.querySelectorAll('#indMenu .ind-item').forEach(function(el){el.classList.toggle('active',el.getAttribute('data-key')===key);});
  closeIndMenu();
  if(klineData&&klineData.length)drawKline(klineData,curSym);
}
function toggleIndMenu(e){if(e)e.stopPropagation();var m=document.getElementById('indMenu'),o=document.getElementById('indOverlay');if(!m)return;m.classList.toggle('show');o.classList.toggle('show');updateIndMenuActive();}
function closeIndMenu(){var m=document.getElementById('indMenu'),o=document.getElementById('indOverlay');if(m)m.classList.remove('show');if(o)o.classList.remove('show');}
function updateIndMenuActive(){document.querySelectorAll('#indMenu .ind-item').forEach(function(el){el.classList.toggle('active',el.getAttribute('data-key')===curInd);});}
function indValue(key,sub,i){var ind=window.klineIndicators||{};return (ind[key]&&ind[key][sub]&&ind[key][sub][i]!=null)?ind[key][sub][i]:null;}
function updateIndValues(idx){
  var el=document.getElementById('indValues');if(!el)return;
  var cfg=IND_CONFIG[curInd],ind=window.klineIndicators||{},id=ind[curInd]||{};
  if(!cfg){el.innerHTML='';return;}
  var parts=['<b>'+cfg.name.split('(')[0]+'</b>'];
  cfg.series.forEach(function(s){
    var v=(id[s.key]&&id[s.key][idx]!=null)?id[s.key][idx]:null;
    var color;
    if(s.type==='bar'){color=(v!=null&&v<0)?(s.down||'#22c55e'):(s.up||'#ff5b5b');}
    else{color=s.color||'#e6edf7';}
    parts.push('<span style="color:'+color+'">'+s.key.toUpperCase()+':'+(v!=null&&isFinite(v)?v.toFixed(3):'--')+'</span>');
  });
  el.innerHTML=parts.join(' ');
}
function renderSubIndicator(ctx,bars,top,h,left,right,gap,barW,cfg){
  cfg=cfg||IND_CONFIG[curInd]; if(!cfg||!bars.length)return;
  var n=bars.length,ind=window.klineIndicators||{},id=ind[curInd]||{},allV=[],hasBar=false,barFromZero=!!cfg.barFromZero;
  cfg.series.forEach(function(s){if(s.type==='bar')hasBar=true; var arr=id[s.key]||[]; for(var i=0;i<n;i++){var v=arr[i]; if(v!=null&&isFinite(v))allV.push(v);} });
  var yMin,yMax;
  if(cfg.min!=null&&cfg.max!=null){yMin=cfg.min;yMax=cfg.max;}
  else if(barFromZero){yMin=0;yMax=allV.length?Math.max.apply(null,allV):1; var pad=yMax*0.08||1;yMax+=pad;}
  else if(hasBar){var vmax=allV.length?Math.max.apply(null,allV.map(Math.abs)):1; if(vmax===0)vmax=1; yMin=-vmax;yMax=vmax;}
  else if(allV.length){yMin=Math.min.apply(null,allV);yMax=Math.max.apply(null,allV); var pad=(yMax-yMin)*0.08||Math.abs(yMax)*0.02; yMin-=pad;yMax+=pad;}
  else{yMin=0;yMax=100;}
  if(yMin===yMax)yMax=yMin+1;
  function sy(y){return top+h*(1-(y-yMin)/(yMax-yMin));}
  ctx.strokeStyle='#1c2740';ctx.lineWidth=0.5;
  for(var t=0;t<=4;t++){var yy=top+h*t/4;ctx.beginPath();ctx.moveTo(left,yy);ctx.lineTo(right,yy);ctx.stroke();}
  if(cfg.zeroLine||hasBar){ctx.strokeStyle='#5b6b85';ctx.lineWidth=0.5;ctx.beginPath();ctx.moveTo(left,sy(barFromZero?yMin:0));ctx.lineTo(right,sy(barFromZero?yMin:0));ctx.stroke();}
  if(cfg.levels){cfg.levels.forEach(function(lv){if(lv<yMin||lv>yMax)return;ctx.strokeStyle='#3a4a66';ctx.setLineDash([3,3]);ctx.lineWidth=0.5;ctx.beginPath();ctx.moveTo(left,sy(lv));ctx.lineTo(right,sy(lv));ctx.stroke();ctx.setLineDash([]);});}
  cfg.series.forEach(function(s){
    var arr=id[s.key]||[];
    if(s.type==='bar'){
      for(var i=0;i<n;i++){var v=arr[i]; if(v==null||!isFinite(v))continue; var x=left+barW*i+barW/2, up=v>=0; ctx.fillStyle=up?(s.up||'#ff5b5b'):(s.down||'#22c55e'); var y0=barFromZero?sy(yMin):sy(0); var y1=sy(v); ctx.fillRect(x-barW/2+gap/2,Math.min(y0,y1),barW-gap,Math.max(1,Math.abs(y1-y0)));}
    }else if(s.type==='line'){
      ctx.strokeStyle=s.color||'#fbbf24';ctx.lineWidth=1.3;ctx.beginPath();var st=false;
      for(var i=0;i<n;i++){var v=arr[i]; if(v==null||!isFinite(v)){st=false;continue;} var x=left+barW*i+barW/2, yy=sy(v); if(!st){ctx.moveTo(x,yy);st=true;}else ctx.lineTo(x,yy);}
      ctx.stroke();
    }
  });
  ctx.fillStyle='#7c8aa3';ctx.font='12px Microsoft YaHei';ctx.textAlign='left';ctx.fillText(cfg.name,left+6,top+13);
  window.klineSubInfo={top:top,h:h,yMin:yMin,yMax:yMax};
}
function renderVolPanel(ctx,bars,top,h,left,right,gap,barW){
  if(!bars.length)return;
  var n=bars.length,ind=window.klineIndicators||{},vol=ind.cjl&&ind.cjl.vol?ind.cjl.vol:[],oi=ind.ccl&&ind.ccl.oi?ind.ccl.oi:[];
  var allV=[],hasOI=false;
  for(var i=0;i<n;i++){var v=vol[i]; if(v!=null&&isFinite(v))allV.push(v);}
  for(var i=0;i<oi.length;i++){var v=oi[i]; if(v!=null&&isFinite(v)){allV.push(v);hasOI=true;}}
  var yMin=0,yMax=allV.length?Math.max.apply(null,allV):1,pad=yMax*0.08||1;yMax+=pad;if(yMax===0)yMax=1;
  function sy(y){return top+h*(1-(y-yMin)/(yMax-yMin));}
  ctx.strokeStyle='#1c2740';ctx.lineWidth=0.5;
  for(var t=0;t<=2;t++){var yy=top+h*t/2;ctx.beginPath();ctx.moveTo(left,yy);ctx.lineTo(right,yy);ctx.stroke();}
  // 成交量柱
  for(var i=0;i<n;i++){var v=vol[i]; if(v==null||!isFinite(v))v=0; var x=left+barW*i+barW/2; var y0=sy(yMin); var y1=sy(v); ctx.fillStyle=bars[i].c>=bars[i].o?'#ff5b5b':'#22c55e'; ctx.fillRect(x-barW/2+gap/2,Math.min(y0,y1),barW-gap,Math.max(1,Math.abs(y1-y0)));}
  // 持仓量线(如数据有效)
  if(hasOI){ctx.strokeStyle='#c084fc';ctx.lineWidth=1.3;ctx.beginPath();var st=false;for(var i=0;i<n;i++){var v=oi[i]; if(v==null||!isFinite(v)){st=false;continue;}var x=left+barW*i+barW/2,yy=sy(v);if(!st){ctx.moveTo(x,yy);st=true;}else ctx.lineTo(x,yy);}ctx.stroke();}
  ctx.fillStyle='#7c8aa3';ctx.font='12px Microsoft YaHei';ctx.textAlign='left';ctx.fillText('CJL(成交量)'+(hasOI?' / CCL(持仓量)':''),left+6,top+13);
}
function drawKline(data,sym){
  var cv=document.getElementById('kc'),ctx=cv.getContext('2d');
  var rect=cv.getBoundingClientRect(),w=Math.max(Math.round(rect.width),320),h=Math.max(Math.round(rect.height),300);
  cv.width=w;cv.height=h;ctx.clearRect(0,0,w,h);ctx.fillStyle='#0b1120';ctx.fillRect(0,0,w,h);
  if(!data||data.length===0){window.klineLayout=null;ctx.fillStyle='#94a3b8';ctx.font='15px Microsoft YaHei';ctx.textAlign='center';var et=curSym?(curTab==='history'?'【'+curSym+'】暂无历史K线数据(可能未上市或天勤未返回)':'【'+curSym+'】暂无实时K线数据'):'点击左侧合约查看实时K线';ctx.fillText(et,w/2,h/2);resetOhlc();return;}
  var bars=[],ind=(window.klineIndicators||{});
  for(var i=0;i<data.length;i++){
    var d=data[i],o=parseFloat(d.open||d.开盘价||0),c=parseFloat(d.close||d.收盘价||0),hh=parseFloat(d.high||d.最高价||0),ll=parseFloat(d.low||d.最低价||0),v=parseFloat(d.volume||d.成交量||0),dt=d.datetime||d.date||d.日期||'';
    hh=hh||Math.max(o,c,1);ll=ll||Math.min(o,c,1);o=o||c||1;c=c||o||1;if(!isFinite(o+hh+ll+c))continue;
    bars.push({idx:i,o:o,c:c,h:hh,l:ll,v:v,dt:dt,
      ma5:(d.ma5!=null?parseFloat(d.ma5):null),ma10:(d.ma10!=null?parseFloat(d.ma10):null),ma60:(d.ma60!=null?parseFloat(d.ma60):null),
      bt:(d.boll_top!=null?parseFloat(d.boll_top):null),bm:(d.boll_mid!=null?parseFloat(d.boll_mid):null),bb:(d.boll_bottom!=null?parseFloat(d.boll_bottom):null)});
  }
  if(!bars.length){window.klineLayout=null;return;}
  var ml=64,mr=10,mt=8,mb=26,cw=w-ml-mr,ch=h-mt-mb;
  // 三面板布局: 主图 + 成交量/持仓量面板 + 指标面板
  var gapH=6,mainH=Math.round(ch*0.55),volH=Math.round(ch*0.17),indH=ch-mainH-volH-gapH*2;
  var mainTop=mt,mainBot=mt+mainH;
  var volTop=mainBot+gapH,volBot=volTop+volH;
  var indTop=volBot+gapH,indBot=indTop+indH;
  var allP=[];bars.forEach(function(b){allP.push(b.h,b.l);if(b.bt)allP.push(b.bt);if(b.bb)allP.push(b.bb);});
  var pHigh=Math.max.apply(null,allP),pLow=Math.min.apply(null,allP),pad=(pHigh-pLow)*0.08||pHigh*0.02,yMax=pHigh+pad,yMin=pLow-pad,yRange=yMax-yMin||1;
  function y2p(y){return mainTop+(mainBot-mainTop)*(1-(y-yMin)/yRange);}
  ctx.strokeStyle='#1c2740';ctx.lineWidth=0.5;
  for(var t=0;t<=5;t++){var yy=mainTop+(mainBot-mainTop)*t/5,val=yMax-(yMax-yMin)*t/5;ctx.beginPath();ctx.moveTo(ml,yy);ctx.lineTo(w-mr,yy);ctx.stroke();ctx.fillStyle='#5b6b85';ctx.font='11px monospace';ctx.textAlign='right';ctx.fillText(val.toFixed(2),ml-6,yy+4);}
  var n=bars.length,barW=cw/n,gap=Math.min(barW*0.28,5);
  for(var i=0;i<n;i++){var b=bars[i],x=ml+barW*i+barW/2,up=b.c>=b.o;ctx.strokeStyle=up?'#ff5b5b':'#22c55e';ctx.fillStyle=up?'#ff5b5b':'#22c55e';ctx.beginPath();ctx.moveTo(x,y2p(b.o));ctx.lineTo(x,y2p(b.c));ctx.stroke();ctx.fillRect(x-barW/2+gap/2,y2p(b.h),barW-gap,Math.max(1,y2p(b.l)-y2p(b.h)));}
  function line(key,color){ctx.strokeStyle=color;ctx.lineWidth=1.5;ctx.beginPath();var st=false;for(var i=0;i<n;i++){var v=bars[i][key];if(v==null||!isFinite(v)){st=false;continue;}var x=ml+barW*i+barW/2,yy=y2p(v);if(!st){ctx.moveTo(x,yy);st=true;}else ctx.lineTo(x,yy);}ctx.stroke();}
  line('ma5','#fbbf24');line('ma10','#38bdf8');line('ma60','#f472b6');
  ctx.setLineDash([5,4]);ctx.lineWidth=1;line('bt','#34d399');line('bm','#5b6b85');line('bb','#34d399');ctx.setLineDash([]);
  ctx.fillStyle='#7c8aa3';ctx.font='12px Microsoft YaHei';ctx.textAlign='left';ctx.fillText('MA5/10/60  ·  BOLL(20,2)',ml+6,mainTop+14);
  renderVolPanel(ctx,bars,volTop,volH,ml,w-mr,gap,barW);
  renderSubIndicator(ctx,bars,indTop,indH,ml,w-mr,gap,barW);
  updateIndValues(n-1);
  window.klineLayout={bars:bars,ml:ml,cw:cw,barW:barW,gap:gap,mainTop:mainTop,mainBot:mainBot,yMin:yMin,yRange:yRange,w:w,h:h,volTop:volTop,volBot:volBot,volH:volH,indTop:indTop,indBot:indBot,indH:indH};
  renderDrawings(ctx);
  var last=bars[n-1];
  document.getElementById('ohlc-o').textContent=last.o.toFixed(2);
  document.getElementById('ohlc-h').textContent=last.h.toFixed(2);
  document.getElementById('ohlc-l').textContent=last.l.toFixed(2);
  document.getElementById('ohlc-c').textContent=last.c.toFixed(2);
  document.getElementById('ohlc-c').style.color=last.c>=last.o?'#ff5b5b':'#22c55e';
  document.getElementById('ohlc-v').textContent=(last.v||0).toLocaleString();
  if(window.klineHoverIndex>=0&&window.klineHoverIndex<n)drawKlineHover(window.klineHoverIndex);
}
function resetOhlc(){['o','h','l','c','v'].forEach(function(k){var e=document.getElementById('ohlc-'+k);e.textContent='--';if(k==='c')e.style.color='';});}
var kcEl=document.getElementById('kc');
kcEl.addEventListener('mousemove',handleKlineHover);
kcEl.addEventListener('mouseleave',function(){window.klineHoverIndex=-1;drawKline(klineData,curSym);});
kcEl.addEventListener('touchstart',function(e){e.preventDefault();handleKlineHover(e.touches[0]);},{passive:false});
kcEl.addEventListener('touchmove',function(e){e.preventDefault();handleKlineHover(e.touches[0]);},{passive:false});
function handleKlineHover(e){if(typeof drawState!=='undefined'&&drawState.active){drawState.temp=getCanvasPoint(e);drawKline(klineData,curSym);return;}var L=window.klineLayout;if(!L||!L.bars.length)return;var cv=document.getElementById('kc'),rect=cv.getBoundingClientRect(),scaleX=(rect.width?cv.width/rect.width:1)||1,mx=(e.clientX-rect.left)*scaleX;var i=Math.round((mx-L.ml-L.barW/2)/L.barW);if(i<0)i=0;if(i>=L.bars.length)i=L.bars.length-1;window.klineHoverIndex=i;drawKline(klineData,curSym);}
function drawKlineHover(idx){var L=window.klineLayout;if(!L||L.bars[idx]==null)return;var cv=document.getElementById('kc'),ctx=cv.getContext('2d'),b=L.bars[idx];var x=L.ml+L.barW*idx+L.barW/2;['ohlc','kp'].forEach(function(p){var o=document.getElementById(p+'-o');if(!o)return;var hh=document.getElementById(p+'-h'),ll=document.getElementById(p+'-l'),cc=document.getElementById(p+'-c'),vv=document.getElementById(p+'-v');o.textContent=b.o.toFixed(2);hh.textContent=b.h.toFixed(2);ll.textContent=b.l.toFixed(2);cc.textContent=b.c.toFixed(2);cc.style.color=b.c>=b.o?'#ff5b5b':'#22c55e';vv.textContent=(b.v||0).toLocaleString();});ctx.strokeStyle='#7c8aa3';ctx.setLineDash([4,4]);ctx.lineWidth=0.8;ctx.beginPath();ctx.moveTo(x,L.mainTop);ctx.lineTo(x,L.indBot||L.mainBot);ctx.stroke();ctx.setLineDash([]);ctx.strokeStyle='#ffffff';ctx.lineWidth=2;var ytop=L.mainTop+(L.mainBot-L.mainTop)*(1-(b.h-L.yMin)/L.yRange),ybot=L.mainTop+(L.mainBot-L.mainTop)*(1-(b.l-L.yMin)/L.yRange);ctx.strokeRect(L.ml+L.barW*idx+L.gap/2-1,Math.min(ytop,ybot)-2,L.barW-L.gap+2,Math.abs(ybot-ytop)+4);var dtStr=String(b.dt),dateLabel=dtStr;if(dtStr.length>6&&dtStr.indexOf('-')<0)dateLabel=dtStr.substring(0,4)+'-'+dtStr.substring(4,6)+'-'+dtStr.substring(6,8);var txt='时间:'+dateLabel+' 开:'+b.o.toFixed(2)+' 高:'+b.h.toFixed(2)+' 低:'+b.l.toFixed(2)+' 收:'+b.c.toFixed(2)+' 量:'+(b.v||0).toLocaleString();ctx.font='bold 13px Microsoft YaHei';var tw=ctx.measureText(txt).width+16;var tx=Math.min(Math.max(x+10,8),L.w-tw-8),ty=L.mainTop+6;  ctx.fillStyle='rgba(13,21,38,0.96)';ctx.fillRect(tx,ty,tw,26);ctx.strokeStyle='#38bdf8';ctx.lineWidth=1;ctx.strokeRect(tx,ty,tw,26);ctx.fillStyle='#e6edf7';ctx.textAlign='left';ctx.textBaseline='middle';ctx.fillText(txt,tx+8,ty+13);updateIndValues(idx);}
// ===== 划线分析核心 =====
function toggleDrawPanel(e){if(e)e.stopPropagation();var p=document.getElementById('drawPanel'),o=document.getElementById('drawOverlay'),b=document.getElementById('drawBtn');if(!p)return;var show=!p.classList.contains('show');p.classList.toggle('show',show);o.classList.toggle('show',show);b.classList.toggle('active',show);if(!show)setDrawTool('');}
function closeDrawPanel(){var p=document.getElementById('drawPanel'),o=document.getElementById('drawOverlay'),b=document.getElementById('drawBtn');if(p)p.classList.remove('show');if(o)o.classList.remove('show');if(b)b.classList.remove('active');setDrawTool('');}
function selectDrawColor(c){drawColor=c;document.querySelectorAll('.draw-colors span').forEach(function(el){el.classList.toggle('active',el.getAttribute('data-color')===c);});}
function setDrawTool(t){drawTool=t;document.querySelectorAll('.draw-tool').forEach(function(el){el.classList.toggle('active',el.getAttribute('data-tool')===t);});var cv=document.getElementById('kc');if(cv)cv.style.cursor=t?'crosshair':'default';drawState.active=false;}
function selectDrawTool(t){setDrawTool(t);}
function getCanvasPoint(e){var cv=document.getElementById('kc'),rect=cv.getBoundingClientRect(),scaleX=(rect.width?cv.width/rect.width:1)||1,scaleY=(rect.height?cv.height/rect.height:1)||1;return{x:(e.clientX-rect.left)*scaleX,y:(e.clientY-rect.top)*scaleY};}
function onDrawMouseDown(e){if(!drawTool)return;var p=getCanvasPoint(e);drawState={active:true,tool:drawTool,start:p,end:p,temp:p};if(drawTool==='text'){var txt=prompt('输入标注文字','');if(txt){drawings.push({tool:'text',text:txt,x:p.x,y:p.y,color:drawColor});}drawState.active=false;setDrawTool('');drawKline(klineData,curSym);}}
function onDrawMouseUp(e){if(!drawState.active)return;drawState.end=getCanvasPoint(e);drawState.active=false;var d={tool:drawTool,start:{x:drawState.start.x,y:drawState.start.y},end:{x:drawState.end.x,y:drawState.end.y},color:drawColor};if(drawTool==='hline'){d.end.y=d.start.y;}
if(drawTool==='vline'){d.end.x=d.start.x;}
drawings.push(d);drawKline(klineData,curSym);}
function clearDrawings(){if(confirm('清空当前所有划线?')){drawings=[];drawKline(klineData,curSym);}}
function renderDrawings(ctx){var L=window.klineLayout;if(!L)return;for(var i=0;i<drawings.length;i++){var d=drawings[i];ctx.strokeStyle=d.color||'#fbbf24';ctx.fillStyle=d.color||'#fbbf24';ctx.lineWidth=1.5;if(d.tool==='text'){ctx.font='bold 13px Microsoft YaHei';ctx.textAlign='left';ctx.textBaseline='top';ctx.fillText(d.text,d.x,d.y);continue;}var x1=d.start.x,y1=d.start.y,x2=(d.end?d.end.x:d.start.x),y2=(d.end?d.end.y:d.start.y);if(d.tool==='hline'){y1=y2=d.start.y;x1=L.ml;x2=L.w-(L.mr||10);}if(d.tool==='vline'){x1=x2=d.start.x;y1=L.mainTop;y2=L.indBot||L.mainBot;}ctx.beginPath();if(d.tool==='line'||d.tool==='ray'){ctx.moveTo(x1,y1);var dx=x2-x1,dy=y2-y1;if(dx===0&&dy===0)continue;var t=Math.max(L.w,L.h)/Math.max(1,Math.sqrt(dx*dx+dy*dy));ctx.lineTo(x1+dx*t,y1+dy*t);}else if(d.tool==='segment'){ctx.moveTo(x1,y1);ctx.lineTo(x2,y2);}else if(d.tool==='rect'){ctx.strokeRect(Math.min(x1,x2),Math.min(y1,y2),Math.abs(x2-x1),Math.abs(y2-y1));}else{ctx.moveTo(x1,y1);ctx.lineTo(x2,y2);}ctx.stroke();}
if(drawState.active&&drawState.temp&&drawState.start){ctx.strokeStyle=drawColor;ctx.setLineDash([4,4]);ctx.lineWidth=1;ctx.beginPath();var s=drawState.start,t=drawState.temp;if(drawTool==='rect'){ctx.strokeRect(Math.min(s.x,t.x),Math.min(s.y,t.y),Math.abs(t.x-s.x),Math.abs(t.y-s.y));}else if(drawTool==='hline'){ctx.moveTo(L.ml,s.y);ctx.lineTo(L.w-(L.mr||10),s.y);}else if(drawTool==='vline'){ctx.moveTo(s.x,L.mainTop);ctx.lineTo(s.x,L.indBot||L.mainBot);}else{ctx.moveTo(s.x,s.y);ctx.lineTo(t.x,t.y);}ctx.stroke();ctx.setLineDash([]);}}
var kcEl=document.getElementById('kc');
kcEl.addEventListener('mousemove',function(e){if(typeof drawState!=='undefined'&&drawState.active){drawState.temp=getCanvasPoint(e);drawKline(klineData,curSym);return;}handleKlineHover(e);});
kcEl.addEventListener('mouseleave',function(){window.klineHoverIndex=-1;if(typeof drawState!=='undefined')drawState.active=false;drawKline(klineData,curSym);});
kcEl.addEventListener('mousedown',onDrawMouseDown);
kcEl.addEventListener('mouseup',onDrawMouseUp);
kcEl.addEventListener('touchstart',function(e){e.preventDefault();handleKlineHover(e.touches[0]);},{passive:false});
kcEl.addEventListener('touchmove',function(e){e.preventDefault();handleKlineHover(e.touches[0]);},{passive:false});
var resizeTimer;window.addEventListener('resize',function(){clearTimeout(resizeTimer);resizeTimer=setTimeout(function(){if(klineData.length)drawKline(klineData,curSym);},200);});
function add(){var s=document.getElementById('si').value.trim();if(!s)return;fetch('/api/symbols/add?symbol='+s).then(function(x){return x.json();}).then(function(){location.reload();});}
function rem(){var s=document.getElementById('si').value.trim();if(!s)return;fetch('/api/symbols/remove?symbol='+s).then(function(x){return x.json();}).then(function(){location.reload();});}
function add2(){var s=document.getElementById('si2').value.trim();if(!s)return;fetch('/api/symbols/add?symbol='+s).then(function(x){return x.json();}).then(function(){location.reload();});}
function rem2(){var s=document.getElementById('si2').value.trim();if(!s)return;fetch('/api/symbols/remove?symbol='+s).then(function(x){return x.json();}).then(function(){location.reload();});}
var pollTimer=setInterval(r,3000);r();
function changeInterval(){var v=parseInt(document.getElementById('refresh-interval').value);currentPollMs=v;clearInterval(pollTimer);pollTimer=setInterval(r,v);if(curSym){if(klinePollTimer)clearInterval(klinePollTimer);klinePollTimer=setInterval(fetchKlineData,v);}document.getElementById('status').textContent='刷新间隔: '+v+'ms | TqSdk';}
</script></body></html>
"""


@app.route("/")
def index():
    return HTML


@app.route("/api/quotes")
def api_quotes():
    tq_user = request.args.get("tq_user", "")
    tq_pass = request.args.get("tq_pass", "")
    slot = get_slot(tq_user, tq_pass)

    if slot.status == "error":
        return safe_json({"code": -1, "msg": f"账户登录失败: {slot.error_msg}"}, 503)
    if slot.status in ("connecting", "init"):
        return safe_json({"code": -1, "msg": "账户连接中,请稍后刷新"}, 503)

    result = slot.get_quotes()
    result.sort(key=lambda x: x.get("symbol", ""))
    clean_result = []
    for q in result:
        clean = {}
        for k, v in q.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                clean[k] = 0.0
            else:
                clean[k] = v
        clean_result.append(clean)
    return jsonify({"code": 0, "data": clean_result, "count": len(clean_result)})


@app.route("/api/kline")
@app.route("/optchain/api/kline")
def api_kline():
    sym = request.args.get("symbol", "")
    tq_user = request.args.get("tq_user", "")
    tq_pass = request.args.get("tq_pass", "")
    if not sym: return safe_json({"code": -1, "msg": "缺少 symbol 参数"}, 400)

    slot = get_slot(tq_user, tq_pass)
    if slot.status == "error":
        return safe_json({"code": -1, "msg": f"账户登录失败: {slot.error_msg}"}, 503)
    if slot.status in ("connecting", "init"):
        return safe_json({"code": -1, "msg": "账户连接中,请稍后刷新"}, 503)

    duration = request.args.get("duration", 86400, type=int)
    # 白名单 + 月线(2592000) 走 history 合成(实时月线无意义)
    ALLOWED = {60, 300, 900, 1800, 3600, 86400, 604800}
    if duration not in ALLOWED:
        return safe_json({"code": -1, "msg": f"不支持的周期 {duration}s(允许 {sorted(ALLOWED)})"}, 400)

    try:
        df = slot.get_kline(sym, duration, 80)
        if df is None or len(df) == 0:
            return safe_json({"code": 0, "data": [], "source": "天勤实时", "duration": duration})
        recs, indicators = _build_kline_response(df)
        return safe_json({"code": 0, "data": recs, "indicators": indicators, "source": "天勤实时", "duration": duration})
    except Exception as e:
        return safe_json({"code": -1, "msg": str(e)}, 500)


@app.route("/api/kline/history")
@app.route("/optchain/api/kline/history")
def api_kline_history():
    sym = request.args.get("symbol", "")
    tq_user = request.args.get("tq_user", "")
    tq_pass = request.args.get("tq_pass", "")
    if not sym:
        return safe_json({"code": -1}, 400)
    slot = get_slot(tq_user, tq_pass)
    if slot.status == "error":
        return safe_json({"code": -1, "msg": f"账户登录失败: {slot.error_msg}"}, 503)
    if slot.status in ("connecting", "init"):
        return safe_json({"code": -1, "msg": "账户连接中,请稍后刷新"}, 503)
    duration = request.args.get("duration", 86400, type=int)
    ALLOWED = {60, 300, 900, 1800, 3600, 86400, 604800, 2592000}
    if duration not in ALLOWED:
        return safe_json({"code": -1, "msg": f"不支持的周期 {duration}s(允许 {sorted(ALLOWED)})"}, 400)
    try:
        if duration == 2592000:
            df = _synth_monthly_kline(slot, sym)
            src = "月线合成(日线 resample MS, open=首开/high=最高/low=最低/close=末收/volume=合计)"
        else:
            df = slot.get_history_kline(sym, duration, 800)
            src = "天勤TqSdk历史"
        if df is None or len(df) == 0:
            return safe_json({"code": 0, "data": [], "msg": "暂无此合约历史数据", "duration": duration})
        recs, indicators = _build_kline_response(df)
        if not recs or all(r["close"] == 0 for r in recs):
            return safe_json({"code": 0, "data": [], "msg": "暂无此合约历史数据(天勤未返回)", "duration": duration})
        return safe_json({"code": 0, "data": recs, "indicators": indicators, "source": src, "duration": duration})
    except Exception as e:
        return safe_json({"code": -1, "msg": str(e)}, 500)


def _synth_monthly_kline(slot, sym):
    """用日线历史 K 线合成月线 OHLCV。
    TqSdk 不原生支持 duration_seconds=2592000(月线),
    取日线 1200 根(~5年, ~60 根月线)按月 resample 聚合。"""
    try:
        df = slot.get_history_kline(sym, 86400, 1200)
    except Exception:
        return None
    if df is None or len(df) == 0:
        return None
    try:
        import pandas as pd
        dts = df["datetime"] if "datetime" in df.columns else df["id"]
        # 过滤无效 bar(全零/空 datetime), 与 _build_kline_response 保持一致
        keep_mask = []
        for i in range(len(df)):
            try:
                c = float(df["close"][i])
            except Exception:
                c = 0
            dt_val = dts[i]
            keep_mask.append(bool(dt_val) and c > 0)
        if not any(keep_mask):
            return None
        df = df[keep_mask] if hasattr(df, "__getitem__") else df.iloc[keep_mask]
        dt_index = pd.to_datetime(dts.values[keep_mask] if hasattr(dts, "values") else [dts[i] for i in range(len(df)) if keep_mask[i]], errors="coerce")
        valid_dt = ~pd.isna(dt_index)
        if not valid_dt.any():
            return None
        pdf = pd.DataFrame({
            "open": df["open"].values[valid_dt] if hasattr(df["open"], "values") else [df["open"][i] for i in range(len(df)) if keep_mask[i] and valid_dt[i]],
            "high": df["high"].values[valid_dt],
            "low": df["low"].values[valid_dt],
            "close": df["close"].values[valid_dt],
            "volume": df["volume"].values[valid_dt],
        }, index=dt_index[valid_dt])
        m = pdf.resample("MS").agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
        }).dropna(subset=["open", "close"])
        if len(m) == 0:
            return None
        out = pd.DataFrame({
            "datetime": [d.strftime("%Y-%m-%d") for d in m.index],
            "open": m["open"].astype(float).values,
            "high": m["high"].astype(float).values,
            "low": m["low"].astype(float).values,
            "close": m["close"].astype(float).values,
            "volume": m["volume"].astype(float).values,
        })
        return out
    except Exception as e:
        print(f"[月线合成] {sym}: {e}")
        return None


# 到期表
expiry_cache = {}
expiry_cache_lock = threading.Lock()
EXPIRY_CACHE_TTL = 300  # 到期日历缓存有效期(秒): 同步任务可能更新库, 到期自动重载

def _get_chain_live_greeks(underlying, chain):
    """尝试用默认账户 TqApi 取实时行情, 计算期权链各合约的希腊值与隐含波动率。
    无连接 / 无行情 / 出错时返回 None（UI 对应位置显示 "—"）。

    重要：AccountSlot 有独立轮询线程持续调用 api.wait_update 驱动行情下载。
    TqSdk 不允许两个线程同时 wait_update（请求线程绝不能再调 wait_update）。
    订阅注册机制（2026-08-19 修复）：TqSdk 的 get_quote 必须在 TqApi 创建线程(主线程)
    调用才会真正发送订阅请求；请求线程通过 slot.ensure_subscribed() 登记待订阅合约，
    由主线程订阅注册循环 get_quote，本函数再用 TqSdkClient 缓存引用读取行情。
    """
    try:
        slot = _resolve_slot()
        api = slot.client.api
    except Exception:
        return None
    try:
        def _num(v):
            return v if (v is not None and isinstance(v, (int, float)) and v == v) else None

        def _q(v):
            n = _num(v)
            return n if (n is not None and n != 0) else None

        uq = slot.client.get_quote(underlying)
        slot.ensure_subscribed([underlying])
        # 顺带订阅上证指数(SSE.000001)用于底部状态栏; 免费版含该指数
        try:
            idx_q = slot.client.get_quote("SSE.000001")
            slot.ensure_subscribed(["SSE.000001"])
        except Exception as e:
            print(f"[希腊值] 上证指数订阅失败: {e}")
            idx_q = None
        opt_syms = []
        for st in chain.get("strikes", []):
            if st.get("call"):
                opt_syms.append(st["call"]["symbol"])
            if st.get("put"):
                opt_syms.append(st["put"]["symbol"])
        # 一次性登记所有期权订阅（真正 get_quote 由主线程执行）；此处用缓存引用读取
        slot.ensure_subscribed(opt_syms)
        oqs = {}
        for s in opt_syms:
            try:
                oqs[s] = slot.client.get_quote(s)
            except Exception as e:
                print(f"[希腊值] {s} 订阅失败: {e}")

        # 非阻塞读取：行情已由轮询线程的 wait_update 持续驱动并写入缓存。
        # 热数据(已订阅并推送过)直接秒读——毫秒级；仅【首次订阅的冷数据】给一个极短窗口
        # (≤0.8s) 让轮询线程推送首包，避免之前 3s+4s 的硬等待把响应拖到 7 秒以上。
        def _read_opt(q):
            # 不再因 last_price=nan 丢弃整行：未成交/无最新价也保留盘口/量仓供 UI 展示，
            # 与 WH6 等终端一致（盘口、成交量、持仓量在最新价缺失时仍可见）。
            raw_last = _num(q.last_price)
            # 展示用最新价：真实成交价优先；无成交(nan/None)时回退 昨结算 → (买+卖)/2 → 昨收
            disp_last = raw_last
            if disp_last is None:
                ps = _num(getattr(q, "pre_settlement", None))
                if ps is not None:
                    disp_last = ps
                else:
                    b = _num(q.bid_price1); a = _num(q.ask_price1)
                    if b is not None and a is not None and b > 0 and a > 0:
                        disp_last = (b + a) / 2.0
                    else:
                        disp_last = _num(q.pre_close)
            return {
                "last": disp_last,            # 展示价(可回退)
                "last_raw": raw_last,         # 真实成交价(用于展示/核对; 计算改用 disp_last)
                "pre_close": _num(q.pre_close),
                "pre_settlement": _num(getattr(q, "pre_settlement", None)),  # 昨结算(涨跌幅基准, 期货惯例)
                "open": _num(q.open), "high": _num(q.highest), "low": _num(q.lowest),
                "average": _num(q.average), "amount": _num(q.amount),
                "bid": _q(q.bid_price1), "bid_v": _num(q.bid_volume1),
                "ask": _q(q.ask_price1), "ask_v": _num(q.ask_volume1),
                "volume": _num(q.volume), "oi": _num(q.open_interest),   # 0 是真实值(未成交), 不再当缺失
                "pre_oi": _num(q.pre_open_interest),  # 昨持仓, 用于计算仓差
                "volume_multiple": _num(q.volume_multiple),  # 合约乘数/行权比例
                "expire_rest_days": _num(q.expire_rest_days),  # 剩余天数
            }

        # 标的 last_price：热数据立即读到；冷数据(首次订阅)最多等 0.8s
        S = _num(uq.last_price)
        if S is None or S <= 0:
            dl = time.time() + 0.8
            while time.time() < dl:
                S = _num(uq.last_price)
                if S is not None and S > 0:
                    break
                time.sleep(0.1)

        # 期权盘口/持仓/成交量/昨收：立即读能读到的(毫秒级)
        quotes = {}
        for s, q in oqs.items():
            r = _read_opt(q)
            if r is not None:
                quotes[s] = r
        # 仅当【一个都没到】时才给一次极短窗口让轮询线程推送首包；热数据直接跳过(毫秒级)
        if len(quotes) == 0:
            dl = time.time() + 0.8
            pending = set(oqs.keys())
            while pending and time.time() < dl:
                for s in list(pending):
                    r = _read_opt(oqs[s])
                    if r is not None:
                        quotes[s] = r
                        pending.discard(s)
                if pending:
                    time.sleep(0.1)
            if pending:
                print(f"[希腊值] {underlying}: 冷数据未完全到达，取到 {len(quotes)}/{len(oqs)}")

        # 上证指数(SSE.000001): 轮询线程已驱动 wait_update, 此处直接读取
        idx = None
        if idx_q is not None:
            try:
                i_last = _num(idx_q.last_price)
                i_pre = _base_price(idx_q)  # 昨结算优先(期货), 股票回退昨收
                idx = {
                    "last": i_last,
                    "pre_close": i_pre,
                    "chg": (i_last - i_pre) if (i_last is not None and i_pre is not None) else None,
                    "pct": ((i_last - i_pre) / i_pre * 100.0) if (i_last is not None and i_pre not in (None, 0)) else None,
                }
            except Exception as e:
                print(f"[希腊值] 读取上证指数失败: {e}")

        # 文华商品指数估算: 天勤无官方 WH 指数, 取主要商品连续合约涨跌幅均值。
        # WH_INDEX_SYMBOLS 已在轮询线程启动时永久订阅并持续预热，此处直接读(毫秒级)。
        wh_idx = None
        try:
            wh_symbols = WH_INDEX_SYMBOLS
            slot.ensure_subscribed(wh_symbols)
            wh_qs = {}
            for s in wh_symbols:
                try:
                    wh_qs[s] = slot.client.get_quote(s)
                except Exception as e:
                    print(f"[希腊值] 文华指数成分 {s} 订阅失败: {e}")
            pcts = []
            for s, q in wh_qs.items():
                lp = _num(q.last_price)
                pc = _base_price(q)
                if lp is not None and pc not in (None, 0):
                    pcts.append((lp - pc) / pc * 100.0)
            # 冷数据(指数成分首次订阅未到)时给一次极短窗口，避免首屏长期 “—”
            if not pcts:
                time.sleep(0.4)
                for s, q in wh_qs.items():
                    lp = _num(q.last_price)
                    pc = _base_price(q)
                    if lp is not None and pc not in (None, 0):
                        pcts.append((lp - pc) / pc * 100.0)
            if pcts:
                avg_pct = sum(pcts) / len(pcts)
                # 基准点位用 1800 加当日涨跌幅反推(仅用于展示, 非真实点位)
                base_point = 1800.0
                wh_idx = {
                    "pct": avg_pct,
                    "chg": base_point * avg_pct / 100.0,
                    "last": base_point * (1 + avg_pct / 100.0),
                    "note": "估算（主要商品连续合约等权涨跌幅均值）",
                }
        except Exception as e:
            print(f"[希腊值] 文华商品指数估算失败: {e}")

        ex = chain.get("exercise_dates") or [None]
        t = option_greeks.annualized_t(ex[0]) if ex and ex[0] else None
        r = option_greeks.RISK_FREE_RATE
        v0 = option_greeks.DEFAULT_VOL

        import datetime as _dt
        def _rest_days(ed):
            if ed is None:
                return None
            try:
                if isinstance(ed, str):
                    ed = _dt.datetime.strptime(ed[:10], "%Y-%m-%d").date()
                elif isinstance(ed, _dt.datetime):
                    ed = ed.date()
                return (ed - _dt.date.today()).days
            except Exception:
                return None

        result = {
            "__underlying__": {
                "last": S,
                "pre_close": _num(uq.pre_close),
                "pre_settlement": _num(getattr(uq, "pre_settlement", None)),  # 昨结算(涨跌幅基准)
                "chg": (S - _base_price(uq)) if S is not None else None,
                "pct_change": ((S - _base_price(uq)) / _base_price(uq) * 100)
                              if S is not None and _base_price(uq) not in (None, 0) else None,
                "volume": _num(uq.volume),
                "oi": _num(uq.open_interest),
            }
        }
        # 简化 VIX 估算(平值期权 IV 均值) 与 上证指数 / 文华商品指数
        # 注意：隐含波动率(impv)用纯 Python 二分迭代求解，若对全部合约逐一求解会拖慢响应；
        # T 型报价表不展示 IV，故【仅对平值(最接近标价的行权价)合约的 call/put 求解 2 次】供 VIX 估算，
        # 其余合约不计算 impv。
        if idx is not None:
            result["__index__"] = idx
        if wh_idx is not None:
            result["__wh_index__"] = wh_idx
        # ---- 逐行权价计算希腊值等(不计算 impv) ----
        for st in chain.get("strikes", []):
            k = st.get("strike")
            for side in ("call", "put"):
                if not st.get(side):
                    continue
                sym = st[side]["symbol"]
                ex_date = st[side].get("exercise", ex[0] if ex else None)
                cls = "CALL" if side == "call" else "PUT"
                q = quotes.get(sym, {})
                opt_price = q.get("last")       # 展示价(真实成交价→昨结算→盘口均价→昨收): 用于IV/时间价值/溢价率/杠杆; 无成交时回退, 与WH6等终端一致
                # 用每行权价自己的 exercise_date 算 t，避免把不同行权日混用同一个 t
                t_i = option_greeks.annualized_t(ex_date) if ex_date else t
                g = option_greeks.compute_greeks(S, k, r, v0, t_i, cls) if (S is not None and t_i and t_i > 0) else None
                delta = (g["delta"] if g else None)
                bs = option_greeks.compute_bs_value(S, k, r, v0, t_i, cls) if (S is not None and t_i and t_i > 0) else None
                intrinsic = option_greeks.compute_intrinsic_value(S, k, cls)
                tv = option_greeks.compute_time_value(opt_price, intrinsic) if opt_price is not None else None
                lev = option_greeks.compute_leverage(S, opt_price) if opt_price else None
                real_lev = option_greeks.compute_real_leverage(lev, delta)
                prem = option_greeks.compute_premium_rate(S, opt_price, k, cls) if (opt_price is not None and k is not None) else None
                _base = q.get("pre_settlement", q.get("pre_close")) or 0  # 涨跌幅基准: 昨结算优先(期货惯例), 回退昨收
                pct = ((opt_price - _base) / _base * 100.0) if (opt_price is not None and _base not in (None, 0)) else None
                moneyness = option_greeks.compute_moneyness_value(S, k, cls) if S is not None else None
                # 振幅 = (最高-最低)/昨收*100 (无成交时 high/low 可能为 0, 跳过)
                hi, lo, pc = q.get("high"), q.get("low"), q.get("pre_close")
                amp = ((hi - lo) / pc * 100.0) if (hi not in (None, 0) and lo not in (None, 0) and pc not in (None, 0)) else None
                # 仓差 = 今持仓 - 昨持仓 (资金流向核心指标, 通达信/同花顺都有)
                oi_now = q.get("oi")
                oi_pre = q.get("pre_oi")
                oi_chg = (oi_now - oi_pre) if (oi_now is not None and oi_pre is not None) else None
                # 每合约隐含波动率(全合约计算, 不再限于平值), 性能: 单合约 ~0.5ms, 整链 ~150ms 可接受
                impv = None
                if S is not None and opt_price is not None and opt_price > 0 and t_i and t_i > 0:
                    impv = option_greeks.compute_impv(S, opt_price, k, r, t_i, cls)
                result[sym] = {
                    "symbol": sym,                # 合约代码(完整)
                    "exercise": ex_date,          # 该合约的行权日
                    "option_class": cls,            # CALL/PUT 期权类型
                    "last": q.get("last"), "last_raw": q.get("last_raw"), "pre_close": q.get("pre_close"), "pre_settlement": q.get("pre_settlement"),
                    "open": q.get("open"), "high": q.get("high"), "low": q.get("low"), "average": q.get("average"),
                    "amount": q.get("amount"),
                    "bid": q.get("bid"), "bid_v": q.get("bid_v"),
                    "ask": q.get("ask"), "ask_v": q.get("ask_v"),
                    "volume": q.get("volume"), "oi": oi_now, "pre_oi": oi_pre, "oi_chg": oi_chg,
                    "chg": (opt_price - _base) if (opt_price is not None and _base not in (None, 0)) else None,
                    "pct": pct, "amp": amp,
                    "delta": delta, "gamma": (g["gamma"] if g else None),
                    "theta": (g["theta"] if g else None), "vega": (g["vega"] if g else None),
                    "rho": (g["rho"] if g else None),
                    "bs": bs, "intrinsic": intrinsic, "time_value": tv,
                    "leverage": lev, "real_leverage": real_lev, "premium_rate": prem,
                    "moneyness": moneyness, "impv": impv,
                    "hv": None,                     # 历史波动率: 天勤无现成字段, 暂占位
                    "volume_multiple": q.get("volume_multiple"),
                    "expire_rest_days": (q.get("expire_rest_days") if q.get("expire_rest_days") is not None else _rest_days(ex_date)),
                }
        # ---- 仍计算全局 VIX 估算(平值 IV 均值) ----
        if S is not None:
            best_st, best_d = None, None
            for st in chain.get("strikes", []):
                k = st.get("strike")
                if k is None:
                    continue
                d = abs(float(k) - float(S))
                if best_d is None or d < best_d:
                    best_d, best_st = d, st
            if best_st is not None:
                # 平值 impv 已由上面循环计算过, 不再重复求解
                pass
        result["__vix__"] = option_greeks.compute_vix_estimate(chain, result)
        return result
    except Exception as e:
        print(f"[希腊值] 取实时行情失败: {e}")
        return None


def _get_market_index_bars():
    """返回 (上证指数 dict, 文华商品指数 dict) 用于期权目录页顶部状态栏。
    优化：只在已有在线 slot 时读取，避免在请求线程里触发新连接；最多等待 1 秒，
    保证页面响应速度。无连接/无行情时返回 (None, None)。"""
    slot = _resolve_slot()
    if slot is None or slot.status != "online" or slot.client is None or slot.client.api is None:
        return None, None

    def _num(v):
        return v if (v is not None and isinstance(v, (int, float)) and v == v) else None

    def _read_index(q):
        i_last = _num(q.last_price)
        i_pre = _base_price(q)  # 昨结算优先(期货), 股票回退昨收
        if i_last is None:
            return None
        return {
            "last": i_last,
            "pre_close": i_pre,
            "chg": (i_last - i_pre) if (i_last is not None and i_pre is not None) else None,
            "pct": ((i_last - i_pre) / i_pre * 100.0) if (i_last is not None and i_pre not in (None, 0)) else None,
        }

    idx = None
    wh_idx = None
    try:
        # 上证指数：登记订阅后立即读取，给轮询线程 0.3s 推送窗口
        try:
            slot.ensure_subscribed(["SSE.000001"])
            idx_q = slot.client.get_quote("SSE.000001")
            time.sleep(0.3)
            idx = _read_index(idx_q)
        except Exception as e:
            print(f"[指数] 上证指数订阅/读取失败: {e}")

        # 文华商品指数(估算)：主要商品连续合约等权涨跌幅均值
        try:
            wh_symbols = ["KQ.m@SHFE.cu", "KQ.m@SHFE.rb", "KQ.m@DCE.i", "KQ.m@INE.sc",
                          "KQ.m@DCE.m", "KQ.m@CZCE.TA", "KQ.m@SHFE.au", "KQ.m@DCE.p",
                          "KQ.m@CZCE.MA", "KQ.m@SHFE.fu"]
            slot.ensure_subscribed(wh_symbols)
            wh_qs = {}
            for s in wh_symbols:
                try:
                    wh_qs[s] = slot.client.get_quote(s)
                except Exception as e:
                    print(f"[指数] 文华成分 {s} 订阅失败: {e}")
            time.sleep(0.5)
            pcts = []
            for s, q in wh_qs.items():
                lp = _num(q.last_price)
                pc = _base_price(q)
                if lp is not None and pc not in (None, 0):
                    pcts.append((lp - pc) / pc * 100.0)
            if pcts:
                avg_pct = sum(pcts) / len(pcts)
                base_point = 1800.0
                wh_idx = {
                    "pct": avg_pct,
                    "chg": base_point * avg_pct / 100.0,
                    "last": base_point * (1 + avg_pct / 100.0),
                    "note": "估算（主要商品连续合约等权涨跌幅均值）",
                }
        except Exception as e:
            print(f"[指数] 文华商品指数估算失败: {e}")
    except Exception as e:
        print(f"[指数] 获取市场指数失败: {e}")
    return idx, wh_idx


def _merge_g(g, impv):
    if g is None:
        g = {kk: None for kk in ("delta", "gamma", "theta", "vega", "rho")}
    g = dict(g)
    # 排除 NaN（compute_impv 反解失败会返回 nan）
    g["impv"] = impv if (impv is not None and impv == impv) else None
    return g


def _greek_html(g):
    """把单个期权的希腊值格式化为一小段 HTML。"""
    if g is None or g.get("delta") is None:
        return "—"
    s = f'Δ{g["delta"]:.2f} Γ{g["gamma"]:.4f} Θ{g["theta"]:.0f} ν{g["vega"]:.0f} ρ{g["rho"]:.0f}'
    impv = g.get("impv")
    if impv is not None and impv == impv:
        s += f' IV{impv:.2f}'
    return s


def _fmt(v, nd=2, pct=False, with_comma=False):
    """期权 T 型报价通用格式化: None -> '—'; 字符串原样返回。"""
    if v is None:
        return "—"
    if isinstance(v, str):
        return html.escape(v)
    try:
        if pct:
            return f"{v:.2f}%"
        if with_comma:
            return f"{v:,.{nd}f}"
        return f"{v:.{nd}f}"
    except Exception:
        return "—"


def _opt_cell_html(sym, d):
    """渲染单个期权(CALL/PUT)的 T 型单元格: 代码 + 多字段网格 + 希腊值小字。(保留兼容)"""
    if d is None:
        return "—"
    chg = None
    _bp = d.get("pre_settlement") or d.get("pre_close")  # 昨结算优先(期货惯例)
    if d.get("last") is not None and _bp is not None:
        chg = d["last"] - _bp
    arrow = ""
    if chg is not None:
        arrow = "↑" if chg >= 0 else "↓"
    trend = "up" if (chg is not None and chg >= 0) else "down"
    g = ""
    if d.get("delta") is not None:
        g = f'Δ{_fmt(d["delta"],2)} Γ{_fmt(d["gamma"],4)} Θ{_fmt(d["theta"],0)} ν{_fmt(d["vega"],0)} ρ{_fmt(d["rho"],0)}'
        if d.get("impv") is not None:
            g += f' IV{_fmt(d["impv"],2)}'
    grid = (
        f'<span class="ok">最新</span><span class="{trend}">{_fmt(d["last"])} {arrow}</span>'
        f'<span class="ok">涨跌</span><span class="{trend}">{_fmt(chg)}</span>'
        f'<span class="ok">买价/量</span><span>{_fmt(d["bid"])} / {_fmt(d["bid_v"],0,True)}</span>'
        f'<span class="ok">卖价/量</span><span>{_fmt(d["ask"])} / {_fmt(d["ask_v"],0,True)}</span>'
        f'<span class="ok">持仓/成交</span><span>{_fmt(d["oi"],0,True)} / {_fmt(d["volume"],0,True)}</span>'
        f'<span class="ok">隐含波</span><span>{_fmt(d["impv"],2)}</span>'
        f'<span class="ok">理论价</span><span>{_fmt(d["bs"])}</span>'
        f'<span class="ok">时间价值</span><span>{_fmt(d["time_value"])}</span>'
        f'<span class="ok">内在价值</span><span>{_fmt(d["intrinsic"])}</span>'
        f'<span class="ok">杠杆率</span><span>{_fmt(d["leverage"],1)}</span>'
        f'<span class="ok">真实杠杆</span><span>{_fmt(d["real_leverage"],1)}</span>'
        f'<span class="ok">溢价率</span><span>{_fmt(d["premium_rate"],2,True)}</span>'
    )
    _sym_disp = futures_db.option_symbol_display(sym)
    return (f'<div class="opt-sym" title="{html.escape(sym)}">{html.escape(_sym_disp)}</div>'
            f'<div class="opt-grid">{grid}</div>'
            f'<div class="greek">{g}</div>')


def _build_vol_smile_svg(points, S, title="波动率微笑 / 偏斜 (IV-Smile)"):
    """用现有每合约 IV 直接绘制波动率微笑/偏斜曲线(SVG, 无需额外行情请求)。

    points: list of (strike:float, call_iv:float|None, put_iv:float|None)  (call_iv/put_iv 为小数, 如 0.18)
    返回内联 <svg> 字符串(可在 innerHTML 中直接渲染, 无需 JS)。"""
    W, H = 720, 280
    pad_l, pad_r, pad_t, pad_b = 56, 16, 30, 38
    plot_w = W - pad_l - pad_r
    plot_h = H - pad_t - pad_b
    call_pts, put_pts = [], []
    for k, civ, piv in points:
        if civ is not None and civ == civ and civ > 0:
            call_pts.append((float(k), float(civ)))
        if piv is not None and piv == piv and piv > 0:
            put_pts.append((float(k), float(piv)))
    if len(call_pts) + len(put_pts) < 2:
        return (f'<div class="smile-box"><div class="smile-title">{title}</div>'
                f'<div class="smile-empty">当前月份可计算 IV 的合约不足，暂无法绘制微笑曲线（多为深度虚值无成交）。</div></div>')
    all_pts = call_pts + put_pts
    ks = [p[0] for p in all_pts]
    ivs = [p[1] for p in all_pts]
    k_min, k_max = min(ks), max(ks)
    iv_min, iv_max = min(ivs), max(ivs)
    # y 轴留白 10%
    iv_span = (iv_max - iv_min) or 0.01
    iv_lo = max(0.0, iv_min - iv_span * 0.15)
    iv_hi = iv_max + iv_span * 0.15

    def x_of(k):
        if k_max == k_min:
            return pad_l + plot_w / 2
        return pad_l + (k - k_min) / (k_max - k_min) * plot_w

    def y_of(iv):
        return pad_t + (iv_hi - iv) / (iv_hi - iv_lo) * plot_h

    # 网格 + 轴标签
    grid = ""
    for i in range(5):
        yy = pad_t + plot_h * i / 4
        ivv = iv_hi - (iv_hi - iv_lo) * i / 4
        grid += f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{W-pad_r}" y2="{yy:.1f}" stroke="#22304a" stroke-width="1"/>'
        grid += f'<text x="{pad_l-6}" y="{yy+4:.1f}" fill="#7e8aa3" font-size="10" text-anchor="end">{ivv*100:.1f}%</text>'
    # x 轴刻度(取 5~6 个行权价)
    nk = min(6, len(ks))
    for i in range(nk):
        idx = int(round(i * (len(ks) - 1) / max(1, nk - 1))) if nk > 1 else 0
        kk = sorted(ks)[idx]
        xx = x_of(kk)
        grid += f'<text x="{xx:.1f}" y="{H-pad_b+16}" fill="#7e8aa3" font-size="10" text-anchor="middle">{kk:g}</text>'
    # ATM 竖虚线
    atm_line = ""
    if S is not None and k_min <= S <= k_max:
        xs = x_of(S)
        atm_line = (f'<line x1="{xs:.1f}" y1="{pad_t}" x2="{xs:.1f}" y2="{pad_t+plot_h}" '
                    f'stroke="#f5b942" stroke-width="1" stroke-dasharray="5 4"/>'
                    f'<text x="{xs:.1f}" y="{pad_t-8}" fill="#f5b942" font-size="10" text-anchor="middle">标的价格 {S:.2f}</text>')

    def poly(pts, color):
        if len(pts) < 1:
            return "", ""
        pts_sorted = sorted(pts, key=lambda p: p[0])
        d = " ".join(f"{x_of(k):.1f},{y_of(iv):.1f}" for k, iv in pts_sorted)
        dots = "".join(f'<circle cx="{x_of(k):.1f}" cy="{y_of(iv):.1f}" r="2.6" fill="{color}"/>'
                       for k, iv in pts_sorted)
        return d, dots

    cd, cdots = poly(call_pts, "#f5a623")
    pd_, pdots = poly(put_pts, "#3fc7d4")
    call_path = f'<polyline points="{cd}" fill="none" stroke="#f5a623" stroke-width="2"/>' if cd else ""
    put_path = f'<polyline points="{pd_}" fill="none" stroke="#3fc7d4" stroke-width="2"/>' if pd_ else ""
    legend = (f'<g font-size="11">'
              f'<rect x="{W-200}" y="{pad_t+4}" width="11" height="11" fill="#f5a623"/><text x="{W-184}" y="{pad_t+13}" fill="#cdd6e6">认购 IV (CALL)</text>'
              f'<rect x="{W-200}" y="{pad_t+22}" width="11" height="11" fill="#3fc7d4"/><text x="{W-184}" y="{pad_t+31}" fill="#cdd6e6">认沽 IV (PUT)</text></g>')
    svg = (
        f'<div class="smile-box">'
        f'<div class="smile-title">{title}</div>'
        f'<svg viewBox="0 0 {W} {H}" width="100%" preserveAspectRatio="xMidYMid meet" class="smile-svg">'
        f'<rect x="0" y="0" width="{W}" height="{H}" fill="#0c1422"/>'
        f'{grid}{atm_line}{call_path}{put_path}{cdots}{pdots}{legend}'
        f'</svg></div>'
    )
    return svg


def _build_smile_placeholder(year, month, underlying):
    """首屏占位：微笑图由 JS 异步加载，避免阻塞 T 型表渲染。"""
    return (
        f'<div id="smile-async" class="smile-box" data-year="{int(year)}" data-month="{int(month)}" data-underlying="{html.escape(underlying)}">'
        f'<div class="smile-title">波动率微笑 / 偏斜 (IV-Smile)</div>'
        f'<div class="smile-loading"><span class="spinner"></span> 正在异步计算隐含波动率曲线…</div>'
        f'</div>'
    )


def _opt_row_split_html(csym, cdata, psym, pdata, und_name=None, strike=None):
    """把同一行的 CALL / PUT 拆成两个 HTML 片段，分别填入左右两个可滚动表格。
    列顺序(32列)：最新→涨跌→涨跌幅→开→最高→最低→振幅→成交量→仓差→持仓量→成交额→卖价→买价→
    Δ→Γ→Θ→V→Rho→内在价值→时间价值→溢价率→杠杆→真实杠杆→理论价→隐含波动率→历史波动率→
    虚实值→行权比例→期权类型→到期日→剩余天数→合约代码。
    PUT 侧镜像(虚实值/类型/日期靠行权价)。"""
    def _td(v, nd=2, pct=False, comma=False, cls=""):
        val = _fmt(v, nd, pct, comma)
        return f'<td class="{cls}">{val}</td>'

    def _td_oi(v, cls=""):
        """仓差专用: 正值红(资金流入/开仓), 负值绿(资金流出/平仓), 0 灰"""
        if v is None:
            return f'<td class="{cls}">—</td>'
        cls2 = "up" if v > 0 else ("down" if v < 0 else "")
        all_cls = (cls + " " + cls2).strip() if cls2 else cls
        return f'<td class="{all_cls} chg">{_fmt(v, 0, False, True)}</td>'

    def _td_greek(v, nd=4, cls=""):
        """希腊值小数, 保留 4 位"""
        if v is None:
            return f'<td class="{cls}">—</td>'
        try:
            all_cls = ("greek " + cls).strip() if cls else "greek"
            return f'<td class="{all_cls}">{v:.{nd}f}</td>'
        except Exception:
            return f'<td class="{cls}">—</td>'

    def _td_impv(v, cls=""):
        """隐含波动率用百分号显示"""
        if v is None:
            return f'<td class="{cls}">—</td>'
        try:
            all_cls = ("iv " + cls).strip() if cls else "iv"
            return f'<td class="{all_cls}">{v*100:.2f}%</td>'
        except Exception:
            return f'<td class="{cls}">—</td>'

    def _td_symbol(sym, cls="", opt_class=None):
        if not sym:
            return f'<td class="{cls}">—</td>'
        # 显示中文可读名, 原始代码保留在 data-sym/title 供点击复制
        disp = futures_db.option_symbol_display(sym, und_name)
        # ETF 期权代码不直观: 用 标的名+方向+行权价 展示
        _ex, _, _code = sym.partition(".")
        if und_name and _ex in ("SSE", "SZSE") and _code.isdigit():
            dc = "购" if str(opt_class or "").upper().startswith(("CALL", "C")) else "沽"
            try:
                sk = f"{float(strike):g}" if strike is not None else ""
            except Exception:
                sk = ""
            disp = f"{und_name}{dc}{sk}"
        all_cls = ("sym " + cls).strip() if cls else "sym"
        return f'<td class="{all_cls}" data-sym="{html.escape(sym)}" title="{html.escape(sym)}">{html.escape(disp)}</td>'

    def _td_date(v, cls=""):
        if not v:
            return f'<td class="{cls}">—</td>'
        try:
            s = v.strftime("%m-%d") if hasattr(v, "strftime") else str(v)
            if isinstance(v, str):
                # 2026-09-23 -> 09-23
                if len(v) == 10 and v[4] == '-' and v[7] == '-':
                    s = v[5:]
            return f'<td class="{cls}" title="{html.escape(str(v))}">{html.escape(s)}</td>'
        except Exception:
            return f'<td class="{cls}">—</td>'

    def _chg_trend(d):
        chg = None
        _bp = d.get("pre_settlement") or d.get("pre_close")  # 昨结算优先(期货惯例)
        if d.get("last") is not None and _bp is not None:
            chg = d["last"] - _bp
        trend = "up" if (chg is not None and chg >= 0) else "down"
        return chg, trend

    def _call_cells(d):
        if d is None:
            return "".join('<td class="empty">—</td>' for _ in range(32))
        chg, trend = _chg_trend(d)
        return (
            _td(d.get("last"), 2, False, False, trend + " last") +
            _td(chg, 2, False, False, trend + " chg") +
            _td(d.get("pct"), 2, True, False, trend) +
            _td(d.get("open"), 2) +
            _td(d.get("high"), 2) +
            _td(d.get("low"), 2) +
            _td(d.get("amp"), 2, True) +
            _td(d.get("volume"), 0, False, True) +
            _td_oi(d.get("oi_chg")) +
            _td(d.get("oi"), 0, False, True) +
            _td(d.get("amount"), 0, False, True) +
            _td(d.get("ask")) +
            _td(d.get("bid")) +
            _td_greek(d.get("delta"), 4, "delta") +
            _td_greek(d.get("gamma"), 4, "gamma") +
            _td_greek(d.get("theta"), 4, "theta") +
            _td_greek(d.get("vega"), 4, "vega") +
            _td_greek(d.get("rho"), 4, "rho") +
            _td(d.get("intrinsic")) +
            _td(d.get("time_value")) +
            _td(d.get("premium_rate"), 2, True) +
            _td(d.get("leverage"), 1) +
            _td(d.get("real_leverage"), 1) +
            _td(d.get("bs")) +
            _td_impv(d.get("impv")) +
            _td_impv(d.get("hv")) +
            _td(d.get("moneyness"), 2) +
            _td(d.get("volume_multiple"), 0) +
            _td(d.get("option_class")) +
            _td_date(d.get("exercise")) +
            _td(d.get("expire_rest_days"), 0) +
            _td_symbol(d.get("symbol"), opt_class=d.get("option_class"))
        )

    def _put_cells(d):
        if d is None:
            return "".join('<td class="empty">—</td>' for _ in range(32))
        chg, trend = _chg_trend(d)
        # 镜像: 合约代码(最外) ... 最新(最内靠行权价)
        return (
            _td_symbol(d.get("symbol"), opt_class=d.get("option_class")) +
            _td(d.get("expire_rest_days"), 0) +
            _td_date(d.get("exercise")) +
            _td(d.get("option_class")) +
            _td(d.get("volume_multiple"), 0) +
            _td(d.get("moneyness"), 2) +
            _td_impv(d.get("hv")) +
            _td_impv(d.get("impv")) +
            _td(d.get("bs")) +
            _td(d.get("real_leverage"), 1) +
            _td(d.get("leverage"), 1) +
            _td(d.get("premium_rate"), 2, True) +
            _td(d.get("time_value")) +
            _td(d.get("intrinsic")) +
            _td_greek(d.get("rho"), 4, "rho") +
            _td_greek(d.get("vega"), 4, "vega") +
            _td_greek(d.get("theta"), 4, "theta") +
            _td_greek(d.get("gamma"), 4, "gamma") +
            _td_greek(d.get("delta"), 4, "delta") +
            _td(d.get("bid")) +
            _td(d.get("ask")) +
            _td(d.get("oi"), 0, False, True) +
            _td(d.get("amount"), 0, False, True) +
            _td_oi(d.get("oi_chg")) +
            _td(d.get("volume"), 0, False, True) +
            _td(d.get("amp"), 2, True) +
            _td(d.get("low"), 2) +
            _td(d.get("high"), 2) +
            _td(d.get("open"), 2) +
            _td(d.get("pct"), 2, True, False, trend) +
            _td(chg, 2, False, False, trend + " chg") +
            _td(d.get("last"), 2, False, False, trend + " last")
        )

    return _call_cells(cdata), _put_cells(pdata)


# ===== 页面共用脚本（期权模块 + 期权日历 都需要）=====
SYNC_SCRIPT = '''
<script>
function syncData(t){
  var msg=document.getElementById('sync-msg');
  msg.textContent='正在同步'+ (t==='futures'?'期货':'期权') +'...';
  // 网关会全局重写单引号/双引号包裹的 /api/ 字面量, 这里用变量拼接路径, 避免被改坏
  var s='/', a='api', g='gw', y='sync';
  var isGw = location.pathname.split(s).slice(1,3).join(s) === a+s+g;
  var url = (isGw ? s+a+s+g+s+y+s : s+a+s+y+s) + t;
  fetch(url,{method:'POST'}).then(function(r){return r.json();}).then(function(j){
    msg.textContent='已触发同步: '+(j.msg||JSON.stringify(j));
    setTimeout(function(){location.reload();}, 2500);
  }).catch(function(e){msg.textContent='同步请求失败: '+e;});
}
// ===== 期权模块(optchain)导航辅助 =====
function optBase(){
  // 始终指向"期权"模块(optchain); 兼容网关前缀 /api/gw/optchain
  var s='/', a='api', g='gw';
  var isGw = location.pathname.split(s).slice(1,3).join(s) === a+s+g;
  return isGw ? s+a+s+g+s+'optchain' : s+'optchain';
}
function apiBase(){
  // 期权相关 API 基础路径; 走 optchain 模块网关(其 route_path=/optchain),
  // 网关据此转发到 stock_app 的 /optchain/api/options/... (已注册同名别名路由)
  var s='/', a='api', g='gw';
  var isGw = location.pathname.split(s).slice(1,3).join(s) === a+s+g;
  return isGw ? s+a+s+g+s+'optchain'+s+a+s+'options' : s+a+s+'options';
}
function gotoUnderlyingOpt(u){
  // 期权目录页: 点击品种树/快捷入口 -> T型报价
  location.href = optBase() + '/chain?underlying=' + encodeURIComponent(u);
}
// T 型报价页: 无刷新替换数据区
function isChainPage(){ return !!document.getElementById('chain-panel-container'); }
function highlightOptBars(u, exchange){
  // 切换上方品种快捷入口为当前交易所
  if(exchange){
    document.querySelectorAll('.prod-bar-group').forEach(function(g){ g.classList.remove('active'); });
    var prodGroup = document.querySelector('.prod-bar-group[data-ex="' + exchange + '"]');
    if(prodGroup) prodGroup.classList.add('active');
  }
  // 高亮品种入口
  if(u){
    document.querySelectorAll('.chain-foot-bar.prod-bar .foot-item').forEach(function(it){ it.classList.remove('active'); });
    var parts = u.split('.');
    var prefix = parts.length >= 2 ? parts[0]+'.'+parts[1].replace(/\d.*/,'') : u;
    var matched = document.querySelector('.prod-bar-group.active .chain-foot-bar.prod-bar .foot-item[data-prefix="' + prefix + '"]');
    if(matched) matched.classList.add('active');
  }
  // 高亮交易所 Tab
  if(exchange){
    document.querySelectorAll('.chain-foot-bar.exchange-bar .foot-item').forEach(function(it){ it.classList.remove('active'); });
    var exTab = document.querySelector('.chain-foot-bar.exchange-bar .foot-item[data-ex="' + exchange + '"]');
    if(exTab) exTab.classList.add('active');
  }
}
function loadChainPanel(u, exerciseDate){
  // T 型报价页: 无刷新重载, 切换标的(u) 或 行权日(exerciseDate, 形如 '2026-09-23')
  var container = document.getElementById('chain-panel-container');
  if(!container){
    var url = optBase() + '/chain?underlying=' + encodeURIComponent(u);
    if(exerciseDate) url += '&exercise_date=' + encodeURIComponent(exerciseDate);
    location.href = url;
    return;
  }
  var url = new URL(location.href);
  url.searchParams.set('underlying', u);
  if(exerciseDate) url.searchParams.set('exercise_date', exerciseDate);
  else url.searchParams.delete('exercise_date');
  var panelUrl = apiBase() + '/chain_panel?year=' + url.searchParams.get('year') + '&month=' + url.searchParams.get('month') + '&underlying=' + encodeURIComponent(u);
  if(exerciseDate) panelUrl += '&exercise_date=' + encodeURIComponent(exerciseDate);
  container.style.opacity = '0.5';
  fetch(panelUrl).then(function(r){ if(!r.ok) throw new Error('HTTP '+r.status); return r.text(); }).then(function(html){
    container.innerHTML = html;
    container.style.opacity = '1';
    history.replaceState(null, '', url.toString());
    if(window.rebindChainScroll) window.rebindChainScroll();
    // 重新应用当前筛选
    applyOptFilter();
    // 高亮底部栏
    var ex = (u.split('.')[0] || '');
    highlightOptBars(u, ex);
    // 异步加载顶部微笑图，避免阻塞首屏
    loadSmileAsync();
    // 滚动到顶部
    window.scrollTo({top:0, behavior:'smooth'});
    // 启动实时跳动(0.5s 行情 + 2s 希腊值)
    startChainLive();
  }).catch(function(e){
    container.style.opacity = '1';
    showFootMsg('加载 T 型报价失败: '+e);
  });
}

// ===== T 型报价实时跳动: 0.5s 行情 + 2s 希腊值, 就地更新单元格(不整体替换, 保留滚动/点击) =====
// 列序号映射(与 _opt_row_split_html 的 32 列顺序一致); CALL 侧合约代码在末列, PUT 侧在首列
var CALL_COLS = {last:0,chg:1,pct:2,open:3,high:4,low:5,amp:6,volume:7,oi_chg:8,oi:9,amount:10,ask:11,bid:12,delta:13,gamma:14,theta:15,vega:16,rho:17,intrinsic:18,time_value:19,premium_rate:20,leverage:21,real_leverage:22,bs:23,impv:24,hv:25,moneyness:26,volume_multiple:27,option_class:28,exercise:29,expire_rest_days:30};
var PUT_COLS = {symbol:0,expire_rest_days:1,exercise:2,option_class:3,volume_multiple:4,moneyness:5,hv:6,impv:7,bs:8,real_leverage:9,leverage:10,premium_rate:11,time_value:12,intrinsic:13,rho:14,vega:15,theta:16,gamma:17,delta:18,bid:19,ask:20,oi:21,amount:22,oi_chg:23,volume:24,amp:25,low:26,high:27,open:28,pct:29,chg:30,last:31};
function chainFmt(v, nd, pct, comma){
  if(v===null||v===undefined||(typeof v==='number'&&isNaN(v))) return '—';
  var s;
  if(comma){ s = String(Math.round(v)).replace(/\B(?=(\d{3})+(?!\d))/g, ','); }
  else { s = Number(v).toFixed(nd); }
  if(pct) s += '%';
  return s;
}
function trendOf(v){ return (v===null||v===undefined) ? '' : (v>=0 ? 'up' : 'down'); }
function chainFindRow(sym){
  var cell = document.querySelector('td.sym[data-sym="'+ (window.CSS&&CSS.escape?CSS.escape(sym):sym) +'"]');
  if(!cell) return null;
  var tr = cell.closest('tr');
  if(!tr) return null;
  var isPut = (tr.children[0] && tr.children[0] === cell);
  return {tr: tr, cols: isPut ? PUT_COLS : CALL_COLS};
}
function chainSetCell(row, field, text, trend){
  var idx = row.cols[field];
  if(idx === undefined) return;
  var td = row.tr.children[idx];
  if(!td) return;
  if(td.textContent === text) return;
  td.textContent = text;
  if(trend==='up'||trend==='down'){ td.classList.remove('up','down'); td.classList.add(trend); }
  var bg = trend==='up' ? 'rgba(239,68,68,0.35)' : (trend==='down' ? 'rgba(16,185,129,0.35)' : 'rgba(250,204,21,0.35)');
  td.style.backgroundColor = bg;
  setTimeout(function(){ td.style.backgroundColor=''; }, 350);
}
function chainSetField(field, text, trend){
  document.querySelectorAll('[data-field="'+field+'"]').forEach(function(td){
    if(td.textContent === text) return;
    td.textContent = text;
    if(trend==='up'||trend==='down'){ td.classList.remove('up','down'); td.classList.add(trend); }
    var bg = trend==='up' ? 'rgba(239,68,68,0.35)' : (trend==='down' ? 'rgba(16,185,129,0.35)' : 'rgba(250,204,21,0.35)');
    td.style.backgroundColor = bg;
    setTimeout(function(){ td.style.backgroundColor=''; }, 350);
  });
}
function chainUpdateQuotes(quotes){
  if(!quotes) return;
  Object.keys(quotes).forEach(function(sym){
    var row = chainFindRow(sym); if(!row) return;
    var q = quotes[sym];
    chainSetCell(row,'last', chainFmt(q.last,2,false,false), trendOf(q.chg));
    chainSetCell(row,'chg', chainFmt(q.chg,2,false,false), trendOf(q.chg));
    chainSetCell(row,'pct', chainFmt(q.pct,2,true,false), trendOf(q.pct));
    chainSetCell(row,'open', chainFmt(q.open,2));
    chainSetCell(row,'high', chainFmt(q.high,2));
    chainSetCell(row,'low', chainFmt(q.low,2));
    chainSetCell(row,'amp', chainFmt(q.amp,2,true));
    chainSetCell(row,'volume', chainFmt(q.volume,0,false,true));
    chainSetCell(row,'oi_chg', chainFmt(q.oi_chg,0,false,true), trendOf(q.oi_chg));
    chainSetCell(row,'oi', chainFmt(q.oi,0,false,true));
    chainSetCell(row,'amount', chainFmt(q.amount,0,false,true));
    chainSetCell(row,'ask', chainFmt(q.ask,2));
    chainSetCell(row,'bid', chainFmt(q.bid,2));
  });
}
function chainUpdateGreeks(greeks){
  if(!greeks) return;
  Object.keys(greeks).forEach(function(sym){
    var row = chainFindRow(sym); if(!row) return;
    var g = greeks[sym];
    chainSetCell(row,'delta', chainFmt(g.delta,4));
    chainSetCell(row,'gamma', chainFmt(g.gamma,4));
    chainSetCell(row,'theta', chainFmt(g.theta,4));
    chainSetCell(row,'vega', chainFmt(g.vega,4));
    chainSetCell(row,'rho', chainFmt(g.rho,4));
    chainSetCell(row,'intrinsic', chainFmt(g.intrinsic,2));
    chainSetCell(row,'time_value', chainFmt(g.time_value,2));
    chainSetCell(row,'premium_rate', chainFmt(g.premium_rate,2,true));
    chainSetCell(row,'leverage', chainFmt(g.leverage,1));
    chainSetCell(row,'real_leverage', chainFmt(g.real_leverage,1));
    chainSetCell(row,'bs', chainFmt(g.bs,2));
    chainSetCell(row,'impv', g.impv==null?'—':(Number(g.impv)*100).toFixed(2)+'%');
    chainSetCell(row,'hv', g.hv==null?'—':(Number(g.hv)*100).toFixed(2)+'%');
    chainSetCell(row,'moneyness', chainFmt(g.moneyness,2));
  });
}
function chainUpdateUnderlying(j){
  if(!j) return;
  if(j.__underlying__){
    var u=j.__underlying__; var t=trendOf(u.chg);
    chainSetField('u-last', chainFmt(u.last,2), t);
    chainSetField('u-chg', chainFmt(u.chg,2)+' ('+chainFmt(u.pct_change,2,true)+')', t);
  }
  if(j.__index__){ var ti=trendOf(j.__index__.chg); chainSetField('idx-last', chainFmt(j.__index__.last,2), ti); chainSetField('idx-chg', chainFmt(j.__index__.chg,2)+' ('+chainFmt(j.__index__.pct,2,true)+')', ti); }
  if(j.__wh_index__){ var tw=trendOf(j.__wh_index__.chg); chainSetField('wh-last', chainFmt(j.__wh_index__.last,2), tw); chainSetField('wh-chg', chainFmt(j.__wh_index__.chg,2)+' ('+chainFmt(j.__wh_index__.pct,2,true)+')', tw); }
  if(j.__vix__!==undefined && j.__vix__!==null) chainSetField('u-vix', chainFmt(j.__vix__,2,true));
}
function startChainLive(){
  if(window.__chainLive){ clearInterval(window.__chainLive.q); clearInterval(window.__chainLive.g); window.__chainLive=null; }
  var url = new URL(location.href);
  var u = url.searchParams.get('underlying') || '';
  var y = url.searchParams.get('year') || '';
  var m = url.searchParams.get('month') || '';
  var ed = url.searchParams.get('exercise_date') || '';
  if(!u) return;
  function build(path){ var p = apiBase()+path+'?year='+y+'&month='+m+'&underlying='+encodeURIComponent(u); if(ed) p+='&exercise_date='+encodeURIComponent(ed); return p; }
  function liveTick(){ var el=document.getElementById('live-ts'); if(!el){ el=document.createElement('span'); el.id='live-ts'; el.style.cssText='position:fixed;right:10px;bottom:10px;font-size:12px;color:#9ca3af;z-index:9999;background:rgba(17,24,39,.55);padding:2px 8px;border-radius:6px;pointer-events:none'; document.body.appendChild(el); } var d=new Date(); function p(n){return (n<10?'0':'')+n;} el.textContent='● 实时刷新 '+p(d.getHours())+':'+p(d.getMinutes())+':'+p(d.getSeconds()); }
  function tickQuotes(){ fetch(build('/chain_quotes')).then(function(r){return r.json();}).then(function(j){ if(j&&j.ok){ chainUpdateUnderlying(j); chainUpdateQuotes(j.quotes||{}); liveTick(); } }).catch(function(e){ if(window.console) console.warn('[chainQuotes]', e); }); }
  function tickGreeks(){ fetch(build('/chain_greeks')).then(function(r){return r.json();}).then(function(j){ if(j&&j.ok){ chainUpdateGreeks(j.greeks||{}); if(j.vix!==undefined&&j.vix!==null) chainSetField('u-vix', chainFmt(j.vix,2,true)); } }).catch(function(e){ if(window.console) console.warn('[chainGreeks]', e); }); }
  tickQuotes(); tickGreeks();
  window.__chainLive = { q: setInterval(tickQuotes, 500), g: setInterval(tickGreeks, 2000) };
}

// 异步加载波动率微笑图(首屏占位，后台单独请求)
function loadSmileAsync(){
  var box = document.getElementById('smile-async');
  if(!box) return;
  var y = box.getAttribute('data-year') || '';
  var m = box.getAttribute('data-month') || '';
  var u = box.getAttribute('data-underlying') || '';
  if(!u) return;
  var url = apiBase() + '/chain_smile?year=' + encodeURIComponent(y) + '&month=' + encodeURIComponent(m) + '&underlying=' + encodeURIComponent(u);
  fetch(url).then(function(r){ if(!r.ok) throw new Error('HTTP '+r.status); return r.json(); }).then(function(j){
    if(j && j.svg){
      var wrapper = document.createElement('div');
      wrapper.innerHTML = j.svg;
      var svgBox = wrapper.firstElementChild;
      if(svgBox) box.parentNode.replaceChild(svgBox, box);
    } else {
      box.innerHTML = '<div class="smile-empty">微笑图数据不可用</div>';
    }
  }).catch(function(e){
    box.innerHTML = '<div class="smile-empty">微笑图加载失败: '+e+'</div>';
  });
}

// 行权月切换: 点击顶部 ex-tab, 重载当前标的+指定行权日
function onExerciseDateChange(ed){
  if(!ed) return;
  var url = new URL(location.href);
  var u = url.searchParams.get('underlying') || '';
  if(!u) return;
  // 高亮当前 tab
  document.querySelectorAll('.ex-tab').forEach(function(b){ b.classList.toggle('active', b.getAttribute('data-ed') === ed); });
  if(isChainPage()) loadChainPanel(u, ed);
  else {
    url.searchParams.set('exercise_date', ed);
    location.href = url.toString();
  }
}

// 合约代码单击复制: 全局事件代理
document.addEventListener('click', function(e){
  var cell = e.target.closest && e.target.closest('.t-type td.sym');
  if(!cell) return;
  var txt = cell.getAttribute('data-sym') || cell.textContent.trim();
  if(!txt || txt === '—') return;
  if(navigator.clipboard && navigator.clipboard.writeText){
    navigator.clipboard.writeText(txt).then(function(){
      cell.style.color = 'var(--accent)';
      setTimeout(function(){ cell.style.color = ''; }, 600);
    }).catch(function(){});
  } else {
    // fallback: 选中
    var range = document.createRange();
    range.selectNode(cell);
    var sel = window.getSelection();
    sel.removeAllRanges(); sel.addRange(range);
  }
});
function switchOptExchange(exchange, el){
  // 期权目录页: 点击父级交易所 Tab -> 切换子级品种 Tab 并显示对应内容区
  // 父级高亮提前, 避免特殊跳转时看起来"选中后没了"
  document.querySelectorAll('.opt-parent-bar .p-tab').forEach(function(it){ it.classList.remove('active'); });
  if(el) el.classList.add('active');

  if(exchange === '__fav__'){ location.href = optBase() + '/fav'; return; }
  if(exchange === '__all__'){ location.href = optBase() + '/'; return; }
  if(exchange === '__main__' || exchange === '__atm__' || exchange === '__filter__' || exchange === '__vix__' || exchange === '__rank__'){
    var map = {'__main__':'main','__atm__':'atm','__filter__':'filter','__vix__':'vix','__rank__':'rank'};
    location.href = optBase() + '/' + map[exchange];
    return;
  }

  // 切换子级品种 Tab: 只显示当前交易所的
  document.querySelectorAll('.opt-child-bar').forEach(function(bar){ bar.classList.remove('active'); });
  var childBar = document.getElementById('child-bar-' + exchange);
  if(childBar) childBar.classList.add('active');

  // 切换内容区: 只显示当前交易所 section
  document.querySelectorAll('.opt-ex-sec').forEach(function(sec){ sec.classList.remove('active'); });
  var activeSec = document.querySelector('.opt-ex-sec[data-exchange="' + exchange + '"]');
  if(activeSec){
    activeSec.classList.add('active');
    // 默认选中该交易所第一个品种
    var firstProd = activeSec.querySelector('.opt-prod-card');
    if(firstProd) switchOptProduct(firstProd.getAttribute('data-prod-key'), null);
  } else if(exchange !== '__all__'){
    showFootMsg('库内暂无 ' + exchange + ' 的期权标的数据');
  }
}

function switchOptProduct(prodKey, el){
  // 点击子级品种 Tab -> 高亮该 Tab 并把对应产品卡片滚动到可视区
  if(!prodKey) return;
  document.querySelectorAll('.opt-child-bar.active .c-tab').forEach(function(it){ it.classList.remove('active'); });
  if(el){
    el.classList.add('active');
  } else {
    var auto = document.querySelector('.opt-child-bar.active .c-tab[data-prod="' + prodKey + '"]');
    if(auto) auto.classList.add('active');
  }
  var card = document.getElementById('prod-' + prodKey);
  if(card){
    var scrollBox = document.querySelector('.optchain-scroll');
    if(scrollBox) scrollBox.scrollTop = card.offsetTop - 12;
  }
}

// 页面加载后默认选中第一个有数据的交易所；URL 带 ?exchange=xxx 时定位到对应交易所
window.addEventListener('load', function(){
  var wanted = new URLSearchParams(location.search).get('exchange');
  if(wanted){
    var wantedTab = document.querySelector('.opt-parent-bar .p-tab[data-ex="' + wanted + '"]');
    if(wantedTab){ switchOptExchange(wanted, wantedTab); return; }
  }
  var firstP = document.querySelector('.opt-parent-bar .p-tab[data-ex]:not([data-ex^="__"])');
  if(firstP) switchOptExchange(firstP.getAttribute('data-ex'), firstP);
});
function gotoOptions(sym){
  // 从期货/期权日历点击 -> 跳转到"期权"模块(optchain)的 T 型报价
  location.href = optBase() + '/chain?underlying=' + encodeURIComponent(sym);
}
function jumpToMonth(y,m){
  // 跳转到指定年/月(保留 underlying), 指向 optchain 的 T 型报价
  var u = new URLSearchParams(location.search).get('underlying') || '';
  location.href = optBase() + '/chain?year='+y+'&month='+m + (u ? ('&underlying='+encodeURIComponent(u)) : '');
}
function filterOptionsByUnderlying(sym){
  // 期权日历点击标的 -> 跳转到"期权"模块(optchain)的 T 型报价
  location.href = optBase() + '/chain?underlying=' + encodeURIComponent(sym);
}

// 底部品种快捷入口：先查询最近月份标的，在 T 型报价页无刷新更新数据区，其他页跳转
function gotoUnderlying(prefix){
  var url = apiBase() + '/nearest_underlying?prefix=' + encodeURIComponent(prefix);
  fetch(url).then(function(r){return r.json();}).then(function(j){
    if(j.ok && j.underlying){
      loadChainPanel(j.underlying);
    } else {
      showFootMsg(j.msg || '未找到该品种的期权标的');
    }
  }).catch(function(e){ showFootMsg('请求失败: '+e); });
}

// 底部交易所 Tab：高亮当前；在 T 型报价页无刷新切换交易所默认标的，在期权日历页过滤日历格子
function onExchangeTab(exchange, el){
  // 特殊动作按钮
  if(exchange === '__filter__'){
    var f = document.getElementById('fMoney');
    if(f) f.scrollIntoView({behavior:'smooth', block:'center'});
    return;
  }
  if(exchange === '__vix__'){
    var vb = document.querySelector('.vix-stat');
    if(vb) vb.scrollIntoView({behavior:'smooth', block:'center'});
    return;
  }
  if(exchange === '__atm__'){
    var fm = document.getElementById('fMoney');
    if(fm){ fm.value='atm'; applyOptFilter(); }
    return;
  }
  if(exchange === '__main__' || exchange === '__rank__'){
    var map2 = {'__main__':'main','__rank__':'rank'};
    location.href = optBase() + '/' + map2[exchange];
    return;
  }
  if(exchange === '__fav__'){ location.href = optBase() + '/fav'; return; }
  if(exchange === '__all__'){ location.href = optBase() + '/'; return; }
  // 高亮当前 Tab
  document.querySelectorAll('.chain-foot-bar.exchange-bar .foot-item').forEach(function(it){ it.classList.remove('active'); });
  if(el) el.classList.add('active');

  // T 型报价页：无刷新加载该交易所默认标的
  if(isChainPage()){
    // 立即切换上方品种快捷入口为该交易所（不等接口返回，视觉跟手）
    document.querySelectorAll('.prod-bar-group').forEach(function(g){ g.classList.remove('active'); });
    var pg = document.querySelector('.prod-bar-group[data-ex="' + exchange + '"]');
    if(pg) pg.classList.add('active');
    // 清除品种入口高亮（等新标的加载后再由 highlightOptBars 重新高亮）
    document.querySelectorAll('.chain-foot-bar.prod-bar .foot-item').forEach(function(it){ it.classList.remove('active'); });
    var url = apiBase() + '/exchange_default_underlying?exchange=' + encodeURIComponent(exchange);
    fetch(url).then(function(r){return r.json();}).then(function(j){
      if(j.ok && j.underlying){
        loadChainPanel(j.underlying);
      } else {
        showFootMsg(j.msg || '未找到该交易所的期权标的');
      }
    }).catch(function(e){ showFootMsg('请求失败: '+e); });
    return;
  }

  // 期权日历页：过滤日历格子
  var cells = document.querySelectorAll('.cal-table td[data-exchange]');
  var anyShown = false;
  cells.forEach(function(td){
    var show = td.getAttribute('data-exchange') === exchange;
    td.style.display = show ? '' : 'none';
    if(show) anyShown = true;
  });
  if(!anyShown){
    showFootMsg('当前月份没有 ' + exchange + ' 的期权到期数据');
  }
}
function showFootMsg(text){
  var msg = document.getElementById('sync-msg');
  if(!msg) return;
  msg.textContent = text;
  msg.style.color = '#f87171';
  setTimeout(function(){ msg.textContent = ''; msg.style.color = ''; }, 3000);
}

// 期权标的下拉切换：T 型报价页无刷新更新数据区，其他页整页跳转
function onUnderlyingChange(sel){
  var u = sel.value;
  if(isChainPage()){
    loadChainPanel(u);
  } else {
    var url = new URL(location.href);
    url.searchParams.set('underlying', u);
    location.href = url.toString();
  }
}

// 期权链筛选：显示方式(全部/实值/虚值/平值) + 行权价区间
function applyOptFilter(){
  var area = document.getElementById('tArea');
  if(!area) return;
  var S = parseFloat(area.getAttribute('data-s'));
  var m = document.getElementById('fMoney').value;
  var kmin = parseFloat(document.getElementById('fKmin').value) || -Infinity;
  var kmax = parseFloat(document.getElementById('fKmax').value) || Infinity;
  document.querySelectorAll('#tArea tr[data-strike]').forEach(function(tr){
    var k = parseFloat(tr.getAttribute('data-strike'));
    var show = (k >= kmin && k <= kmax);
    if(show && m !== 'all' && !isNaN(S)){
      if(m === 'itm') show = k < S;          // 实值：认购实值(行权价低于标的)
      else if(m === 'otm') show = k > S;     // 虚值
      else if(m === 'atm') show = Math.abs(k - S) <= Math.max(0.03 * S, 1); // 平值：标的±3%内
    }
    tr.style.display = show ? '' : 'none';
  });
}
function resetOptFilter(){
  document.getElementById('fMoney').value = 'all';
  document.getElementById('fKmin').value = '';
  document.getElementById('fKmax').value = '';
  applyOptFilter();
}

// ===== 期权链双滚轮互斥同步 =====
(function(){
  function setupChainScroll(){
    var callWrap=document.getElementById('callWrap');
    var putWrap=document.getElementById('putWrap');
    var topScroll=document.getElementById('tScrollTop');
    var topInner=document.getElementById('tScrollTopInner');
    var bottomScroll=document.getElementById('tScrollBottom');
    var bottomInner=document.getElementById('tScrollBottomInner');
    if(!callWrap||!putWrap||!topScroll||!bottomScroll) return;

    function callMax(){ return Math.max(0, callWrap.scrollWidth-callWrap.clientWidth); }
    function putMax(){ return Math.max(0, putWrap.scrollWidth-putWrap.clientWidth); }
    function topMax(){ return Math.max(0, topScroll.scrollWidth-topScroll.clientWidth); }
    function bottomMax(){ return Math.max(0, bottomScroll.scrollWidth-bottomScroll.clientWidth); }

    function syncPutFromCall(){
      var cMax=callMax(), pMax=putMax();
      var ratio=cMax>0?callWrap.scrollLeft/cMax:0;
      putWrap.scrollLeft=pMax*(1-ratio);
    }
    function syncBarsFromCall(){
      var cMax=callMax();
      var ratio=cMax>0?callWrap.scrollLeft/cMax:0;
      topScroll.scrollLeft=topMax()*ratio;
      bottomScroll.scrollLeft=bottomMax()*ratio;
    }
    function setCallRatio(ratio){
      var cMax=callMax();
      callWrap.scrollLeft=cMax*ratio;
      syncPutFromCall();
    }

    // 初始化自定义滚动条宽度与CALL内容一致
    function initWidths(){
      var w=callWrap.scrollWidth+'px';
      topInner.style.width=w;
      bottomInner.style.width=w;
      syncBarsFromCall();
      syncPutFromCall();
    }

    var ignore=false;
    function wrapListener(el, handler){
      el.addEventListener('scroll',function(){
        if(ignore) return;
        ignore=true;
        handler();
        requestAnimationFrame(function(){ ignore=false; });
      });
    }

    wrapListener(callWrap,function(){
      syncBarsFromCall();
      syncPutFromCall();
    });
    wrapListener(putWrap,function(){
      var pMax=putMax(), cMax=callMax();
      var ratio=pMax>0?(1-putWrap.scrollLeft/pMax):0;
      callWrap.scrollLeft=cMax*ratio;
      syncBarsFromCall();
    });
    wrapListener(topScroll,function(){
      var ratio=topMax()>0?topScroll.scrollLeft/topMax():0;
      setCallRatio(ratio);
      bottomScroll.scrollLeft=bottomMax()*ratio;
    });
    wrapListener(bottomScroll,function(){
      var ratio=bottomMax()>0?bottomScroll.scrollLeft/bottomMax():0;
      setCallRatio(ratio);
      topScroll.scrollLeft=topMax()*ratio;
    });

    // ===== 垂直同步：CALL / 行权价 / PUT 三栏随任意一栏滚动而同步 =====
    // 用独立的 ignore 锁(不等水平)，避免水平/垂直事件相互吞掉。
    // 关键: 三表 tr 高度不一致(CALL/PUT 含合约代码换行, STRIKE 单列无换行),
    //       若按 scrollTop 像素同步, STRIKE 滚到底时 CALL/PUT 对应行号偏少, 底部数据看不见。
    //       改为按比例(0~1)同步, 三表同时滚到底/顶, 不论每行高度差多少。
    var strikeWrap=document.getElementById('strikeWrap');
    var vSyncEls=[callWrap, strikeWrap, putWrap].filter(Boolean);
    var vIgnore=false;
    function syncV(src){
      var maxSrc=Math.max(0, src.scrollHeight - src.clientHeight);
      var ratio=maxSrc>0 ? src.scrollTop / maxSrc : 0;
      for(var i=0;i<vSyncEls.length;i++){
        var el=vSyncEls[i];
        if(el && el!==src){
          var maxEl=Math.max(0, el.scrollHeight - el.clientHeight);
          // 保留到比例位, 防止某个表内容短先到底时它停、其他表继续同步
          el.scrollTop=maxEl * ratio;
        }
      }
    }
    for(var i=0;i<vSyncEls.length;i++){
      (function(el){
        el.addEventListener('scroll', function(){
          if(vIgnore) return;
          vIgnore=true;
          syncV(el);
          requestAnimationFrame(function(){ vIgnore=false; });
        }, {passive:true});
      })(vSyncEls[i]);
    }

    initWidths();
    window.addEventListener('resize',initWidths);
    // 表格数据异步加载后尺寸可能变化，每秒校准一次（最多10秒）
    var checks=0, timer=setInterval(function(){
      if(++checks>10) clearInterval(timer);
      initWidths();
    },1000);
  }
  // 暴露给 AJAX 刷新后重新绑定
  window.rebindChainScroll = setupChainScroll;
  if(document.readyState==='loading'){
    document.addEventListener('DOMContentLoaded',setupChainScroll);
  } else {
    setupChainScroll();
  }
  // 首屏 T 型表渲染完成后，再异步加载微笑图（不阻塞核心报价）
  if(document.readyState==='loading'){
    document.addEventListener('DOMContentLoaded',loadSmileAsync);
  } else {
    loadSmileAsync();
  }
})();
// ===== T型表双击看涨/看跌行 -> 弹出该期权K线 =====
function openKlinePopup(sym){
  if(!sym) return;
  var modal=document.getElementById('kline-popup-modal');
  var frame=document.getElementById('kline-popup-frame');
  if(!modal||!frame) return;
  document.getElementById('kline-popup-title').textContent=sym;
  frame.src=optBase()+'/kline-popup?symbol='+encodeURIComponent(sym);
  modal.classList.add('open');
  document.body.style.overflow='hidden';
  setTimeout(function(){ try{frame.contentWindow.focus();}catch(e){} }, 100);
}
function closeKlinePopup(){
  var modal=document.getElementById('kline-popup-modal');
  var frame=document.getElementById('kline-popup-frame');
  if(!modal) return;
  modal.classList.remove('open');
  document.body.style.overflow='';
  if(frame) frame.src='about:blank';
}
document.addEventListener('keydown',function(e){
  if(e.key==='Escape'){
    var m=document.getElementById('kline-popup-modal');
    if(m && m.classList.contains('open')) closeKlinePopup();
  }
});
</script>
'''


def _build_expiry_html(year, month, view, base_path, underlying=None, back_js=None, back_label=None, show_calendar=True, exercise_date=None):
    """渲染到期日历页面. view=futures 期货到期表 / view=options 期权日历; base_path 为当前路由路径.
    show_calendar=False 用于期权模块 T 型报价页，只展示期权链面板，不渲染日历视图与按日期汇总。"""
    from services import futures_db  # 函数级绑定: 供日历格子/汇总使用, 避免嵌套块内 import 造成的 free-variable 作用域问题
    import datetime as dt
    if view not in ("futures", "options"):
        view = "futures"
    now = dt.datetime.now()
    if show_calendar:
        title = f"{year}年{month}月 {('期权日历' if view=='options' else '期货到期表')}"
        subtitle = ('期权行权日汇总' if view=='options' else '期货合约到期日汇总')
    else:
        # T 型报价页：去掉日历标题，改为标的名称
        title = f"期权 T 型报价 · {html.escape(underlying or '')}" if view == 'options' and underlying else f"{year}年{month}月 期权链"
        subtitle = ''
    year_opts = "".join(f'<option value="{y}"{" selected" if y==year else ""}>{y}年</option>' for y in range(now.year-3, now.year+4))
    month_opts = "".join(f'<option value="{m}"{" selected" if m==month else ""}>{m}月</option>' for m in range(1, 13))
    weekday_headers = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    cache_key = f"{year}-{month}"
    # 期权条件查询: 关联值(用于搜索框回填与翻月链接)
    underlying_esc = html.escape(underlying or "", quote=True)
    underlying_qs = ("&underlying=" + _urlquote(underlying)) if underlying else ""


    import time as _time
    with expiry_cache_lock:
        _ce = expiry_cache.get(cache_key)
        if _ce is None:
            expiry_cache[cache_key] = {"status": "loading", "data": {}, "ts": _time.time()}
            threading.Thread(target=_load_expiry_cache, args=(cache_key, year, month), daemon=True).start()
        elif _ce.get("status") == "done" and _time.time() - _ce.get("ts", 0) > EXPIRY_CACHE_TTL:
            # TTL 到期: 先用旧数据渲染, 后台刷新(数据库可能被同步任务更新)
            expiry_cache[cache_key] = {"status": "loading", "data": _ce.get("data", {}), "ts": _time.time()}
            threading.Thread(target=_load_expiry_cache, args=(cache_key, year, month), daemon=True).start()
        cache_entry = expiry_cache[cache_key]

    query_result = cache_entry.get("data", {})
    error_msg = cache_entry.get("error", "")
    # T 型报价页不依赖到期缓存，不显示"正在查询"的 loading 提示
    if show_calendar and cache_entry.get("status") == "loading" and not query_result:
        error_msg = "正在从天勤 TqSdk 查询到期数据，请稍后刷新页面..."

    # 期权视图 + 指定标的: 过滤期权日历仅显示该标的, 并准备期权链面板
    disp_options = query_result.get('options', {})
    chain_panel = ""
    opt_hint = ""
    if view == 'options' and underlying:
        from services import futures_db
        matches = futures_db.resolve_underlying_matches(underlying) or []
        match_set = set(matches)
        is_specific = (len(matches) == 1 and matches[0] == underlying)
        # 按匹配集合过滤当月期权日历(支持精确合约 与 品种前缀)
        disp_options = {}
        for _d, _o in query_result.get('options', {}).items():
            us = _o.get('underlyings', {})
            hit = {u: c for u, c in us.items() if u in match_set}
            if hit:
                disp_options[_d] = {'exchange': _o.get('exchange', ''), 'underlyings': hit}
        ret_js = back_js or "location.href=location.pathname"
        back_label_html = html.escape(back_label or "← 返回完整期权日历")
        if not matches:
            chain_panel = (f'<div class="chain-box"><div class="chain-head">📊 期货关联值 <b>{html.escape(underlying)}</b></div>'
                           f'<div class="error-msg">该期货关联值在库中暂无对应期权数据（可能尚未同步期权行情，请点上方“同步期权”后重试）。</div>'
                           f'<a href="#" class="chain-back" onclick="{ret_js};return false;">{back_label_html}</a></div>')
        elif is_specific:
            chain = futures_db.query_option_chain(underlying, exercise_date=exercise_date)
            if chain is None:
                chain_panel = (f'<div class="chain-box"><div class="chain-head">📊 标的 <b>{html.escape(underlying)}</b> 对应期权</div>'
                               f'<div class="error-msg">该合约暂无对应期权数据。</div>'
                               f'<a href="#" class="chain-back" onclick="{ret_js};return false;">{back_label_html}</a></div>')
            else:
                _live = _get_chain_live_greeks(underlying, chain)
                _meta = (_live.pop("__underlying__", {}) if isinstance(_live, dict) else {}) or {}
                _vix = (_live.pop("__vix__", None) if isinstance(_live, dict) else None)
                _idx = (_live.pop("__index__", None) if isinstance(_live, dict) else None)
                _wh_idx = (_live.pop("__wh_index__", None) if isinstance(_live, dict) else None)
                _months = futures_db.get_underlying_months_with_days(underlying) or []
                call_vol = put_vol = call_oi = put_oi = 0
                for st in chain["strikes"]:
                    k = st["strike"]
                    csym = st["call"]["symbol"] if st.get("call") else None
                    psym = st["put"]["symbol"] if st.get("put") else None
                    cd = _live.get(csym) if (_live and csym) else None
                    pd = _live.get(psym) if (_live and psym) else None
                    if cd:
                        call_vol += cd.get("volume") or 0
                        call_oi += cd.get("oi") or 0
                    if pd:
                        put_vol += pd.get("volume") or 0
                        put_oi += pd.get("oi") or 0
                total_vol = call_vol + put_vol
                total_oi = call_oi + put_oi
                u_last = _meta.get("last")
                u_pre = _meta.get("pre_close")
                u_chg = _meta.get("chg")
                u_pct = _meta.get("pct_change")
                u_trend = "up" if (u_chg is not None and u_chg >= 0) else "down"
                _ex = "、".join(chain["exercise_dates"])
                # 沽购比 PCR: 认沽/认购 (成交量 PCR + 持仓量 PCR), 核心情绪指标
                pcr_vol = (put_vol / call_vol) if call_vol else None
                pcr_oi = (put_oi / call_oi) if call_oi else None
                # 波动率微笑改为异步加载，避免阻塞首屏 T 型表渲染
                _smile_html = _build_smile_placeholder(year, month, underlying)
                # 期权标的下拉(同品种月份 + 剩余天数, 切换时联动重载, 到期日随之改变)
                _months_opts = "".join(
                    f'<option value="{html.escape(u)}"{" selected" if u == underlying else ""}>'
                    f'{futures_db.option_underlying_name(u)}（{("已到期" if (rem is not None and rem < 0) else (str(rem) + "天")) if rem is not None else "—"}）</option>'
                    for u, ds, rem in _months
                )
                _und_name = futures_db.option_underlying_name(underlying)
                _summary_bar = (
                    f'<div class="underlying-bar">'
                    f'<span class="u-name">{html.escape(_und_name)}</span><span class="u-code">{html.escape(underlying)}</span>'
                    f'<span class="u-last {u_trend}" data-field="u-last">{_fmt(u_last)}</span>'
                    f'<span class="u-chg {u_trend}" data-field="u-chg">{_fmt(u_chg, 2)} ({_fmt(u_pct, 2, True)})</span>'
                    f'<span class="u-sep"></span>'
                    f'<span class="u-stat"><b>期权成交</b>{total_vol:,.0f}</span>'
                    f'<span class="u-stat"><b>期权持仓</b>{total_oi:,.0f}</span>'
                    f'<span class="u-stat call-stat"><b>看涨成交</b>{call_vol:,.0f}</span>'
                    f'<span class="u-stat put-stat"><b>看跌成交</b>{put_vol:,.0f}</span>'
                    f'<span class="u-stat call-stat"><b>看涨持仓</b>{call_oi:,.0f}</span>'
                    f'<span class="u-stat put-stat"><b>看跌持仓</b>{put_oi:,.0f}</span>'
                    f'<span class="u-sep"></span>'
                    f'<span class="u-stat pcr-stat"><b>成交量PCR</b>{_fmt(pcr_vol, 3) if pcr_vol is not None else "—"}</span>'
                    f'<span class="u-stat pcr-stat"><b>持仓量PCR</b>{_fmt(pcr_oi, 3) if pcr_oi is not None else "—"}</span>'
                    f'<span class="u-stat vix-stat" data-field="u-vix"><b>VIX(估算)</b>{_fmt(_vix, 2, True) if _vix is not None else "—"}</span>'
                    f'</div>'
                )
                _idx_html = ""
                if _idx or _wh_idx:
                    _idx_html = f'<div class="idx-bar">'
                    if _idx:
                        i_trend = "up" if (_idx.get("chg") is not None and _idx["chg"] >= 0) else "down"
                        _idx_html += (
                            f'<span class="idx-name">📈 上证指数</span>'
                            f'<span class="idx-last {i_trend}" data-field="idx-last">{_fmt(_idx.get("last"))}</span>'
                            f'<span class="idx-chg {i_trend}" data-field="idx-chg">{_fmt(_idx.get("chg"), 2)}（{_fmt(_idx.get("pct"), 2, True)}）</span>'
                        )
                    if _wh_idx:
                        w_trend = "up" if (_wh_idx.get("chg") is not None and _wh_idx["chg"] >= 0) else "down"
                        _idx_html += (
                            f'<span class="idx-sep"></span>'
                            f'<span class="idx-name" style="color:var(--gold)">🏭 文华商品</span>'
                            f'<span class="idx-last {w_trend}" data-field="wh-last">{_fmt(_wh_idx.get("last"))}</span>'
                            f'<span class="idx-chg {w_trend}" data-field="wh-chg">{_fmt(_wh_idx.get("chg"), 2)}（{_fmt(_wh_idx.get("pct"), 2, True)}）</span>'
                            f'<span class="idx-note" title="{_wh_idx.get("note", "")}">估算</span>'
                        )
                    _idx_html += '</div>'

                # 底部品种快捷入口 + 交易所分类 Tab (对齐 WH6 底部工具条)
                # 按交易所分组生成上方品种快捷入口；点击下方交易所 Tab 时 JS 切换显示
                _exchange_items = [
                    ("我的关注", "__fav__"), ("上期所期权", "SHFE"), ("上期能源期权", "INE"),
                    ("大商所期权", "DCE"), ("郑商所期权", "CZCE"), ("广期所期权", "GFEX"),
                    ("中金所期权", "CFFEX"), ("上交所期权", "SSE"), ("深交所期权", "SZSE"),
                    ("期权主力合约", "__main__"), ("平值期权", "__atm__"),
                    ("条件筛选", "__filter__"), ("期权VIX指数", "__vix__"), ("标的排名", "__rank__"),
                ]
                _current_ex = (underlying.split('.')[0] if '.' in underlying else chain.get("exchange", "SHFE"))
                try:
                    _tree_groups = futures_db.list_option_product_tree()
                except Exception as _e:
                    _tree_groups = []
                _prod_bars_parts = []
                for _ex, _ex_name, _prods in _tree_groups:
                    if not _prods:
                        continue
                    _is_active = "active" if _ex == _current_ex else ""
                    _items_html = "".join(
                        f'<span class="foot-item" data-prefix="{html.escape(pk)}" data-ex="{html.escape(_ex)}" '
                        f'onclick="gotoUnderlying(\'{html.escape(pk).replace(chr(39), chr(92)+chr(39))}\')">{html.escape(pname)}</span>'
                        for pk, pname, _ in _prods
                    )
                    _prod_bars_parts.append(
                        f'<div class="prod-bar-group {_is_active}" data-ex="{html.escape(_ex)}">'
                        f'<div class="chain-foot-bar prod-bar">{_items_html}</div></div>'
                    )
                _prod_bars_html = "".join(_prod_bars_parts)
                _exchange_bar = (
                    '<div class="chain-foot-bar exchange-bar">'
                    + "".join(f'<span class="foot-item" data-ex="{html.escape(v)}" onclick="onExchangeTab(\'{html.escape(v).replace(chr(39), chr(92)+chr(39))}\', this)">{html.escape(n)}</span>' for n, v in _exchange_items)
                    + '</div>'
                )

                _filter_html = (
                    '<div class="ob-filter">'
                    '<label>显示 <select id="fMoney">'
                    '<option value="all">全部</option><option value="itm">实值</option>'
                    '<option value="otm">虚值</option><option value="atm">平值</option></select></label>'
                    '<label>行权价≥ <input id="fKmin" type="number" placeholder="最小" style="width:90px"></label>'
                    '<label>行权价≤ <input id="fKmax" type="number" placeholder="最大" style="width:90px"></label>'
                    '<button type="button" onclick="applyOptFilter()">筛选</button>'
                    '<button type="button" onclick="resetOptFilter()">重置</button>'
                    '</div>'
                )
                _call_headers = (
                    '<th class="last">最新</th><th>涨跌</th><th>涨跌幅</th><th>开</th><th>最高</th><th>最低</th><th>振幅</th>'
                    '<th>成交量</th><th>仓差</th><th>持仓量</th><th>成交额</th>'
                    '<th>卖价</th><th>买价</th><th class="greek">Δ</th><th class="greek">Γ</th><th class="greek">Θ</th><th class="greek">V</th><th class="greek">Rho</th>'
                    '<th>内在价值</th><th>时间价值</th><th>溢价率</th>'
                    '<th>杠杆</th><th>真实杠杆</th><th>理论价</th><th class="iv">隐含波动率</th><th class="iv">历史波动率</th><th>虚实值</th><th>行权比例</th><th>期权类型</th><th>到期日</th><th>剩余天数</th><th class="sym">合约代码</th>'
                )
                _put_headers = (
                    '<th class="sym">合约代码</th><th>剩余天数</th><th>到期日</th><th>期权类型</th><th>行权比例</th><th>虚实值</th><th class="iv">历史波动率</th><th class="iv">隐含波动率</th><th>理论价</th><th>真实杠杆</th><th>杠杆</th><th>溢价率</th>'
                    '<th>时间价值</th><th>内在价值</th><th class="greek">Rho</th><th class="greek">V</th><th class="greek">Θ</th><th class="greek">Γ</th><th class="greek">Δ</th>'
                    '<th>买价</th><th>卖价</th><th>持仓量</th><th>成交额</th><th>仓差</th><th>成交量</th><th>振幅</th><th>最低</th><th>最高</th><th>开</th><th>涨跌幅</th><th>涨跌</th><th class="last">最新</th>'
                )
                _call_rows = ""
                _strike_rows = ""
                _put_rows = ""
                for st in chain["strikes"]:
                    k = st["strike"]
                    csym = st["call"]["symbol"] if st.get("call") else None
                    psym = st["put"]["symbol"] if st.get("put") else None
                    cd = _live.get(csym) if (_live and csym) else None
                    pd = _live.get(psym) if (_live and psym) else None
                    c_html, p_html = _opt_row_split_html(csym, cd, psym, pd, und_name=_und_name, strike=k)
                    _call_rows += (f'<tr data-strike="{k:g}" ondblclick="openKlinePopup(\'{csym}\')">{c_html}</tr>'
                                   if csym else f'<tr data-strike="{k:g}">{c_html}</tr>')
                    _put_rows += (f'<tr data-strike="{k:g}" ondblclick="openKlinePopup(\'{psym}\')">{p_html}</tr>'
                                  if psym else f'<tr data-strike="{k:g}">{p_html}</tr>')
                    _strike_rows += f'<tr data-strike="{k:g}"><td class="strike">{k:g}</td></tr>'
                _s_val = "" if u_last is None else (f"{u_last:.4f}")
                # 行权月切换器: 同一 underlying 多个行权日(如沪镍 1型横跨多月)时显示,
                # 让用户能切换单月 T 型表; active = chain["exercise_date_filter"] 或第一个
                _ex_tabs_html = ""
                _ex_list = list(chain.get("exercise_dates") or [])
                if len(_ex_list) > 1:
                    _cur_ed = chain.get("exercise_date_filter") or _ex_list[0]
                    import datetime as _dt
                    _today = _dt.date.today()
                    _tab_btns = []
                    for _i, _ed in enumerate(_ex_list):
                        try:
                            _ed_date = _dt.datetime.strptime(_ed, "%Y-%m-%d").date()
                            _rem = (_ed_date - _today).days
                            _tag = ""
                            if _i == 0:
                                _tag = '<span class="ex-tag">主</span>'
                            elif _i == 1:
                                _tag = '<span class="ex-tag">次</span>'
                            _rem_html = f'<span class="ex-rem">{_rem}天</span>' if _rem > 0 else f'<span class="ex-rem exp">已到期</span>'
                            _ed_disp = _ed[5:]  # 仅显示 MM-DD
                            _ed_esc = html.escape(_ed)
                            _is_active = " active" if _ed == _cur_ed else ""
                            _tab_btns.append(
                                f'<button class="ex-tab{_is_active}" data-ed="{_ed_esc}" onclick="onExerciseDateChange(\'{_ed_esc}\')">'
                                f'{_ed_disp} {_tag} {_rem_html}</button>'
                            )
                        except Exception:
                            continue
                    if _tab_btns:
                        _ex_tabs_html = (
                            f'<div class="ex-tabs">'
                            f'<span class="ex-tabs-label">📅 行权月</span>'
                            f'{"".join(_tab_btns)}'
                            f'<span class="ex-tabs-hint">（共 {len(_ex_list)} 个到期日，当前仅显示 <b>{(_cur_ed or "")[5:]}</b>）</span>'
                            f'</div>'
                        )
                chain_panel = (
                    f'<div class="chain-box">'
                    f'<div class="chain-head">📊 期权 T 型报价 · 标的 <b>{html.escape(underlying)}</b>（{chain["exchange"]}）· 共 {chain["count"]} 条 · 行权日 {_ex}</div>'
                    f'{_ex_tabs_html}'
                    f'<div class="ob-ctrl">'
                    f'<label>期权标的 <select id="fUnderlying" onchange="onUnderlyingChange(this)">{_months_opts}</select></label>'
                    f'{_filter_html}'
                    f'</div>'
                    f'{_summary_bar}'
                    f'{_smile_html}'
                    f'<div class="t-scroll top" id="tScrollTop"><div class="t-scroll-inner" id="tScrollTopInner"></div></div>'
                    f'<div class="t-area" id="tArea" data-s="{_s_val}">'
                    f'<div class="side-wrap call-wrap" id="callWrap"><table class="t-type side call">'
                    f'<thead><tr>{_call_headers}</tr></thead><tbody>{_call_rows}</tbody></table></div>'
                    f'<div class="strike-wrap" id="strikeWrap"><table class="t-type strike-col">'
                    f'<thead><tr><th class="strike">行权价</th></tr></thead><tbody>{_strike_rows}</tbody></table></div>'
                    f'<div class="side-wrap put-wrap" id="putWrap"><table class="t-type side put">'
                    f'<thead><tr>{_put_headers}</tr></thead><tbody>{_put_rows}</tbody></table></div>'
                    f'</div>'
                    f'<div class="t-scroll bottom" id="tScrollBottom"><div class="t-scroll-inner" id="tScrollBottomInner"></div></div>'
                    f'<div class="greek-note">T 型报价：左认购 CALL / 右认沽 PUT。列顺序（CALL）：最新→涨跌→涨跌幅→开→最高→最低→振幅→成交量→仓差→持仓量→成交额→卖价→买价→Δ/Γ/Θ/V/Rho→内在价值→时间价值→溢价率→杠杆→真实杠杆→理论价→隐含波动率→历史波动率→虚实值→行权比例→期权类型→到期日→剩余天数→合约代码（PUT 镜像）。合约代码单元格单击即可复制。虚实值=标的价格-行权价（正为实值、负为虚值）。仓差=今持仓-昨持仓（红=开仓，绿=平仓）。隐含波动率=每合约 BS 反解 IV。历史波动率=天勤未提供现成字段，暂占位。成交量PCR/持仓量PCR=认沽量÷认购量（>1 偏空，<1 偏多）。上方"波动率微笑"曲线为各行权价 IV 连线。VIX(估算)=平值期权 IV 均值，仅供参考。</div>'
                    f'{_prod_bars_html}'
                    f'{_exchange_bar}'
                    f'<a href="#" class="chain-back" onclick="{ret_js};return false;">{back_label_html}</a></div>'
                )
        else:
            # 品种(多合约): 汇总旗下各合约行权日与认购/认沽数量
            _sum = futures_db.build_option_chain_summary(matches)
            _rows = ""
            for u, info in _sum.items():
                _rows += (f'<tr><td class="strike">{u}</td><td>{info["exercise_date"]}</td>'
                          f'<td class="call">{info["calls"]}</td><td class="put">{info["puts"]}</td>'
                          f'<td>{info["count"]}</td></tr>')
            if not _rows:
                _rows = '<tr><td colspan="5">暂无期权数据</td></tr>'
            chain_panel = (f'<div class="chain-box"><div class="chain-head">📊 期货关联值 <b>{html.escape(underlying)}</b> 旗下期权 · 共 {len(_sum)} 个合约</div>'
                           f'<table class="chain-table"><thead><tr><th>标的合约</th><th>行权日</th><th>认购 CALL</th><th>认沽 PUT</th><th>条数</th></tr></thead><tbody>{_rows}</tbody></table>'
                           f'<a href="#" class="chain-back" onclick="{ret_js};return false;">{back_label_html}</a></div>')
        # 指定标的当月无期权, 但库中存在(其他月份) -> 给出跳转提示
        if matches and not disp_options:
            ex_months = futures_db.get_underlyings_exercise_months(matches)
            if ex_months:
                ey, em = ex_months[0]
                opt_hint = (f'<div class="hint-box">该期货关联值在 {year}年{month}月 暂无期权，其期权行权月为 '
                            f'<a href="#" class="hint-link" onclick="jumpToMonth({ey},{em});return false;">{ey}年{em}月</a>，可点击前往查看。</div>')

    weeks = build_calendar_data(query_result.get('futures', {}), disp_options, year, month, view)
    summary = build_weekly_summary(query_result.get('futures', {}), disp_options, year, month, view)

    SEP = "<hr class='sep'>"
    # 生成日历HTML
    calendar_rows = []
    for week in weeks:
        row = '<tr>'
        for day in week:
            if not day["is_current_month"]:
                row += '<td class="other-month"></td>'
            elif day["is_weekend"]:
                row += f'<td class="weekend">{day["day"]}</td>'
            elif day.get("futures") or day.get("options"):
                expired_cls = " expired" if day.get("is_expired") else ""
                expired_badge = '<span class="expired-badge">已过期</span>' if day.get("is_expired") else ''
                parts = []
                if view in ('all', 'futures') and day.get("futures"):
                    f = day["futures"]
                    ex = f'<span class="ex-badge">{f["exchange"]}</span>' if f.get("exchange") else ''
                    prods_html = ""
                    for sym, nm in f.get("products", []):
                        safe = sym.replace("'", "\\'")
                        prods_html += f'<span class="fut-link" onclick="gotoOptions(\'{safe}\')" title="查看对应期权合约">{nm}</span>'
                    parts.append(f'{ex}<span class="tag-f">期货</span>{prods_html}')
                if view in ('all', 'options') and day.get("options"):
                    o = day["options"]
                    und_lines = [f'<span class="opt-und" onclick="filterOptionsByUnderlying(\'{html.escape(u).replace(chr(39), chr(92)+chr(39))}\')" title="只看 {html.escape(u)} 的期权">{html.escape(futures_db.option_underlying_name(u))}</span>'
                                 for u, c in o.get("underlyings", {}).items()]
                    parts.append("".join(und_lines))
                cell = f'<div class="day-num">{day["day"]}{expired_badge}</div><div class="products">{SEP.join(parts)}</div>'
                ex_attr = ""
                if day.get("options"):
                    ex_attr = html.escape(day["options"].get("exchange", ""))
                elif day.get("futures"):
                    ex_attr = html.escape(day["futures"].get("exchange", ""))
                row += f'<td class="expiry-day{expired_cls}" data-exchange="{ex_attr}">{cell}</td>'
            else:
                row += f'<td class="normal-day"><div class="day-num">{day["day"]}</div></td>'
        row += '</tr>'; calendar_rows.append(row)
    cal_body = "\n".join(calendar_rows) if calendar_rows else '<tr><td colspan="7">暂无数据</td></tr>'

    # 2026-09-02 用户要求去掉页面下方"按日期汇总"模块，summary_body 留空即可
    summary_body = ""

    prev_month = month - 1; prev_year = year
    if prev_month < 1: prev_month = 12; prev_year -= 1
    next_month = month + 1; next_year = year
    if next_month > 12: next_month = 1; next_year += 1

    # T 型报价页(show_calendar=False)不渲染日历标题与查询工具栏，避免占用空间；日历/到期表页保留。
    _calendar_header = (
        f'<div class="page-title">📅 {title}</div>'
        f'<p class="page-subtitle">{subtitle} | 数据源: 数据库(天勤TqSdk 同步入库)</p>'
        f'<div class="calendar-toolbar">'
        f'  <form method="get" action="" style="display:inline-flex;align-items:center;gap:6px;margin:0;flex-wrap:wrap">'
        f'    <select name="year" onchange="this.form.submit()">{year_opts}</select>'
        f'    <select name="month" onchange="this.form.submit()">{month_opts}</select>'
        f'    <input type="text" name="underlying" value="{underlying_esc}" placeholder="期货关联值 如 SHFE.cu 或 SHFE.cu2508" class="toolbar-input">'
        f'    <button type="submit" class="toolbar-btn">查询期权</button>'
        f'  </form>'
        f'  <a href="?year={prev_year}&month={prev_month}{underlying_qs}">◀</a>'
        f'  <span class="current">{year}年{month}月</span>'
        f'  <a href="?year={next_year}&month={next_month}{underlying_qs}">▶</a>'
        f'  <span class="spacer"></span>'
        f'  <a href="#" class="sync-btn" onclick="syncData(\'futures\');return false;">同步期货</a>'
        f'  <a href="#" class="sync-btn" onclick="syncData(\'options\');return false;">同步期权</a>'
        f'</div>'
    ) if show_calendar else '<div style="height:6px"></div>'

    EXPIRY_HTML = f'''<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate"><meta http-equiv="Pragma" content="no-cache"><meta http-equiv="Expires" content="0"><title>{title}</title>
<style>:root{{
  --bg:#0b1120; --bg2:#0d1526; --surface:#111827; --surface2:#1b2538;
  --border:#27344b; --border-soft:#1f2a3d;
  --text:#e6edf7; --muted:#94a3b8; --accent:#38bdf8; --accent2:#f472b6; --violet:#c084fc;
  --up:#ff5b5b; --down:#22c55e; --gold:#fbbf24; --teal:#34d399;
  --call-bg:rgba(255,91,91,.07); --put-bg:rgba(34,197,94,.07);
}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Microsoft YaHei','PingFang SC',sans-serif;background:var(--bg);color:var(--text);padding:8px;min-height:100vh;font-size:16px;line-height:1.45}}
.page-title{{color:var(--gold);margin-bottom:2px;font-size:20px;font-weight:800}}
h2{{color:var(--accent);font-size:16px;margin:8px 0 6px;padding-bottom:4px;border-bottom:1px solid var(--border);font-weight:700}}
.page-subtitle{{font-size:12px;color:var(--muted);margin-bottom:6px}}
/* ===== 工具栏 ===== */
.calendar-toolbar{{margin-bottom:8px;display:flex;gap:6px;flex-wrap:wrap;align-items:center;background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:6px 10px}}
.calendar-toolbar select{{background:#0d1626;color:var(--text);border:1px solid var(--border);border-radius:6px;padding:5px 7px;font-size:13px;cursor:pointer}}
.calendar-toolbar select:focus{{outline:none;border-color:var(--accent)}}
.calendar-toolbar a{{color:var(--muted);text-decoration:none;padding:5px 10px;border-radius:6px;background:#0d1626;font-size:13px;font-weight:600;transition:.2s}}
.calendar-toolbar a:hover{{background:#243049;color:var(--text);border-color:var(--accent)}}
.calendar-toolbar .current{{color:var(--gold);font-weight:800;font-size:13px;padding:5px 8px;background:#0d1626;border-radius:6px;border:1px solid var(--border)}}
.calendar-toolbar .spacer{{flex:1}}
.toolbar-input{{background:#0d1626;color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 8px;font-size:13px;min-width:180px}}
.toolbar-input:focus{{outline:none;border-color:var(--accent);box-shadow:0 0 0 2px rgba(56,189,248,.15)}}
.toolbar-btn{{padding:6px 12px;border-radius:6px;border:1px solid var(--border);background:#1b2538;color:var(--text);cursor:pointer;font-size:13px;font-weight:600;transition:.2s}}
.toolbar-btn:hover{{background:#243049;border-color:var(--accent)}}
.sync-btn{{background:linear-gradient(135deg,var(--teal),#22c55e)!important;color:#06210f!important;border-color:transparent!important;font-weight:700}}
.sync-btn:hover{{filter:brightness(1.08)}}
/* ===== 期权链面板（双滚轮互斥同步） ===== */
.chain-box{{background:var(--surface2);border:1px solid var(--border);border-radius:12px;padding:14px;margin-bottom:14px}}
.chain-head{{font-size:20px;color:var(--text);margin-bottom:10px;font-weight:700}}
.underlying-bar{{display:flex;align-items:center;gap:18px;flex-wrap:wrap;background:linear-gradient(90deg,rgba(56,189,248,.12),rgba(192,132,252,.08));border:1px solid var(--border);border-radius:10px;padding:12px 18px;margin-bottom:12px;font-size:17px}}
.underlying-bar .u-name{{font-size:22px;font-weight:800;color:var(--accent)}}
.underlying-bar .u-code{{font-size:13px;color:var(--muted);font-weight:600;margin-left:2px;align-self:flex-end;padding-bottom:3px}}
.underlying-bar .u-last{{font-size:24px;font-weight:800}}
.underlying-bar .u-chg{{font-size:18px;font-weight:700}}
.underlying-bar .u-sep{{flex:1}}
.underlying-bar .u-stat{{display:inline-flex;align-items:center;gap:6px;color:var(--muted)}}
.underlying-bar .u-stat b{{color:var(--text);font-weight:700}}
.underlying-bar .call-stat{{color:var(--up)}}
.underlying-bar .put-stat{{color:var(--down)}}
.underlying-bar .pcr-stat{{color:var(--violet);font-weight:700}}
.underlying-bar .pcr-stat b{{color:var(--violet)}}
/* 自定义顶部/底部滚动条 */
.t-scroll{{width:100%;height:16px;overflow-x:auto;overflow-y:hidden;background:#080d18;border-left:1px solid var(--border);border-right:1px solid var(--border);position:relative}}
.t-scroll.top{{border-top:1px solid var(--border);border-radius:8px 8px 0 0;border-bottom:none}}
.t-scroll.bottom{{border-bottom:1px solid var(--border);border-radius:0 0 8px 8px;border-top:none}}
.t-scroll::-webkit-scrollbar{{height:14px}}
.t-scroll::-webkit-scrollbar-track{{background:#080d18;border-radius:7px}}
.t-scroll::-webkit-scrollbar-thumb{{background:linear-gradient(180deg,#334155,#1e293b);border-radius:7px;border:2px solid #080d18}}
.t-scroll::-webkit-scrollbar-thumb:hover{{background:linear-gradient(180deg,#475569,#334155)}}
.t-scroll-inner{{height:1px}}
/* 三栏T型表区域：中间行权价固定，两侧独立横向滚动 */
.t-area{{display:flex;width:100%;max-height:min(calc(100vh - 340px),780px);overflow:hidden;border-left:1px solid var(--border);border-right:1px solid var(--border);background:#0b1120;position:relative}}
.side-wrap{{flex:1 1 0;min-width:0;overflow-x:auto;overflow-y:auto;background:#0b1120}}
.side-wrap::-webkit-scrollbar{{height:10px}}
.side-wrap::-webkit-scrollbar-track{{background:#080d18}}
.side-wrap::-webkit-scrollbar-thumb{{background:#334155;border-radius:5px}}
/* 三栏垂直同步滚动，但只显示 PUT 栏(最右)的滚动条，CALL 与行权价栏隐藏视觉滚动条 */
/* 滚动功能由 JS syncV 同步设置 scrollTop 实现，隐藏 scrollbar 不影响滚动行为 */
.call-wrap,.strike-wrap{{scrollbar-width:none;-ms-overflow-style:none}}
.call-wrap::-webkit-scrollbar,.strike-wrap::-webkit-scrollbar{{width:0;height:0;display:none}}
.call-wrap{{border-right:1px solid var(--border)}}
.put-wrap{{border-left:1px solid var(--border)}}
.strike-wrap{{flex:0 0 96px;width:96px;min-width:96px;background:#0d1626;border-left:3px solid var(--border);border-right:3px solid var(--border);z-index:15;overflow-x:hidden;overflow-y:auto}}
.t-type{{border-collapse:collapse;table-layout:fixed;font-size:14px;white-space:nowrap}}
.t-type.side{{min-width:2500px;width:100%}}
/* 波动率微笑/偏斜曲线 */
.smile-box{{margin:0 0 12px 0;background:#0c1422;border:1px solid var(--border);border-radius:10px;padding:10px 14px 6px}}
.smile-title{{font-size:14px;font-weight:700;color:var(--accent);margin-bottom:6px;letter-spacing:.5px}}
.smile-svg{{display:block;width:100%;height:auto}}
.smile-empty{{color:var(--muted);font-size:13px;padding:18px 6px}}
.smile-loading{{color:var(--muted);font-size:13px;padding:22px 6px;display:flex;align-items:center;justify-content:center;gap:8px}}
.smile-loading .spinner{{display:inline-block;width:14px;height:14px;border:2px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin 1s linear infinite}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
.t-type th{{background:linear-gradient(180deg,#1b2740,#141d30);padding:8px 4px;color:var(--accent);font-size:13px;border:1px solid var(--border-soft);font-weight:700;text-align:center;white-space:nowrap;position:sticky;top:0;z-index:12;height:34px;line-height:1;box-sizing:border-box}}
.t-type td{{border:1px solid var(--border-soft);padding:6px 4px;text-align:right;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;line-height:1;vertical-align:middle;color:var(--text);height:34px;box-sizing:border-box}}
.t-type tbody tr{{height:34px;box-sizing:border-box}}
.t-type td.empty{{color:#4b5870}}
.t-type tbody tr:nth-child(even){{background:rgba(255,255,255,.02)}}
.t-type tbody tr:hover{{background:rgba(56,189,248,.08)}}
.t-type.call td{{background:var(--call-bg)}}
.t-type.put td{{background:var(--put-bg)}}
.t-type .up{{color:var(--up);font-weight:700}}
.t-type .down{{color:var(--down);font-weight:700}}
.t-type .last{{font-weight:800}}
.t-type .chg{{font-weight:700}}
/* 希腊值列: 与主表统一字号, 等宽字体保持数字对齐 + 紫色底纹 */
.t-type td.greek{{font-size:13px;font-family:Consolas,Monaco,monospace;color:#cbd5e1;background:rgba(167,139,250,.06)}}
.t-type th.greek{{color:var(--violet);font-size:12px;background:linear-gradient(180deg,#25193a,#1a1230)}}
/* IV 列: 紫色高亮, 统一字号 */
.t-type td.iv{{font-size:13px;font-family:Consolas,Monaco,monospace;color:var(--violet);font-weight:700;background:rgba(167,139,250,.1)}}
.t-type th.iv{{color:var(--violet)}}
/* 合约代码列: 固定150px宽, 单行截断, hover显示完整 */
.t-type td.sym{{font-size:12px;font-family:Consolas,Monaco,monospace;color:#94a3b8;width:150px;max-width:150px;background:rgba(148,163,184,.06);cursor:copy;user-select:all;text-align:left;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.t-type td.sym:hover{{color:#fff;background:rgba(56,189,248,.15)}}
.t-type th.sym{{color:#94a3b8;font-size:12px;background:linear-gradient(180deg,#1a1f30,#10141f);width:150px;max-width:150px}}
.t-type.strike-col{{width:96px;min-width:96px}}
.t-type.strike-col th{{background:linear-gradient(180deg,#2a1f10,#1a1206);color:var(--gold);z-index:16}}
.t-type .strike{{text-align:center;color:var(--gold);font-weight:800;font-size:19px;background:transparent}}
.greek-note{{font-size:14px;color:var(--muted);margin:10px 0 8px;line-height:1.7}}
.chain-back{{display:inline-block;margin-top:4px;color:var(--accent);text-decoration:none;font-size:15px;font-weight:600}}
.chain-back:hover{{color:var(--gold)}}
/* 期权链控制条：标的下拉 + 筛选 */
.ob-ctrl{{display:flex;align-items:center;gap:16px;flex-wrap:wrap;background:#0d1626;border:1px solid var(--border);border-radius:10px;padding:10px 14px;margin-bottom:12px}}
.ob-ctrl label{{font-size:15px;color:var(--muted);display:inline-flex;align-items:center;gap:6px}}
.ob-ctrl select,.ob-ctrl input{{background:#0b1120;color:var(--text);border:1px solid var(--border);border-radius:7px;padding:8px 10px;font-size:15px}}
.ob-ctrl select:focus,.ob-ctrl input:focus{{outline:none;border-color:var(--accent)}}
.ob-filter{{display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap;margin-left:auto}}
.ob-filter button{{padding:8px 14px;border-radius:7px;border:1px solid var(--border);background:#1b2538;color:var(--text);cursor:pointer;font-size:14px}}
.ob-filter button:hover{{background:#243049;border-color:var(--accent)}}
.underlying-bar .vix-stat{{color:var(--violet);font-weight:700}}
.underlying-bar .vix-stat b{{color:var(--violet)}}
.idx-bar{{display:flex;align-items:center;gap:16px;flex-wrap:wrap;background:linear-gradient(90deg,rgba(52,211,153,.12),rgba(56,189,248,.06));border:1px solid var(--border);border-radius:10px;padding:10px 18px;margin:10px 0;font-size:17px}}
.idx-bar .idx-name{{font-size:18px;font-weight:800;color:var(--teal)}}
.idx-bar .idx-last{{font-size:22px;font-weight:800}}
.idx-bar .idx-note{{margin-left:auto;color:var(--muted);font-size:13px}}
.idx-bar .idx-sep{{flex:1;min-width:20px}}
/* ===== 期权链底部快捷入口 & 交易所 Tab ===== */
.chain-foot-bar{{display:flex;flex-wrap:wrap;gap:6px;background:#0b1120;border:1px solid var(--border);border-radius:10px;padding:10px 12px;margin-top:10px}}
.chain-foot-bar .foot-item{{display:inline-block;padding:7px 13px;border-radius:6px;background:#141d30;color:var(--muted);font-size:14px;font-weight:600;cursor:pointer;transition:.15s;border:1px solid transparent;white-space:nowrap}}
.chain-foot-bar .foot-item:hover{{background:#1b2538;color:var(--text);border-color:var(--accent)}}
.chain-foot-bar .foot-item.active,.chain-foot-bar .foot-item:active{{background:linear-gradient(90deg,var(--accent),var(--accent2));color:#04121f;border-color:transparent;box-shadow:0 0 8px rgba(56,189,248,.35)}}
.chain-foot-bar .foot-item:focus-visible{{outline:2px solid var(--accent);outline-offset:2px}}
.prod-bar-group{{display:none}}
.prod-bar-group.active{{display:block}}
.prod-bar{{margin-bottom:6px}}
.exchange-bar{{}}
/* ===== 日历视图 ===== */
.table-wrap{{width:100%;overflow-x:auto}}
/* ===== 期权链行权月切换器 ===== */
.ex-tabs{{display:flex;align-items:center;gap:10px;flex-wrap:wrap;background:linear-gradient(90deg,rgba(56,189,248,.1),rgba(167,139,250,.06));border:1px solid var(--border);border-radius:10px;padding:10px 14px;margin-bottom:12px}}
.ex-tabs-label{{font-size:16px;font-weight:700;color:var(--accent);flex-shrink:0}}
.ex-tab{{padding:8px 14px;border-radius:7px;border:1px solid var(--border);background:#0b1120;color:var(--text);cursor:pointer;font-size:14px;font-weight:600;display:inline-flex;align-items:center;gap:6px;transition:.15s}}
.ex-tab:hover{{background:#1b2538;border-color:var(--accent)}}
.ex-tab.active{{background:linear-gradient(90deg,var(--accent),var(--accent2));color:#04121f;border-color:transparent;box-shadow:0 0 8px rgba(56,189,248,.35)}}
.ex-tag{{font-size:11px;background:rgba(251,191,36,.25);color:var(--gold);padding:1px 5px;border-radius:3px;font-weight:700}}
.ex-tab.active .ex-tag{{background:rgba(0,0,0,.15);color:#fff}}
.ex-rem{{font-size:12px;color:var(--muted);font-weight:500}}
.ex-tab.active .ex-rem{{color:rgba(4,18,31,.75)}}
.ex-rem.exp{{color:var(--down)}}
.ex-tabs-hint{{margin-left:auto;font-size:13px;color:var(--muted)}}
/* ===== T型表双击弹K线弹窗 ===== */
.kp-modal{{display:none;position:fixed;inset:0;background:rgba(4,10,22,.78);z-index:9999;align-items:center;justify-content:center;padding:24px;backdrop-filter:blur(3px)}}
.kp-modal.open{{display:flex}}
.kp-modal-box{{width:min(1100px,94vw);height:min(80vh,820px);background:#0b1120;border:1px solid var(--border);border-radius:12px;box-shadow:0 20px 60px rgba(0,0,0,.6);display:flex;flex-direction:column;overflow:hidden;resize:both;min-width:420px;min-height:300px}}
.kp-modal-head{{display:flex;align-items:center;justify-content:space-between;padding:10px 16px;background:linear-gradient(180deg,#1b2740,#141d30);border-bottom:1px solid var(--border);flex-shrink:0}}
.kp-modal-head span{{font-weight:800;font-size:15px;color:var(--gold);user-select:all;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.kp-modal-close{{background:transparent;border:1px solid var(--border);color:var(--muted);border-radius:6px;width:30px;height:30px;font-size:14px;cursor:pointer;line-height:1;transition:.15s;flex-shrink:0}}
.kp-modal-close:hover{{background:#3b1d2a;color:#ff5b5b;border-color:#ff5b5b}}
.kp-modal-box iframe{{flex:1;width:100%;border:none;background:#0b1120}}
.kp-modal-box iframe:focus{{outline:none}}
:root{{--top-offset:120px;--cal-rows:6;--data-h:150px}}
.cal-table{{width:100%;border-collapse:collapse;font-size:14px;margin-bottom:12px;background:var(--surface2);border-radius:8px;overflow:hidden;border:1px solid var(--border);table-layout:fixed}}
.cal-table th{{background:linear-gradient(180deg,#1b2740,#141d30);padding:4px 2px;text-align:center;border:1px solid var(--border-soft);font-weight:700;color:var(--accent);font-size:12px;height:24px;box-sizing:border-box}}
.cal-table td{{border:1px solid var(--border-soft);padding:3px;vertical-align:top;width:14.28%;height:auto;min-height:34px;transition:background .15s;overflow:hidden;box-sizing:border-box}}
.cal-table td:hover{{background:#16213a}}
.cal-table .other-month{{background:#0a0e14;color:#4b5870;height:26px;min-height:26px}}
.cal-table .weekend{{background:#141b29;color:#4b5870;text-align:center;vertical-align:middle;font-size:14px;height:36px;min-height:36px}}
.cal-table .normal-day{{text-align:right;vertical-align:top;min-height:36px}}
.cal-table .expiry-day{{vertical-align:top;background:rgba(52,211,153,.06);min-height:var(--data-h)}}
.cal-table .expiry-day:hover{{background:rgba(52,211,153,.12)}}
.cal-table .day-num{{text-align:right;font-size:13px;color:var(--muted);margin-bottom:3px;font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.ex-badge{{display:inline-block;background:linear-gradient(135deg,var(--accent),var(--violet));color:#08111f;font-size:7px;padding:1px 2px;border-radius:2px;margin-bottom:1px;font-weight:800;vertical-align:middle}}
.products{{font-size:11px;color:#b8d4f0;line-height:1.5;margin-top:3px;background:rgba(56,189,248,.06);border:1px solid rgba(56,189,248,.1);border-radius:3px;padding:3px 4px;max-height:calc(var(--data-h) - 36px);overflow:hidden;columns:5;column-gap:6px;column-fill:auto}}
.products > .fut-link,.products > .opt-und{{display:inline-block;width:100%;break-inside:avoid;box-sizing:border-box;padding:2px 0}}
.products > .ex-badge,.products > .tag-f,.products > .tag-o{{display:inline-block;vertical-align:middle;margin-right:2px;break-inside:avoid}}
.tag-f,.tag-o{{display:inline-block;font-size:7px;padding:1px 2px;border-radius:2px;margin:1px 0;font-weight:800;vertical-align:middle}}
.tag-f{{background:linear-gradient(135deg,var(--accent),#2563eb);color:#08111f}}
.tag-o{{background:linear-gradient(135deg,var(--gold),#d97706);color:#1a1206}}
.fut-link{{color:#5ee7ff;cursor:pointer;text-decoration:none;font-size:8px}}
.fut-link:hover{{color:var(--gold);text-decoration:underline}}
.opt-und{{cursor:pointer;color:#5ee7ff;text-decoration:none;font-size:11px}}
.opt-und:hover{{color:var(--gold);text-decoration:underline}}
hr.sep{{border:none;border-top:1px dashed rgba(56,189,248,.25);margin:1px 0;width:100%;column-span:all}}
.error-msg{{background:rgba(255,91,91,.12);border:1px solid var(--up);color:#ff8a8a;padding:12px 16px;border-radius:10px;margin-bottom:12px;font-size:15px}}
/* ===== 周汇总 ===== */
.week-block{{background:var(--surface2);border:1px solid var(--border);border-radius:12px;padding:16px;margin-bottom:12px}}
.week-block.empty{{opacity:.5}}
.week-title{{font-size:20px;color:var(--accent);font-weight:800;margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid var(--border)}}
.week-empty{{color:var(--muted);font-size:15px;text-align:center;padding:10px}}
.summary-day{{background:#0d1626;border:1px solid var(--border-soft);border-radius:10px;padding:12px 14px;margin-bottom:8px}}
.summary-day:last-child{{margin-bottom:0}}
.sd-header{{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:6px}}
.sd-day{{font-weight:800;color:var(--gold);font-size:18px}}
.sd-exchange{{background:linear-gradient(135deg,var(--accent),#2563eb);color:#08111f;font-size:13px;padding:3px 9px;border-radius:5px;font-weight:800}}
.sd-products{{font-size:15px;color:var(--text);line-height:1.9;padding-left:4px}}
.opt-und{{cursor:pointer;color:var(--accent);text-decoration:underline}}
.opt-und:hover{{color:var(--gold)}}
.expired{{background:#222!important;opacity:.55}}
.expired .day-num{{color:#6e7681}}
.expired .products,.expired .fut-link{{color:#8b949e}}
.expired-badge,.expired-tag{{display:inline-block;background:#6e7681;color:#0d1117;font-size:12px;padding:2px 7px;border-radius:5px;margin-left:5px;vertical-align:middle;font-weight:800}}
.hint-box{{background:#0d1626;border:1px solid var(--border);color:var(--text);padding:12px 16px;border-radius:10px;margin-bottom:12px;font-size:15px}}
.hint-link{{color:var(--accent);text-decoration:none;font-weight:700}}
.hint-link:hover{{color:var(--gold)}}
.source-info{{font-size:13px;color:#5b6b85;margin-top:4px}}
@media(max-width:1024px){{
  body{{padding:12px;font-size:19px}}
  .page-title{{font-size:26px}}
  h2{{font-size:22px}}
  .calendar-toolbar{{flex-direction:column;align-items:stretch;gap:10px;padding:14px}}
  .calendar-toolbar select,.calendar-toolbar a,.calendar-toolbar .current{{font-size:18px;width:100%;text-align:center;padding:14px}}
  .underlying-bar{{gap:12px;padding:12px}}
  .underlying-bar .u-name{{font-size:20px}}
  .underlying-bar .u-last{{font-size:22px}}
  .t-area{{max-height:480px}}
  .side-wrap{{overflow-x:auto}}
  .strike-wrap{{flex:0 0 84px;width:84px;min-width:84px}}
  .t-type{{font-size:14px}}
  .t-type.side{{min-width:780px}}
  .t-type td{{padding:7px 4px}}
  .t-type .strike{{font-size:18px}}
  .t-type.strike-col{{width:84px;min-width:84px}}
  .cal-table,.cal-table tbody,.cal-table tr{{display:block;width:100%}}
  .cal-table tr{{margin-bottom:12px;border:2px solid var(--border);border-radius:12px;overflow:hidden}}
  .cal-table th{{display:none}}
  .cal-table td{{display:block;width:100%!important;height:auto;min-height:0;max-height:none;padding:16px;border:1px solid var(--border-soft)}}
  .cal-table td.normal-day,.cal-table td.expiry-day,.cal-table td.weekend,.cal-table td.other-month{{text-align:left}}
  .cal-table .day-num{{text-align:left;font-size:22px;margin-bottom:8px}}
  .products{{font-size:17px;line-height:2;max-height:none;columns:1}}
  .ex-badge,.tag-f,.tag-o{{font-size:15px;padding:4px 9px}}
  .fut-link,.opt-und{{font-size:16px}}
  .toolbar-input,.toolbar-btn{{width:100%;font-size:18px;min-width:0;padding:15px}}
  .chain-box{{padding:12px}}
  .chain-head{{font-size:19px}}
  .week-block{{padding:14px}}
  .week-title{{font-size:21px}}
  .sd-day{{font-size:20px}}
  .sd-products{{font-size:17px}}
}}
@media(max-width:480px){{
  body{{padding:8px;font-size:20px}}
  .page-title{{font-size:24px}}
  .calendar-toolbar select,.calendar-toolbar a,.calendar-toolbar .current{{font-size:19px;padding:16px}}
  .cal-table .day-num{{font-size:24px}}
  .products{{font-size:19px}}
  .t-area{{max-height:380px}}
  .t-type{{font-size:13px}}
  .t-type.side{{min-width:680px}}
  .t-type td{{padding:6px 3px}}
}}
</style></head><body style="--cal-rows:{len(weeks)}">
{_calendar_header}
<div id="sync-msg" style="font-size:15px;color:#58a6ff;margin-bottom:8px"></div>
{'' if not error_msg else f'<div class="error-msg">{error_msg}</div>'}
<div id="chain-panel-container">{chain_panel}{opt_hint}</div><!-- /chain-panel-container -->
<div class="kp-modal" id="kline-popup-modal" onclick="if(event.target===this)closeKlinePopup()">
  <div class="kp-modal-box">
    <div class="kp-modal-head">
      <span id="kline-popup-title">期权K线</span>
      <button class="kp-modal-close" onclick="closeKlinePopup()" title="关闭 (Esc)">✕</button>
    </div>
    <iframe id="kline-popup-frame" src="about:blank" title="期权K线"></iframe>
  </div>
</div>
{'' if not show_calendar else f'<h2>🗓 日历视图</h2><table class="cal-table"><thead><tr>{"".join(f"<th>{h}</th>" for h in weekday_headers)}</tr></thead><tbody>{cal_body}</tbody></table>'}
<p class="source-info" style="font-size:14px;color:#484f58;margin-top:4px">数据源: 数据库(天勤TqSdk 同步入库) | 查询时间: {now.strftime("%Y-%m-%d %H:%M:%S")}</p>
</body></html>'''
    return render_template_string(EXPIRY_HTML) + SYNC_SCRIPT


@app.route("/option-expiry")
def option_expiry_page():
    import datetime as dt
    now = dt.datetime.now()
    year = request.args.get("year", now.year, type=int)
    month = request.args.get("month", now.month, type=int)
    if month < 1 or month > 12: month = now.month
    if year < 2020 or year > 2030: year = now.year
    return _build_expiry_html(year, month, "futures", "/option-expiry")


@app.route("/options-calendar")
def options_calendar_page():
    import datetime as dt
    now = dt.datetime.now()
    year = request.args.get("year", now.year, type=int)
    month = request.args.get("month", now.month, type=int)
    if month < 1 or month > 12: month = now.month
    if year < 2020 or year > 2030: year = now.year
    underlying = request.args.get("underlying", "").strip()
    if underlying:
        from services import futures_db
        matches = futures_db.resolve_underlying_matches(underlying)
        # 单一合约(精确匹配): 若当前展示月不是其期权行权月, 网关安全地跳转到行权月, 保证期权链可见
        if matches and len(matches) == 1 and matches[0] == underlying:
            ex_months = futures_db.get_underlying_exercise_months(underlying)
            if ex_months and (year, month) not in ex_months:
                ey, em = ex_months[0]
                return render_template_string(
                    "<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'><title>跳转中</title></head><body><script>"
                    "var s='/';var isGw=location.pathname.split(s).slice(1,3).join(s)==='api'+s+'gw';"
                    "var base=isGw?s+'api'+s+'gw'+s+'options'+s+'options-calendar':s+'options-calendar';"
                    "location.replace(base+'?year=" + str(ey) + "&month=" + str(em) + "&underlying='+encodeURIComponent(" + json.dumps(underlying) + "));"
                    "</script></body></html>")
    return _build_expiry_html(year, month, "options", "/options-calendar", underlying=underlying or None)


_OPT_PROD_ITEMS = [
    ("沪铜", "SHFE.cu"), ("沪铝", "SHFE.al"), ("沪锌", "SHFE.zn"),
    ("沪铅", "SHFE.pb"), ("沪镍", "SHFE.ni"), ("沪锡", "SHFE.sn"),
    ("氧化铝", "SHFE.ao"), ("铝合金", "SHFE.alu"), ("沪金", "SHFE.au"),
    ("沪银", "SHFE.ag"), ("螺纹钢", "SHFE.rb"), ("燃料油", "SHFE.fu"),
    ("沥青", "SHFE.bu"), ("BR橡胶", "SHFE.br"), ("橡胶", "SHFE.ru"),
    ("纸浆", "SHFE.sp"), ("双胶纸", "CZCE.cj"),
]
_OPT_EXCHANGE_ITEMS = [
    ("全部", "__all__"), ("我的关注", "__fav__"), ("上期所期权", "SHFE"), ("上期能源期权", "INE"),
    ("大商所期权", "DCE"), ("郑商所期权", "CZCE"), ("广期所期权", "GFEX"),
    ("中金所期权", "CFFEX"), ("上交所期权", "SSE"), ("深交所期权", "SZSE"),
    ("期权主力合约", "__main__"), ("平值期权", "__atm__"),
    ("条件筛选", "__filter__"), ("期权VIX指数", "__vix__"), ("标的排名", "__rank__"),
]


def _build_optchain_html():
    """期权模块(optchain)目录首页: WH6 风格。
    父级交易所 Tab + 子级品种 Tab 锁定在页面底部；点击父级切换子级并显示对应内容；
    内容区独立滚动，底部 Tab 始终固定。所有跳转均通过 optBase() 走 /optchain/chain。
    """
    from services import futures_db
    idx, wh_idx = _get_market_index_bars()

    # 顶部指数条(上证指数 + 文华商品估算)
    idx_html = ""
    if idx or wh_idx:
        idx_html = '<div class="idx-bar">'
        if idx:
            t = "up" if (idx.get("chg") is not None and idx["chg"] >= 0) else "down"
            idx_html += (
                f'<span class="idx-name">上证指数</span>'
                f'<span class="idx-last {t}">{_fmt(idx.get("last"))}</span>'
                f'<span class="idx-chg {t}">{_fmt(idx.get("chg"), 2)}（{_fmt(idx.get("pct"), 2, True)}）</span>'
            )
        if wh_idx:
            t = "up" if (wh_idx.get("chg") is not None and wh_idx["chg"] >= 0) else "down"
            idx_html += (
                f'<span class="idx-sep"></span>'
                f'<span class="idx-name" style="color:var(--gold)">文华商品</span>'
                f'<span class="idx-last {t}">{_fmt(wh_idx.get("last"))}</span>'
                f'<span class="idx-chg {t}">{_fmt(wh_idx.get("chg"), 2)}（{_fmt(wh_idx.get("pct"), 2, True)}）</span>'
                f'<span class="idx-note" title="{html.escape(wh_idx.get("note", ""))}">估算</span>'
            )
        idx_html += '</div>'

    # 父级交易所 Tab(锁定底部)
    parent_bar = '<div class="opt-parent-bar">'
    for name, val in _OPT_EXCHANGE_ITEMS:
        parent_bar += f'<span class="p-tab" data-ex="{html.escape(val)}" onclick="switchOptExchange(\'{html.escape(val).replace(chr(39), chr(92)+chr(39))}\', this)">{html.escape(name)}</span>'
    parent_bar += '</div>'

    # 数据: 交易所 -> 品种 -> 标的
    tree_groups = []
    try:
        tree_groups = futures_db.list_option_product_tree()
    except Exception as e:
        print(f"[期权目录] 品种树查询失败: {e}")

    # 子级品种 Tab: 每个交易所一个 tabbar, JS 控制显示
    child_bars = '<div class="opt-child-bars">'
    for ex, ex_name, prods in tree_groups:
        bar_id = f'child-bar-{html.escape(ex)}'
        child_bars += f'<div class="opt-child-bar" id="{bar_id}" data-exchange="{html.escape(ex)}">'
        for pk, pname, items in prods:
            child_bars += f'<span class="c-tab" data-prod="{html.escape(pk)}" onclick="switchOptProduct(\'{html.escape(pk).replace(chr(39), chr(92)+chr(39))}\', this)">{html.escape(pname)}</span>'
        child_bars += '</div>'
    child_bars += '</div>'

    # 内容区: 每个交易所一个 section, 内按品种分组(卡片)
    content_html = '<div class="opt-content">'
    if not tree_groups:
        content_html += '<div class="tree-empty">库内暂无期权标的数据（请先在“期权日历”页点“同步期权”从天勤拉取）</div>'
    else:
        for ex, ex_name, prods in tree_groups:
            prod_cards = ""
            for pk, pname, items in prods:
                # 按行权日升序, 每个品种最多展示 8 个最近标的, 避免历史合约过多
                sorted_items = sorted(items, key=lambda x: x[1])
                shown = sorted_items[:8]
                more = len(sorted_items) - len(shown)
                leaves = ""
                for u, exd in shown:
                    safe = html.escape(u).replace("'", "\\'")
                    leaves += f'<span class="opt-leaf" onclick="gotoUnderlyingOpt(\'{safe}\')">{html.escape(u)}<em>{html.escape(exd)}</em></span>'
                if more > 0:
                    leaves += f'<span class="opt-leaf opt-more">+{more} 个月份</span>'
                # 点品种头部默认跳转到最近月份标的的 T 型报价
                nearest = sorted_items[0][0] if sorted_items else None
                nearest_safe = html.escape(nearest).replace("'", "\\'") if nearest else ""
                head_click = f'onclick="gotoUnderlyingOpt(\'{nearest_safe}\')"' if nearest else ""
                prod_cards += (
                    f'<div class="opt-prod-card" id="prod-{html.escape(pk)}" data-prod-key="{html.escape(pk)}" data-exchange="{html.escape(ex)}">'
                    f'<div class="opt-prod-head" {head_click} title="查看 {html.escape(pname)} 最近月份 T 型报价">'
                    f'<span class="opt-prod-name">{html.escape(pname)}</span>'
                    f'<span class="opt-prod-cnt">{len(items)}</span></div>'
                    f'<div class="opt-leaves">{leaves}</div></div>'
                )
            content_html += (
                f'<section class="opt-ex-sec" data-exchange="{html.escape(ex)}">'
                f'<div class="opt-ex-head"><span class="opt-ex-badge">{html.escape(ex)}</span>'
                f'<span class="opt-ex-name">{html.escape(ex_name or ex)}</span>'
                f'<span class="opt-ex-cnt">{sum(len(it) for _,_,it in prods)} 个标的</span></div>'
                f'{prod_cards}</section>'
            )
    content_html += '</div>'

    CSS = """<style>
:root{--bg:#06080f;--surface:#0b1120;--surface2:#141c2e;--border:#222d42;--border-soft:#1a2435;--text:#e8eef7;--muted:#8595ac;--accent:#2dd4ff;--accent2:#f472b6;--violet:#a855f7;--up:#ff4d4f;--down:#19c37d;--gold:#f7b500;--teal:#2dd4bf}
*{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%;overflow:hidden}
body{font-family:'Microsoft YaHei','PingFang SC',sans-serif;background:var(--bg);color:var(--text);font-size:15px;line-height:1.5}
.optchain-page{display:flex;flex-direction:column;height:100vh;background:var(--bg)}
/* 可滚动内容区 */
.optchain-scroll{flex:1;overflow-y:auto;padding:12px 14px 90px}
.page-title{color:var(--gold);margin-bottom:2px;font-size:22px;font-weight:800}
.page-subtitle{font-size:13px;color:var(--muted);margin-bottom:10px}
.idx-bar{display:flex;align-items:center;gap:18px;flex-wrap:wrap;background:linear-gradient(90deg,rgba(45,212,191,.12),rgba(45,212,255,.05));border:1px solid var(--border);border-radius:8px;padding:8px 16px;margin:8px 0;font-size:14px}
.idx-bar .idx-name{font-size:15px;font-weight:800;color:var(--teal)}
.idx-bar .idx-last{font-size:19px;font-weight:800}
.idx-bar .idx-note{margin-left:auto;color:var(--muted);font-size:12px}
.idx-bar .idx-sep{flex:1;min-width:16px}
/* 内容区 */
.opt-content{display:flex;flex-direction:column;gap:10px;margin-top:10px}
.opt-ex-sec{background:var(--surface);border:1px solid var(--border);border-radius:9px;padding:10px 12px;display:none}
.opt-ex-sec.active{display:block}
.opt-ex-head{display:flex;align-items:center;gap:8px;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border-soft)}
.opt-ex-badge{background:linear-gradient(135deg,var(--accent),var(--violet));color:#05101c;font-size:12px;padding:2px 7px;border-radius:4px;font-weight:800}
.opt-ex-name{font-size:16px;font-weight:800}
.opt-ex-cnt{margin-left:auto;color:var(--muted);font-size:12px}
.opt-prod-card{margin-bottom:10px}
.opt-prod-head{display:flex;align-items:center;gap:6px;margin-bottom:6px;cursor:pointer;padding:4px 6px;border-radius:6px;transition:.15s}
.opt-prod-head:hover{background:var(--surface2)}
.opt-prod-name{font-size:14px;font-weight:700;color:var(--gold)}
.opt-prod-cnt{font-size:11px;color:var(--muted);background:var(--surface2);padding:1px 6px;border-radius:8px}
.opt-leaves{display:grid;grid-template-columns:repeat(auto-fill,minmax(118px,1fr));gap:5px}
.opt-leaf{display:flex;flex-direction:column;align-items:flex-start;padding:6px 9px;border-radius:6px;background:#0d1424;border:1px solid var(--border-soft);color:var(--text);font-size:13px;font-weight:600;cursor:pointer;transition:.15s;min-width:0;overflow:hidden}
.opt-leaf:hover{background:#16223a;border-color:var(--accent)}
.opt-leaf em{font-style:normal;font-size:11px;color:var(--muted);margin-top:1px}
.opt-leaf.opt-more{background:transparent;border-style:dashed;color:var(--muted);cursor:default;align-items:center;justify-content:center}
.tree-empty{padding:24px;color:var(--muted);text-align:center;background:var(--surface);border:1px dashed var(--border);border-radius:8px}
/* 底部锁定 Tab 容器 */
.optchain-tabs{flex-shrink:0;background:var(--surface);border-top:1px solid var(--border);box-shadow:0 -4px 12px rgba(0,0,0,.35)}
/* 子级品种 Tab(上层) */
.opt-child-bars{position:relative;height:38px;background:#0b1120;border-bottom:1px solid var(--border-soft)}
.opt-child-bar{position:absolute;inset:0;display:none;flex-wrap:nowrap;gap:2px;overflow-x:auto;padding:4px 6px;scrollbar-width:thin}
.opt-child-bar.active{display:flex}
.opt-child-bar .c-tab{flex:0 0 auto;padding:5px 12px;font-size:13px;font-weight:600;color:var(--muted);cursor:pointer;white-space:nowrap;border-radius:4px;transition:.15s}
.opt-child-bar .c-tab:hover{color:var(--text);background:var(--surface2)}
.opt-child-bar .c-tab.active{color:#05101c;background:linear-gradient(90deg,var(--accent),var(--violet))}
/* 父级交易所 Tab(下层) */
.opt-parent-bar{display:flex;flex-wrap:nowrap;gap:2px;overflow-x:auto;padding:5px 6px;scrollbar-width:thin}
.opt-parent-bar .p-tab{flex:0 0 auto;padding:6px 12px;font-size:13px;font-weight:600;color:var(--muted);cursor:pointer;white-space:nowrap;border-bottom:2px solid transparent;transition:.15s}
.opt-parent-bar .p-tab:hover{color:var(--text)}
.opt-parent-bar .p-tab.active{color:var(--accent);border-bottom-color:var(--accent)}
/* 占位提示 */
.opt-empty-state{padding:40px 20px;text-align:center;color:var(--muted);font-size:15px}
</style>"""

    HTML = (
        CSS
        + f'''<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><title>期权目录</title></head><body>
<div class="optchain-page">
  <div class="optchain-scroll">
    <div class="page-title">📊 期权</div>
    <p class="page-subtitle">期权目录 · 数据源: 天勤 TqSdk（实时行情）+ 数据库（合约列表）</p>
    {idx_html}
    {content_html}
  </div>
  <div class="optchain-tabs">
    {child_bars}
    {parent_bar}
  </div>
</div>
</body></html>'''
    )
    return HTML + SYNC_SCRIPT


@app.route("/optchain")
def optchain_dir_page():
    return _build_optchain_html()


@app.route("/optchain/chain")
def optchain_chain_page():
    import datetime as dt
    now = dt.datetime.now()
    year = request.args.get("year", now.year, type=int)
    month = request.args.get("month", now.month, type=int)
    underlying = request.args.get("underlying", "").strip()
    if not underlying:
        return render_template_string(
            "<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'><title>跳转中</title></head><body><script>"
            "var s='/';var isGw=location.pathname.split(s).slice(1,3).join(s)==='api'+s+'gw';"
            "var base=isGw?s+'api'+s+'gw'+s+'optchain':s+'optchain';location.replace(base+'/');"
            "</script></body></html>")
    if month < 1 or month > 12: month = now.month
    if year < 2020 or year > 2030: year = now.year
    from services import futures_db
    matches = futures_db.resolve_underlying_matches(underlying)
    if matches and len(matches) == 1 and matches[0] == underlying:
        ex_months = futures_db.get_underlying_exercise_months(underlying)
        if ex_months and (year, month) not in ex_months:
            ey, em = ex_months[0]
            return render_template_string(
                "<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'><title>跳转中</title></head><body><script>"
                "var s='/';var isGw=location.pathname.split(s).slice(1,3).join(s)==='api'+s+'gw';"
                "var base=isGw?s+'api'+s+'gw'+s+'optchain':s+'optchain';"
                "location.replace(base+'/chain?year=" + str(ey) + "&month=" + str(em) + "&underlying='+encodeURIComponent(" + json.dumps(underlying) + "));"
                "</script></body></html>")
    return _build_expiry_html(year, month, "options", "/optchain/chain", underlying=underlying,
                              back_js="location.href=optBase()", back_label="← 返回期权目录",
                              show_calendar=False)


# ===== 期权 K 线弹窗页（T 型报价表双击看涨/看跌行打开）=====
KLINE_POPUP_HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>期权K线</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%;background:#0b1120;color:#e6edf7;font-family:Microsoft YaHei,system-ui,sans-serif;overflow:hidden}
.kp-top{display:flex;align-items:center;gap:12px;padding:8px 12px;background:linear-gradient(180deg,#1b2740,#141d30);border-bottom:1px solid #2a3650;flex-wrap:wrap}
.kp-sym{font-weight:800;font-size:15px;color:#fbbf24;cursor:pointer;user-select:all}
.kp-tabs{display:flex;gap:6px}
.kp-tab{padding:5px 14px;border:1px solid #2a3650;border-radius:6px;background:transparent;color:#94a3b8;font-size:13px;cursor:pointer}
.kp-tab.active{background:linear-gradient(180deg,#0ea5e9,#0284c7);color:#fff;border-color:#0ea5e9;font-weight:700}
.kp-tab:hover{color:#fff}
.kp-period{display:inline-flex;align-items:center;gap:4px;background:#0b1120;border:1px solid #2a3650;border-radius:6px;padding:2px 8px;margin-left:4px}
.kp-period label{font-size:12px;color:#5b6b85}
.kp-period select{background:#1b2740;color:#fbbf24;border:1px solid #2a3650;border-radius:5px;padding:3px 8px;font-size:13px;font-weight:700;cursor:pointer;outline:none}
.kp-period select:focus{border-color:#38bdf8}
.kp-source{font-size:12px;color:#5b6b85;margin-left:auto}
.kp-ohlc{display:flex;gap:16px;padding:6px 12px;background:#0f1728;border-bottom:1px solid #1c2740;font-size:13px;flex-wrap:wrap;user-select:none}
.kp-ohlc b{font-weight:700}
#kp-o,#kp-h,#kp-c{color:#ff5b5b}
#kp-l{color:#22c55e}
.kp-chart-wrap{height:calc(100% - 80px);position:relative}
canvas{display:block;width:100%;height:100%}
.ind-title{position:absolute;left:74px;bottom:4px;font-size:12px;color:#7c8aa3;cursor:pointer;background:rgba(13,22,38,.8);padding:2px 8px;border-radius:4px;border:1px solid #2a3650;z-index:5}
.ind-title:hover{color:#fff;border-color:#38bdf8}
.ind-values{position:absolute;left:74px;top:66%;font-size:12px;color:#e6edf7;background:rgba(13,22,38,.85);padding:3px 10px;border-radius:4px;border:1px solid #2a3650;z-index:5;pointer-events:none;white-space:nowrap}
.ind-menu{position:absolute;left:10px;bottom:30px;width:520px;max-width:90vw;background:#0d1626;border:1px solid #2a3650;border-radius:10px;box-shadow:0 10px 30px rgba(0,0,0,.5);padding:14px;z-index:20;display:none;grid-template-columns:repeat(4,1fr);gap:8px}
.ind-menu.show{display:grid}
.ind-menu.vol-menu{grid-template-columns:repeat(3,1fr);width:360px}
.ind-menu h4{grid-column:1/-1;margin:0 0 6px;font-size:13px;color:#38bdf8}
.ind-menu .ind-item{padding:5px 8px;border-radius:5px;font-size:13px;color:#e6edf7;cursor:pointer;text-align:center;border:1px solid transparent;white-space:nowrap}
.ind-menu .ind-item:hover{background:#1a2744;border-color:#38bdf8}
.ind-menu .ind-item.active{background:linear-gradient(180deg,#0ea5e9,#0284c7);color:#fff;font-weight:700}
.ind-overlay{position:fixed;inset:0;z-index:15;display:none}
.ind-overlay.show{display:block}
</style>
</head>
<body>
<div class="kp-top">
  <span class="kp-sym" id="kp-sym">--</span>
  <div class="kp-tabs">
    <button class="kp-tab active" id="kp-tab-realtime" onclick="kpSwitch('realtime')">实时K线</button>
    <button class="kp-tab" id="kp-tab-history" onclick="kpSwitch('history')">历史K线</button>
  </div>
  <span class="kp-period"><label>周期</label><select id="kp-duration" onchange="kpChangePeriod()">
    <option value="60">1分钟</option>
    <option value="300">5分钟</option>
    <option value="900">15分钟</option>
    <option value="1800">30分钟</option>
    <option value="3600">1小时</option>
    <option value="86400" selected>日线</option>
    <option value="604800">周线</option>
    <option value="2592000">月线</option>
  </select></span>
  <span class="kp-source" id="kline-source"></span>
</div>
<div class="kp-ohlc">
  <span>开 <b id="kp-o">--</b></span><span>高 <b id="kp-h">--</b></span>
  <span>低 <b id="kp-l">--</b></span><span>收 <b id="kp-c">--</b></span><span>量 <b id="kp-v">--</b></span>
</div>
<div class="kp-chart-wrap">
  <canvas id="kc"></canvas>
  <div class="ind-values" id="indValues"></div>
  <div class="ind-title" id="indTitle" onclick="toggleIndMenu(event)">MACD(12,26,9) ▾</div>
  <div class="ind-overlay" id="indOverlay" onclick="closeIndMenu()"></div>
  <div class="ind-menu" id="indMenu">
    <h4>价格指标</h4>
    <div class="ind-item" data-key="macd" onclick="selectInd('macd')">MACD</div>
    <div class="ind-item" data-key="rsi" onclick="selectInd('rsi')">RSI</div>
    <div class="ind-item" data-key="kdj" onclick="selectInd('kdj')">KDJ</div>
    <div class="ind-item" data-key="wr" onclick="selectInd('wr')">WR</div>
    <div class="ind-item" data-key="mtm" onclick="selectInd('mtm')">MTM</div>
    <div class="ind-item" data-key="dmi" onclick="selectInd('dmi')">DMI</div>
    <div class="ind-item" data-key="bias" onclick="selectInd('bias')">BIAS</div>
    <div class="ind-item" data-key="psy" onclick="selectInd('psy')">PSY</div>
    <div class="ind-item" data-key="arbr" onclick="selectInd('arbr')">ARBR</div>
    <div class="ind-item" data-key="roc" onclick="selectInd('roc')">ROC</div>
    <div class="ind-item" data-key="adtm" onclick="selectInd('adtm')">ADTM</div>
    <div class="ind-item" data-key="asi" onclick="selectInd('asi')">ASI</div>
    <div class="ind-item" data-key="atr" onclick="selectInd('atr')">ATR</div>
    <div class="ind-item" data-key="b3612" onclick="selectInd('b3612')">B3612</div>
    <div class="ind-item" data-key="cr" onclick="selectInd('cr')">CR</div>
    <div class="ind-item" data-key="dbcd" onclick="selectInd('dbcd')">DBCD</div>
    <div class="ind-item" data-key="ddi" onclick="selectInd('ddi')">DDI</div>
    <div class="ind-item" data-key="dma" onclick="selectInd('dma')">DMA</div>
    <div class="ind-item" data-key="mi" onclick="selectInd('mi')">MI</div>
    <div class="ind-item" data-key="micd" onclick="selectInd('micd')">MICD</div>
    <div class="ind-item" data-key="qhlsr" onclick="selectInd('qhlsr')">QHLSR</div>
    <div class="ind-item" data-key="rc" onclick="selectInd('rc')">RC</div>
    <div class="ind-item" data-key="rccd" onclick="selectInd('rccd')">RCCD</div>
    <div class="ind-item" data-key="srdm" onclick="selectInd('srdm')">SRDM</div>
    <div class="ind-item" data-key="zdzb" onclick="selectInd('zdzb')">ZDZB</div>
    <div class="ind-item" data-key="cci" onclick="selectInd('cci')">CCI</div>
    <div class="ind-item" data-key="lon" onclick="selectInd('lon')">LON</div>
    <div class="ind-item" data-key="short" onclick="selectInd('short')">SHORT</div>
    <h4>量能指标</h4>
    <div class="ind-item" data-key="cjl" onclick="selectInd('cjl')">CJL</div>
    <div class="ind-item" data-key="ccl" onclick="selectInd('ccl')">CCL</div>
    <div class="ind-item" data-key="mv" onclick="selectInd('mv')">MV</div>
    <div class="ind-item" data-key="obv" onclick="selectInd('obv')">OBV</div>
    <div class="ind-item" data-key="vr" onclick="selectInd('vr')">VR</div>
    <div class="ind-item" data-key="vroc" onclick="selectInd('vroc')">VROC</div>
    <div class="ind-item" data-key="vrsi" onclick="selectInd('vrsi')">VRSI</div>
    <div class="ind-item" data-key="wvad" onclick="selectInd('wvad')">WVAD</div>
  </div>
</div>
<script>
var curSym='__INIT_SYM__',curTab='realtime',klineData=[],klinePollTimer=null,currentPollMs=3000;
window.klineHoverIndex=-1;window.klineIndicators=null;window.klineLayout=null;
// 数据接口用 /api/kline 与 /api/kline/history 相对路径：
// stock_main 网关代理会扫描 HTML 里的字符串字面量, 自动把 '/api/' 前缀改写成 '/api/gw/<module>/api/'
// 避免在 JS 内手动拼接网关前缀造成双重 /api/gw/<module>/api/gw/<module>/...
function loadKline(sym){
  curSym=sym;
  var el=document.getElementById('kp-sym'); if(el) el.textContent=sym;
  document.title='期权K线 · '+sym;
  fetchKlineData();
}
function kpSwitch(tab){
  curTab=tab;
  document.getElementById('kp-tab-realtime').classList.toggle('active',tab==='realtime');
  document.getElementById('kp-tab-history').classList.toggle('active',tab==='history');
  fetchKlineData();
}
function kpChangePeriod(){
  // 切换周期: 重置 hover/indicators/data, 重拉
  window.klineHoverIndex=-1;window.klineIndicators=null;window.klineLayout=null;
  klineData=[];
  fetchKlineData();
}
function fetchKlineData(){
  if(!curSym)return;
  var durEl=document.getElementById('kp-duration');
  var dur=durEl?durEl.value:'86400';
  var base=curTab==='realtime'?'/api/kline':'/api/kline/history';
  var url=base+'?symbol='+encodeURIComponent(curSym)+'&duration='+dur+(curTab==='realtime'?'&count=120':'');
  fetch(url).then(function(x){return x.json();}).then(function(d){
    klineData=d.data||[];window.klineIndicators=d.indicators||null;
    var src=document.getElementById('kline-source');
    src.textContent=(d.source||'')+(klineData.length?(' · '+klineData.length+'根'):'')+(d.msg?(' · '+d.msg):'');
    if(d.code===-1){src.textContent='错误: '+(d.msg||'未知');}
    drawKline(klineData,curSym);
  }).catch(function(e){
    klineData=[];window.klineIndicators=null;drawKline([],curSym);
    document.getElementById('kline-source').textContent='加载失败: '+e;
  });
}
// ===== 副图指标配置 (与后端 _compute_all_indicators 返回的键对齐) =====
var curInd='macd';
var IND_CONFIG={
  macd:{name:'MACD(12,26,9)',zeroLine:true,series:[{key:'bar',type:'bar',up:'#ff5b5b',down:'#22c55e'},{key:'dif',type:'line',color:'#fbbf24'},{key:'dea',type:'line',color:'#38bdf8'}]},
  rsi:{name:'RSI(14)',min:0,max:100,levels:[30,50,70],series:[{key:'rsi',type:'line',color:'#fbbf24'}]},
  kdj:{name:'KDJ(9,3,3)',min:0,max:100,levels:[20,50,80],series:[{key:'k',type:'line',color:'#fbbf24'},{key:'d',type:'line',color:'#38bdf8'},{key:'j',type:'line',color:'#f472b6'}]},
  wr:{name:'WR(14)',min:-100,max:0,levels:[-80,-50,-20],series:[{key:'wr',type:'line',color:'#fbbf24'}]},
  mtm:{name:'MTM(12)',zeroLine:true,series:[{key:'mtm',type:'line',color:'#fbbf24'},{key:'mtmma',type:'line',color:'#38bdf8'}]},
  roc:{name:'ROC(12)',zeroLine:true,series:[{key:'roc',type:'line',color:'#fbbf24'}]},
  bias:{name:'BIAS(6,12,24)',zeroLine:true,series:[{key:'bias6',type:'line',color:'#fbbf24'},{key:'bias12',type:'line',color:'#38bdf8'},{key:'bias24',type:'line',color:'#f472b6'}]},
  psy:{name:'PSY(12)',min:0,max:100,levels:[25,50,75],series:[{key:'psy',type:'line',color:'#fbbf24'}]},
  arbr:{name:'ARBR(26)',series:[{key:'ar',type:'line',color:'#fbbf24'},{key:'br',type:'line',color:'#38bdf8'}]},
  dmi:{name:'DMI(14)',min:0,max:100,levels:[20,50],series:[{key:'pdi',type:'line',color:'#fbbf24'},{key:'mdi',type:'line',color:'#38bdf8'},{key:'adx',type:'line',color:'#f472b6'}]},
  cci:{name:'CCI(20)',zeroLine:true,levels:[-100,100],series:[{key:'cci',type:'line',color:'#fbbf24'}]},
  dma:{name:'DMA(10,50)',zeroLine:true,series:[{key:'dma',type:'line',color:'#fbbf24'},{key:'ama',type:'line',color:'#38bdf8'}]},
  atr:{name:'ATR(14)',series:[{key:'atr',type:'line',color:'#fbbf24'}]},
  mi:{name:'MI(25,9)',series:[{key:'mi',type:'line',color:'#fbbf24'}]},
  adtm:{name:'ADTM(23,8)',zeroLine:true,series:[{key:'adtm',type:'line',color:'#fbbf24'}]},
  asi:{name:'ASI',zeroLine:true,series:[{key:'asi',type:'line',color:'#fbbf24'}]},
  b3612:{name:'B3612',zeroLine:true,series:[{key:'b36',type:'line',color:'#fbbf24'},{key:'b12',type:'line',color:'#38bdf8'}]},
  cr:{name:'CR(26)',series:[{key:'cr',type:'line',color:'#fbbf24'}]},
  dbcd:{name:'DBCD',zeroLine:true,series:[{key:'dbcd',type:'line',color:'#fbbf24'}]},
  ddi:{name:'DDI',zeroLine:true,series:[{key:'ddi',type:'line',color:'#fbbf24'}]},
  micd:{name:'MICD',zeroLine:true,series:[{key:'micd',type:'line',color:'#fbbf24'}]},
  qhlsr:{name:'QHLSR(10)',min:0,max:100,levels:[20,50,80],series:[{key:'qhlsr',type:'line',color:'#fbbf24'}]},
  rc:{name:'RC(12)',zeroLine:true,series:[{key:'rc',type:'line',color:'#fbbf24'}]},
  rccd:{name:'RCCD',zeroLine:true,series:[{key:'rccd',type:'line',color:'#fbbf24'}]},
  srdm:{name:'SRDM',zeroLine:true,series:[{key:'srdm',type:'line',color:'#fbbf24'}]},
  zdzb:{name:'ZDZB',zeroLine:true,series:[{key:'zdzb',type:'line',color:'#fbbf24'}]},
  lon:{name:'LON',zeroLine:true,series:[{key:'lon',type:'line',color:'#fbbf24'}]},
  short:{name:'SHORT',zeroLine:true,series:[{key:'short',type:'line',color:'#fbbf24'}]},
  cjl:{name:'CJL(成交量)',barFromZero:true,series:[{key:'vol',type:'bar',up:'#ff5b5b',down:'#22c55e'}]},
  ccl:{name:'CCL(持仓量)',barFromZero:true,series:[{key:'oi',type:'bar',up:'#ff5b5b',down:'#22c55e'}]},
  mv:{name:'MV(均量)',barFromZero:true,series:[{key:'mv5',type:'line',color:'#fbbf24'},{key:'mv10',type:'line',color:'#38bdf8'}]},
  obv:{name:'OBV',series:[{key:'obv',type:'line',color:'#fbbf24'}]},
  vr:{name:'VR(26)',min:0,max:200,levels:[70,100,150],series:[{key:'vr',type:'line',color:'#fbbf24'}]},
  vroc:{name:'VROC(12)',zeroLine:true,series:[{key:'vroc',type:'line',color:'#fbbf24'}]},
  vrsi:{name:'VRSI(14)',min:0,max:100,levels:[30,50,70],series:[{key:'vrsi',type:'line',color:'#fbbf24'}]},
  wvad:{name:'WVAD(24)',zeroLine:true,series:[{key:'wvad',type:'line',color:'#fbbf24'}]}
};
function selectInd(key){
  curInd=key;
  var cfg=IND_CONFIG[key];
  var t=document.getElementById('indTitle'); if(t)t.textContent=(cfg?cfg.name:key)+' ▾';
  document.querySelectorAll('#indMenu .ind-item').forEach(function(el){el.classList.toggle('active',el.getAttribute('data-key')===key);});
  closeIndMenu();
  if(klineData&&klineData.length)drawKline(klineData,curSym);
}
function toggleIndMenu(e){if(e)e.stopPropagation();var m=document.getElementById('indMenu'),o=document.getElementById('indOverlay');if(!m)return;m.classList.toggle('show');o.classList.toggle('show');updateIndMenuActive();}
function closeIndMenu(){var m=document.getElementById('indMenu'),o=document.getElementById('indOverlay');if(m)m.classList.remove('show');if(o)o.classList.remove('show');}
function updateIndMenuActive(){document.querySelectorAll('#indMenu .ind-item').forEach(function(el){el.classList.toggle('active',el.getAttribute('data-key')===curInd);});}
function updateIndValues(idx){
  var el=document.getElementById('indValues');if(!el)return;
  var cfg=IND_CONFIG[curInd],ind=window.klineIndicators||{},id=ind[curInd]||{};
  if(!cfg){el.innerHTML='';return;}
  var parts=['<b>'+cfg.name.split('(')[0]+'</b>'];
  cfg.series.forEach(function(s){
    var v=(id[s.key]&&id[s.key][idx]!=null)?id[s.key][idx]:null;
    var color;
    if(s.type==='bar'){color=(v!=null&&v<0)?(s.down||'#22c55e'):(s.up||'#ff5b5b');}
    else{color=s.color||'#e6edf7';}
    parts.push('<span style="color:'+color+'">'+s.key.toUpperCase()+':'+(v!=null&&isFinite(v)?v.toFixed(3):'--')+'</span>');
  });
  el.innerHTML=parts.join(' ');
}
function renderSubIndicator(ctx,bars,top,h,left,right,gap,barW,cfg){
  cfg=cfg||IND_CONFIG[curInd]; if(!cfg||!bars.length)return;
  var n=bars.length,ind=window.klineIndicators||{},id=ind[curInd]||{},allV=[],hasBar=false,barFromZero=!!cfg.barFromZero;
  cfg.series.forEach(function(s){if(s.type==='bar')hasBar=true; var arr=id[s.key]||[]; for(var i=0;i<n;i++){var v=arr[i]; if(v!=null&&isFinite(v))allV.push(v);} });
  var yMin,yMax;
  if(cfg.min!=null&&cfg.max!=null){yMin=cfg.min;yMax=cfg.max;}
  else if(barFromZero){yMin=0;yMax=allV.length?Math.max.apply(null,allV):1; var pad=yMax*0.08||1;yMax+=pad;}
  else if(hasBar){var vmax=allV.length?Math.max.apply(null,allV.map(Math.abs)):1; if(vmax===0)vmax=1; yMin=-vmax;yMax=vmax;}
  else if(allV.length){yMin=Math.min.apply(null,allV);yMax=Math.max.apply(null,allV); var pad=(yMax-yMin)*0.08||Math.abs(yMax)*0.02; yMin-=pad;yMax+=pad;}
  else{yMin=0;yMax=100;}
  if(yMin===yMax)yMax=yMin+1;
  function sy(y){return top+h*(1-(y-yMin)/(yMax-yMin));}
  ctx.strokeStyle='#1c2740';ctx.lineWidth=0.5;
  for(var t=0;t<=4;t++){var yy=top+h*t/4;ctx.beginPath();ctx.moveTo(left,yy);ctx.lineTo(right,yy);ctx.stroke();}
  if(cfg.zeroLine||hasBar){ctx.strokeStyle='#5b6b85';ctx.lineWidth=0.5;ctx.beginPath();ctx.moveTo(left,sy(barFromZero?yMin:0));ctx.lineTo(right,sy(barFromZero?yMin:0));ctx.stroke();}
  if(cfg.levels){cfg.levels.forEach(function(lv){if(lv<yMin||lv>yMax)return;ctx.strokeStyle='#3a4a66';ctx.setLineDash([3,3]);ctx.lineWidth=0.5;ctx.beginPath();ctx.moveTo(left,sy(lv));ctx.lineTo(right,sy(lv));ctx.stroke();ctx.setLineDash([]);});}
  cfg.series.forEach(function(s){
    var arr=id[s.key]||[];
    if(s.type==='bar'){
      for(var i=0;i<n;i++){var v=arr[i]; if(v==null||!isFinite(v))continue; var x=left+barW*i+barW/2, up=v>=0; ctx.fillStyle=up?(s.up||'#ff5b5b'):(s.down||'#22c55e'); var y0=barFromZero?sy(yMin):sy(0); var y1=sy(v); ctx.fillRect(x-barW/2+gap/2,Math.min(y0,y1),barW-gap,Math.max(1,Math.abs(y1-y0)));}
    }else if(s.type==='line'){
      ctx.strokeStyle=s.color||'#fbbf24';ctx.lineWidth=1.3;ctx.beginPath();var st=false;
      for(var i=0;i<n;i++){var v=arr[i]; if(v==null||!isFinite(v)){st=false;continue;} var x=left+barW*i+barW/2, yy=sy(v); if(!st){ctx.moveTo(x,yy);st=true;}else ctx.lineTo(x,yy);}
      ctx.stroke();
    }
  });
  ctx.fillStyle='#7c8aa3';ctx.font='12px Microsoft YaHei';ctx.textAlign='left';ctx.fillText(cfg.name,left+6,top+13);
  window.klineSubInfo={top:top,h:h,yMin:yMin,yMax:yMax};
}
function renderVolPanel(ctx,bars,top,h,left,right,gap,barW){
  if(!bars.length)return;
  var n=bars.length,ind=window.klineIndicators||{},vol=ind.cjl&&ind.cjl.vol?ind.cjl.vol:[],oi=ind.ccl&&ind.ccl.oi?ind.ccl.oi:[];
  var allV=[],hasOI=false;
  for(var i=0;i<n;i++){var v=vol[i]; if(v!=null&&isFinite(v))allV.push(v);}
  for(var i=0;i<oi.length;i++){var v=oi[i]; if(v!=null&&isFinite(v)){allV.push(v);hasOI=true;}}
  var yMin=0,yMax=allV.length?Math.max.apply(null,allV):1,pad=yMax*0.08||1;yMax+=pad;if(yMax===0)yMax=1;
  function sy(y){return top+h*(1-(y-yMin)/(yMax-yMin));}
  ctx.strokeStyle='#1c2740';ctx.lineWidth=0.5;
  for(var t=0;t<=2;t++){var yy=top+h*t/2;ctx.beginPath();ctx.moveTo(left,yy);ctx.lineTo(right,yy);ctx.stroke();}
  // 成交量柱
  for(var i=0;i<n;i++){var v=vol[i]; if(v==null||!isFinite(v))v=0; var x=left+barW*i+barW/2; var y0=sy(yMin); var y1=sy(v); ctx.fillStyle=bars[i].c>=bars[i].o?'#ff5b5b':'#22c55e'; ctx.fillRect(x-barW/2+gap/2,Math.min(y0,y1),barW-gap,Math.max(1,Math.abs(y1-y0)));}
  // 持仓量线(如数据有效)
  if(hasOI){ctx.strokeStyle='#c084fc';ctx.lineWidth=1.3;ctx.beginPath();var st=false;for(var i=0;i<n;i++){var v=oi[i]; if(v==null||!isFinite(v)){st=false;continue;}var x=left+barW*i+barW/2,yy=sy(v);if(!st){ctx.moveTo(x,yy);st=true;}else ctx.lineTo(x,yy);}ctx.stroke();}
  ctx.fillStyle='#7c8aa3';ctx.font='12px Microsoft YaHei';ctx.textAlign='left';ctx.fillText('CJL(成交量)'+(hasOI?' / CCL(持仓量)':''),left+6,top+13);
}
function drawKline(data,sym){
  var cv=document.getElementById('kc'),ctx=cv.getContext('2d');
  var rect=cv.getBoundingClientRect(),w=Math.max(Math.round(rect.width),320),h=Math.max(Math.round(rect.height),300);
  cv.width=w;cv.height=h;ctx.clearRect(0,0,w,h);ctx.fillStyle='#0b1120';ctx.fillRect(0,0,w,h);
  if(!data||data.length===0){window.klineLayout=null;ctx.fillStyle='#94a3b8';ctx.font='15px Microsoft YaHei';ctx.textAlign='center';var et=curSym?(curTab==='history'?'【'+curSym+'】暂无历史K线数据(可能未上市或天勤未返回)':'【'+curSym+'】暂无实时K线数据'):'暂无K线数据';ctx.fillText(et,w/2,h/2);resetOhlc();return;}
  var bars=[],ind=(window.klineIndicators||{});
  for(var i=0;i<data.length;i++){
    var d=data[i],o=parseFloat(d.open||d.开盘价||0),c=parseFloat(d.close||d.收盘价||0),hh=parseFloat(d.high||d.最高价||0),ll=parseFloat(d.low||d.最低价||0),v=parseFloat(d.volume||d.成交量||0),dt=d.datetime||d.date||d.日期||'';
    hh=hh||Math.max(o,c,1);ll=ll||Math.min(o,c,1);o=o||c||1;c=c||o||1;if(!isFinite(o+hh+ll+c))continue;
    bars.push({idx:i,o:o,c:c,h:hh,l:ll,v:v,dt:dt,
      ma5:(d.ma5!=null?parseFloat(d.ma5):null),ma10:(d.ma10!=null?parseFloat(d.ma10):null),ma60:(d.ma60!=null?parseFloat(d.ma60):null),
      bt:(d.boll_top!=null?parseFloat(d.boll_top):null),bm:(d.boll_mid!=null?parseFloat(d.boll_mid):null),bb:(d.boll_bottom!=null?parseFloat(d.boll_bottom):null)});
  }
  if(!bars.length){window.klineLayout=null;return;}
  var ml=64,mr=10,mt=8,mb=26,cw=w-ml-mr,ch=h-mt-mb;
  var mainH=Math.round(ch*0.65),subTop=mt+mainH+8,subH=h-mb-subTop;
  var mainTop=mt,mainBot=mt+mainH;
  var allP=[];bars.forEach(function(b){allP.push(b.h,b.l);if(b.bt)allP.push(b.bt);if(b.bb)allP.push(b.bb);});
  var pHigh=Math.max.apply(null,allP),pLow=Math.min.apply(null,allP),pad=(pHigh-pLow)*0.08||pHigh*0.02,yMax=pHigh+pad,yMin=pLow-pad,yRange=yMax-yMin||1;
  function y2p(y){return mainTop+(mainBot-mainTop)*(1-(y-yMin)/yRange);}
  ctx.strokeStyle='#1c2740';ctx.lineWidth=0.5;
  for(var t=0;t<=5;t++){var yy=mainTop+(mainBot-mainTop)*t/5,val=yMax-(yMax-yMin)*t/5;ctx.beginPath();ctx.moveTo(ml,yy);ctx.lineTo(w-mr,yy);ctx.stroke();ctx.fillStyle='#5b6b85';ctx.font='11px monospace';ctx.textAlign='right';ctx.fillText(val.toFixed(2),ml-6,yy+4);}
  var n=bars.length,barW=cw/n,gap=Math.min(barW*0.28,5);
  for(var i=0;i<n;i++){var b=bars[i],x=ml+barW*i+barW/2,up=b.c>=b.o;ctx.strokeStyle=up?'#ff5b5b':'#22c55e';ctx.fillStyle=up?'#ff5b5b':'#22c55e';ctx.beginPath();ctx.moveTo(x,y2p(b.o));ctx.lineTo(x,y2p(b.c));ctx.stroke();ctx.fillRect(x-barW/2+gap/2,y2p(b.h),barW-gap,Math.max(1,y2p(b.l)-y2p(b.h)));}
  function line(key,color){ctx.strokeStyle=color;ctx.lineWidth=1.5;ctx.beginPath();var st=false;for(var i=0;i<n;i++){var v=bars[i][key];if(v==null||!isFinite(v)){st=false;continue;}var x=ml+barW*i+barW/2,yy=y2p(v);if(!st){ctx.moveTo(x,yy);st=true;}else ctx.lineTo(x,yy);}ctx.stroke();}
  line('ma5','#fbbf24');line('ma10','#38bdf8');line('ma60','#f472b6');
  ctx.setLineDash([5,4]);ctx.lineWidth=1;line('bt','#34d399');line('bm','#5b6b85');line('bb','#34d399');ctx.setLineDash([]);
  ctx.fillStyle='#7c8aa3';ctx.font='12px Microsoft YaHei';ctx.textAlign='left';ctx.fillText('MA5/10/60  ·  BOLL(20,2)',ml+6,mainTop+14);
  renderSubIndicator(ctx,bars,subTop,subH,ml,w-mr,gap,barW);
  updateIndValues(n-1);
  window.klineLayout={bars:bars,ml:ml,cw:cw,barW:barW,gap:gap,mainTop:mainTop,mainBot:mainBot,yMin:yMin,yRange:yRange,w:w,h:h,subTop:subTop,subH:subH};
  var last=bars[n-1];
  document.getElementById('kp-o').textContent=last.o.toFixed(2);
  document.getElementById('kp-h').textContent=last.h.toFixed(2);
  document.getElementById('kp-l').textContent=last.l.toFixed(2);
  document.getElementById('kp-c').textContent=last.c.toFixed(2);
  document.getElementById('kp-c').style.color=last.c>=last.o?'#ff5b5b':'#22c55e';
  document.getElementById('kp-v').textContent=(last.v||0).toLocaleString();
  if(window.klineHoverIndex>=0&&window.klineHoverIndex<n)drawKlineHover(window.klineHoverIndex);
}
function resetOhlc(){['o','h','l','c','v'].forEach(function(k){var e=document.getElementById('kp-'+k);e.textContent='--';if(k==='c')e.style.color='';});}
var kcEl=document.getElementById('kc');
kcEl.addEventListener('mousemove',handleKlineHover);
kcEl.addEventListener('mouseleave',function(){window.klineHoverIndex=-1;drawKline(klineData,curSym);});
kcEl.addEventListener('touchstart',function(e){e.preventDefault();handleKlineHover(e.touches[0]);},{passive:false});
kcEl.addEventListener('touchmove',function(e){e.preventDefault();handleKlineHover(e.touches[0]);},{passive:false});
function handleKlineHover(e){if(typeof drawState!=='undefined'&&drawState.active){drawState.temp=getCanvasPoint(e);drawKline(klineData,curSym);return;}var L=window.klineLayout;if(!L||!L.bars.length)return;var cv=document.getElementById('kc'),rect=cv.getBoundingClientRect(),scaleX=(rect.width?cv.width/rect.width:1)||1,mx=(e.clientX-rect.left)*scaleX;var i=Math.round((mx-L.ml-L.barW/2)/L.barW);if(i<0)i=0;if(i>=L.bars.length)i=L.bars.length-1;window.klineHoverIndex=i;drawKline(klineData,curSym);}
function drawKlineHover(idx){var L=window.klineLayout;if(!L||L.bars[idx]==null)return;var cv=document.getElementById('kc'),ctx=cv.getContext('2d'),b=L.bars[idx];var x=L.ml+L.barW*idx+L.barW/2;['ohlc','kp'].forEach(function(p){var o=document.getElementById(p+'-o');if(!o)return;var hh=document.getElementById(p+'-h'),ll=document.getElementById(p+'-l'),cc=document.getElementById(p+'-c'),vv=document.getElementById(p+'-v');o.textContent=b.o.toFixed(2);hh.textContent=b.h.toFixed(2);ll.textContent=b.l.toFixed(2);cc.textContent=b.c.toFixed(2);cc.style.color=b.c>=b.o?'#ff5b5b':'#22c55e';vv.textContent=(b.v||0).toLocaleString();});ctx.strokeStyle='#7c8aa3';ctx.setLineDash([4,4]);ctx.lineWidth=0.8;ctx.beginPath();ctx.moveTo(x,L.mainTop);ctx.lineTo(x,L.indBot||L.mainBot);ctx.stroke();ctx.setLineDash([]);ctx.strokeStyle='#ffffff';ctx.lineWidth=2;var ytop=L.mainTop+(L.mainBot-L.mainTop)*(1-(b.h-L.yMin)/L.yRange),ybot=L.mainTop+(L.mainBot-L.mainTop)*(1-(b.l-L.yMin)/L.yRange);ctx.strokeRect(L.ml+L.barW*idx+L.gap/2-1,Math.min(ytop,ybot)-2,L.barW-L.gap+2,Math.abs(ybot-ytop)+4);var dtStr=String(b.dt),dateLabel=dtStr;if(dtStr.length>6&&dtStr.indexOf('-')<0)dateLabel=dtStr.substring(0,4)+'-'+dtStr.substring(4,6)+'-'+dtStr.substring(6,8);var txt='时间:'+dateLabel+' 开:'+b.o.toFixed(2)+' 高:'+b.h.toFixed(2)+' 低:'+b.l.toFixed(2)+' 收:'+b.c.toFixed(2)+' 量:'+(b.v||0).toLocaleString();ctx.font='bold 13px Microsoft YaHei';var tw=ctx.measureText(txt).width+16;var tx=Math.min(Math.max(x+10,8),L.w-tw-8),ty=L.mainTop+6;  ctx.fillStyle='rgba(13,21,38,0.96)';ctx.fillRect(tx,ty,tw,26);ctx.strokeStyle='#38bdf8';ctx.lineWidth=1;ctx.strokeRect(tx,ty,tw,26);ctx.fillStyle='#e6edf7';ctx.textAlign='left';ctx.textBaseline='middle';ctx.fillText(txt,tx+8,ty+13);updateIndValues(idx);}
// ===== 划线分析核心 =====
function toggleDrawPanel(e){if(e)e.stopPropagation();var p=document.getElementById('drawPanel'),o=document.getElementById('drawOverlay'),b=document.getElementById('drawBtn');if(!p)return;var show=!p.classList.contains('show');p.classList.toggle('show',show);o.classList.toggle('show',show);b.classList.toggle('active',show);if(!show)setDrawTool('');}
function closeDrawPanel(){var p=document.getElementById('drawPanel'),o=document.getElementById('drawOverlay'),b=document.getElementById('drawBtn');if(p)p.classList.remove('show');if(o)o.classList.remove('show');if(b)b.classList.remove('active');setDrawTool('');}
function selectDrawColor(c){drawColor=c;document.querySelectorAll('.draw-colors span').forEach(function(el){el.classList.toggle('active',el.getAttribute('data-color')===c);});}
function setDrawTool(t){drawTool=t;document.querySelectorAll('.draw-tool').forEach(function(el){el.classList.toggle('active',el.getAttribute('data-tool')===t);});var cv=document.getElementById('kc');if(cv)cv.style.cursor=t?'crosshair':'default';drawState.active=false;}
function selectDrawTool(t){setDrawTool(t);}
function getCanvasPoint(e){var cv=document.getElementById('kc'),rect=cv.getBoundingClientRect(),scaleX=(rect.width?cv.width/rect.width:1)||1,scaleY=(rect.height?cv.height/rect.height:1)||1;return{x:(e.clientX-rect.left)*scaleX,y:(e.clientY-rect.top)*scaleY};}
function onDrawMouseDown(e){if(!drawTool)return;var p=getCanvasPoint(e);drawState={active:true,tool:drawTool,start:p,end:p,temp:p};if(drawTool==='text'){var txt=prompt('输入标注文字','');if(txt){drawings.push({tool:'text',text:txt,x:p.x,y:p.y,color:drawColor});}drawState.active=false;setDrawTool('');drawKline(klineData,curSym);}}
function onDrawMouseUp(e){if(!drawState.active)return;drawState.end=getCanvasPoint(e);drawState.active=false;var d={tool:drawTool,start:{x:drawState.start.x,y:drawState.start.y},end:{x:drawState.end.x,y:drawState.end.y},color:drawColor};if(drawTool==='hline'){d.end.y=d.start.y;}
if(drawTool==='vline'){d.end.x=d.start.x;}
drawings.push(d);drawKline(klineData,curSym);}
function clearDrawings(){if(confirm('清空当前所有划线?')){drawings=[];drawKline(klineData,curSym);}}
function renderDrawings(ctx){var L=window.klineLayout;if(!L)return;for(var i=0;i<drawings.length;i++){var d=drawings[i];ctx.strokeStyle=d.color||'#fbbf24';ctx.fillStyle=d.color||'#fbbf24';ctx.lineWidth=1.5;if(d.tool==='text'){ctx.font='bold 13px Microsoft YaHei';ctx.textAlign='left';ctx.textBaseline='top';ctx.fillText(d.text,d.x,d.y);continue;}var x1=d.start.x,y1=d.start.y,x2=(d.end?d.end.x:d.start.x),y2=(d.end?d.end.y:d.start.y);if(d.tool==='hline'){y1=y2=d.start.y;x1=L.ml;x2=L.w-(L.mr||10);}if(d.tool==='vline'){x1=x2=d.start.x;y1=L.mainTop;y2=L.indBot||L.mainBot;}ctx.beginPath();if(d.tool==='line'||d.tool==='ray'){ctx.moveTo(x1,y1);var dx=x2-x1,dy=y2-y1;if(dx===0&&dy===0)continue;var t=Math.max(L.w,L.h)/Math.max(1,Math.sqrt(dx*dx+dy*dy));ctx.lineTo(x1+dx*t,y1+dy*t);}else if(d.tool==='segment'){ctx.moveTo(x1,y1);ctx.lineTo(x2,y2);}else if(d.tool==='rect'){ctx.strokeRect(Math.min(x1,x2),Math.min(y1,y2),Math.abs(x2-x1),Math.abs(y2-y1));}else{ctx.moveTo(x1,y1);ctx.lineTo(x2,y2);}ctx.stroke();}
if(drawState.active&&drawState.temp&&drawState.start){ctx.strokeStyle=drawColor;ctx.setLineDash([4,4]);ctx.lineWidth=1;ctx.beginPath();var s=drawState.start,t=drawState.temp;if(drawTool==='rect'){ctx.strokeRect(Math.min(s.x,t.x),Math.min(s.y,t.y),Math.abs(t.x-s.x),Math.abs(t.y-s.y));}else if(drawTool==='hline'){ctx.moveTo(L.ml,s.y);ctx.lineTo(L.w-(L.mr||10),s.y);}else if(drawTool==='vline'){ctx.moveTo(s.x,L.mainTop);ctx.lineTo(s.x,L.indBot||L.mainBot);}else{ctx.moveTo(s.x,s.y);ctx.lineTo(t.x,t.y);}ctx.stroke();ctx.setLineDash([]);}}
var kcEl=document.getElementById('kc');
kcEl.addEventListener('mousemove',function(e){if(typeof drawState!=='undefined'&&drawState.active){drawState.temp=getCanvasPoint(e);drawKline(klineData,curSym);return;}handleKlineHover(e);});
kcEl.addEventListener('mouseleave',function(){window.klineHoverIndex=-1;if(typeof drawState!=='undefined')drawState.active=false;drawKline(klineData,curSym);});
kcEl.addEventListener('mousedown',onDrawMouseDown);
kcEl.addEventListener('mouseup',onDrawMouseUp);
kcEl.addEventListener('touchstart',function(e){e.preventDefault();handleKlineHover(e.touches[0]);},{passive:false});
kcEl.addEventListener('touchmove',function(e){e.preventDefault();handleKlineHover(e.touches[0]);},{passive:false});
var resizeTimer;window.addEventListener('resize',function(){clearTimeout(resizeTimer);resizeTimer=setTimeout(function(){if(klineData.length)drawKline(klineData,curSym);},200);});
if(curSym) loadKline(curSym);
setInterval(function(){ if(curTab==='realtime'&&curSym) fetchKlineData(); }, currentPollMs);
</script>
</body></html>
"""


@app.route("/optchain/kline-popup")
def optchain_kline_popup_page():
    sym = request.args.get("symbol", "").strip()
    if not sym:
        return render_template_string(
            "<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'><title>期权K线</title></head>"
            "<body style='background:#0b1120;color:#94a3b8;font-family:Microsoft YaHei;padding:24px'>"
            "缺少 symbol 参数，请从 T 型报价表双击合约行打开。</body></html>")
    return KLINE_POPUP_HTML.replace("__INIT_SYM__", html.escape(sym, quote=True))


# 注册期权模块特殊页面（主力合约 / 平值 / 筛选 / VIX / 标的排名）
from services import opt_special_pages
opt_special_pages.register_routes(app, _slots, DEFAULT_ACCT_KEY, _get_chain_live_greeks, _resolve_slot)

# 注册短线量化模块（截图：短线行情识别 + 短线入场点量化）
from services import short_term_quant
short_term_quant.register_routes(app, _slots, DEFAULT_ACCT_KEY, _resolve_slot)

from services import futures_kline
futures_kline.register_routes(app, _slots, DEFAULT_ACCT_KEY, _resolve_slot)

# 注册股票模块（全部 A 股 + 周/月 MACD 金叉）
from services import stock_kline
stock_kline.register_routes(app, _slots, DEFAULT_ACCT_KEY, _resolve_slot, make_slot=get_slot)

# 注册期货全市场实时监控预警模块
from services import future_monitor
future_monitor.register_routes(app, _slots, DEFAULT_ACCT_KEY, _resolve_slot)


def _load_expiry_cache(cache_key, year, month):
    import time as _time
    try:
        result = fetch_expiry_data(year, month)
        with expiry_cache_lock:
            expiry_cache[cache_key] = {"status": "done", "data": result, "error": "", "ts": _time.time()}
        print(f"[到期日历] {cache_key} 缓存更新完成，期货 {len(result.get('futures', {}))} 个 / 期权 {len(result.get('options', {}))} 个日期")
    except Exception as e:
        import time as _time
        with expiry_cache_lock:
            expiry_cache[cache_key] = {"status": "error", "data": {}, "error": f"查询失败: {e}", "ts": _time.time()}
        print(f"[到期日历] {cache_key} 查询失败: {e}")


@app.route("/api/symbols/add")
def api_add():
    sym = request.args.get("symbol", "").strip()
    if not sym: return jsonify({"code": -1}), 400
    if sym not in DEFAULT_SYMBOLS:
        DEFAULT_SYMBOLS.append(sym)
    return jsonify({"code": 0})

@app.route("/api/symbols/remove")
def api_rem():
    sym = request.args.get("symbol", "").strip()
    if not sym: return jsonify({"code": -1}), 400
    if sym in DEFAULT_SYMBOLS: DEFAULT_SYMBOLS.remove(sym)
    return jsonify({"code": 0})


@app.route("/api/sync/futures", methods=["POST"])
def api_sync_futures():
    from services.futures_db import sync_from_tqsdk
    # 默认增量(只拉新合约); ?full=1 时全量回填补全历史
    full = request.args.get("full") == "1"
    ok, msg = sync_service.start_sync("futures", sync_from_tqsdk, 12, 12, incremental=not full)
    return jsonify({"ok": ok, "msg": msg})

@app.route("/api/sync/options", methods=["POST"])
def api_sync_options():
    from services.futures_db import sync_options_from_tqsdk
    full = request.args.get("full") == "1"
    ok, msg = sync_service.start_sync("options", sync_options_from_tqsdk, 12, 12, incremental=not full)
    return jsonify({"ok": ok, "msg": msg})

@app.route("/api/sync/status")
def api_sync_status():
    return jsonify({"futures": sync_service.get_state("futures"), "options": sync_service.get_state("options")})


@app.route("/api/options/nearest_underlying")
@app.route("/optchain/api/options/nearest_underlying")
def api_nearest_underlying():
    """按品种前缀返回最近的期权标的 underlying，用于底部快捷入口跳转。"""
    from services import futures_db
    prefix = request.args.get("prefix", "").strip()
    if not prefix:
        return jsonify({"ok": False, "msg": "缺少 prefix 参数"}), 400
    u = futures_db.get_nearest_underlying(prefix)
    if not u:
        return jsonify({"ok": False, "msg": f"未找到 {prefix} 的期权标的"}), 404
    return jsonify({"ok": True, "prefix": prefix, "underlying": u})


@app.route("/api/options/exchange_default_underlying")
@app.route("/optchain/api/options/exchange_default_underlying")
def api_exchange_default_underlying():
    """按交易所返回默认期权标的，用于 T 型报价页底部交易所 Tab 无刷新切换。"""
    from services import futures_db
    exchange = request.args.get("exchange", "").strip()
    if not exchange:
        return jsonify({"ok": False, "msg": "缺少 exchange 参数"}), 400
    u = futures_db.get_exchange_default_underlying(exchange)
    if not u:
        return jsonify({"ok": False, "msg": f"未找到 {exchange} 的期权标的"}), 404
    return jsonify({"ok": True, "exchange": exchange, "underlying": u})


@app.route("/api/options/chain_panel")
@app.route("/optchain/api/options/chain_panel")
def api_chain_panel():
    """返回 T 型报价数据区片段，用于 T 型报价页无刷新切换标的/到期月。"""
    from services import futures_db
    import datetime as dt
    now = dt.datetime.now()
    year = request.args.get("year", now.year, type=int)
    month = request.args.get("month", now.month, type=int)
    underlying = request.args.get("underlying", "").strip()
    exercise_date = request.args.get("exercise_date", "").strip() or None
    if not underlying:
        return jsonify({"ok": False, "msg": "缺少 underlying 参数"}), 400
    # 复用完整页面渲染，再提取 chain-panel-container 内部片段
    # 注: _build_expiry_html 内部已支持 exercise_date 过滤(通过 kwargs 透传)
    full_html = _build_expiry_html(year, month, "options", "/optchain/chain", underlying=underlying,
                                   back_js="location.href=optBase()", back_label="← 返回期权目录",
                                   show_calendar=False, exercise_date=exercise_date)
    html_str = full_html.get_data(as_text=True) if hasattr(full_html, "get_data") else str(full_html)
    start = html_str.find('<div id="chain-panel-container">')
    end = html_str.find('<!-- /chain-panel-container -->')
    if start == -1 or end == -1:
        return jsonify({"ok": False, "msg": "无法生成 T 型报价片段"}), 500
    # 从 start 标签之后截取到结束注释之前，保留内部原始 HTML
    fragment = html_str[start + len('<div id="chain-panel-container">'):end]
    return Response(fragment, mimetype='text/html')


@app.route("/api/options/chain_quotes")
@app.route("/optchain/api/options/chain_quotes")
def api_chain_quotes():
    """T 型报价实时行情(0.5s 轮询用): 返回标的/指数 + 各合约行情字段(不含希腊值)。
    复用 _get_chain_live_greeks 计算结果，仅摘出行情列，避免每次重算希腊值。"""
    import datetime as dt
    now = dt.datetime.now()
    year = request.args.get("year", now.year, type=int)
    month = request.args.get("month", now.month, type=int)
    underlying = request.args.get("underlying", "").strip()
    exercise_date = request.args.get("exercise_date", "").strip() or None
    if not underlying:
        return jsonify({"ok": False, "msg": "缺少 underlying 参数"}), 400
    chain = futures_db.query_option_chain(underlying, exercise_date=exercise_date)
    if chain is None:
        return jsonify({"ok": False, "msg": "暂无期权链数据"}), 404
    live = _get_chain_live_greeks(underlying, chain)
    if live is None:
        return jsonify({"ok": False, "msg": "行情未连接"}), 503
    _qf = ("last", "last_raw", "pre_close", "pre_settlement", "open", "high", "low", "average",
           "amount", "bid", "bid_v", "ask", "ask_v", "volume", "oi", "pre_oi", "oi_chg",
           "chg", "pct", "amp", "volume_multiple", "expire_rest_days", "option_class", "exercise")
    out = {"ok": True}
    for key in ("__underlying__", "__index__", "__wh_index__", "__vix__"):
        if key in live:
            out[key] = live[key]
    out["quotes"] = {sym: {k: d.get(k) for k in _qf}
                     for sym, d in live.items() if not sym.startswith("__")}
    return jsonify(out)


@app.route("/api/options/chain_greeks")
@app.route("/optchain/api/options/chain_greeks")
def api_chain_greeks():
    """T 型报价希腊值(2s 慢刷用): 返回各合约希腊值/IV/BS + VIX 估算。"""
    import datetime as dt
    now = dt.datetime.now()
    year = request.args.get("year", now.year, type=int)
    month = request.args.get("month", now.month, type=int)
    underlying = request.args.get("underlying", "").strip()
    exercise_date = request.args.get("exercise_date", "").strip() or None
    if not underlying:
        return jsonify({"ok": False, "msg": "缺少 underlying 参数"}), 400
    chain = futures_db.query_option_chain(underlying, exercise_date=exercise_date)
    if chain is None:
        return jsonify({"ok": False, "msg": "暂无期权链数据"}), 404
    live = _get_chain_live_greeks(underlying, chain)
    if live is None:
        return jsonify({"ok": False, "msg": "行情未连接"}), 503
    _gf = ("delta", "gamma", "theta", "vega", "rho", "bs", "intrinsic", "time_value",
           "leverage", "real_leverage", "premium_rate", "moneyness", "impv", "hv")
    out = {
        "ok": True,
        "greeks": {sym: {k: d.get(k) for k in _gf}
                   for sym, d in live.items() if not sym.startswith("__")},
        "vix": live.get("__vix__"),
    }
    return jsonify(out)


@app.route("/api/options/chain_smile")
@app.route("/optchain/api/options/chain_smile")
def api_chain_smile():
    """异步加载波动率微笑图：首屏只渲染占位，页面加载后再单独请求本接口。"""
    from services import futures_db
    import datetime as dt
    now = dt.datetime.now()
    year = request.args.get("year", now.year, type=int)
    month = request.args.get("month", now.month, type=int)
    underlying = request.args.get("underlying", "").strip()
    exercise_date = request.args.get("exercise_date", "").strip() or None
    if not underlying:
        return jsonify({"ok": False, "msg": "缺少 underlying 参数"}), 400
    chain = futures_db.query_option_chain(underlying, exercise_date=exercise_date)
    if chain is None:
        return jsonify({"ok": False, "msg": "暂无期权链数据"}), 404
    _live = _get_chain_live_greeks(underlying, chain)
    _meta = (_live.pop("__underlying__", {}) if isinstance(_live, dict) else {}) or {}
    u_last = _meta.get("last")
    _smile_pts = []
    _live_safe = _live or {}
    for _st in chain.get("strikes", []):
        _k = _st.get("strike")
        _csym = _st.get("call", {}).get("symbol") if _st.get("call") else None
        _psym = _st.get("put", {}).get("symbol") if _st.get("put") else None
        _civ = (_live_safe.get(_csym) or {}).get("impv") if _csym else None
        _piv = (_live_safe.get(_psym) or {}).get("impv") if _psym else None
        if _k is None:
            continue
        _smile_pts.append((_k, _civ, _piv))
    svg = _build_vol_smile_svg(_smile_pts, u_last)
    return jsonify({"ok": True, "svg": svg})


def main():
    print("=" * 50)
    print("  stock_app 多账户版 | http://127.0.0.1:5088")
    print("=" * 50)
    # ===== 防重复启动守卫 =====
    # 若 :5088 已有 stock_app 在跑（连接成功），直接退出，**不调用 register_service**，
    # 避免 DB 中 stock-app-01 的 service_token 被无意义地轮换、导致与长跑进程 client.token 失同步。
    # 真正的重启流程：先停旧进程 → 再启新进程。
    import socket as _socket
    _probe = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    _probe.settimeout(0.5)
    try:
        if _probe.connect_ex(('127.0.0.1', 5088)) == 0:
            print("[启动] ✗ 检测到 :5088 已被占用（疑似已有 stock_app 在跑）。本进程退出，避免重复注册导致 token 漂移。\n"
                  "       如确需重启，请先停止旧进程后再启动。")
            sys.exit(1)
    finally:
        _probe.close()
    # ===== 注册到 stock_main 并挂载 /health + 网关令牌校验 =====
    # 注意：register_service 必须在 `python main.py` 直接运行时才执行（下面 __main__ 守护），
    # 严禁在模块级别调用，否则任何 `import main`（包括测试/调试子进程）都会触发
    # POST /register → 轮换 DB 中 stock-app-01 的 service_token / gw_secret，导致与
    # 长跑进程的 client.token / gw_secret 失同步 → 心跳 403 → OFFLINE。
    try:
        from center_client import ServiceConfig, register_service
        register_service(app, ServiceConfig(
            center_url="http://127.0.0.1:9000",
            service_name="stock-app-01",
            host="127.0.0.1",
            port=5088,
            routes=["/", "/api/quotes", "/api/kline", "/api/kline/history",
                    "/api/options/nearest_underlying", "/api/options/exchange_default_underlying", "/api/options/chain_panel", "/api/options/chain_smile", "/api/options/main_extras",
                    "/option-expiry", "/options-calendar",
                    "/optchain", "/optchain/chain", "/optchain/kline-popup",
                    "/optchain/api/kline", "/optchain/api/kline/history", "/optchain/api/options/main_extras",
                    "/optchain/main", "/optchain/atm", "/optchain/filter", "/optchain/vix", "/optchain/rank"]
        ))
    except ImportError:
        print("[注册] center_client 未安装, 跳过注册")
    # 启动默认账户
    print("[启动] 连接默认账户 zhubajie...")
    _slot = get_slot("zhubajie", "jinhu303")
    # 定时同步: 每日凌晨2:00 增量拉取(只拉库里没有的新合约), 不在启动时全量刷天勤.
    # 仅当两张表都还空着(首次部署)时才在启动后补一次全量初始化.
    try:
        from services import futures_db
        need_init = (futures_db.count_rows("t_futures_expiry") == 0 or
                     futures_db.count_rows("t_option_expiry") == 0)
    except Exception:
        need_init = False
    sync_service.start_auto_timer(hour=2, incremental=True, run_on_start=need_init)
    print("[OK] 服务就绪")
    # ===== 关键架构：TqSdk 的 wait_update / get_quote 都必须在 TqApi 创建线程(主线程)调用 =====
    # 之前把 wait_update 放到独立后台线程，TqSdk 会报"不能在任意线程中调用 wait_update"。
    # 因此 Flask 移入后台线程，主线程同时负责：① 驱动 wait_update 轮询 ② 注册请求线程登记的订阅。
    import threading as _th
    _flask = _th.Thread(target=lambda: app.run(host="0.0.0.0", port=5088,
                                               debug=False, use_reloader=False), daemon=True)
    _flask.start()
    print("[OK] Flask 已在后台线程监听 :5088；主线程运行 TqSdk 轮询/订阅循环...")
    # 关键修正：原先只轮询默认账户 _slot(zhubajie)，用户切换选中的账户(Mr qia 等)虽建了 Slot
    # 却从未被 poll_step 驱动，导致 self.latest_data 永远为空 -> 行情看板"无数据" / 加自选无效。
    # 现改为在主线程统一驱动【所有】已注册 Slot；所有 Slot 的 TqApi 都在主线程创建(满足 TqSdk 同线程约束)。
    import time as _t
    while True:
        try:
            # 仅取快照后释放锁，避免持锁调用可能回灌 get_slot 的 poll 方法造成死锁
            with _slots_lock:
                _all = list(_slots.values())
            for slot in _all:
                if slot.status == "init":
                    # 在主线程创建 TqApi 并预加载(满足 TqSdk 同线程约束)
                    slot.connect()
                    if slot.status == "online":
                        try:
                            slot.startup_poll()
                        except Exception as _e:
                            print(f"[账户:{slot.tq_user}] 预加载异常: {_e}")
                elif slot.status == "online":
                    slot.poll_step()
                elif slot.status == "error":
                    # 轻量重连：每 300s 重试一次，应对临时网络/限流/天勤 trial 短暂不可用
                    _now = _t.time()
                    if _now - getattr(slot, "_last_err_try", 0) > 300:
                        slot._last_err_try = _now
                        print(f"[账户:{slot.tq_user}] 尝试重连...")
                        slot.connect()
                        if slot.status == "online":
                            try:
                                slot.startup_poll()
                            except Exception:
                                pass
        except Exception as _e:
            print(f"[主循环] 异常(已忽略): {_e}")


WORKBENCH_HTML = r"""<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>行情终端 · WH 风格工作台</title>
<style>
:root{--bg:#0b1120;--bg2:#0f172a;--panel:#111c30;--panel2:#16213a;--border:#1e2d4a;--border2:#27395c;--text:#e6edf7;--muted:#8aa0c0;--accent:#38bdf8;--accent2:#f472b6;--gold:#f5c451;--up:#ef4444;--down:#22c55e;}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{font-family:'Microsoft YaHei','PingFang SC',sans-serif;background:var(--bg);color:var(--text);font-size:16px;overflow:hidden}
a{color:var(--accent);text-decoration:none}
button{font-family:inherit;cursor:pointer}
/* 顶栏 */
.topbar{height:52px;display:flex;align-items:center;gap:14px;padding:0 16px;background:linear-gradient(90deg,#0d1b30,#0b1120);border-bottom:1px solid var(--border2);flex:none}
.logo{font-size:20px;font-weight:800;color:var(--gold);letter-spacing:1px;white-space:nowrap}
.logo small{font-size:12px;color:var(--muted);font-weight:400;margin-left:6px}
.idx-bar{display:flex;gap:18px;flex:1;overflow:hidden;align-items:center}
.idx{display:flex;flex-direction:column;line-height:1.15;min-width:78px}
.idx .nm{font-size:11px;color:var(--muted)}
.idx .vl{font-size:15px;font-weight:700}
.idx .up{color:var(--up)} .idx .down{color:var(--down)} .idx .flat{color:var(--muted)}
.top-right{display:flex;align-items:center;gap:12px;white-space:nowrap}
.clock{font-size:15px;color:var(--text);font-variant-numeric:tabular-nums}
.chip{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:20px;background:rgba(34,197,94,.15);color:#4ade80;font-size:13px;border:1px solid rgba(34,197,94,.35)}
.chip.warn{background:rgba(245,158,11,.15);color:#fbbf24;border-color:rgba(245,158,11,.35)}
.chip .dot{width:8px;height:8px;border-radius:50%;background:currentColor;animation:pulse 1.6s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
/* 主体 */
.main{flex:1;display:flex;min-height:0}
/* 左侧品种树 */
.sidebar{width:248px;min-width:248px;background:var(--panel);border-right:1px solid var(--border2);display:flex;flex-direction:column;transition:width .25s ease,min-width .25s ease,opacity .2s ease;position:relative}
.sidebar.collapsed{width:0;min-width:0;opacity:0;overflow:hidden;border-right-color:transparent}
.sidebar-toggle{position:absolute;right:-14px;top:12px;width:28px;height:28px;border-radius:50%;background:var(--panel);border:1px solid var(--border2);color:var(--text);display:flex;align-items:center;justify-content:center;cursor:pointer;z-index:100;box-shadow:0 2px 8px rgba(0,0,0,.35);font-size:16px;line-height:1;transition:transform .25s ease,background .2s}
.sidebar-toggle:hover{background:var(--surface2);color:var(--accent)}
.sidebar.collapsed .sidebar-toggle{transform:rotate(180deg);right:-14px}
.side-head{padding:10px 12px;font-size:15px;font-weight:700;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px}
.side-search{padding:8px 10px;border-bottom:1px solid var(--border)}
.side-search input{width:100%;padding:8px 10px;border-radius:6px;border:1px solid var(--border2);background:#0b1422;color:var(--text);font-size:14px}
.tree{flex:1;overflow-y:auto;padding:4px 0}
.tree::-webkit-scrollbar{width:9px}
.tree::-webkit-scrollbar-thumb{background:#27395c;border-radius:5px}
.grp>.ghead{display:flex;align-items:center;gap:6px;padding:9px 12px;font-size:15px;font-weight:700;color:var(--accent);cursor:pointer;user-select:none}
.grp>.ghead .arrow{transition:.2s;font-size:12px}
.grp.collapsed>.ghead .arrow{transform:rotate(-90deg)}
.grp>.gbody{display:block}
.grp.collapsed>.gbody{display:none}
.sub{margin-left:10px;border-left:1px solid var(--border)}
.subhead{padding:7px 12px;font-size:14px;color:var(--text);cursor:pointer;display:flex;align-items:center;gap:6px}
.subhead .arrow{font-size:10px;color:var(--muted)}
.sub.collapsed .subhead .arrow{transform:rotate(-90deg)}
.sub .leafs{display:block}
.sub.collapsed .leafs{display:none}
.leaf{padding:6px 14px;font-size:13px;color:var(--muted);cursor:pointer;display:flex;justify-content:space-between;gap:6px}
.leaf:hover{background:var(--panel2);color:var(--text)}
.leaf .sym{color:var(--accent);font-size:12px}
.leaf.pro{color:#6b7a99}
.leaf.pro .sym{color:#6b7a99}
/* 主区 */
.content{flex:1;display:flex;flex-direction:column;min-width:0}
.tabs{display:flex;gap:2px;background:var(--panel);border-bottom:1px solid var(--border2);padding:0 8px;flex:none}
.tab{padding:12px 22px;font-size:16px;font-weight:700;color:var(--muted);border-bottom:3px solid transparent;cursor:pointer;background:none;border-top:none;border-left:none;border-right:none}
.tab:hover{color:var(--text)}
.tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.frame-wrap{flex:1;position:relative;min-height:0}
.frame-wrap iframe{width:100%;height:100%;border:0;background:var(--bg)}
.trade-pane{flex:1;overflow:auto;padding:24px;display:none}
.trade-pane.show{display:block}
/* 交易占位 */
.trade-card{max-width:560px;margin:0 auto;background:var(--panel);border:1px solid var(--border2);border-radius:12px;padding:24px}
.trade-card h3{font-size:19px;margin-bottom:6px}
.trade-card .hint{color:var(--muted);font-size:14px;margin-bottom:18px}
.fld{display:flex;align-items:center;gap:12px;margin-bottom:14px}
.fld label{width:84px;color:var(--muted);font-size:15px}
.fld input,.fld select{flex:1;padding:10px 12px;border-radius:6px;border:1px solid var(--border2);background:#0b1422;color:var(--text);font-size:15px}
.seg{display:flex;gap:8px}
.seg button{flex:1;padding:10px;border-radius:6px;border:1px solid var(--border2);background:#0b1422;color:var(--muted);font-size:15px;font-weight:700}
.seg button.on{background:var(--accent);color:#04121f;border-color:var(--accent)}
.trade-submit{width:100%;margin-top:8px;padding:13px;border-radius:8px;border:none;background:linear-gradient(90deg,var(--accent),var(--accent2));color:#04121f;font-size:17px;font-weight:800}
.trade-note{margin-top:14px;padding:12px;border-radius:8px;background:rgba(245,158,11,.12);color:#fbbf24;font-size:14px;line-height:1.6}
@media(max-width:1024px){.sidebar{width:170px;min-width:170px}.idx-bar{gap:10px}.idx{min-width:64px}}
@media(max-width:680px){.sidebar{display:none}.topbar{flex-wrap:wrap;height:auto;padding:8px}.idx-bar{order:3;width:100%;flex-wrap:wrap}.tab{padding:10px 12px;font-size:14px}}
</style></head><body>
<div class="topbar">
  <div class="logo">行情终端<small>WH 风格工作台</small></div>
  <div class="idx-bar" id="idx-bar">
    <div class="idx"><span class="nm">上证指数</span><span class="vl flat" id="i-sh">--</span></div>
    <div class="idx"><span class="nm">深证成指</span><span class="vl flat" id="i-sz">--</span></div>
    <div class="idx"><span class="nm">文华商品</span><span class="vl flat" id="i-wh">--</span></div>
    <div class="idx"><span class="nm">沪深300</span><span class="vl flat" id="i-hs">--</span></div>
  </div>
  <div class="top-right">
    <span class="clock" id="clock">--:--:--</span>
    <span class="chip" id="conn-chip"><span class="dot"></span><span id="conn-text">已连接</span></span>
  </div>
</div>
<div class="main">
  <div class="sidebar" id="sidebar">
    <div class="sidebar-toggle" id="sidebar-toggle" title="收起/展开左侧栏" onclick="toggleSidebar()">◀</div>
    <div class="side-head">📚 品种树</div>
    <div class="side-search"><input id="tree-search" placeholder="搜索品种 / 代码" oninput="filterTree()"></div>
    <div class="tree" id="tree"></div>
  </div>
  <div class="content">
    <div class="tabs">
      <button class="tab active" data-tab="quote" onclick="switchTab('quote')">行情报价</button>
      <button class="tab" data-tab="kline" onclick="switchTab('kline')">K线图</button>
      <button class="tab" data-tab="option" onclick="switchTab('option')">期权T型</button>
      <button class="tab" data-tab="trade" onclick="switchTab('trade')">交易</button>
    </div>
    <div class="frame-wrap">
      <iframe id="frame" src="/?embed=1"></iframe>
      <div class="trade-pane" id="trade-pane">
        <div class="trade-card">
          <h3>📈 交易终端（占位）</h3>
          <div class="hint">模拟 WH6「F12 登录交易系统」的下单界面。当前系统为行情分析终端，未对接期货公司柜台（CTP / 易盛 / 金仕达等），下单按钮仅作界面演示。</div>
          <div class="fld"><label>合约</label><input id="t-sym" placeholder="如 DCE.b2601 / CFFEX.IF2609"></div>
          <div class="fld"><label>方向</label>
            <div class="seg"><button id="t-buy" class="on" onclick="seg('t-buy','t-sell')">买</button><button id="t-sell" onclick="seg('t-sell','t-buy')">卖</button></div>
          </div>
          <div class="fld"><label>开平</label>
            <div class="seg"><button id="t-open" class="on" onclick="seg('t-open','t-close')">开仓</button><button id="t-close" onclick="seg('t-close','t-open')">平仓</button></div>
          </div>
          <div class="fld"><label>价格</label><input id="t-price" type="number" placeholder="限价"></div>
          <div class="fld"><label>手数</label><input id="t-vol" type="number" value="1" min="1"></div>
          <button class="trade-submit" onclick="doTrade()">下 单</button>
          <div class="trade-note">⚠️ 提示：交易柜台未接入。如需实盘/模拟交易，需后续对接期货公司 CTP 接口（通常为付费/授权服务）。本按钮不会发送任何真实委托。</div>
        </div>
      </div>
    </div>
  </div>
</div>
<script>
/* 品种树数据：期货/期权 走天勤(可点击加载)，股票/外盘标注"需专业版" */
var TREE=[
 {name:'期货',subs:[
   {name:'股指期货',leafs:[{s:'CFFEX.IF2609',t:'沪深300股指'},{s:'CFFEX.IH2609',t:'上证50股指'},{s:'CFFEX.IC2609',t:'中证500股指'}]},
   {name:'国债期货',leafs:[{s:'CFFEX.TF2609',t:'5年国债'},{s:'CFFEX.T2609',t:'10年国债'}]},
   {name:'贵金属',leafs:[{s:'SHFE.au2512',t:'沪金'},{s:'SHFE.ag2512',t:'沪银'}]},
   {name:'有色金属',leafs:[{s:'SHFE.cu2510',t:'沪铜'},{s:'SHFE.al2510',t:'沪铝'},{s:'SHFE.zn2510',t:'沪锌'},{s:'SHFE.ni2510',t:'沪镍'}]},
   {name:'黑色金属',leafs:[{s:'SHFE.rb2510',t:'螺纹钢'},{s:'DCE.i2601',t:'铁矿石'},{s:'SHFE.hc2510',t:'热卷'}]},
   {name:'煤炭',leafs:[{s:'CZCE.ZC509',t:'动力煤'},{s:'DCE.jm2601',t:'焦煤'},{s:'DCE.j2601',t:'焦炭'}]},
   {name:'化工',leafs:[{s:'DCE.pvc2601',t:'PVC'},{s:'CZCE.TA509',t:'PTA'},{s:'CZCE.MA509',t:'甲醇'},{s:'SHFE.ru2601',t:'橡胶'}]},
   {name:'油脂油料',leafs:[{s:'DCE.m2601',t:'豆粕'},{s:'DCE.y2601',t:'豆油'},{s:'CZCE.OI509',t:'菜油'},{s:'DCE.p2601',t:'棕榈'}]},
   {name:'谷物',leafs:[{s:'CZCE.WH509',t:'强麦'},{s:'DCE.c2601',t:'玉米'}]},
   {name:'软商品',leafs:[{s:'CZCE.SR509',t:'白糖'},{s:'CZCE.CF509',t:'棉花'}]},
   {name:'原油',leafs:[{s:'INE.sc2601',t:'上海原油'}]}
 ]},
 {name:'期权',subs:[
   {name:'商品期权',leafs:[{s:'DCE.bz2610',t:'豆二期权'},{s:'DCE.m_o2601',t:'豆粕期权'},{s:'CZCE.SR_o509',t:'白糖期权'},{s:'SHFE.au_o2512',t:'沪金期权'}]},
   {name:'股指期权',leafs:[{s:'CFFEX.IO2609',t:'沪深300股指期权'},{s:'CFFEX.HO2609',t:'上证50股指期权'},{s:'CFFEX.MO2609',t:'中证1000股指期权'}]}
 ]},
 {name:'外盘(需专业版)',pro:true,subs:[
   {name:'金属/能源',pro:true,leafs:[{s:'',t:'纽约金 COMEX',pro:true},{s:'',t:'美原油 NYMEX',pro:true}]},
   {name:'农产品',pro:true,leafs:[{s:'',t:'美豆 CBOT',pro:true}]},
   {name:'指数',pro:true,leafs:[{s:'',t:'恒指',pro:true},{s:'',t:'A50',pro:true}]}
 ]},
 {name:'股票(需专业版)',pro:true,subs:[
   {name:'行业板块',pro:true,leafs:[{s:'',t:'半导体',pro:true},{s:'',t:'证券',pro:true},{s:'',t:'银行',pro:true}]},
   {name:'指数',pro:true,leafs:[{s:'',t:'上证指数',pro:true},{s:'',t:'深证成指',pro:true}]}
 ]}
];
var frame=document.getElementById('frame');
function renderTree(){
  var h='';
  TREE.forEach(function(g,gi){
    h+='<div class="grp" data-g="'+gi+'"><div class="ghead" onclick="toggleGrp('+gi+')"><span class="arrow">▼</span>'+g.name+'</div><div class="gbody">';
    (g.subs||[]).forEach(function(s,si){
      h+='<div class="sub" data-g="'+gi+'" data-s="'+si+'"><div class="subhead" onclick="toggleSub('+gi+','+si+')"><span class="arrow">▼</span>'+s.name+'</div><div class="leafs">';
      (s.leafs||[]).forEach(function(l){
        var cls=l.pro?'leaf pro':'leaf';
        h+='<div class="'+cls+'" '+(l.pro?'onclick="alert(\'该品种需天勤专业版/第三方授权，当前免费版不支持\')"':'onclick="openSym(\''+l.s+'\')"')+'><span>'+l.t+'</span>'+(l.s?'<span class="sym">'+l.s+'</span>':'<span class="sym">专业版</span>')+'</div>';
      });
      h+='</div></div>';
    });
    h+='</div></div>';
  });
  document.getElementById('tree').innerHTML=h;
}
function toggleGrp(gi){document.querySelector('.grp[data-g="'+gi+'"]').classList.toggle('collapsed');}
function toggleSub(gi,si){document.querySelector('.sub[data-g="'+gi+'"][data-s="'+si+'"]').classList.toggle('collapsed');}
function openSym(sym){switchTab('quote');frame.src='/?embed=1&symbol='+encodeURIComponent(sym);}
function filterTree(){var q=document.getElementById('tree-search').value.trim().toLowerCase();document.querySelectorAll('#tree .leaf').forEach(function(el){var txt=el.textContent.toLowerCase();el.style.display=(!q||txt.indexOf(q)>=0)?'':'none';});}
function switchTab(tab){
  document.querySelectorAll('.tab').forEach(function(b){b.classList.toggle('active',b.dataset.tab===tab);});
  if(tab==='trade'){frame.style.display='none';document.getElementById('trade-pane').classList.add('show');}
  else{frame.style.display='block';document.getElementById('trade-pane').classList.remove('show');
    if(tab==='quote')frame.src='/';
    else if(tab==='kline')frame.src='/?embed=1';
    else if(tab==='option')frame.src='/option-expiry';}
}
function seg(on,off){document.getElementById(on).classList.add('on');document.getElementById(off).classList.remove('on');}
function doTrade(){alert('交易柜台未接入：当前为行情分析终端，未对接期货公司 CTP 等柜台，无法发送真实委托。');}
function tickClock(){var d=new Date();function p(n){return n<10?'0'+n:''+n;}document.getElementById('clock').textContent=p(d.getHours())+':'+p(d.getMinutes())+':'+p(d.getSeconds());}
setInterval(tickClock,1000);tickClock();
function toggleSidebar(){
  var sb=document.getElementById('sidebar');
  var btn=document.getElementById('sidebar-toggle');
  sb.classList.toggle('collapsed');
  var collapsed=sb.classList.contains('collapsed');
  btn.textContent=collapsed?'▶':'◀';
  btn.title=collapsed?'展开左侧栏':'收起左侧栏';
  try{localStorage.setItem('sidebarCollapsed', collapsed?'1':'0');}catch(e){}
}
function restoreSidebar(){
  var sb=document.getElementById('sidebar');
  var btn=document.getElementById('sidebar-toggle');
  var v='0';
  try{v=localStorage.getItem('sidebarCollapsed')||'0';}catch(e){}
  if(v==='1'){sb.classList.add('collapsed');btn.textContent='▶';btn.title='展开左侧栏';}
}
renderTree();
restoreSidebar();
</script>
</body></html>"""



@app.route("/workbench")
def workbench():
    return WORKBENCH_HTML

if __name__ == "__main__":
    main()