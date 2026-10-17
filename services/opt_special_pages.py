# -*- coding: utf-8 -*-
"""期权模块特殊页面：期权主力合约 / 平值期权 / 条件筛选 / VIX 指数 / 标的排名。

注: 期权费率已于 2026-10-12 独立为【大模块】(services/option_rate.py, 路由 /optrate)，
    不再是期权模块的子页/底部 Tab。

实现原则：
- 不阻塞轮询线程，仅通过 api.get_quote 注册订阅 + 小窗口 sleep 等待首包。
- 不逐合约反解隐含波动率：VIX/平值页只对每个品种最近月份的平值 CALL/PUT 求解 2 次。
- 默认取每个品种最近月份标的（即期权主力月份），避免一次性加载全量合约。
"""
import html
import time
from flask import request, jsonify
from services import futures_db, option_greeks

# 由 register_routes 注入
_SLOTS = None
_DEFAULT_KEY = None
_GET_CHAIN_LIVE_GREEKS = None
_GET_SLOT = None  # 请求感知的账号解析器（来自 main.py 的 _resolve_slot）


def _get_slot():
    """解析当前请求应选用的账户 Slot。

    优先使用注入的请求感知解析器 _GET_SLOT（读取网关注入的选中账号），
    回落到模块级 _SLOTS.get(_DEFAULT_KEY)。取代各处写死的 _SLOTS.get(_DEFAULT_KEY)。
    """
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


def _fmt(v, nd=2, pct=False, with_comma=False):
    if v is None:
        return '—'
    try:
        if pct:
            return f'{v:.2f}%'
        if with_comma:
            return f'{v:,.{nd}f}'
        return f'{v:.{nd}f}'
    except Exception:
        return '—'


def _safe_num(v):
    return v if (v is not None and isinstance(v, (int, float)) and v == v) else None


def _q(v):
    """与 main.py 的 _q 语义一致：0 是有效值，nan/None 才当缺失。"""
    if v is None:
        return None
    try:
        if isinstance(v, float) and v != v:
            return None
        return float(v)
    except Exception:
        return None


def _base_of(q):
    """涨跌幅基准：昨结算优先(期货/期权行业惯例,与文华WH6一致)，无则回退昨收。"""
    ps = _safe_num(getattr(q, "pre_settlement", 0) or 0)
    if ps and ps > 0:
        return ps
    return _safe_num(getattr(q, "pre_close", 0) or 0)


def _quote_chg_pct(lp, pre):
    if lp is None or pre in (None, 0):
        return None, None
    chg = lp - pre
    return chg, chg / pre * 100.0


def _batch_quote_map(symbols, max_wait=1.0, require_last=True):
    """批量订阅并读取行情；返回 symbol -> {last,pre_close,chg,pct,volume,oi,bid,ask}。
    require_last=False 时，只要合约有成交量/持仓量/盘口等任一行情字段即返回（用于盘后仍有持仓的主力筛选）。
    """
    if not _SLOTS or not _DEFAULT_KEY:
        if _GET_SLOT is None:
            return {}
    slot = _get_slot()
    if slot is None or slot.status != 'online' or slot.client is None or slot.client.api is None:
        return {}
    seen = []
    seen_set = set()
    for s in symbols:
        if not s or s in seen_set:
            continue
        seen_set.add(s)
        seen.append(s)
    slot.ensure_subscribed(seen)
    out = {}
    deadline = time.time() + max_wait
    pending = set(seen)
    while pending and time.time() < deadline:
        for s in list(pending):
            try:
                q = slot.client.get_quote(s)
                r = _read_opt_full(q)
                # 至少要有最新价（含回退）或成交量/持仓量，才认为行情到达
                has_data = (r.get('last') is not None or r.get('volume') is not None
                            or r.get('oi') is not None or r.get('bid') is not None
                            or r.get('ask') is not None)
                if require_last and r.get('last') is None:
                    has_data = False
                if has_data:
                    lp = r.get('last')
                    pre = r.get('pre_settlement') or r.get('pre_close')
                    chg, pct = _quote_chg_pct(lp, pre)
                    out[s] = {
                        'last': lp, 'pre_close': pre, 'chg': chg, 'pct': pct,
                        'volume': r.get('volume'), 'oi': r.get('oi'),
                        'bid': r.get('bid'), 'bid_v': r.get('bid_v'),
                        'ask': r.get('ask'), 'ask_v': r.get('ask_v'),
                        'amount': r.get('amount'),
                        'open': r.get('open'), 'high': r.get('high'), 'low': r.get('low'),
                        'pre_settlement': r.get('pre_settlement'),
                        'volume_multiple': r.get('volume_multiple'),
                    }
                    pending.discard(s)
            except Exception:
                pass
        if pending:
            time.sleep(0.05)
    return out


def _read_opt_full(q):
    """读取期权合约实时行情，字段与 main.py _read_opt 对齐。"""
    raw_last = _safe_num(q.last_price)
    disp_last = raw_last
    if disp_last is None:
        ps = _safe_num(getattr(q, "pre_settlement", None))
        if ps is not None:
            disp_last = ps
        else:
            b = _safe_num(q.bid_price1); a = _safe_num(q.ask_price1)
            if b is not None and a is not None and b > 0 and a > 0:
                disp_last = (b + a) / 2.0
            else:
                disp_last = _safe_num(q.pre_close)
    return {
        "last": disp_last,
        "last_raw": raw_last,
        "pre_close": _safe_num(q.pre_close),
        "pre_settlement": _safe_num(getattr(q, "pre_settlement", None)),
        "open": _safe_num(q.open), "high": _safe_num(q.highest), "low": _safe_num(q.lowest),
        "average": _safe_num(q.average), "amount": _safe_num(q.amount),
        "bid": _q(q.bid_price1), "bid_v": _safe_num(q.bid_volume1),
        "ask": _q(q.ask_price1), "ask_v": _safe_num(q.ask_volume1),
        "volume": _safe_num(q.volume), "oi": _safe_num(q.open_interest),
        "pre_oi": _safe_num(q.pre_open_interest),
        "volume_multiple": _safe_num(q.volume_multiple),
        "expire_rest_days": _safe_num(q.expire_rest_days),
    }


def _wait_quotes(symbols, max_wait=1.5):
    """针对特定合约等待行情到达（用于补抓平值 CALL/PUT）。"""
    if not _SLOTS or not _DEFAULT_KEY:
        if _GET_SLOT is None:
            return {}
    slot = _get_slot()
    if slot is None or slot.status != 'online' or slot.client is None or slot.client.api is None:
        return {}
    api = slot.client.api
    seen = [s for s in symbols if s]
    slot.ensure_subscribed(seen)
    out = {}
    deadline = time.time() + max_wait
    pending = set(seen)
    while pending and time.time() < deadline:
        for s in list(pending):
            try:
                q = slot.client.get_quote(s)
                lp = _safe_num(q.last_price)
                if lp is not None:
                    pre = _base_of(q)
                    chg, pct = _quote_chg_pct(lp, pre)
                    out[s] = {'last': lp, 'pre_close': pre, 'chg': chg, 'pct': pct}
                    pending.discard(s)
            except Exception:
                pass
        if pending:
            time.sleep(0.05)
    return out


def _rest_days(ed):
    if ed is None:
        return None
    try:
        import datetime as _dt
        if isinstance(ed, str):
            ed = _dt.datetime.strptime(ed[:10], "%Y-%m-%d").date()
        elif isinstance(ed, _dt.datetime):
            ed = ed.date()
        return (ed - _dt.date.today()).days
    except Exception:
        return None


def _fetch_underlying_hv(underlying, slot, max_wait=3.0):
    """拉取标的过去25日日线，计算20日年化历史波动率。"""
    try:
        df = slot.get_kline(underlying, duration=86400, count=25)
        if df is None or df.empty or len(df) < 21:
            return None
        closes = df["close"].tolist()
        return option_greeks.compute_historical_volatility(closes, window=20)
    except Exception as e:
        print(f"[HV] {underlying} 计算失败: {e}")
        return None


def _fetch_option_volume_ratio(symbol, slot, max_wait=3.0):
    """拉取期权过去6日日线成交量，计算量比 = 今日成交量 / 过去5日平均成交量。"""
    try:
        df = slot.get_kline(symbol, duration=86400, count=6)
        if df is None or df.empty or len(df) < 2:
            return None
        volumes = df["volume"].tolist()
        if len(volumes) < 2:
            return None
        today = volumes[-1]
        past = volumes[:-1]
        if not past or all(v == 0 for v in past):
            return None
        return option_greeks.compute_volume_ratio(today, past)
    except Exception as e:
        print(f"[量比] {symbol} 计算失败: {e}")
        return None


def _select_main_options(chain, qmap, S=None):
    """从期权链中按成交量×2 + 持仓量 选出主力认购和主力认沽；
    分数相同时优先选择最接近平值（|S-K|最小）的行权价。
    qmap: symbol -> _read_opt_full 风格的行情 dict；S: 标的最新价（可选）。
    返回 (call_entry, put_entry)，每个元素为 dict 或 None。
    """
    if not chain or not chain.get("strikes"):
        return None, None
    def score(sym):
        q = qmap.get(sym) or {}
        return (q.get("volume") or 0) * 2 + (q.get("oi") or 0)
    def better(s, d, best_s, best_d):
        """s=分数, d=距标的价格距离；分数更高优先，分数相同则距离更小优先。"""
        if s != best_s:
            return s > best_s
        if d is None:
            return False
        if best_d is None:
            return True
        return d < best_d
    best_call = None
    best_put = None
    best_c_score = -1.0
    best_p_score = -1.0
    best_c_dist = None
    best_p_dist = None
    for st in chain["strikes"]:
        k = st.get("strike")
        dist = abs(float(k) - float(S)) if (S is not None and k is not None) else None
        c = st.get("call")
        if c and c.get("symbol"):
            s = score(c["symbol"])
            if better(s, dist, best_c_score, best_c_dist):
                best_c_score = s
                best_c_dist = dist
                best_call = {"strike": k, **c}
        p = st.get("put")
        if p and p.get("symbol"):
            s = score(p["symbol"])
            if better(s, dist, best_p_score, best_p_dist):
                best_p_score = s
                best_p_dist = dist
                best_put = {"strike": k, **p}
    return best_call, best_put


def _list_option_products():
    """每个期权品种取最近月份标的（主力月份）。"""
    rows = []
    try:
        tree = futures_db.list_option_product_tree()
    except Exception as e:
        print(f'[期权产品列表] 查询失败: {e}')
        return rows
    for ex, ex_name, prods in tree:
        for pk, pname, items in prods:
            if not items:
                continue
            items_sorted = sorted(items, key=lambda x: x[1])
            u, ed = items_sorted[0]
            rows.append({
                'exchange': ex, 'ex_name': ex_name or ex,
                'prod_key': pk, 'prod_name': pname,
                'underlying': u, 'expiry': ed,
                'month_count': len(items),
            })
    return rows


_OPT_SPECIAL_STYLE = '''<style>
:root{--bg:#06080f;--surface:#0b1120;--surface2:#141c2e;--border:#222d42;--text:#e8eef7;--muted:#8595ac;--accent:#2dd4ff;--violet:#a855f7;--up:#ff4d4f;--down:#19c37d;--gold:#f7b500}
html,body{margin:0;font-family:'Microsoft YaHei','PingFang SC',sans-serif;background:var(--bg);color:var(--text);font-size:14px}
.opt-page{min-height:100vh;padding:12px 14px 110px}
.page-title{color:var(--gold);font-size:22px;font-weight:800;margin-bottom:4px}
.page-sub{color:var(--muted);font-size:13px;margin-bottom:14px}
.opt-table{width:100%;border-collapse:collapse;background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden}
.opt-table th,.opt-table td{padding:8px 10px;border-bottom:1px solid #1a2435;text-align:center}
.opt-table th{background:#0f172a;color:var(--accent);font-size:13px;position:sticky;top:0;z-index:10}
.opt-table tr:hover{background:#16223a}
    .opt-table td{font-size:13px;white-space:nowrap}
    .opt-table th{white-space:nowrap}
    .opt-table .left{text-align:left}
    .opt-table .right{text-align:right}
    .table-wrap{overflow-x:auto;width:100%;border-radius:8px}
    .main-opt-table{min-width:1600px}
.up{color:var(--up)} .down{color:var(--down)}
.btn{display:inline-block;padding:4px 10px;background:linear-gradient(90deg,var(--accent),var(--violet));color:#05101c;border-radius:4px;font-size:12px;font-weight:700;text-decoration:none;cursor:pointer}
.btn:hover{opacity:.9}
.note{color:var(--muted);font-size:12px;margin-top:8px}
.empty{padding:30px;color:var(--muted);text-align:center;background:var(--surface);border:1px dashed var(--border);border-radius:8px}
.filter-form{display:flex;flex-wrap:wrap;gap:10px;background:var(--surface);padding:12px;border:1px solid var(--border);border-radius:8px;margin-bottom:14px;align-items:flex-end}
.filter-form label{display:flex;flex-direction:column;gap:4px;font-size:12px;color:var(--muted)}
.filter-form input,.filter-form select{padding:5px 8px;background:#0d1424;border:1px solid var(--border);color:var(--text);border-radius:4px}
.opt-tabbar{position:fixed;bottom:0;left:0;right:0;background:var(--surface);border-top:1px solid var(--border);display:flex;gap:2px;padding:6px;overflow-x:auto;box-shadow:0 -4px 12px rgba(0,0,0,.35)}
.opt-tabbar a{flex:0 0 auto;padding:6px 12px;color:var(--muted);text-decoration:none;font-size:13px;border-bottom:2px solid transparent;white-space:nowrap}
.opt-tabbar a.active{color:var(--accent);border-bottom-color:var(--accent)}
.rank-section{margin-bottom:16px}
.rank-section h3{color:var(--accent);font-size:15px;margin:10px 0 6px}
.table-wrap{width:100%;overflow-x:auto;border:1px solid var(--border);border-radius:8px}
.main-opt-table{min-width:2100px;width:max-content;border:none}
.main-opt-table th,.main-opt-table td{white-space:nowrap}
.main-opt-table td.sym{max-width:220px;overflow:hidden;text-overflow:ellipsis}
.sort-tabs{display:flex;gap:4px;padding:6px 0;margin-bottom:10px;overflow-x:auto}
.sort-tabs a{flex:0 0 auto;padding:5px 10px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--muted);text-decoration:none;font-size:12px;white-space:nowrap}
.sort-tabs a.active{background:var(--accent);color:#05101c;border-color:var(--accent);font-weight:700}
</style>'''


_MINI_SCRIPT = '''<script>
function optBase(){var s='/',a='api',g='gw';var isGw=location.pathname.split(s).slice(1,3).join(s)===a+s+g;return isGw?s+a+s+g+s+'optchain':s+'optchain';}
function gotoUnderlyingOpt(u){location.href=optBase()+'/chain?underlying='+encodeURIComponent(u);}
function openKlinePopup(sym){window.open(optBase()+'/kline-popup?symbol='+encodeURIComponent(sym),'_blank','width=900,height=600');}
function favKey(){return 'stock_opt_fav_'+(location.hostname||'local');}
function loadFavList(){
  try{return JSON.parse(localStorage.getItem(favKey())||'[]');}catch(e){return [];}
}
function saveFavList(arr){try{localStorage.setItem(favKey(),JSON.stringify(arr));}catch(e){}}
function removeFav(u){
  var arr=loadFavList().filter(function(x){return x.underlying!==u;});
  saveFavList(arr); renderFavList();
}
function addFavFromInput(){
  var inp=document.getElementById('favInput'); var u=(inp.value||'').trim();
  if(!u){showFootMsg('请输入合约代码，如 SHFE.cu2609'); return;}
  var arr=loadFavList(); var exists=arr.some(function(x){return x.underlying===u;});
  if(exists){showFootMsg('该标的已在关注列表'); return;}
  arr.push({underlying:u, added:new Date().toLocaleString()});
  saveFavList(arr); inp.value=''; renderFavList();
}
function renderFavList(){
  var arr=loadFavList(); var box=document.getElementById('favListBox');
  if(!box) return;
  if(arr.length===0){ box.innerHTML='<div class="empty">暂无关注标的，可在下方添加或前往 T 型报价页收藏。</div>'; return; }
  var html='<table class="opt-table"><thead><tr><th class="left">标的合约</th><th>添加时间</th><th>操作</th></tr></thead><tbody>';
  for(var i=0;i<arr.length;i++){
    var u=arr[i].underlying; var safe=u.replace(/'/g,"\\'");
    html+='<tr><td class="left"><b>'+u+'</b></td><td>'+(arr[i].added||'-')+'</td><td><a class="btn" href="#" onclick="gotoUnderlyingOpt(\''+safe+'\');return false;">T型</a> <button type="button" class="btn" style="background:#334155" onclick="removeFav(\''+safe+'\')">删除</button></td></tr>';
  }
  html+='</tbody></table>'; box.innerHTML=html;
}
document.addEventListener('DOMContentLoaded', function(){ if(document.getElementById('favListBox')) renderFavList(); });
</script>'''


def _opt_tabbar(active):
    tabs = [
        ('期权目录', "optBase() + '/'", 'dir'),
        ('我的关注', "optBase() + '/fav'", 'fav'),
        ('期权主力合约', "optBase() + '/main'", 'main'),
        # 注: 期权费率已独立为大模块(/optrate), 不在此处作为子页入口。
        # 需求人 2026-08-24 要求先隐藏以下四个入口，页面路由保留
        # ('平值期权', "optBase() + '/atm'", 'atm'),
        # ('条件筛选', "optBase() + '/filter'", 'filter'),
        # ('期权VIX指数', "optBase() + '/vix'", 'vix'),
        # ('标的排名', "optBase() + '/rank'", 'rank'),
    ]
    parts = []
    for name, href, key in tabs:
        cls = 'active' if key == active else ''
        parts.append(f'<a class="{cls}" href="#" onclick="location.href={href};return false;">{html.escape(name)}</a>')
    return '<div class="opt-tabbar">' + ''.join(parts) + '</div>'


_SORT_TABS = [
    ('全部', ''), ('涨幅', 'pct'), ('成交量', 'volume'), ('持仓量', 'oi'),
    ('趋势度', 'trend'), ('量比', 'vr'), ('内在价值', 'intrinsic'),
    ('时间价值', 'tv'), ('溢价率', 'prem'), ('虚实度', 'moneyness'),
    ('真实杠杆率', 'real_lev'), ('隐含波动率', 'impv'), ('剩余天数', 'days'),
]
_SORT_FIELD_MAP = {k: k for _, k in _SORT_TABS}


def _sort_tabbar(sort_key):
    parts = ['<div class="sort-tabs">']
    for label, key in _SORT_TABS:
        cls = 'active' if key == sort_key else ''
        href = f"optBase() + '/main?sort={key}'" if key else "optBase() + '/main'"
        parts.append(f'<a class="{cls}" href="#" onclick="location.href={href};return false;">{html.escape(label)}</a>')
    parts.append('</div>')
    return ''.join(parts)


def _opt_page_shell(title, subtitle, body, active):
    return (_OPT_SPECIAL_STYLE + _MINI_SCRIPT +
            f'''<!DOCTYPE html><html lang='zh'><head><meta charset='UTF-8'><title>{html.escape(title)}</title></head><body>
<div class='opt-page'>
  <div class='page-title'>{html.escape(title)}</div>
  <div class='page-sub'>{html.escape(subtitle)}</div>
  {body}
</div>
{_opt_tabbar(active)}
</body></html>''')


def optchain_fav_page():
    body = (
        '<div class="filter-form">'
        '<label style="flex:1 1 240px">合约代码 '
        '<input id="favInput" type="text" placeholder="如 SHFE.cu2609" style="width:100%"></label>'
        '<button type="button" class="btn" onclick="addFavFromInput()">添加关注</button>'
        '</div>'
        '<div id="favListBox"><div class="empty">正在加载关注列表...</div></div>'
        '<p class="note">我的关注仅保存在当前浏览器本地（localStorage），换浏览器/清缓存会丢失。</p>'
    )
    return _opt_page_shell('我的关注', '本地收藏的期权标的快速入口', body, 'fav')


def optchain_main_page():
    """期权主力合约：每个品种最近月份标的中，按成交量+持仓量选出主力认购/认沽，
    展示完整行情 + Greeks + 趋势度 + 量比 + 历史波动率，对齐 WH6 主力合约列表。
    性能控制：只取最活跃的 15 个品种，每个品种仅订阅平值附近（±15%）的期权合约。
    支持通过 sort 参数按字段排序。
    """
    sort_key = (request.args.get('sort') or '').strip().lower()
    slot = _get_slot()
    if slot is None or slot.status != 'online' or slot.client is None or slot.client.api is None:
        return _opt_page_shell('期权主力合约', '行情未连接，请稍后再试',
                               '<div class="empty">未分配有效账户</div>', 'main')

    products = _list_option_products()
    # 先批量读标的行情，按标的热度排序，并过滤掉无行情的品种
    u_symbols = [p['underlying'] for p in products]
    uqmap = _batch_quote_map(u_symbols, max_wait=0.8)
    products = [p for p in products if uqmap.get(p['underlying'], {}).get('last') is not None]
    # 热度：涨跌幅绝对值优先，其次成交量
    products.sort(key=lambda p: (abs(uqmap[p['underlying']].get('pct') or 0),
                                  uqmap[p['underlying']].get('volume') or 0),
                  reverse=True)
    # 限制数量，避免一次性订阅过多期权合约导致响应过慢
    products = products[:15]

    # 收集主力候选合约 symbol：每个品种只取平值附近 ±15% 行权价，减少订阅量
    main_entries = []  # list of dict: p, S, call/put syms/meta
    all_opt_symbols = []
    for p in products:
        S = uqmap[p['underlying']]['last']
        chain = futures_db.query_option_chain(p['underlying'])
        if not chain or not chain.get('strikes'):
            continue
        low_k = S * 0.85
        high_k = S * 1.15
        call_syms = []
        put_syms = []
        call_meta = {}
        put_meta = {}
        for st in chain['strikes']:
            k = st['strike']
            if k is None or k <= 0:
                continue
            if k < low_k or k > high_k:
                continue
            if st.get('call') and st['call'].get('symbol'):
                sym = st['call']['symbol']
                call_syms.append(sym)
                call_meta[sym] = {'strike': k, 'exercise': st['call']['exercise']}
            if st.get('put') and st['put'].get('symbol'):
                sym = st['put']['symbol']
                put_syms.append(sym)
                put_meta[sym] = {'strike': k, 'exercise': st['put']['exercise']}
        # 若平值附近无合约，放宽到全部行权价（异常保守）
        if not call_syms and not put_syms:
            for st in chain['strikes']:
                k = st['strike']
                if st.get('call') and st['call'].get('symbol'):
                    sym = st['call']['symbol']
                    call_syms.append(sym)
                    call_meta[sym] = {'strike': k, 'exercise': st['call']['exercise']}
                if st.get('put') and st['put'].get('symbol'):
                    sym = st['put']['symbol']
                    put_syms.append(sym)
                    put_meta[sym] = {'strike': k, 'exercise': st['put']['exercise']}
        all_opt_symbols.extend(call_syms)
        all_opt_symbols.extend(put_syms)
        main_entries.append({
            'p': p, 'S': S,
            'call_syms': call_syms, 'put_syms': put_syms,
            'call_meta': call_meta, 'put_meta': put_meta,
        })

    # 批量订阅候选期权（去重），等待首包
    opt_qmap = _batch_quote_map(list(set(all_opt_symbols)), max_wait=3.0, require_last=False)

    # 为每个品种选出主力 call/put
    r = option_greeks.RISK_FREE_RATE
    selected = []
    for e in main_entries:
        strikes_map = {}
        for s in e['call_syms']:
            meta = e['call_meta'][s]
            strikes_map.setdefault(meta['strike'], {})['call'] = {'symbol': s, 'exercise': meta['exercise']}
            strikes_map[meta['strike']].setdefault('strike', meta['strike'])
        for s in e['put_syms']:
            meta = e['put_meta'][s]
            strikes_map.setdefault(meta['strike'], {})['put'] = {'symbol': s, 'exercise': meta['exercise']}
            strikes_map[meta['strike']].setdefault('strike', meta['strike'])
        fake_chain = {'strikes': list(strikes_map.values())}
        c_main, p_main = _select_main_options(fake_chain, opt_qmap, S=e['S'])
        if c_main:
            selected.append({'p': e['p'], 'S': e['S'], 'entry': c_main, 'side': 'CALL'})
        if p_main:
            selected.append({'p': e['p'], 'S': e['S'], 'entry': p_main, 'side': 'PUT'})

    # 历史波动率/量比：天勤历史K线拉取较耗时，本页改为异步加载（见页面JS），
    # 首次渲染时显示 “—”，避免阻塞页面超过数秒。
    for e in selected:
        e['hv'] = None
        e['volume_ratio'] = None

    # 计算完整字段并渲染
    rows = []
    for idx, e in enumerate(selected, start=1):
        p = e['p']
        S = e['S']
        entry = e['entry']
        sym = entry['symbol']
        K = entry['strike']
        cls = e['side']
        q = opt_qmap.get(sym) or {}
        ex_date = entry.get('exercise')
        t = option_greeks.annualized_t(ex_date) if ex_date else None

        opt_price = q.get('last')
        _base = q.get('pre_settlement') or q.get('pre_close') or 0
        chg = (opt_price - _base) if (opt_price is not None and _base not in (None, 0)) else None
        pct = (chg / _base * 100.0) if (chg is not None and _base not in (None, 0)) else None
        g = option_greeks.compute_greeks(S, K, r, option_greeks.DEFAULT_VOL, t, cls) if (S is not None and t and t > 0) else None
        bs = option_greeks.compute_bs_value(S, K, r, option_greeks.DEFAULT_VOL, t, cls) if (S is not None and t and t > 0) else None
        intrinsic = option_greeks.compute_intrinsic_value(S, K, cls)
        tv = option_greeks.compute_time_value(opt_price, intrinsic) if opt_price is not None else None
        lev = option_greeks.compute_leverage(S, opt_price) if opt_price else None
        real_lev = option_greeks.compute_real_leverage(lev, g['delta'] if g else None)
        prem = option_greeks.compute_premium_rate(S, opt_price, K, cls) if (opt_price is not None and K is not None) else None
        moneyness = option_greeks.compute_moneyness_value(S, K, cls) if S is not None else None
        impv = option_greeks.compute_impv(S, opt_price, K, r, t, cls) if (S is not None and opt_price is not None and opt_price > 0 and t and t > 0) else None
        trend = option_greeks.compute_trend_degree(q.get('last'), q.get('open'), q.get('high'), q.get('low'))
        hv = e.get('hv')
        vr = e.get('volume_ratio')

        und_name = futures_db.option_underlying_name(p['underlying'])
        disp = futures_db.option_symbol_display(sym, und_name)
        exch = p['exchange']
        exercise_type = '欧式' if exch in ('CFFEX', 'SSE', 'SZSE') else '美式'

        trend_cls = 'up' if (chg is not None and chg >= 0) else 'down'
        safe_sym = html.escape(sym).replace("'", "\\'")
        # 根据 sort 参数提取排序值（None 放最后）
        sort_val = None
        if sort_key == 'pct': sort_val = pct
        elif sort_key == 'volume': sort_val = q.get('volume')
        elif sort_key == 'oi': sort_val = q.get('oi')
        elif sort_key == 'trend': sort_val = trend
        elif sort_key == 'vr': sort_val = vr
        elif sort_key == 'intrinsic': sort_val = intrinsic
        elif sort_key == 'tv': sort_val = tv
        elif sort_key == 'prem': sort_val = prem
        elif sort_key == 'moneyness': sort_val = moneyness
        elif sort_key == 'real_lev': sort_val = real_lev
        elif sort_key == 'impv': sort_val = impv
        elif sort_key == 'days': sort_val = _rest_days(ex_date)
        rows.append((sort_val, f'<tr ondblclick="openKlinePopup(\'{safe_sym}\')">'
            f'<td>{idx}</td>'
            f'<td class="left sym" title="{html.escape(sym)}">{html.escape(disp)}</td>'
            f'<td class="{trend_cls} last">{_fmt(opt_price)}</td>'
            f'<td class="{trend_cls}">{_fmt(chg)}</td>'
            f'<td class="{trend_cls}">{_fmt(pct, 2, True)}</td>'
            f'<td>{_fmt(q.get("volume"), 0, False, True)}</td>'
            f'<td>{_fmt(q.get("amount"), 0, False, True)}</td>'
            f'<td>{_fmt(q.get("oi"), 0, False, True)}</td>'
            f'<td>{_fmt(q.get("bid"))}</td>'
            f'<td>{_fmt(q.get("bid_v"), 0)}</td>'
            f'<td>{_fmt(q.get("ask"))}</td>'
            f'<td>{_fmt(q.get("ask_v"), 0)}</td>'
            f'<td>{_fmt(trend, 2)}</td>'
            f'<td data-vr-sym="{html.escape(sym)}">{_fmt(vr, 2)}</td>'
            f'<td>{_fmt(intrinsic)}</td>'
            f'<td>{_fmt(tv)}</td>'
            f'<td>{_fmt(prem, 2, True)}</td>'
            f'<td>{_fmt(moneyness, 2)}</td>'
            f'<td>{_fmt(lev, 1)}</td>'
            f'<td>{_fmt(real_lev, 1)}</td>'
            f'<td>{_fmt(impv, 2, True) if impv is not None else _fmt(impv)}</td>'
            f'<td data-hv-u="{html.escape(p["underlying"])}">{_fmt(hv, 2, True) if hv is not None else _fmt(hv)}</td>'
            f'<td>{_fmt(bs)}</td>'
            f'<td>{_fmt(g["delta"] if g else None, 4)}</td>'
            f'<td>{_fmt(g["gamma"] if g else None, 4)}</td>'
            f'<td>{_fmt(g["theta"] if g else None, 4)}</td>'
            f'<td>{_fmt(g["vega"] if g else None, 4)}</td>'
            f'<td>{_fmt(g["rho"] if g else None, 4)}</td>'
            f'<td>{_fmt(q.get("volume_multiple"), 0)}</td>'
            f'<td>{_fmt(K)}</td>'
            f'<td>{html.escape(str(ex_date)[:10] if ex_date else "—")}</td>'
            f'<td>{_fmt(_rest_days(ex_date), 0)}</td>'
            f'<td>{html.escape(exercise_type)}</td>'
            f'</tr>'))

    if sort_key and sort_key in _SORT_FIELD_MAP:
        rows.sort(key=lambda x: _safe_num(x[0]) if _safe_num(x[0]) is not None else float('-inf'), reverse=True)
    rows = [r[1] for r in rows]

    if rows:
        # 收集需要异步补数据的标的/合约
        async_underlyings = ','.join(html.escape(e['p']['underlying']) for e in selected)
        async_symbols = ','.join(html.escape(e['entry']['symbol']) for e in selected)
        async_script = f'''<script>
(function(){{
  var us="{async_underlyings}".split(',').filter(function(x){{return x;}});
  var ss="{async_symbols}".split(',').filter(function(x){{return x;}});
  if(!us.length && !ss.length) return;
  var url=apiBase()+'/api/options/main_extras?underlyings='+encodeURIComponent(us.join(','))+'&symbols='+encodeURIComponent(ss.join(','));
  fetch(url).then(function(r){{return r.json();}}).then(function(j){{
    if(!j || !j.ok) return;
    var hv=j.hv || {{}}, vr=j.ratio || {{}};
    document.querySelectorAll('td[data-hv-u]').forEach(function(td){{
      var v=hv[td.getAttribute('data-hv-u')];
      if(v!==undefined && v!==null) td.textContent=(v*100).toFixed(2)+'%';
    }});
    document.querySelectorAll('td[data-vr-sym]').forEach(function(td){{
      var v=vr[td.getAttribute('data-vr-sym')];
      if(v!==undefined && v!==null) td.textContent=v.toFixed(2);
    }});
  }}).catch(function(e){{ if(window.console) console.warn('[main_extras]', e); }});
}})();
</script>'''
        body = (
            _sort_tabbar(sort_key)
            + '<div class="table-wrap">'
            + '<table class="opt-table main-opt-table"><thead><tr>'
            '<th>序号</th><th class="left">合约名称</th><th>最新</th><th>涨跌</th><th>涨幅%</th>'
            '<th>成交量</th><th>成交额</th><th>持仓量</th><th>买价</th><th>买量</th><th>卖价</th><th>卖量</th><th>趋势度</th><th>量比</th>'
            '<th>内在价值</th><th>时间价值</th><th>溢价率</th><th>虚实度</th><th>杠杆比例</th><th>真实杠杆率</th>'
            '<th>隐含波动率</th><th>历史波动率</th><th>理论价</th>'
            '<th>Delta</th><th>Gamma</th><th>Theta</th><th>Vega</th><th>Rho</th>'
            '<th>行权比例</th><th>行权价</th><th>到期日</th><th>剩余天数</th><th>期权类型</th>'
            '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table></div>'
            + async_script
            + '<p class="note">主力合约：每个品种最近月份标的中，按“成交量×2 + 持仓量”在平值附近（±15%）选出主力认购/认沽各1个。'
            '趋势度 = (最新-开盘)/(最高-最低)；量比与历史波动率页面加载后异步从天勤历史K线补全，首次可能短暂显示 “—”。'
            '新增字段买量/卖量/成交额来自 TqSdk 实时行情，横向滚动条随列数自动出现。</p>'
        )
    else:
        body = '<div class="empty">暂无期权主力合约数据</div>'
    return _opt_page_shell('期权主力合约', '按品种最近月份选出主力认购/认沽合约', body, 'main')


def optchain_atm_page():
    products = _list_option_products()
    symbols = [p['underlying'] for p in products]
    uqmap = _batch_quote_map(symbols, max_wait=0.8)
    # 优先展示有实时行情的品种，限制数量以保证订阅/读取在秒内完成
    products = [p for p in products if uqmap.get(p['underlying'], {}).get('last') is not None]
    products.sort(key=lambda p: abs(uqmap[p['underlying']].get('pct') or 0), reverse=True)
    products = products[:20]

    # 每品种只取平值 CALL/PUT（2 个合约），避免整链冷订阅（0.8s 等不到首包）
    atm_entries = []
    opt_symbols = []
    for p in products:
        S = uqmap.get(p['underlying'], {}).get('last')
        if S is None:
            continue
        chain = futures_db.query_option_chain(p['underlying'])
        if not chain or not chain.get('strikes'):
            continue
        best = min(chain['strikes'], key=lambda st: abs(float(st['strike']) - S))
        csym = best['call']['symbol'] if best.get('call') else None
        psym = best['put']['symbol'] if best.get('put') else None
        ex = (best['call']['exercise'] if best.get('call')
              else (best['put']['exercise'] if best.get('put') else chain['exercise_dates'][0]))
        atm_entries.append({'p': p, 'S': S, 'K': best['strike'], 'exercise': ex,
                            'csym': csym, 'psym': psym})
        if csym:
            opt_symbols.append(csym)
        if psym:
            opt_symbols.append(psym)

    # 只订阅平值合约：冷数据 3s 足够首包；已预热的热数据毫秒级
    opt_qmap = _batch_quote_map(opt_symbols, max_wait=3.0)
    r = option_greeks.RISK_FREE_RATE
    rows = []
    for e in atm_entries:
        S, K, ex = e['S'], e['K'], e['exercise']
        t = option_greeks.annualized_t(ex)
        for side, key in (('call', 'csym'), ('put', 'psym')):
            sym = e[key]
            q = opt_qmap.get(sym, {})
            last = q.get('last')
            iv = None
            if last is not None and t > 0:
                iv = option_greeks.compute_impv(S, last, K, r, t, side.upper())
            e[f'{side}_last'] = last
            e[f'{side}_chg'] = q.get('chg')
            e[f'{side}_iv'] = iv
        p = e['p']
        safe_u = html.escape(p['underlying']).replace("'", "\\'")
        trend_c = 'up' if (e['call_chg'] is not None and e['call_chg'] >= 0) else 'down'
        trend_p = 'up' if (e['put_chg'] is not None and e['put_chg'] >= 0) else 'down'
        rows.append(
            f'<tr>'
            f'<td class="left">{html.escape(p["ex_name"])}</td>'
            f'<td class="left">{html.escape(p["prod_name"])}</td>'
            f'<td class="left">{html.escape(p["underlying"])}</td>'
            f'<td>{_fmt(e["S"])}</td><td>{_fmt(e["K"])}</td>'
            f'<td class="left">{html.escape(e["csym"] or "")}<br><span class="{trend_c}">{_fmt(e["call_last"])} {_fmt(e["call_chg"])}</span></td>'
            f'<td>{_fmt(e["call_iv"], 2)}</td>'
            f'<td class="left">{html.escape(e["psym"] or "")}<br><span class="{trend_p}">{_fmt(e["put_last"])} {_fmt(e["put_chg"])}</span></td>'
            f'<td>{_fmt(e["put_iv"], 2)}</td>'
            f'<td><a class="btn" href="#" onclick="gotoUnderlyingOpt(\'{safe_u}\');return false;">T型</a></td>'
            f'</tr>'
        )
    body = (
        '<table class="opt-table"><thead><tr>'
        '<th class="left">交易所</th><th class="left">品种</th><th class="left">标的</th>'
        '<th>标的价格</th><th>行权价</th><th class="left">平值认购 CALL</th><th>认购IV</th>'
        '<th class="left">平值认沽 PUT</th><th>认沽IV</th><th>操作</th>'
        '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table>'
        '<p class="note">平值期权：取最接近标的价格的行权价；IV 由 BS 反解，仅供估算。</p>'
    )
    return _opt_page_shell('平值期权', '各品种最近月份平值认购/认沽合约', body, 'atm')


def optchain_vix_page():
    products = _list_option_products()
    symbols = [p['underlying'] for p in products]
    uqmap = _batch_quote_map(symbols, max_wait=0.8)
    # 优先展示有实时行情的品种，限制数量以保证订阅/读取在秒内完成
    products = [p for p in products if uqmap.get(p['underlying'], {}).get('last') is not None]
    products.sort(key=lambda p: abs(uqmap[p['underlying']].get('pct') or 0), reverse=True)
    products = products[:20]

    atm_entries = []
    opt_symbols = []
    for p in products:
        S = uqmap.get(p['underlying'], {}).get('last')
        if S is None:
            continue
        chain = futures_db.query_option_chain(p['underlying'])
        if not chain or not chain.get('strikes'):
            continue
        best = min(chain['strikes'], key=lambda st: abs(float(st['strike']) - S))
        csym = best['call']['symbol'] if best.get('call') else None
        psym = best['put']['symbol'] if best.get('put') else None
        ex = (best['call']['exercise'] if best.get('call')
              else (best['put']['exercise'] if best.get('put') else chain['exercise_dates'][0]))
        atm_entries.append({'p': p, 'S': S, 'K': best['strike'], 'exercise': ex,
                            'csym': csym, 'psym': psym})
        if csym:
            opt_symbols.append(csym)
        if psym:
            opt_symbols.append(psym)

    # 只订阅平值合约：冷数据 3s 足够首包；IV 仅对平值 CALL/PUT 各解 1 次（性能约定）
    opt_qmap = _batch_quote_map(opt_symbols, max_wait=3.0)
    r = option_greeks.RISK_FREE_RATE
    ranked = []
    for e in atm_entries:
        S, K, ex = e['S'], e['K'], e['exercise']
        t = option_greeks.annualized_t(ex)
        ivs = []
        for side, key in (('call', 'csym'), ('put', 'psym')):
            sym = e[key]
            q = opt_qmap.get(sym, {})
            last = q.get('last')
            iv = None
            if last is not None and t > 0:
                iv = option_greeks.compute_impv(S, last, K, r, t, side.upper())
            if iv is not None and iv == iv:
                ivs.append(iv)
            e[f'{side}_iv'] = iv
        if ivs:
            ranked.append({
                **e,
                'vix': sum(ivs) / len(ivs) * 100.0,
            })
    ranked.sort(key=lambda x: x['vix'], reverse=True)

    rows = []
    for e in ranked:
        p = e['p']
        safe_u = html.escape(p['underlying']).replace("'", "\\'")
        rows.append(
            f'<tr>'
            f'<td class="left">{html.escape(p["ex_name"])}</td>'
            f'<td class="left">{html.escape(p["prod_name"])}</td>'
            f'<td class="left">{html.escape(p["underlying"])}</td>'
            f'<td>{_fmt(e["S"])}</td><td>{_fmt(e["K"])}</td>'
            f'<td><b>{_fmt(e["vix"], 2)}%</b></td>'
            f'<td>{_fmt(e.get("call_iv"), 2)}</td>'
            f'<td>{_fmt(e.get("put_iv"), 2)}</td>'
            f'<td>{html.escape(e["exercise"])}</td>'
            f'<td><a class="btn" href="#" onclick="gotoUnderlyingOpt(\'{safe_u}\');return false;">T型</a></td>'
            f'</tr>'
        )
    body = (
        '<table class="opt-table"><thead><tr>'
        '<th class="left">交易所</th><th class="left">品种</th><th class="left">标的</th>'
        '<th>标的价格</th><th>行权价</th><th>VIX 估算</th><th>认购IV</th><th>认沽IV</th><th>行权日</th><th>操作</th>'
        '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table>'
        '<p class="note">VIX 估算 = 平值 CALL/PUT 隐含波动率均值 ×100；天勤无官方 VIX 接口，数据仅供参考。</p>'
    )
    return _opt_page_shell('期权VIX指数', '各品种平值期权隐含波动率估算', body, 'vix')


def optchain_rank_page():
    products = _list_option_products()
    symbols = [p['underlying'] for p in products]
    qmap = _batch_quote_map(symbols, max_wait=0.8)
    items = []
    for p in products:
        q = qmap.get(p['underlying'], {})
        if q.get('last') is None:
            continue
        items.append({**p, **q})

    gainers = sorted(items, key=lambda x: x.get('pct') if x.get('pct') is not None else -999, reverse=True)[:20]
    losers = sorted(items, key=lambda x: x.get('pct') if x.get('pct') is not None else 999)[:20]
    volume = sorted(items, key=lambda x: x.get('volume') if x.get('volume') is not None else -1, reverse=True)[:20]

    def row(item):
        trend = 'up' if (item.get('chg') is not None and item['chg'] >= 0) else 'down'
        safe_u = html.escape(item['underlying']).replace("'", "\\'")
        return (
            f'<tr>'
            f'<td class="left">{html.escape(item["prod_name"])}</td>'
            f'<td class="left">{html.escape(item["underlying"])}</td>'
            f'<td>{_fmt(item["last"])}</td>'
            f'<td class="{trend}">{_fmt(item["chg"])}</td>'
            f'<td class="{trend}">{_fmt(item["pct"], 2, True)}</td>'
            f'<td>{_fmt(item.get("volume"), 0, False, True)}</td>'
            f'<td><a class="btn" href="#" onclick="gotoUnderlyingOpt(\'{safe_u}\');return false;">T型</a></td>'
            f'</tr>'
        )

    def table(title, data):
        if not data:
            return ''
        return (
            f'<div class="rank-section"><h3>{html.escape(title)}</h3>'
            f'<table class="opt-table"><thead><tr><th class="left">品种</th><th class="left">标的</th><th>最新价</th><th>涨跌</th><th>涨跌幅</th><th>成交量</th><th>操作</th></tr></thead>'
            f'<tbody>{"" .join(row(x) for x in data)}</tbody></table></div>'
        )

    body = (
        table('涨幅榜', gainers) + table('跌幅榜', losers) + table('成交量榜', volume) +
        '<p class="note">标的排名：按期权对应标的（期货合约）实时行情排序。</p>'
    )
    return _opt_page_shell('标的排名', '按期权标的价格涨跌/成交量排序', body, 'rank')


def optchain_filter_page():
    exchange = request.args.get('exchange', '')
    prefix = request.args.get('prefix', '').strip()
    moneyness = request.args.get('moneyness', 'all')
    kmin = request.args.get('kmin', type=float)
    kmax = request.args.get('kmax', type=float)
    emin = request.args.get('emin', '')
    emax = request.args.get('emax', '')
    apply = request.args.get('apply')

    exchanges = [
        ('', '全部'), ('SHFE', '上期所'), ('INE', '上期能源'), ('DCE', '大商所'),
        ('CZCE', '郑商所'), ('GFEX', '广期所'), ('CFFEX', '中金所'), ('SSE', '上交所'), ('SZSE', '深交所')
    ]
    money_opts = [('all', '全部'), ('itm', '实值'), ('otm', '虚值'), ('atm', '平值')]
    form = (
        '<form class="filter-form" method="get">'
        '<label>交易所<select name="exchange">' +
        ''.join(f'<option value="{html.escape(v)}" {"selected" if v == exchange else ""}>{html.escape(n)}</option>' for v, n in exchanges) +
        '</select></label>'
        '<label>标的/代码关键字<input type="text" name="prefix" placeholder="如 SHFE.cu" value="' + html.escape(prefix) + '"></label>'
        '<label>实虚值<select name="moneyness">' +
        ''.join(f'<option value="{v}" {"selected" if v == moneyness else ""}>{html.escape(n)}</option>' for v, n in money_opts) +
        '</select></label>'
        '<label>行权价≥<input type="number" step="any" name="kmin" value="' + (str(kmin) if kmin is not None else '') + '"></label>'
        '<label>行权价≤<input type="number" step="any" name="kmax" value="' + (str(kmax) if kmax is not None else '') + '"></label>'
        '<label>到期日≥<input type="date" name="emin" value="' + html.escape(emin) + '"></label>'
        '<label>到期日≤<input type="date" name="emax" value="' + html.escape(emax) + '"></label>'
        '<input type="hidden" name="apply" value="1">'
        '<button type="submit" class="btn">筛选</button>'
        '</form>'
    )

    rows_html = ''
    if apply:
        conditions = ['exercise_date >= CURDATE()']
        params = []
        if exchange:
            conditions.append('exchange = %s')
            params.append(exchange)
        if prefix:
            conditions.append('(underlying LIKE %s OR symbol LIKE %s)')
            params.extend([prefix + '%', prefix + '%'])
        if emin:
            conditions.append('exercise_date >= %s')
            params.append(emin)
        if emax:
            conditions.append('exercise_date <= %s')
            params.append(emax)
        sql = 'SELECT DISTINCT underlying FROM t_option_expiry WHERE ' + ' AND '.join(conditions) + ' ORDER BY underlying LIMIT 30'
        conn = futures_db.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                underlyings = [r[0] for r in cur.fetchall()]
        finally:
            conn.close()

        uqmap = _batch_quote_map(underlyings, max_wait=0.8)
        meta_rows = []
        opt_symbols = []
        for u in underlyings:
            S = uqmap.get(u, {}).get('last')
            chain = futures_db.query_option_chain(u)
            if not chain or not chain.get('strikes'):
                continue
            for st in chain['strikes']:
                K = float(st['strike'])
                if kmin is not None and K < kmin:
                    continue
                if kmax is not None and K > kmax:
                    continue
                for side in ('call', 'put'):
                    entry = st.get(side)
                    if not entry:
                        continue
                    if moneyness != 'all' and S is not None:
                        if side == 'call':
                            mval = S - K
                        else:
                            mval = K - S
                        if moneyness == 'itm' and mval <= 0:
                            continue
                        if moneyness == 'otm' and mval >= 0:
                            continue
                        if moneyness == 'atm' and abs(mval) > 0.005 * S + 1e-9:
                            continue
                    meta_rows.append({
                        'underlying': u, 'S': S, 'side': side, 'K': K,
                        'symbol': entry['symbol'], 'exercise': entry['exercise'],
                    })
                    opt_symbols.append(entry['symbol'])
                if len(meta_rows) >= 200:
                    break
            if len(meta_rows) >= 200:
                break

        opt_qmap = _batch_quote_map(opt_symbols, max_wait=4.0)
        r = option_greeks.RISK_FREE_RATE
        display_rows = []
        for rm in meta_rows:
            q = opt_qmap.get(rm['symbol'], {})
            last = q.get('last')
            rm['last'] = last
            rm['chg'] = q.get('chg')
            rm['pct'] = q.get('pct')
            if rm['S'] is not None:
                intrinsic = option_greeks.compute_intrinsic_value(rm['S'], rm['K'], rm['side'].upper())
                rm['intrinsic'] = intrinsic
                rm['time_value'] = option_greeks.compute_time_value(last, intrinsic) if last is not None else None
                t = option_greeks.annualized_t(rm['exercise'])
                if t > 0:
                    g = option_greeks.compute_greeks(rm['S'], rm['K'], r, option_greeks.DEFAULT_VOL, t, rm['side'].upper())
                    rm['delta'] = g['delta'] if g else None
            display_rows.append(rm)

        if display_rows:
            tbody = []
            for rm in display_rows:
                trend = 'up' if (rm.get('chg') is not None and rm['chg'] >= 0) else 'down'
                side_cn = '认购' if rm['side'] == 'call' else '认沽'
                tbody.append(
                    f'<tr>'
                    f'<td class="left">{html.escape(rm["underlying"])}</td>'
                    f'<td class="left">{html.escape(rm["symbol"])}</td>'
                    f'<td>{side_cn}</td><td>{_fmt(rm["K"])}</td>'
                    f'<td>{html.escape(rm["exercise"])}</td>'
                    f'<td>{_fmt(rm["S"])}</td>'
                    f'<td class="{trend}">{_fmt(rm.get("last"))}</td>'
                    f'<td class="{trend}">{_fmt(rm.get("chg"))}</td>'
                    f'<td class="{trend}">{_fmt(rm.get("pct"), 2, True)}</td>'
                    f'<td>{_fmt(rm.get("intrinsic"))}</td>'
                    f'<td>{_fmt(rm.get("time_value"))}</td>'
                    f'<td>{_fmt(rm.get("delta"), 2)}</td>'
                    f'</tr>'
                )
            rows_html = (
                '<table class="opt-table"><thead><tr>'
                '<th class="left">标的</th><th class="left">期权代码</th><th>方向</th><th>行权价</th><th>到期日</th>'
                '<th>标的价格</th><th>最新价</th><th>涨跌</th><th>涨跌幅</th><th>内在价值</th><th>时间价值</th><th>Delta</th>'
                '</tr></thead><tbody>' + ''.join(tbody) + '</tbody></table>'
                f'<p class="note">共 {len(display_rows)} 条（最多展示 200 条）</p>'
            )
        else:
            rows_html = '<div class="empty">无匹配结果，请放宽条件后重试。</div>'
    else:
        rows_html = '<div class="empty">请选择筛选条件后点击“筛选”。</div>'

    body = form + rows_html
    return _opt_page_shell('条件筛选', '按交易所、标的、行权价、实虚值等条件筛选期权', body, 'filter')


def api_main_extras():
    """异步补全期权主力合约页的量比/历史波动率。
    参数：underlyings=U1,U2,...&symbols=SYM1,SYM2,...
    返回：{ok:true, hv:{underlying:val}, ratio:{symbol:val}}
    """
    from flask import jsonify
    slot = _get_slot()
    if slot is None or slot.status != 'online' or slot.client is None or slot.client.api is None:
        return jsonify({'ok': False, 'msg': '行情未连接'}), 503

    us = [s.strip() for s in (request.args.get('underlyings', '') or '').split(',') if s.strip()]
    ss = [s.strip() for s in (request.args.get('symbols', '') or '').split(',') if s.strip()]

    hv = {}
    # 历史波动率：限制最多 15 个标的，避免单次请求过长
    for u in us[:15]:
        val = _fetch_underlying_hv(u, slot)
        if val is not None:
            hv[u] = val

    ratio = {}
    # 量比：限制最多 15 个合约
    for sym in ss[:15]:
        val = _fetch_option_volume_ratio(sym, slot)
        if val is not None:
            ratio[sym] = val

    return jsonify({'ok': True, 'hv': hv, 'ratio': ratio})


def register_routes(app, slots, default_key, get_chain_live_greeks=None, get_slot=None):
    global _SLOTS, _DEFAULT_KEY, _GET_CHAIN_LIVE_GREEKS, _GET_SLOT
    _SLOTS = slots
    _DEFAULT_KEY = default_key
    _GET_CHAIN_LIVE_GREEKS = get_chain_live_greeks
    _GET_SLOT = get_slot
    app.add_url_rule('/optchain/main', view_func=optchain_main_page)
    app.add_url_rule('/optchain/atm', view_func=optchain_atm_page)
    app.add_url_rule('/optchain/filter', view_func=optchain_filter_page)
    app.add_url_rule('/optchain/vix', view_func=optchain_vix_page)
    app.add_url_rule('/optchain/rank', view_func=optchain_rank_page)
    app.add_url_rule('/optchain/fav', view_func=optchain_fav_page)
    # 注: 期权费率已独立为大模块 /optrate(services/option_rate.py), 不在此注册。
    app.add_url_rule('/optchain/api/options/main_extras', view_func=api_main_extras)
    app.add_url_rule('/api/options/main_extras', view_func=api_main_extras)
