# -*- coding: utf-8 -*-
"""期权模块特殊页面：期权主力合约 / 平值期权 / 条件筛选 / VIX 指数 / 标的排名。

实现原则：
- 不阻塞轮询线程，仅通过 api.get_quote 注册订阅 + 小窗口 sleep 等待首包。
- 不逐合约反解隐含波动率：VIX/平值页只对每个品种最近月份的平值 CALL/PUT 求解 2 次。
- 默认取每个品种最近月份标的（即期权主力月份），避免一次性加载全量合约。
"""
import html
import time
from flask import request
from services import futures_db, option_greeks

# 由 register_routes 注入
_SLOTS = None
_DEFAULT_KEY = None
_GET_CHAIN_LIVE_GREEKS = None


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


def _quote_chg_pct(lp, pre):
    if lp is None or pre in (None, 0):
        return None, None
    chg = lp - pre
    return chg, chg / pre * 100.0


def _batch_quote_map(symbols, max_wait=1.0):
    """批量订阅并读取行情；返回 symbol -> {last,pre_close,chg,pct,volume,oi,bid,ask}。"""
    if not _SLOTS or not _DEFAULT_KEY:
        return {}
    slot = _SLOTS.get(_DEFAULT_KEY)
    if slot is None or slot.status != 'online' or slot.client is None or slot.client.api is None:
        return {}
    api = slot.client.api
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
                lp = _safe_num(q.last_price)
                if lp is not None:
                    pre = _safe_num(q.pre_close)
                    chg, pct = _quote_chg_pct(lp, pre)
                    out[s] = {
                        'last': lp, 'pre_close': pre, 'chg': chg, 'pct': pct,
                        'volume': _safe_num(q.volume), 'oi': _safe_num(q.open_interest),
                        'bid': _safe_num(q.bid_price1), 'ask': _safe_num(q.ask_price1),
                    }
                    pending.discard(s)
            except Exception:
                pass
        if pending:
            time.sleep(0.05)
    return out


def _wait_quotes(symbols, max_wait=1.5):
    """针对特定合约等待行情到达（用于补抓平值 CALL/PUT）。"""
    if not _SLOTS or not _DEFAULT_KEY:
        return {}
    slot = _SLOTS.get(_DEFAULT_KEY)
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
                    pre = _safe_num(q.pre_close)
                    chg, pct = _quote_chg_pct(lp, pre)
                    out[s] = {'last': lp, 'pre_close': pre, 'chg': chg, 'pct': pct}
                    pending.discard(s)
            except Exception:
                pass
        if pending:
            time.sleep(0.05)
    return out


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
.opt-table td{font-size:13px}
.opt-table .left{text-align:left}
.opt-table .right{text-align:right}
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
</style>'''


_MINI_SCRIPT = '''<script>
function optBase(){var s='/',a='api',g='gw';var isGw=location.pathname.split(s).slice(1,3).join(s)===a+s+g;return isGw?s+a+s+g+s+'optchain':s+'optchain';}
function gotoUnderlyingOpt(u){location.href=optBase()+'/chain?underlying='+encodeURIComponent(u);}
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
        ('平值期权', "optBase() + '/atm'", 'atm'),
        ('条件筛选', "optBase() + '/filter'", 'filter'),
        ('期权VIX指数', "optBase() + '/vix'", 'vix'),
        ('标的排名', "optBase() + '/rank'", 'rank'),
    ]
    parts = []
    for name, href, key in tabs:
        cls = 'active' if key == active else ''
        parts.append(f'<a class="{cls}" href="#" onclick="location.href={href};return false;">{html.escape(name)}</a>')
    return '<div class="opt-tabbar">' + ''.join(parts) + '</div>'


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
    products = _list_option_products()
    symbols = [p['underlying'] for p in products]
    qmap = _batch_quote_map(symbols, max_wait=0.8)

    def sort_key(p):
        q = qmap.get(p['underlying'], {})
        pct = q.get('pct')
        return (abs(pct) if pct is not None else -1, q.get('volume') or 0)
    products.sort(key=sort_key, reverse=True)

    rows = []
    for p in products:
        q = qmap.get(p['underlying'], {})
        trend = 'up' if (q.get('chg') is not None and q['chg'] >= 0) else 'down'
        safe_u = html.escape(p['underlying']).replace("'", "\\'")
        rows.append(
            f'<tr><td class="left">{html.escape(p["ex_name"])}</td>'
            f'<td class="left">{html.escape(p["prod_name"])}</td>'
            f'<td class="left"><b>{html.escape(p["underlying"])}</b></td>'
            f'<td>{html.escape(p["expiry"])}</td>'
            f'<td>{_fmt(q.get("last"))}</td>'
            f'<td class="{trend}">{_fmt(q.get("chg"))}</td>'
            f'<td class="{trend}">{_fmt(q.get("pct"), 2, True)}</td>'
            f'<td>{_fmt(q.get("volume"), 0, False, True)}</td>'
            f'<td><a class="btn" href="#" onclick="gotoUnderlyingOpt(\'{safe_u}\');return false;">T型</a></td></tr>'
        )
    body = (
        '<table class="opt-table"><thead><tr>'
        '<th class="left">交易所</th><th class="left">品种</th><th class="left">标的合约</th>'
        '<th>行权日</th><th>最新价</th><th>涨跌</th><th>涨跌幅</th><th>成交量</th><th>操作</th>'
        '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table>'
        '<p class="note">主力合约：取各品种最近月份期权标的；行情缺失时显示 “—”。</p>'
    )
    return _opt_page_shell('期权主力合约', '按品种最近月份标的排序（主力月份）', body, 'main')


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


def register_routes(app, slots, default_key, get_chain_live_greeks=None):
    global _SLOTS, _DEFAULT_KEY, _GET_CHAIN_LIVE_GREEKS
    _SLOTS = slots
    _DEFAULT_KEY = default_key
    _GET_CHAIN_LIVE_GREEKS = get_chain_live_greeks
    app.add_url_rule('/optchain/main', view_func=optchain_main_page)
    app.add_url_rule('/optchain/atm', view_func=optchain_atm_page)
    app.add_url_rule('/optchain/filter', view_func=optchain_filter_page)
    app.add_url_rule('/optchain/vix', view_func=optchain_vix_page)
    app.add_url_rule('/optchain/rank', view_func=optchain_rank_page)
    app.add_url_rule('/optchain/fav', view_func=optchain_fav_page)
