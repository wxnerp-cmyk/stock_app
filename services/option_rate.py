# -*- coding: utf-8 -*-
"""期权费率模块（独立大模块，路由前缀 /optrate）。

需求人定义：
    期权费率 = 平值期权价格 / 对应标的价格 × 365 / 到期天数
即把"期权价格占标的的百分比"按剩余天数【年化】后的比率(%)。

用途：
    费率极低(< 5%)  = 期权相对标的极便宜（隐含波动率极低 / 情绪冰点）
    费率极高(> 500%) = 期权相对标的极贵（隐含波动率爆表 / 恐慌或投机）
    两端都是"异常值"，故分两个分区各自排序展示。

🔴 排序方向（需求人确认无误）：
    低费率区(< 下限)：【升序】—— 最便宜的排最前
    高费率区(> 上限)：【降序】—— 最贵的排最前
    两区都是"最极端的排最上面"，逻辑自洽。

模块结构（与 股票/期货/期权 等大模块一致）：
    - 本文件是**独立模块**，不寄生在 opt_special_pages 里；
    - 网关 stock_main 的 modules 表有一行 module_key='optrate'，
      与 stockkline / stocktech / optchain 并列出现在左侧菜单；
    - 路由前缀 /optrate，由 main.py 调register_routes() 挂载。

数据层复用 opt_special_pages 已有的行情工具（_list_option_products /
_batch_quote_map / _rest_days / _fmt），不重复实现，避免两套口径漂移。
性能约定（同VIX 页）：只订阅每个品种最近月份的平值 CALL/PUT，不整链冷订阅。
"""
import html

from flask import request, jsonify

from services import futures_db, option_greeks
from services.opt_special_pages import (
    _batch_quote_map, _list_option_products, _rest_days, _fmt,
)

# 由 register_routes 注入
_SLOTS = None
_DEFAULT_KEY = None
_GET_SLOT = None  # 请求感知的账号解析器（来自 main.py 的 _resolve_slot）

# ===================== 费率口径与阈值 =====================
# 单位约定：rate / 阈值 / 显示一律用【百分数】（5 表示 5%，500 表示 500%），
#   仅在计算瞬间做小数相除，避免 0.05 与 5 混用产生歧义。
RATE_DEF_LOW = 5.0        # 低费率上限(%)：低于此值进低分区
RATE_DEF_HIGH = 500.0     # 高费率下限(%)：高于此值进高分区
RATE_DEF_MIN_DAYS = 15    # 低分区额外要求：到期天数 > 此值(天)
RATE_BASIS = 365.0        # 年化基准天数


def rate_params(src=None):
    """从 query string 取三个阈值；非法/空/负数一律回落默认（保证页面永远能开）。

    上限必须大于下限，否则两区重叠或恒空 —— 直接交换之（而非报错）。
    """
    def _f(key, dflt):
        try:
            raw = (src or {}).get(key)
            if raw is None or str(raw).strip() == '':
                return dflt
            v = float(raw)
            if v != v or v < 0:        # NaN / 负数
                return dflt
            return v
        except (TypeError, ValueError):
            return dflt

    def _i(key, dflt):
        try:
            raw = (src or {}).get(key)
            if raw is None or str(raw).strip() == '':
                return dflt
            v = int(float(raw))
            return v if v >= 0 else dflt
        except (TypeError, ValueError):
            return dflt

    low = _f('low', RATE_DEF_LOW)
    high = _f('high', RATE_DEF_HIGH)
    mind = _i('mindays', RATE_DEF_MIN_DAYS)
    if high <= low:
        low, high = high, low
    return {'low': low, 'high': high, 'mindays': mind}


def calc_option_rate(opt_price, S, rest_days):
    """期权费率(%) = 平值期权价 / 标的价格 × 365 / 到期天数。

    返回 None 表示无法计算（价格/标的缺失、非正数、NaN、到期天数 <= 0）。
    🔴 到期天数 <= 0（已到期）必须拒绝 —— 否则 365/天数 会放大成天文数字，
       把高分区彻底污染（真实高费率只有 500%~2000% 量级）。
    """
    if opt_price is None or S is None:
        return None
    try:
        p, s, d = float(opt_price), float(S), float(rest_days)
    except (TypeError, ValueError):
        return None
    if p != p or s != s or d != d:
        return None
    if p <= 0 or s <= 0 or d <= 0:
        return None
    return p / s * RATE_BASIS / d * 100.0


def pick_atm(chain, S):
    """从期权链取最接近标的价格 S 的行权价条目（= 平值）。跳过 strike 非法的条目。"""
    strikes = (chain or {}).get('strikes') or []
    best, best_d = None, None
    for st in strikes:
        try:
            k = float(st['strike'])
        except (KeyError, TypeError, ValueError):
            continue
        d = abs(k - S)
        if best_d is None or d < best_d:
            best, best_d = st, d
    return best


def _collect_rows(prm, max_products=20):
    """采集费率行数据。返回 (low_rows, high_rows, stats)。

    每个品种取最近月份标的的主力月份链，定位平值 CALL/PUT，各算一次费率。
    CALL/PUT 各出一行 —— 平值附近两者费率接近但不相同，分开列便于客户对比。
    ⚠ 不用 break 提前退出：早先用 break 导致"CALL 没算出费率时 PUT 也被跳过"，
      整行丢失。两侧各自入分区。

    ⚠ 阈值通过 prm 参数传入（不用模块级可变变量）—— Flask 多线程下
      模块级共享状态会被并发请求互相污染，导致 A 请求的阈值影响 B 请求。

    stats 供页面自诊断展示（2026-10-12：用户两区全空截图实锤为冷槽位下
    标的行情 0.8s 没等到 → 品种全被滤光，页面却显示"无符合条件的合约"）。
    """
    low_thr = prm['low']
    high_thr = prm['high']
    stats = {'products': 0, 'with_quote': 0, 'entries': 0, 'computed': 0,
             'low': 0, 'high': 0}
    all_products = _list_option_products()
    stats['products'] = len(all_products)
    uq_syms = [p['underlying'] for p in all_products]
    uqmap = _batch_quote_map(uq_syms, max_wait=0.8)
    if not uqmap:
        # 冷槽位兜底：首次 0.8s 可能一无所获（连接刚建立/订阅未回包），
        # 加长到 3s 重试一次，避免"冷启动必空"的假象。
        uqmap = _batch_quote_map(uq_syms, max_wait=3.0)
    # 优先展示有实时行情的品种，并限制数量，保证订阅/读取在秒内完成
    products = [p for p in all_products if uqmap.get(p['underlying'], {}).get('last') is not None]
    stats['with_quote'] = len(products)
    products.sort(key=lambda p: abs(uqmap[p['underlying']].get('pct') or 0), reverse=True)
    products = products[:max_products]

    entries, opt_symbols = [], []
    for p in products:
        S = uqmap.get(p['underlying'], {}).get('last')
        if S is None:
            continue
        chain = futures_db.query_option_chain(p['underlying'])
        if not chain or not chain.get('strikes'):
            continue
        best = pick_atm(chain, S)
        if not best:
            continue
        stats['entries'] += 1
        csym = best['call']['symbol'] if best.get('call') else None
        psym = best['put']['symbol'] if best.get('put') else None
        ex = (best['call']['exercise'] if best.get('call')
              else (best['put']['exercise'] if best.get('put')
                    else (chain.get('exercise_dates') or [None])[0]))
        entries.append({'p': p, 'S': S, 'K': best['strike'], 'exercise': ex,
                        'days': _rest_days(ex), 'csym': csym, 'psym': psym})
        for s in (csym, psym):
            if s:
                opt_symbols.append(s)

    # 只订阅平值合约：冷数据 3s 足够首包；已预热的热数据毫秒级
    opt_qmap = _batch_quote_map(opt_symbols, max_wait=3.0)
    if opt_symbols and not opt_qmap:
        # 冷槽位兜底：期权订阅首包可能超过 3s，加长重试一次
        opt_qmap = _batch_quote_map(opt_symbols, max_wait=6.0)

    low_rows, high_rows = [], []
    for e in entries:
        S, days = e['S'], e['days']
        for side, key, label in (('call', 'csym', '平值认购 CALL'),
                                 ('put', 'psym', '平值认沽 PUT')):
            sym = e[key]
            if not sym:
                continue
            q = opt_qmap.get(sym, {})
            rate = calc_option_rate(q.get('last'), S, days)
            if rate is None:
                continue
            stats['computed'] += 1
            row = {'ex_name': e['p']['ex_name'], 'prod_name': e['p']['prod_name'],
                   'underlying': e['p']['underlying'], 'side_label': label, 'sym': sym,
                   'S': S, 'K': e['K'], 'price': q.get('last'), 'days': days,
                   'rate': rate, 'exercise': e['exercise'], 'chg': q.get('chg')}
            if rate < low_thr:
                low_rows.append(row)
            elif rate > high_thr:
                high_rows.append(row)

    # 🔴 到期天数门槛【只作用于低分区】。
    #   需求原文只对"小于 5%"这一段要求到期天数 > 15天；高费率本身即极端信号，
    #   若再加天数限制，会把"临近到期费率爆表"（恰恰最值得看）全部滤掉。
    min_days = prm['mindays']
    low_rows = [r for r in low_rows if (r['days'] is not None and r['days'] > min_days)]

    # 排序：低区升序（最便宜在前）/ 高区降序（最贵在前）—— 需求人确认的方向
    low_rows.sort(key=lambda r: r['rate'])
    high_rows.sort(key=lambda r: r['rate'], reverse=True)
    stats['low'] = len(low_rows)
    stats['high'] = len(high_rows)
    return low_rows, high_rows, stats


# ===================== 页面 =====================
_STYLE = """<style>
:root{--bg:#06080f;--surface:#0b1120;--border:#222d42;--text:#e8eef7;--muted:#8595ac;--accent:#2dd4ff;--up:#ff4d4f;--down:#19c37d;--gold:#f7b500}
html,body{margin:0;font-family:'Microsoft YaHei','PingFang SC',sans-serif;background:var(--bg);color:var(--text);font-size:14px}
.opt-page{min-height:100vh;padding:12px 14px 40px}
.page-title{color:var(--gold);font-size:22px;font-weight:800;margin-bottom:4px}
.page-sub{color:var(--muted);font-size:13px;margin-bottom:14px}
.opt-table{width:100%;border-collapse:collapse;background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden}
.opt-table th,.opt-table td{padding:8px 10px;border-bottom:1px solid #1a2435;text-align:center;white-space:nowrap;font-size:13px}
.opt-table th{background:#0f172a;color:var(--accent);font-size:13px;position:sticky;top:0;z-index:10}
.opt-table tr:hover{background:#16223a}
.opt-table .left{text-align:left}
.table-wrap{overflow-x:auto;width:100%;border-radius:8px}
.main-opt-table{min-width:1500px}
.up{color:var(--up)} .down{color:var(--down)}
.btn{display:inline-block;padding:4px 10px;background:linear-gradient(90deg,var(--accent),#a855f7);color:#05101c;border-radius:4px;font-size:12px;font-weight:700;text-decoration:none;cursor:pointer;border:none}
.btn:hover{opacity:.9}
.note{color:var(--muted);font-size:12px;margin-top:8px}
.empty{padding:30px;color:var(--muted);text-align:center;background:var(--surface);border:1px dashed var(--border);border-radius:8px}
.filter-form{display:flex;flex-wrap:wrap;gap:10px;background:var(--surface);padding:12px;border:1px solid var(--border);border-radius:8px;margin-bottom:14px;align-items:flex-end}
.filter-form label{display:flex;flex-direction:column;gap:4px;font-size:12px;color:var(--muted)}
.filter-form input{padding:5px 8px;background:#0d1424;border:1px solid var(--border);color:var(--text);border-radius:4px;width:110px}
</style>"""

_SCRIPT = """
<script>
function optBase(){var s='/',a='api',g='gw';
  var isGw=location.pathname.split(s).slice(1,3).join(s)===a+s+g;
  return isGw?s+a+s+g+s+'optrate':s+'optrate';}
function applyRate(){
  var l=document.getElementById('rLow'),h=document.getElementById('rHigh'),m=document.getElementById('rMin');
  var q=[];
  if(l&&l.value!=='') q.push('low='+encodeURIComponent(l.value));
  if(h&&h.value!=='') q.push('high='+encodeURIComponent(h.value));
  if(m&&m.value!=='') q.push('mindays='+encodeURIComponent(m.value));
  location.href=optBase()+(q.length?('?'+q.join('&')):'');
}
function resetRate(){ location.href=optBase(); }
function gotoOptChain(u){
  var s='/',a='api',g='gw';
  var isGw=location.pathname.split(s).slice(1,3).join(s)===a+s+g;
  var base=isGw?s+a+s+g+s+'optchain':s+'optchain';
  location.href=base+'/chain?underlying='+encodeURIComponent(u);
}
</script>
"""


def option_rate_page():
    """期权费率页（独立模块首页）：两分区各自排序 + 三阈值可设置。"""
    prm = rate_params(request.args if request else None)

    try:
        low_rows, high_rows, stats = _collect_rows(prm)
    except Exception as e:
        print(f'[期权费率] 采集失败: {e}')
        low_rows, high_rows, stats = [], [], {'products': 0, 'with_quote': 0,
                                              'entries': 0, 'computed': 0,
                                              'low': 0, 'high': 0}

    def _rows_html(rows):
        out = []
        for r in rows:
            safe_u = html.escape(r['underlying']).replace("'", "\\'")
            chg = r.get('chg')
            if chg is None:
                chg_html = '<span style="color:var(--muted)">—</span>'
            else:
                cls = 'up' if chg >= 0 else 'down'
                chg_html = f'<span class="{cls}">{_fmt(chg)}</span>'
            out.append(
                f'<tr>'
                f'<td class="left">{html.escape(r["ex_name"])}</td>'
                f'<td class="left">{html.escape(r["prod_name"])}</td>'
                f'<td class="left">{html.escape(r["underlying"])}</td>'
                f'<td class="left">{html.escape(r["side_label"])}<br>'
                f'<span style="color:var(--muted);font-size:11px">{html.escape(r["sym"])}</span></td>'
                f'<td>{_fmt(r["S"])}</td><td>{_fmt(r["K"])}</td>'
                f'<td>{_fmt(r["price"])}</td><td>{chg_html}</td>'
                f'<td><b>{_fmt(r["rate"], 2)}%</b></td>'
                f'<td>{_fmt(r["days"], 0) if r["days"] is not None else "—"}</td>'
                f'<td>{html.escape(str(r["exercise"] or ""))}</td>'
                f'<td><a class="btn" href="#" onclick="gotoOptChain(\'{safe_u}\');return false;">T型报价</a></td>'
                f'</tr>'
            )
        return ''.join(out)

    head = ('<tr><th class="left">交易所</th><th class="left">品种</th><th class="left">标的</th>'
            '<th class="left">合约</th><th>标的价格</th><th>行权价</th><th>期权价</th><th>涨跌</th>'
            '<th>期权费率</th><th>剩余天数</th><th>行权日</th><th>操作</th></tr>')

    def _table(title, tip, rows, zone_key):
        if rows:
            inner = _rows_html(rows)
            body = ('<div class="table-wrap"><table class="opt-table main-opt-table">'
                    f'<thead>{head}</thead><tbody>{inner}</tbody></table></div>')
        elif stats['entries'] == 0:
            # 采集层就没拿到平值组合 = 行情未就绪/账户无期权权限，不是"筛选无结果"
            body = ('<div class="empty">行情未就绪（本次采集到 0 个品种的平值组合），'
                    '请稍后刷新页面；若持续为空，请检查右上角账户是否有期权行情权限</div>')
        else:
            body = (f'<div class="empty">本次共算出 {stats["computed"]} 条费率，'
                    f'均未落入该区（阈值外属正常 —— 极端费率本就稀少）</div>')
        return (f'<h3 style="color:var(--gold);margin:16px 0 6px;font-size:15px">{title}</h3>'
                f'<p class="note" style="margin:0 0 8px">{tip}</p>{body}')

    form = (
        '<form class="filter-form" method="get" onsubmit="return false">'
        f'<label>低费率上限(%)<input type="number" step="0.1" name="low" value="{prm["low"]}" id="rLow"></label>'
        f'<label>高费率下限(%)<input type="number" step="1" name="high" value="{prm["high"]}" id="rHigh"></label>'
        f'<label>低分区最小到期天数<input type="number" step="1" name="mindays" value="{prm["mindays"]}" id="rMin"></label>'
        '<button class="btn" onclick="applyRate()">应用</button>'
        '<button class="btn" onclick="resetRate()">恢复默认</button>'
        '<span class="note" style="margin:0">调整后自动刷新页面</span>'
        '</form>'
    )
    summary = (f'<div class="note" style="margin:0 0 10px">本次采集统计：期权品种 '
               f'{stats["products"]} 个 ｜ 有实时行情 {stats["with_quote"]} 个 ｜ '
               f'平值组合 {stats["entries"]} 组 ｜ 算出费率 {stats["computed"]} 条 '
               f'（低区命中 {stats["low"]}，高区命中 {stats["high"]}）</div>')
    body = (
        form
        + summary
        + _table(f'一、低费率区（费率 &lt; {_fmt(prm["low"], 1)}%，升序）',
                 f'期权相对标的极便宜（隐含波动率低）。仅显示到期天数 &gt; {_fmt(prm["mindays"], 0)} 天的合约，'
                 f'按费率<b>升序</b>排列，越便宜越靠前。', low_rows, 'low')
        + _table(f'二、高费率区（费率 &gt; {_fmt(prm["high"], 1)}%，降序）',
                 f'期权相对标的极贵（隐含波动率爆表）。<b>不限到期天数</b>（临近到期爆表最值得看），'
                 f'按费率<b>降序</b>排列，越贵越靠前。', high_rows, 'high')
    )
    return ('<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8">'
            '<title>期权费率</title>'
            + _STYLE + _SCRIPT
            + '</head><body>'
              '<div class="opt-page">'
              '<div class="page-title">期权费率</div>'
              '<div class="page-sub">费率 = 平值期权价 / 标的价格 × 365 / 到期天数；'
              '阈值可自定义，仅统计各品种最近月份的主力链</div>'
            + body + '</div></body></html>')


def api_rate_params():
    """GET /optrate/api/params —— 回显当前生效的三个阈值（前端自检/调试用）。"""
    return jsonify({'ok': True, **rate_params(request.args)})


def register_routes(app, slots=None, default_key=None, get_slot=None):
    """挂载 /optrate 全部路由。由 main.py 在服务启动时调用。"""
    global _SLOTS, _DEFAULT_KEY, _GET_SLOT
    _SLOTS = slots
    _DEFAULT_KEY = default_key
    _GET_SLOT = get_slot

    app.add_url_rule('/optrate', view_func=option_rate_page, methods=['GET'], strict_slashes=False)
    app.add_url_rule('/optrate/', view_func=option_rate_page, methods=['GET'], strict_slashes=False)
    app.add_url_rule('/optrate/api/params', view_func=api_rate_params, methods=['GET'],
                     strict_slashes=False)
    print("[期权费率模块] 路由已注册: /optrate, /optrate/api/params", flush=True)