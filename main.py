import threading, json, sys, traceback, math
from datetime import datetime
from flask import Flask, render_template_string, request, Response, jsonify
from services import sync_service
from core.tqsdk_client import TqSdkClient
from services.market_data import MarketDataService
from services.option_expiry import fetch_expiry_data, build_calendar_data, build_weekly_summary
try:
    from services.history_kline import HistoryKlineService
    _has_history = True
except ImportError:
    _has_history = False
    HistoryKlineService = None
    print("[WARN] akshare 未安装, 历史K线功能不可用")

app = Flask(__name__)

def safe_json(data, status=200):
    """将含 NaN/Inf 的数据转为安全 JSON"""
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

# 默认自选合约（用户可自行添加）
DEFAULT_SYMBOLS = [
    "KQ.m@SHFE.cu", "KQ.m@SHFE.rb", "KQ.m@DCE.i", "KQ.m@DCE.m",
]

from typing import Optional
client: Optional[TqSdkClient] = None
market: Optional[MarketDataService] = None
latest_data = {}
data_lock = threading.Lock()

HTML = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>行情系统</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Microsoft YaHei','Segoe UI',sans-serif;background:#0d1117;color:#c9d1d9;padding:16px;min-height:100vh}
h1{color:#58a6ff;margin-bottom:8px;font-size:20px}
.subtitle{font-size:12px;color:#8b949e;margin-bottom:12px}

/* 表格 */
table{width:100%;border-collapse:collapse;font-size:13px;margin-bottom:12px}
th{background:#161b22;padding:7px 8px;text-align:left;border-bottom:2px solid #30363d;font-weight:600;color:#58a6ff;white-space:nowrap}
td{padding:5px 8px;border-bottom:1px solid #21262d;white-space:nowrap}
tr:hover{background:#1c2129}
tr{cursor:pointer}
.up{color:#26a641}.down{color:#f85149}
.price{font-weight:bold;font-size:13px}

/* 控制栏 */
.controls{margin-bottom:12px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.controls input{flex:1;min-width:180px;padding:7px 10px;border-radius:6px;border:1px solid #30363d;background:#161b22;color:#c9d1d9;font-size:13px}
.controls input:focus{outline:none;border-color:#58a6ff}
.controls button{padding:7px 14px;border-radius:6px;border:1px solid #30363d;background:#21262d;color:#c9d1d9;cursor:pointer;font-size:13px;transition:all .2s}
.controls button:hover{background:#30363d;border-color:#58a6ff}

/* 标签页 */
.tabs{display:flex;gap:2px;margin-bottom:0;border-bottom:2px solid #30363d}
.tab{padding:8px 20px;border:none;background:transparent;color:#8b949e;cursor:pointer;font-size:13px;border-radius:6px 6px 0 0;transition:all .2s;position:relative;bottom:-2px}
.tab:hover{color:#c9d1d9;background:#1c2129}
.tab.active{color:#58a6ff;background:#161b22;border:2px solid #30363d;border-bottom:2px solid #161b22}

/* K线容器 */
.kline-area{background:#161b22;border:2px solid #30363d;border-top:none;border-radius:0 0 8px 8px;padding:16px;position:relative}
.kline-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.kline-header .symbol-name{color:#58a6ff;font-size:15px;font-weight:bold}
.kline-header .info{color:#8b949e;font-size:11px}
.kline-header .ohlc{display:flex;gap:12px;font-size:12px}
.kline-header .ohlc span{color:#8b949e}
.kline-header .ohlc b{color:#c9d1d9}
.chart-wrap{position:relative;width:100%}
canvas{display:block;width:100%;border-radius:4px}

.section-title{color:#58a6ff;font-size:15px;margin:14px 0 8px;border-bottom:1px solid #30363d;padding-bottom:4px;display:flex;align-items:center;gap:8px}
#status{font-size:11px;color:#8b949e;margin:4px 0 8px}

.loading{display:flex;align-items:center;justify-content:center;height:350px;color:#8b949e;font-size:14px}
.loading::after{content:'';display:inline-block;width:16px;height:16px;border:2px solid #30363d;border-top-color:#58a6ff;border-radius:50%;animation:spin .8s linear infinite;margin-left:8px}
@keyframes spin{to{transform:rotate(360deg)}}

/* 移动端适配 */
@media(max-width:768px){
  body{padding:8px}
  .controls{flex-direction:column}
  .controls input{min-width:100%}
  .kline-header{flex-direction:column;align-items:flex-start;gap:4px}
  table{font-size:11px}
  th,td{padding:4px 5px}
}
</style>
</head><body>

<h1>&#x1f4c8; &#x884c;&#x60c5;&#x770b;&#x677f;</h1>
<p class="subtitle">TqSdk &#x5b9e;&#x65f6;&#x884c;&#x60c5; + &#x4e1c;&#x65b9;&#x8d22;&#x5bcc;&#x5386;&#x53f2;&#x6570;&#x636e; | &#x53cc;&#x51fb;&#x5408;&#x7ea6;&#x67e5;&#x770b;K&#x7ebf;</p>

<nav style="margin-bottom:12px;display:flex;gap:4px">
  <a href="/" style="color:#58a6ff;text-decoration:none;padding:6px 14px;border-radius:6px;background:#161b22;font-size:13px">&#x1f4ca; &#x884c;&#x60c5;&#x770b;&#x677f;</a>
  <a href="/option-expiry" style="color:#8b949e;text-decoration:none;padding:6px 14px;border-radius:6px;background:#161b22;font-size:13px">&#x1f4c5; &#x671f;&#x8d27;&#x5230;&#x671f;&#x8868;</a>
  <a href="/option-exercise" style="color:#8b949e;text-decoration:none;padding:6px 14px;border-radius:6px;background:#161b22;font-size:13px">&#x1f4cb; &#x671f;&#x6743;&#x884c;&#x6743;&#x65e5;</a>
</nav>

<div class="controls">
  <input type="text" id="si" placeholder="&#x8f93;&#x5165;&#x5408;&#x7ea6;&#x4ee3;&#x7801;, &#x5982; SHFE.cu2501">
  <button onclick="add()">&#x2795; &#x6dfb;&#x52a0;</button>
  <button onclick="rem()">&#x2796; &#x79fb;&#x9664;</button>
  <span style="font-size:12px;color:#8b949e;margin-left:8px">&#x5237;&#x65b0;:</span>
  <select id="refresh-interval" onchange="changeInterval()" style="background:#161b22;color:#c9d1d9;border:1px solid #30363d;border-radius:6px;padding:6px 8px;font-size:12px">
    <option value="500">0.5&#x79d2;</option>
    <option value="1000">1&#x79d2;</option>
    <option value="2000">2&#x79d2;</option>
    <option value="3000" selected>3&#x79d2;</option>
    <option value="5000">5&#x79d2;</option>
    <option value="10000">10&#x79d2;</option>
  </select>
  <span style="font-size:10px;color:#484f58" id="rate-hint">(&#x5929;&#x52e4;&#x514d;&#x8d39;&#x8d26;&#x53f7;&#x65e0;&#x9891;&#x6b21;&#x4e0a;&#x9650;, &#x6700;&#x4f4e;&#x53ef;&#x8bbe;500ms)</span>
</div>

<div class="section-title">&#x81ea;&#x9009;&#x5408;&#x7ea6; <span style="font-size:11px;color:#8b949e;font-weight:normal">(&#x53cc;&#x51fb;&#x67d0;&#x884c;&#x67e5;&#x770b;K&#x7ebf;)</span></div>
<table>
  <thead><tr>
    <th>&#x5408;&#x7ea6;</th><th>&#x540d;&#x79f0;</th><th>&#x6700;&#x65b0;&#x4ef7;</th><th>&#x6da8;&#x8dcc;&#x5e45;</th>
    <th>&#x5f00;&#x76d8;</th><th>&#x6700;&#x9ad8;</th><th>&#x6700;&#x4f4e;</th>
    <th>&#x4e70;&#x4e00;</th><th>&#x5356;&#x4e00;</th><th>&#x6210;&#x4ea4;&#x91cf;</th><th>&#x66f4;&#x65b0;</th>
  </tr></thead>
  <tbody id="tb"><tr><td colspan="11">&#x52a0;&#x8f7d;&#x4e2d;...</td></tr></tbody>
</table>

<!-- K线区域 -->
<div class="section-title" style="margin-top:16px">&#x1f4ca; K&#x7ebf;&#x56fe; <span id="kline-sym" style="color:#58a6ff;font-size:14px"></span></div>

<div class="tabs">
  <button class="tab active" id="tab-realtime" onclick="switchTab('realtime')">&#x5b9e;&#x65f6;K&#x7ebf;</button>
  <button class="tab" id="tab-history" onclick="switchTab('history')">&#x5386;&#x53f2;K&#x7ebf;</button>
</div>

<div class="kline-area" id="kline-area">
  <div class="kline-header">
    <div style="display:flex;align-items:center;gap:12px">
      <span class="symbol-name" id="ohlc-symbol">--</span>
      <div class="ohlc" id="ohlc-info">
        <span>O: <b id="ohlc-o">--</b></span>
        <span>H: <b id="ohlc-h">--</b></span>
        <span>L: <b id="ohlc-l">--</b></span>
        <span>C: <b id="ohlc-c">--</b></span>
        <span>V: <b id="ohlc-v">--</b></span>
      </div>
    </div>
    <span class="info" id="kline-source"></span>
  </div>
  <div class="chart-wrap">
    <canvas id="kc" height="420"></canvas>
  </div>
</div>

<div id="status">&#x521d;&#x59cb;&#x5316;&#x4e2d;...</div>

<script>
/* ── 状态 ── */
var curSym = '';
var curTab = 'realtime';
var klineData = [];
var klinePollTimer = null;
var currentPollMs = 3000;

/* ── 行情轮询 ── */
function r(){
  fetch('/api/quotes').then(x=>x.json()).then(d=>{
    var h='';
    d.data.forEach(function(q){
      var u=q.pct_change>=0?'up':'down';
      var sig=q.pct_change>0?'+':'';
      var lp=isFinite(q.last_price)?q.last_price.toFixed(2):'--';
      var op=isFinite(q.open)?q.open.toFixed(2):'--';
      var hi=isFinite(q.high)?q.high.toFixed(2):'--';
      var lo=isFinite(q.low)?q.low.toFixed(2):'--';
      var bp=isFinite(q.bid_price1)?q.bid_price1.toFixed(2):'--';
      var ap=isFinite(q.ask_price1)?q.ask_price1.toFixed(2):'--';
      h+='<tr ondblclick="loadKline(\''+q.symbol+'\',\'realtime\')">'+
        '<td>'+q.symbol+'</td><td>'+(q.name||'-')+'</td>'+
        '<td class="price '+u+'">'+lp+'</td>'+
        '<td class="'+u+'">'+sig+q.pct_change+'%</td>'+
        '<td>'+op+'</td><td>'+hi+'</td><td>'+lo+'</td>'+
        '<td>'+bp+'</td><td>'+ap+'</td>'+
        '<td>'+q.volume+'</td><td>'+q.update_time+'</td></tr>';
    });
    document.getElementById('tb').innerHTML=h||'<tr><td colspan="11">无数据</td></tr>';
    document.getElementById('status').textContent='更新: '+new Date().toLocaleTimeString()+' | 天勤TqSdk';
  }).catch(function(e){
    document.getElementById('status').textContent='连接中...';
  });
}

/* ── 标签页切换 ── */
function switchTab(tab){
  curTab = tab;
  document.getElementById('tab-realtime').classList.toggle('active', tab==='realtime');
  document.getElementById('tab-history').classList.toggle('active', tab==='history');
  if(curSym) loadKline(curSym);
}

/* ── 加载K线 ── */
function loadKline(sym, tab){
  if(tab) curTab = tab;
  // 如果切换了合约，清除旧K线轮询
  if(sym !== curSym){
    if(klinePollTimer) clearInterval(klinePollTimer);
    curSym = sym;
    // 启动K线自动刷新（跟随行情刷新频率）
    if(!klinePollTimer || sym !== curSym){
      klinePollTimer = setInterval(function(){ fetchKlineData(); }, currentPollMs);
    }
  }
  // 同步标签页UI
  document.getElementById('tab-realtime').classList.toggle('active', curTab==='realtime');
  document.getElementById('tab-history').classList.toggle('active', curTab==='history');

  document.getElementById('kline-sym').textContent = sym;
  document.getElementById('ohlc-symbol').textContent = sym;
  fetchKlineData();
}

function fetchKlineData(){
  if(!curSym) return;
  var url;
  if(curTab === 'realtime'){
    url = '/api/kline?symbol='+encodeURIComponent(curSym)+'&count=100';
  } else {
    url = '/api/kline/history?symbol='+encodeURIComponent(curSym);
  }
  fetch(url).then(function(x){return x.json()}).then(function(d){
    klineData = d.data || [];
    document.getElementById('kline-source').textContent = d.source || '';
    drawKline(klineData, curSym);
  }).catch(function(e){
    klineData = [];
    drawKline([], curSym);
    document.getElementById('kline-source').textContent = '加载失败';
  });
}

/* ── 绘制K线（专业版 - 带坐标轴 + 时间标签 + 十字线） ── */
function drawKline(data, sym){
  var cv = document.getElementById('kc');
  var ctx = cv.getContext('2d');

  // ── Canvas 尺寸适配 ──
  var wrap = cv.parentElement;
  var w = Math.max(wrap.clientWidth, 600);
  var h = cv.height || 420;
  cv.width = w;
  cv.height = h;

  // 边距
  var ml = 72, mr = 20, mt = 12, mb = 32;
  var cw = w - ml - mr;
  var ch = h - mt - mb;

  // 清屏
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = '#0d1117';
  ctx.fillRect(0, 0, w, h);

  if(!data || data.length === 0){
    ctx.fillStyle = '#8b949e';
    ctx.font = '14px Microsoft YaHei';
    ctx.textAlign = 'center';
    ctx.fillText(curTab==='history' ? '暂无历史K线数据' : '双击上方合约查看实时K线', w/2, h/2);
    // 更新OHLC
    document.getElementById('ohlc-o').textContent = '--';
    document.getElementById('ohlc-h').textContent = '--';
    document.getElementById('ohlc-l').textContent = '--';
    document.getElementById('ohlc-c').textContent = '--';
    document.getElementById('ohlc-v').textContent = '--';
    return;
  }

  // ── 解析数据 ──
  var bars = [];
  for(var i=0;i<data.length;i++){
    var d = data[i];
    var o = parseFloat(d.open||d.开盘价||0);
    var c = parseFloat(d.close||d.收盘价||0);
    var hh = parseFloat(d.high||d.最高价||0);
    var ll = parseFloat(d.low||d.最低价||0);
    var v = parseInt(d.volume||d.成交量||0);
    var dt = d.datetime || d.date || d.日期 || '';
    // 修正值为0的情况
    hh = hh||Math.max(o,c,1);
    ll = ll||Math.min(o,c,1);
    o = o||c||1;
    c = c||o||1;
    if(!isFinite(o+hh+ll+c)) continue;
    bars.push({o:o,c:c,h:hh,l:ll,v:v,dt:dt});
  }
  if(!bars.length) return;

  // ── 价格范围 ──
  var allPrices = [];
  bars.forEach(function(b){ allPrices.push(b.h,b.l); });
  var pHigh = Math.max.apply(null, allPrices);
  var pLow  = Math.min.apply(null, allPrices);
  var pad = (pHigh-pLow)*0.08 || pHigh*0.02;
  var yMax = pHigh + pad;
  var yMin = pLow  - pad;
  var yRange = yMax - yMin || 1;

  // ── 价格刻度 ──
  var tickStep = niceStep(yRange, 5);
  var tickMin = Math.floor(yMin/tickStep)*tickStep;
  var tickMax = Math.ceil(yMax/tickStep)*tickStep;

  // ── 背景绘制 ──
  // 网格线
  ctx.strokeStyle = '#1a2332';
  ctx.lineWidth = 0.5;
  for(var ty=tickMin; ty<=tickMax+tickStep/2; ty+=tickStep){
    var yy = mt + ch - (ty-yMin)/yRange*ch;
    ctx.beginPath();
    ctx.moveTo(ml, yy);
    ctx.lineTo(w-mr, yy);
    ctx.stroke();

    // 左边价格标签
    ctx.fillStyle = '#8b949e';
    ctx.font = '10px monospace';
    ctx.textAlign = 'right';
    ctx.fillText(ty.toFixed(tickStep<1?2:1), ml-6, yy+4);

    // 右边的虚线值
    ctx.fillStyle = '#484f58';
    ctx.textAlign = 'left';
    ctx.fillText(ty.toFixed(tickStep<1?2:1), w-mr+4, yy+4);
  }

  // 边框
  ctx.strokeStyle = '#30363d';
  ctx.lineWidth = 1;
  ctx.strokeRect(ml, mt, cw, ch);

  // ── K线柱子 ──
  var barW = Math.max(cw/bars.length*0.65, 1);
  var barGap = Math.max(cw/bars.length*0.35, 0.5);
  if(cw/bars.length < 2){ barW=1; barGap=0.5; }

  bars.forEach(function(b,i){
    var x = ml + (barW+barGap)*i + barGap/2;
    var yh = mt + ch - (b.h-yMin)/yRange*ch;
    var yl = mt + ch - (b.l-yMin)/yRange*ch;
    var yo = mt + ch - (b.o-yMin)/yRange*ch;
    var yc = mt + ch - (b.c-yMin)/yRange*ch;

    // 影线
    var isUp = b.c >= b.o;
    ctx.strokeStyle = isUp ? '#26a641' : '#f85149';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(x+barW/2, yh);
    ctx.lineTo(x+barW/2, yl);
    ctx.stroke();

    // 实体
    var bodyTop = Math.min(yo, yc);
    var bodyH = Math.max(Math.abs(yc-yo), 1);
    ctx.fillStyle = isUp ? '#26a641' : '#f85149';
    ctx.fillRect(x, bodyTop, barW, bodyH);
  });

  // ── 底部时间标签 ──
  ctx.fillStyle = '#8b949e';
  ctx.font = '9px monospace';
  ctx.textAlign = 'center';
  var labelStep = Math.max(Math.floor(bars.length/8), 1);
  for(var i=0;i<bars.length;i+=labelStep){
    var b = bars[i];
    var dtStr = String(b.dt);
    // 尝试提取日期中的月-日部分
    var label = dtStr.length>6 ? dtStr.substring(4,6)+'/'+dtStr.substring(6,8) : dtStr.substring(0,10);
    // 如果含 - 则取月-日
    if(dtStr.indexOf('-')>0){
      var parts = dtStr.split('-');
      label = parts[1]+'/'+parts[2];
    }
    var tx = ml + (barW+barGap)*i + barW/2;
    ctx.fillText(label, tx, h-6);
  }

  // ── 合约名称 ──
  ctx.fillStyle = '#58a6ff';
  ctx.font = 'bold 11px Microsoft YaHei';
  ctx.textAlign = 'left';
  ctx.fillText(sym, ml+6, mt-2);

  // ── 更新顶部OHLC面板 ──
  var last = bars[bars.length-1];
  document.getElementById('ohlc-o').textContent = last.o.toFixed(2);
  document.getElementById('ohlc-h').textContent = last.h.toFixed(2);
  document.getElementById('ohlc-l').textContent = last.l.toFixed(2);
  document.getElementById('ohlc-c').textContent = last.c.toFixed(2);
  document.getElementById('ohlc-v').textContent = (last.v||0).toLocaleString();

  var chgColor = last.c >= last.o ? '#26a641' : '#f85149';
  document.getElementById('ohlc-c').style.color = chgColor;

  console.log('K线绘制完成:', bars.length, '条 | 价格范围:', pLow.toFixed(2), '-', pHigh.toFixed(2));
}

/* ── 刻度优化 ── */
function niceStep(range, targetTicks){
  var raw = range / targetTicks;
  var mag = Math.pow(10, Math.floor(Math.log10(raw)));
  var norm = raw / mag;
  if(norm <= 1.5) return mag;
  if(norm <= 3) return 2*mag;
  if(norm <= 7) return 5*mag;
  return 10*mag;
}

/* ── 窗口resize重绘 ── */
var resizeTimer;
window.addEventListener('resize', function(){
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(function(){
    if(klineData.length) drawKline(klineData, curSym);
  }, 200);
});

/* ── 添加/移除合约 ── */
function add(){
  var s=document.getElementById('si').value.trim();
  if(!s)return;
  fetch('/api/symbols/add?symbol='+s).then(function(x){return x.json()}).then(function(){location.reload()});
}
function rem(){
  var s=document.getElementById('si').value.trim();
  if(!s)return;
  fetch('/api/symbols/remove?symbol='+s).then(function(x){return x.json()}).then(function(){location.reload()});
}

/* ── 启动 ── */
var pollTimer = setInterval(r, 3000);
r();

/* ── 刷新频率切换 ── */
function changeInterval(){
  var v = parseInt(document.getElementById('refresh-interval').value);
  currentPollMs = v;
  clearInterval(pollTimer);
  pollTimer = setInterval(r, v);
  // K线也跟随刷新频率
  if(curSym){
    if(klinePollTimer) clearInterval(klinePollTimer);
    klinePollTimer = setInterval(function(){ fetchKlineData(); }, v);
  }
  document.getElementById('status').textContent = '刷新间隔: '+v+'ms | 天勤TqSdk';
}
</script>
</body></html>"""

@app.route("/")
def index():
    return HTML

@app.route("/api/quotes")
def api_quotes():
    with data_lock:
        result = list(latest_data.values())
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
def api_kline():
    sym = request.args.get("symbol", "")
    if not sym: return safe_json({"code": -1}, 400)
    try:
        if market is None: return safe_json({"code": -1, "msg": "not ready"}, 500)
        import time
        df = market.get_kline_data(sym, 86400, 80)
        if df.empty:
            # 没有缓存 => 按需订阅 (短暂等待, 后台预加载会后续补齐)
            try:
                market.client.get_kline(sym, 86400, 80)
                market.client.api.wait_update(deadline=time.time() + 1)
                df = market.get_kline_data(sym, 86400, 80)
            except Exception:
                pass
        count = len(df) if not df.empty else 0
        if count == 0:
            print(f"[K线] {sym}: 暂无数据 (可稍后重试)")
        else:
            print(f"[K线] {sym}: 返回 {count} 条")
        return safe_json({"code": 0, "data": df.to_dict(orient="records") if not df.empty else [], "source": "天勤实时"})
    except Exception as e:
        return safe_json({"code": -1, "msg": str(e)}, 500)

@app.route("/api/kline/history")
def api_kline_history():
    """历史K线: 从东方财富免费数据源拉取(盘后/周末可用)"""
    sym = request.args.get("symbol", "")
    if not sym: return safe_json({"code": -1}, 400)
    try:
        df = HistoryKlineService.get_daily_kline(sym, days=200)
        if df.empty:
            return safe_json({"code": 0, "data": [], "msg": "暂无此品种历史数据"})
        return safe_json({"code": 0, "data": df.to_dict(orient="records"), "source": "东方财富历史"})
    except Exception as e:
        return safe_json({"code": -1, "msg": str(e)}, 500)

# 到期表缓存（后台线程查询，Flask 线程只读）
expiry_cache = {}
expiry_cache_lock = threading.Lock()

@app.route("/option-expiry")
def option_expiry_page():
    """期货到期一览表 - 动态查询天勤 TqSdk（后台异步加载 + 缓存）"""
    import datetime as dt
    now = dt.datetime.now()
    
    year = request.args.get("year", now.year, type=int)
    month = request.args.get("month", now.month, type=int)
    
    if month < 1 or month > 12:
        month = now.month
    if year < 2020 or year > 2030:
        year = now.year
    
    title = f"{year}年{month}月期货到期一览表"
    weekday_headers = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    
    cache_key = f"{year}-{month}"
    
    # 检查缓存，没有则触发后台查询
    with expiry_cache_lock:
        if cache_key not in expiry_cache:
            expiry_cache[cache_key] = {"status": "loading", "data": {}}
            if client is not None:
                threading.Thread(
                    target=_load_expiry_cache,
                    args=(cache_key, year, month),
                    daemon=True
                ).start()
        cache_entry = expiry_cache[cache_key]
    
    query_result = cache_entry.get("data", {})
    error_msg = cache_entry.get("error", "")
    is_loading = cache_entry.get("status") == "loading"
    
    if is_loading and not query_result:
        error_msg = "正在从天勤 TqSdk 查询到期数据，请稍后刷新页面..."
    
    weeks = build_calendar_data(query_result, year, month)
    summary = build_weekly_summary(query_result, year, month)

    # 生成日历表格HTML
    calendar_rows = []
    for week in weeks:
        row = '<tr>'
        for day in week:
            if not day["is_current_month"]:
                row += '<td class="other-month"></td>'
            elif day["is_weekend"]:
                row += f'<td class="weekend">{day["day"]}</td>'
            elif day["expiry"] and day["expiry"]["products"]:
                products_html = "<br>".join(day["expiry"]["products"])
                exchange = day["expiry"]["exchange"]
                underlying = day["expiry"]["underlying"]
                ex_badge = f'<span class="ex-badge">{exchange}</span><br>' if exchange else ''
                ul_badge = f'<span class="ul-badge">{underlying}</span><br>' if underlying else ''
                row += f'<td class="expiry-day"><div class="day-num">{day["day"]}</div>{ex_badge}{ul_badge}<div class="products">{products_html}</div></td>'
            else:
                row += f'<td class="normal-day"><div class="day-num">{day["day"]}</div></td>'
        row += '</tr>'
        calendar_rows.append(row)
    
    cal_body = "\n".join(calendar_rows) if calendar_rows else '<tr><td colspan="7">暂无数据</td></tr>'
    
    # 生成周列表HTML
    summary_rows = []
    if summary:
        for ws in summary:
            if not ws["days"]:
                summary_rows.append(f'<div class="week-block empty"><div class="week-title">{ws["label"]} ({ws["date_range"]})</div><div class="week-empty">无期货到期</div></div>')
            else:
                days_html = ""
                for d in ws["days"]:
                    prods = "<br>".join(d["products"])
                    days_html += f'''<div class="summary-day">
                        <div class="sd-header"><span class="sd-day">{d["day"]}日 {d["weekday_name"]}</span><span class="sd-exchange">{d["exchange"]}</span><span class="sd-underlying">{d["underlying"]}</span></div>
                        <div class="sd-products">{prods}</div>
                    </div>'''
                summary_rows.append(f'<div class="week-block"><div class="week-title">{ws["label"]} ({ws["date_range"]})</div>{days_html}</div>')
    else:
        summary_rows.append('<div class="week-block empty"><div class="week-title">暂无数据</div><div class="week-empty">未查询到任何期货到期信息</div></div>')
    
    summary_body = "\n".join(summary_rows)
    
    # 月份切换按钮
    prev_month = month - 1
    prev_year = year
    if prev_month < 1:
        prev_month = 12
        prev_year -= 1
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year += 1
    
    EXPIRY_HTML = f'''<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Microsoft YaHei','Segoe UI',sans-serif;background:#0d1117;color:#c9d1d9;padding:16px;min-height:100vh}}
h1{{color:#f0c000;margin-bottom:4px;font-size:20px}}
h2{{color:#58a6ff;font-size:17px;margin:16px 0 10px;border-bottom:1px solid #30363d;padding-bottom:6px}}
.subtitle{{font-size:12px;color:#8b949e;margin-bottom:12px}}

nav{{margin-bottom:12px;display:flex;gap:4px;flex-wrap:wrap;align-items:center}}
nav a{{color:#8b949e;text-decoration:none;padding:6px 14px;border-radius:6px;background:#161b22;font-size:13px;transition:all .2s}}
nav a:hover{{background:#21262d;color:#c9d1d9}}
nav a.active{{color:#f0c000;background:#21262d}}
nav .month-nav{{display:flex;align-items:center;gap:6px;margin-left:auto}}
nav .month-nav a{{padding:4px 10px;font-size:12px}}
nav .month-nav .current{{color:#f0c000;font-weight:bold;font-size:14px;padding:4px 12px;background:#1c2129;border-radius:6px}}
.source-info{{font-size:11px;color:#484f58;margin-top:4px}}

/* 日历表格 */
.cal-table{{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:20px;background:#161b22;border-radius:8px;overflow:hidden}}
.cal-table th{{background:#1a2332;padding:10px 8px;text-align:center;border:1px solid #21262d;font-weight:600;color:#58a6ff;font-size:13px}}
.cal-table td{{border:1px solid #21262d;padding:8px;vertical-align:top;min-height:100px;height:130px;width:14.28%}}
.cal-table td:hover{{background:#1c2636}}
.cal-table .other-month{{background:#0a0e14;color:#484f58}}
.cal-table .weekend{{background:#1a1f2a;color:#484f58;text-align:center;vertical-align:middle;font-size:14px}}
.cal-table .normal-day{{text-align:right;vertical-align:top}}
.cal-table .expiry-day{{vertical-align:top;background:#1d2a1a}}
.cal-table .day-num{{text-align:right;font-size:14px;color:#8b949e;margin-bottom:6px;font-weight:bold}}
.ex-badge{{display:inline-block;background:#1f6feb;color:#fff;font-size:10px;padding:1px 6px;border-radius:3px;margin-bottom:3px;font-weight:bold}}
.ul-badge{{display:inline-block;background:#6e40c9;color:#fff;font-size:10px;padding:1px 6px;border-radius:3px;margin-bottom:3px}}
.products{{font-size:11px;color:#c9d1d9;line-height:1.6;margin-top:4px}}

/* 周列表 */
.week-block{{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:14px;margin-bottom:12px}}
.week-block.empty{{opacity:0.4}}
.week-title{{font-size:15px;color:#58a6ff;font-weight:bold;margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid #30363d}}
.week-empty{{color:#8b949e;font-size:12px;text-align:center;padding:8px}}
.summary-day{{background:#1c2129;border-radius:6px;padding:10px 12px;margin-bottom:8px}}
.summary-day:last-child{{margin-bottom:0}}
.sd-header{{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:6px}}
.sd-day{{font-weight:bold;color:#f0c000;font-size:14px}}
.sd-exchange{{background:#1f6feb;color:#fff;font-size:10px;padding:2px 8px;border-radius:3px;font-weight:bold}}
.sd-underlying{{color:#6e40c9;font-size:10px;font-weight:bold}}
.sd-products{{font-size:12px;color:#c9d1d9;line-height:1.8;padding-left:4px}}

/* 图例 */
.legend{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px;font-size:11px;color:#8b949e}}
.legend span{{display:flex;align-items:center;gap:4px}}
.legend .dot{{width:10px;height:10px;border-radius:2px;display:inline-block}}
.legend .dot.exp{{background:#1d2a1a;border:1px solid #26a641}}
.legend .dot.nor{{background:#161b22;border:1px solid #30363d}}

.error-msg{{background:#3d1a1a;border:1px solid #f85149;color:#f85149;padding:12px 16px;border-radius:8px;margin-bottom:16px;font-size:13px}}

@media(max-width:768px){{
  .cal-table td{{height:auto;padding:6px 4px}}
  .cal-table{{font-size:10px}}
  .products{{font-size:9px}}
  .ex-badge,.ul-badge{{font-size:8px}}
  h1{{font-size:16px}}
}}
</style>
</head><body>

<h1>&#x1f4c5; {title}</h1>
<p class="subtitle">&#x5404;&#x5927;&#x671f;&#x8d27;&#x4ea4;&#x6613;&#x6240;&#x5408;&#x7ea6;&#x5230;&#x671f;&#x65e5;&#x6c47;&#x603b; | &#x6570;&#x636e;&#x6e90;: &#x5929;&#x52e4;TqSdk &#x5b9e;&#x65f6;</p>

<nav>
  <a href="/">&#x1f4ca; &#x884c;&#x60c5;&#x770b;&#x677f;</a>
  <a href="/option-expiry" class="active">&#x1f4c5; &#x671f;&#x8d27;&#x5230;&#x671f;&#x8868;</a>
  <div class="month-nav">
    <a href="/option-expiry?year={prev_year}&#38;month={prev_month}">&#x25c0; &#x4e0a;&#x6708;</a>
    <span class="current">{year}&#x5e74;{month}&#x6708;</span>
    <a href="/option-expiry?year={next_year}&#38;month={next_month}">&#x4e0b;&#x6708; &#x25b6;</a>
  </div>
</nav>

{'' if not error_msg else f'<div class="error-msg">{error_msg}</div>'}

<h2>&#x1f5d3; &#x65e5;&#x5386;&#x89c6;&#x56fe;</h2>
<table class="cal-table">
  <thead><tr>{''.join(f'<th>{h}</th>' for h in weekday_headers)}</tr></thead>
  <tbody>{cal_body}</tbody>
</table>

<h2>&#x1f4cb; &#x6309;&#x65e5;&#x671f;&#x6c47;&#x603b;</h2>
{summary_body}

<p class="source-info">&#x6570;&#x636e;&#x6e90;: &#x5929;&#x52e4;TqSdk&#x5b9e;&#x65f6;&#x67e5;&#x8be2; | &#x514d;&#x8d39;&#x8d26;&#x53f7;&#x53ef;&#x7528; | &#x67e5;&#x8be2;&#x65f6;&#x95f4;: {now.strftime("%Y-%m-%d %H:%M:%S")}</p>

</body></html>'''
    
    return render_template_string(EXPIRY_HTML)

@app.route("/api/sync/futures", methods=["POST"])
def api_sync_futures():
    from services.futures_db import sync_from_tqsdk
    ok, msg = sync_service.start_sync("futures", sync_from_tqsdk, 6)
    return jsonify({"ok": ok, "msg": msg})

@app.route("/api/sync/options", methods=["POST"])
def api_sync_options():
    from services.futures_db import sync_options_from_tqsdk
    ok, msg = sync_service.start_sync("options", sync_options_from_tqsdk, 3)
    return jsonify({"ok": ok, "msg": msg})

@app.route("/api/sync/status")
def api_sync_status():
    return jsonify({
        "futures": sync_service.get_state("futures"),
        "options": sync_service.get_state("options"),
    })

def _load_expiry_cache(cache_key, year, month):
    """后台线程：查询到期数据并写入缓存"""
    import time
    try:
        result = fetch_expiry_data(client, year, month)
        with expiry_cache_lock:
            expiry_cache[cache_key] = {"status": "done", "data": result, "error": ""}
        print(f"[到期表] {cache_key} 缓存更新完成，{len(result)} 个到期日")
    except Exception as e:
        with expiry_cache_lock:
            expiry_cache[cache_key] = {"status": "error", "data": {}, "error": f"查询失败: {e}"}
        print(f"[到期表] {cache_key} 查询失败: {e}")

@app.route("/api/symbols/add")
def api_add():
    sym = request.args.get("symbol", "").strip()
    if not sym: return jsonify({"code": -1}), 400
    if sym not in DEFAULT_SYMBOLS:
        DEFAULT_SYMBOLS.append(sym)
    # 立即订阅新合约的实时行情 + 预加载K线
    if market is not None:
        threading.Thread(target=_preload_new_symbol, args=(sym,), daemon=True).start()
    return jsonify({"code": 0})

def _preload_new_symbol(sym):
    """后台预加载新合约的K线和行情（仅标记订阅，不调用 wait_update）"""
    if market is None:
        return
    try:
        print(f"[添加] {sym}: 标记预加载K线和行情...")
        market.client.get_kline(sym, 86400, 200)
        market.client.api.get_quote(sym)
        # wait_update 不能从 Flask 线程调用，由数据轮询线程统一处理
        print(f"[添加] {sym}: 已标记 (等待轮询线程推送数据)")
    except Exception as e:
        print(f"[添加] {sym}: 标记失败 ({e})")

@app.route("/api/symbols/remove")
def api_rem():
    sym = request.args.get("symbol", "").strip()
    if not sym: return jsonify({"code": -1}), 400
    if sym in DEFAULT_SYMBOLS: DEFAULT_SYMBOLS.remove(sym)
    return jsonify({"code": 0})

kline_cache = {}
kline_lock = threading.Lock()

def data_polling():
    global client, market, latest_data, kline_cache
    import time
    time.sleep(3)

    assert market is not None and client is not None

    retry = 0
    while retry < 3:
        print(f"预加载K线数据... (尝试{retry+1}/3)")
        market.preload_kline(DEFAULT_SYMBOLS, 86400, 200)
        client.api.wait_update(deadline=time.time() + 5)
        sample = market.get_kline_data(DEFAULT_SYMBOLS[0], 86400, 200)
        if not sample.empty and len(sample) > 0:
            print(f"  ✓ K线预加载完成，{DEFAULT_SYMBOLS[0]} {len(sample)} 条")
            break
        retry += 1
        print(f"  K线暂无数据，等待3秒后重试...")
        time.sleep(3)

    # 标记哪些符号已经订阅过
    subscribed_symbols = set()
    print("订阅实时行情...")
    for sym in DEFAULT_SYMBOLS:
        try:
            client.api.get_quote(sym)
            subscribed_symbols.add(sym)
            print(f"  sub: {sym}")
        except Exception as e:
            print(f"  FAIL {sym}: {e}")
    print(f"Subscribed {len(subscribed_symbols)}, polling...")
    while True:
        try:
            client.api.wait_update(deadline=time.time() + 0.8)
            # 确保所有自选合约都已订阅（包括运行时新增的）
            for sym in DEFAULT_SYMBOLS:
                if sym not in subscribed_symbols:
                    try:
                        client.api.get_quote(sym)
                        subscribed_symbols.add(sym)
                        print(f"  [动态] sub: {sym}")
                    except Exception as e:
                        print(f"  [动态] FAIL {sym}: {e}")
            quotes = market.get_realtime_quotes(DEFAULT_SYMBOLS)
            with data_lock:
                for q in quotes:
                    if "error" not in q and q.get("last_price", 0) > 0:
                        latest_data[q["symbol"]] = q
                    elif "error" in q:
                        # 如果报错"没有订阅"，尝试订阅后再取一次
                        sym = q.get("symbol", "")
                        if sym and sym not in subscribed_symbols:
                            try:
                                client.api.get_quote(sym)
                                subscribed_symbols.add(sym)
                            except Exception:
                                pass
            if len(latest_data) > 0:
                sku = list(latest_data.keys())[0]
                print(f"[{datetime.now().strftime('%H:%M:%S')}] data={len(latest_data)} sample={sku}:{latest_data[sku].get('last_price','?')}")
        except Exception as e:
            print(f"poll err: {e}")
            time.sleep(2)

def main():
    global client, market
    print("=" * 50)
    print("  TqSdk | zhubajie | http://127.0.0.1:5088")
    print("=" * 50)
    client = TqSdkClient("zhubajie", "jinhu303")
    market = MarketDataService(client)
    print("[OK] connected")
    threading.Thread(target=data_polling, daemon=True).start()
    print("[OK] polling started, opening browser...")
    app.run(host="0.0.0.0", port=5088, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()