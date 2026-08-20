"""测试天勤 TqSdk 获取期货合约到期日 — 2609 合约"""
from tqsdk import TqApi, TqAuth
import time, datetime

api = TqApi(auth=TqAuth("zhubajie", "jinhu303"))

# 各交易所 2609 合约
symbols = [
    "SHFE.cu2609","SHFE.rb2609","SHFE.ag2609","SHFE.al2609","SHFE.zn2609",
    "SHFE.pb2609","SHFE.au2609","SHFE.ru2609","SHFE.fu2609","SHFE.ao2609",
    "SHFE.br2609","SHFE.ni2609","SHFE.sn2609","SHFE.hc2609","SHFE.ss2609",
    "SHFE.wr2609","SHFE.sp2609",
    "DCE.i2609","DCE.m2609","DCE.j2609","DCE.p2609","DCE.y2609",
    "DCE.a2609","DCE.jm2609","DCE.c2609","DCE.cs2609","DCE.eg2609",
    "DCE.eb2609","DCE.pp2609","DCE.v2609","DCE.l2609","DCE.b2609",
    "DCE.rr2609","DCE.pg2609","DCE.lh2609","DCE.fb2609",
    "CZCE.SR609","CZCE.TA609","CZCE.MA609","CZCE.FG609",
    "CZCE.CF609","CZCE.RM609","CZCE.OI609","CZCE.SF609",
    "CZCE.SM609","CZCE.SA609","CZCE.UR609","CZCE.PF609",
    "CZCE.PR609","CZCE.SH609","CZCE.AP609","CZCE.CJ609",
    "CZCE.CY609","CZCE.LR609","CZCE.PM609","CZCE.RI609",
    "CZCE.JR609","CZCE.WH609","CZCE.ZC609","CZCE.ER609",
    "CZCE.GN609","CZCE.RS609","CZCE.SM609","CZCE.RO609",
    "CFFEX.IF2609","CFFEX.IC2609","CFFEX.IH2609","CFFEX.IM2609",
    "INE.sc2609","INE.bc2609","INE.lu2609","INE.nr2609",
]

quotes = {}
for sym in symbols:
    try:
        quotes[sym] = api.get_quote(sym)
    except Exception as e:
        print(f"  [跳过] {sym}: {e}")

print("等待行情数据...")
api.wait_update(deadline=time.time() + 5)

print("\n===== 各交易所 2609 合约到期日 =====")
exchanges = {"SHFE": "上期所", "DCE": "大商所", "CZCE": "郑商所", "CFFEX": "中金所", "INE": "上期能源"}
by_date = {}
for sym, q in sorted(quotes.items()):
    try:
        name = q.instrument_name if hasattr(q, 'instrument_name') else sym
        exp = q.expire_datetime if hasattr(q, 'expire_datetime') else None
        if exp and exp > 0:
            d = datetime.datetime.fromtimestamp(exp)
            d_str = d.strftime("%Y-%m-%d")
            ex = sym.split(".")[0]
            ex_name = exchanges.get(ex, ex)
            by_date.setdefault(d_str, []).append((ex_name, name, sym))
            print(f"{sym:20s} {name:12s} 到期: {d.strftime('%Y-%m-%d %H:%M')}  {ex_name}")
        else:
            print(f"{sym:20s} {name:12s} 到期: 无数据")
    except Exception as e:
        print(f"{sym:20s} 获取失败: {e}")

print("\n===== 按到期日汇总 =====")
for d in sorted(by_date.keys()):
    items = by_date[d]
    # 按交易所分组
    by_ex = {}
    for ex_name, name, sym in items:
        by_ex.setdefault(ex_name, []).append(name)
    
    print(f"\n{d} (共{len(items)}个合约)")
    for ex_name in sorted(by_ex.keys()):
        names = ", ".join(sorted(by_ex[ex_name]))
        print(f"  [{ex_name}] {names}")

api.close()