# -*- coding: utf-8 -*-
"""期权同步修复版 - 使用批量订阅，分批处理避免卡死"""
import sys,os,time,datetime,math,re
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))
os.environ['NO_PROXY']='shinnytech.com'
from tqsdk import TqApi,TqAuth
import pymysql

DB={"host":"192.168.0.108","port":3306,"user":"zhq","password":"zhq","database":"d_lnx_tqhq","charset":"utf8"}
EX={"SHFE":"上期所","DCE":"大商所","CZCE":"郑商所","CFFEX":"中金所","INE":"上期能源"}

a=TqApi(auth=TqAuth("zhubajie","jinhu303"))
opts=a.query_quotes(ins_class="OPTION",expired=False)
print(f"期权总数: {len(opts)}")

# 按月份分组
by_m={}
for s in opts:
    parts=s.split("."); 
    if len(parts)<2: continue
    n=re.findall(r'\d+',parts[1])
    if not n: continue
    ms=n[-1]
    if len(ms) in (3,4): by_m.setdefault(ms,[]).append(s)

# 确定要同步的月份
now=datetime.datetime.now()
targets=[]
for i in range(3):
    m=now.month+i; y=now.year
    if m>12: m-=12; y+=1
    targets.append(f"{str(y)[-2:]}{m:02d}")

print(f"目标月份: {targets}")
total=0
conn=pymysql.connect(**DB)
cur=conn.cursor()

for ms in targets:
    batch=by_m.get(ms,[])
    if not batch: continue
    print(f"\n{ms}: {len(batch)} 个期权, 批量订阅...")
    
    # 分批订阅 (每批500个)
    BS=500
    for i in range(0,len(batch),BS):
        sub=batch[i:i+BS]
        try:
            qs=a.get_quote_list(sub)
            a.wait_update(deadline=time.time()+15)
        except Exception as e:
            print(f"  批量订阅失败: {e}, 跳过此批")
            continue
        
        recs=[]
        for s,q in zip(sub,qs):
            if getattr(q,'expired',False): continue
            lexp=getattr(q,'last_exercise_datetime',0)
            if not lexp or lexp<=0: continue
            d=datetime.datetime.fromtimestamp(lexp).strftime("%Y-%m-%d")
            ex=s.split(".")[0] if "." in s else ""
            recs.append((s,ex,EX.get(ex,ex),getattr(q,'underlying_symbol',''),getattr(q,'option_class',''),float(getattr(q,'strike_price',0) or 0),ms,d,"last_exercise"))
        
        if recs:
            cur.executemany("""INSERT INTO t_option_expiry(symbol,exchange,exchange_name,underlying,option_class,strike_price,contract_month,exercise_date,data_source,create_time) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) ON DUPLICATE KEY UPDATE exercise_date=VALUES(exercise_date)""",recs)
            conn.commit()
            total+=len(recs)
        print(f"  [{ms}] 批{i//BS+1}: +{len(recs)} 条 (累计 {total})")

conn.close()
a.close()
print(f"\n全部完成! 共入库 {total} 条")