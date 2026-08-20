"""验证 AKShare 上期所 2026年8月到期日数据"""
import os
os.environ.setdefault("NO_PROXY", "*.shfe.com.cn,*.dce.com.cn,*.czce.com.cn,*.cffex.com.cn")
import akshare as ak
import datetime

print("=" * 70)
print("上期所 2026年8月到期合约验证")
print("=" * 70)

df = ak.futures_contract_info_shfe()
print(f"总行数: {len(df)}")
print(f"列名: {list(df.columns)}")

# 解析到期日，筛选2026年8月到期
aug_2026 = []
for _, row in df.iterrows():
    dt_str = str(row.get("到期日", ""))
    code = str(row.get("合约代码", ""))
    try:
        if " " in dt_str:
            dt_str = dt_str.split(" ")[0]
        d = datetime.datetime.strptime(dt_str, "%Y-%m-%d").date()
        if d.year == 2026 and d.month == 8:
            aug_2026.append((d, code, dt_str))
    except:
        pass

print(f"\n2026年8月到期合约: {len(aug_2026)} 条")
print(f"{'日期':>12s}  {'合约代码':<15s}")
print("-" * 30)
for d, code, dt_str in sorted(aug_2026):
    print(f"{str(d):>12s}  {code:<15s}")

print("\n" + "=" * 70)
print("CZCE 2026年8月到期合约验证")
print("=" * 70)
try:
    df2 = ak.futures_contract_info_czce()
    date_col = "到期时间待国家公布2025年节假日安排后进行调整"
    aug_czce = []
    for _, row in df2.iterrows():
        dt_str = str(row.get(date_col, ""))
        code = str(row.get("合约代码", ""))
        try:
            if " " in dt_str:
                dt_str = dt_str.split(" ")[0]
            d = datetime.datetime.strptime(dt_str, "%Y-%m-%d").date()
            if d.year == 2026 and d.month == 8:
                aug_czce.append((d, code))
        except:
            pass
    print(f"2026年8月到期合约: {len(aug_czce)} 条")
    for d, code in sorted(aug_czce):
        print(f"{str(d):>12s}  {code:<15s}")
except Exception as e:
    print(f"CZCE 失败: {e}")

print("\n" + "=" * 70)
print("CFFEX 2026年8月到期合约验证")
print("=" * 70)
try:
    df3 = ak.futures_contract_info_cffex()
    aug_cffex = []
    for _, row in df3.iterrows():
        dt_str = str(row.get("最后交易日", ""))
        code = str(row.get("合约代码", ""))
        try:
            d = datetime.datetime.strptime(dt_str, "%Y-%m-%d").date()
            if d.year == 2026 and d.month == 8:
                aug_cffex.append((d, code))
        except:
            pass
    print(f"2026年8月到期合约: {len(aug_cffex)} 条")
    for d, code in sorted(aug_cffex):
        print(f"{str(d):>12s}  {code:<15s}")
except Exception as e:
    print(f"CFFEX 失败: {e}")