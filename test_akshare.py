"""
测试 AKShare 期货合约信息接口
"""
import os
os.environ["NO_PROXY"] = "*.dce.com.cn,*.shfe.com.cn,*.czce.com.cn,*.cffex.com.cn"

import akshare as ak

exchanges = [
    ("DCE", "大商所", "futures_contract_info_dce"),
    ("SHFE", "上期所", "futures_contract_info_shfe"),
    ("CZCE", "郑商所", "futures_contract_info_czce"),
    ("CFFEX", "中金所", "futures_contract_info_cffex"),
]

for code, name, func_name in exchanges:
    try:
        func = getattr(ak, func_name)
        df = func()
        print(f"\n{'='*60}")
        print(f"{name} ({code}) - 行数: {len(df)}")
        print(f"列名: {list(df.columns)[:12]}")
        # 找到期相关列
        expiry_cols = [c for c in df.columns if any(w in str(c) for w in ["交易日","到期","交割","最后"])]
        print(f"到期相关列: {expiry_cols}")
        if expiry_cols:
            print(df[expiry_cols].head(8).to_string())
        else:
            print("前3行:")
            print(df.head(3).to_string())
    except Exception as e:
        print(f"\n{name} ({code}) - FAIL: {type(e).__name__}: {str(e)[:180]}")