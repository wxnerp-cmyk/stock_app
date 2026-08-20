# -*- coding: utf-8 -*-
"""重建期权表，确保中文注释正确"""
import pymysql
c = pymysql.connect(host='192.168.0.108', port=3306, user='zhq', password='zhq',
                     database='d_lnx_tqhq', charset='utf8')
cur = c.cursor()
cur.execute("SET NAMES utf8")
cur.execute("DROP TABLE IF EXISTS t_option_expiry")
# 建表
sql = (
    "CREATE TABLE t_option_expiry ("
    "  id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',"
    "  symbol VARCHAR(32) NOT NULL COMMENT '合约代码',"
    "  exchange VARCHAR(16) NOT NULL COMMENT '交易所代码',"
    "  exchange_name VARCHAR(32) NOT NULL COMMENT '交易所中文名称',"
    "  underlying VARCHAR(32) NOT NULL COMMENT '标的合约',"
    "  option_class VARCHAR(8) NOT NULL COMMENT '看涨看跌',"
    "  strike_price DECIMAL(10,2) NOT NULL COMMENT '行权价',"
    "  contract_month VARCHAR(6) NOT NULL COMMENT '合约月份',"
    "  exercise_date DATE NOT NULL COMMENT '最后行权日',"
    "  data_source VARCHAR(16) DEFAULT 'last_exercise' COMMENT '数据来源',"
    "  create_time DATETIME COMMENT '创建时间',"
    "  UNIQUE KEY uk_symbol (symbol),"
    "  KEY idx_month (contract_month),"
    "  KEY idx_exercise (exercise_date)"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '期权行权日表'"
)
cur.execute(sql)
c.commit()

# 验证
print("=== 期权表字段注释 ===")
cur.execute("SHOW FULL COLUMNS FROM t_option_expiry")
for r in cur.fetchall():
    print(f"  {r[0]:20s} -> {r[8]}")

c.close()
print("\nDone - 期权表已重建, 注释正确!")