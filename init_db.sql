SET NAMES utf8;

DROP TABLE IF EXISTS t_futures_expiry;
CREATE TABLE t_futures_expiry (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    symbol VARCHAR(32) NOT NULL COMMENT '合约代码',
    exchange VARCHAR(16) NOT NULL COMMENT '交易所代码',
    exchange_name VARCHAR(32) NOT NULL COMMENT '交易所中文名称',
    product_code VARCHAR(8) NOT NULL COMMENT '品种代码',
    product_name VARCHAR(32) NOT NULL COMMENT '品种中文名称',
    contract_month VARCHAR(6) NOT NULL COMMENT '合约月份',
    expire_date DATE NOT NULL COMMENT '到期日期',
    data_source VARCHAR(16) DEFAULT 'expire_datetime' COMMENT '数据来源',
    create_time DATETIME COMMENT '创建时间',
    UNIQUE KEY uk_symbol (symbol),
    KEY idx_month (contract_month),
    KEY idx_expire_date (expire_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='期货到期日表';

DROP TABLE IF EXISTS t_product_info;
CREATE TABLE t_product_info (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    exchange VARCHAR(16) NOT NULL COMMENT '交易所代码',
    exchange_name VARCHAR(32) NOT NULL COMMENT '交易所中文名称',
    product_code VARCHAR(8) NOT NULL COMMENT '品种代码',
    product_name VARCHAR(32) NOT NULL COMMENT '品种中文名称',
    UNIQUE KEY uk_product (exchange, product_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='期货品种中文名称对照表';

INSERT IGNORE INTO t_product_info (exchange, exchange_name, product_code, product_name) VALUES
('SHFE','上期所','cu','沪铜'),('SHFE','上期所','al','沪铝'),('SHFE','上期所','zn','沪锌'),('SHFE','上期所','pb','沪铅'),
('SHFE','上期所','au','沪金'),('SHFE','上期所','ag','沪银'),('SHFE','上期所','rb','螺纹钢'),('SHFE','上期所','wr','线材'),
('SHFE','上期所','hc','热卷'),('SHFE','上期所','ss','不锈钢'),('SHFE','上期所','ru','橡胶'),('SHFE','上期所','bu','沥青'),
('SHFE','上期所','fu','燃油'),('SHFE','上期所','ao','氧化铝'),('SHFE','上期所','br','丁二烯橡胶'),('SHFE','上期所','ni','沪镍'),
('SHFE','上期所','sn','沪锡'),('SHFE','上期所','sp','纸浆'),
('DCE','大商所','a','豆一'),('DCE','大商所','b','豆二'),('DCE','大商所','c','玉米'),('DCE','大商所','cs','淀粉'),
('DCE','大商所','i','铁矿石'),('DCE','大商所','j','焦炭'),('DCE','大商所','jm','焦煤'),('DCE','大商所','l','塑料'),
('DCE','大商所','m','豆粕'),('DCE','大商所','p','棕榈油'),('DCE','大商所','v','PVC'),('DCE','大商所','y','豆油'),
('DCE','大商所','eg','乙二醇'),('DCE','大商所','eb','苯乙烯'),('DCE','大商所','pp','聚丙烯'),('DCE','大商所','pg','液化气'),
('DCE','大商所','lh','生猪'),('DCE','大商所','rr','粳米'),('DCE','大商所','fb','纤维板'),
('CZCE','郑商所','SR','白糖'),('CZCE','郑商所','TA','PTA'),('CZCE','郑商所','MA','甲醇'),('CZCE','郑商所','FG','玻璃'),
('CZCE','郑商所','CF','棉花'),('CZCE','郑商所','RM','菜粕'),('CZCE','郑商所','OI','菜油'),('CZCE','郑商所','SF','硅铁'),
('CZCE','郑商所','SM','锰硅'),('CZCE','郑商所','SA','纯碱'),('CZCE','郑商所','UR','尿素'),('CZCE','郑商所','PF','短纤'),
('CZCE','郑商所','PR','瓶片'),('CZCE','郑商所','SH','烧碱'),('CZCE','郑商所','CJ','红枣'),('CZCE','郑商所','AP','苹果'),
('CZCE','郑商所','CY','棉纱'),('CZCE','郑商所','WH','强麦'),('CZCE','郑商所','PM','普麦'),('CZCE','郑商所','PX','对二甲苯'),
('CFFEX','中金所','IF','沪深300'),('CFFEX','中金所','IC','中证500'),('CFFEX','中金所','IH','上证50'),('CFFEX','中金所','IM','中证1000'),
('INE','上期能源','sc','原油'),('INE','上期能源','bc','国际铜'),('INE','上期能源','lu','低硫燃料油'),('INE','上期能源','nr','20号胶');