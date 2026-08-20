# 期货行情系统 - 开发文档

## 1. 技术栈

| 技术 | 版本 | 用途 |
|------|------|------|
| Python | 3.8 | 后端语言 |
| Flask | 3.0.3 | Web 框架 |
| tqsdk | 3.10.0 | 天勤量化（行情 + 到期日） |
| PyMySQL | 1.1.1 | MySQL 驱动 |
| pandas | 2.0.3 | 数据处理 |
| MySQL | 8.0 | 数据库（端口 3304） |

## 2. 环境准备

### 2.1 创建虚拟环境

```bash
cd 项目根目录
python -m venv venv
```

### 2.2 安装依赖

```bash
venv\Scripts\activate          # Windows
source venv/bin/activate       # Linux/Mac

pip install tqsdk>=3.0.0 flask>=2.0 pandas>=1.3 pymysql
```

### 2.3 环境变量

```bash
# 天勤直连，绕过代理
NO_PROXY=shinnytech.com,.shinnytech.com
```

## 3. 数据库（MySQL 8.0）

### 3.1 连接信息

| 参数 | 值 |
|------|-----|
| 地址 | `192.168.0.108` |
| 端口 | `3304` |
| 用户名 | `zhq` |
| 密码 | `zhq` |
| 数据库 | `d_lnx_tqhq` |
| 字符集 | `utf8` |
| 排序规则 | `utf8_general_ci` |

### 3.2 数据表

```sql
-- 期货到期日表
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
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UNIQUE KEY uk_symbol (symbol),
    KEY idx_month (contract_month),
    KEY idx_expire_date (expire_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='期货到期日表';

-- 期权行权日表
CREATE TABLE t_option_expiry (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    symbol VARCHAR(32) NOT NULL COMMENT '合约代码',
    exchange VARCHAR(16) NOT NULL COMMENT '交易所代码',
    exchange_name VARCHAR(32) NOT NULL COMMENT '交易所中文名称',
    underlying VARCHAR(32) NOT NULL COMMENT '标的合约',
    option_class VARCHAR(8) NOT NULL COMMENT '看涨看跌',
    strike_price DECIMAL(10,2) NOT NULL COMMENT '行权价',
    contract_month VARCHAR(6) NOT NULL COMMENT '合约月份',
    exercise_date DATE NOT NULL COMMENT '最后行权日',
    data_source VARCHAR(16) DEFAULT 'last_exercise' COMMENT '数据来源',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UNIQUE KEY uk_symbol (symbol),
    KEY idx_month (contract_month),
    KEY idx_exercise (exercise_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='期权行权日表';

-- 品种中文名称对照表
CREATE TABLE t_product_info (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    exchange VARCHAR(16) NOT NULL COMMENT '交易所代码',
    exchange_name VARCHAR(32) NOT NULL COMMENT '交易所中文名称',
    product_code VARCHAR(8) NOT NULL COMMENT '品种代码',
    product_name VARCHAR(32) NOT NULL COMMENT '品种中文名称',
    UNIQUE KEY uk_product (exchange, product_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='期货品种中文名称对照表';
```

> **MySQL 8.0 注意事项**：
> - 字符集使用 `utf8mb4`（区别于 5.x 的 `utf8`）
> - `CURRENT_TIMESTAMP` 支持多个列使用，5.x 有限制
> - 身份验证插件默认 `caching_sha2_password`，PyMySQL 兼容

### 3.3 执行建库建表

```bash
mysql -h 192.168.0.108 -P 3304 -u zhq -pzhq < init_db.sql
```

## 4. 天勤 TqSdk

### 4.1 账号

| 参数 | 值 |
|------|-----|
| 用户名 | `zhubajie` |
| 密码 | `jinhu303` |
| 服务器 | `free-api.shinnytech.com` |
| 文档 | https://doc.shinnytech.com/tqsdk/latest/ |

### 4.2 核心 API

```python
from tqsdk import TqApi, TqAuth

api = TqApi(auth=TqAuth("zhubajie", "jinhu303"))

# 获取所有未过期期货合约
futures = api.query_quotes(ins_class="FUTURE", expired=False)

# 获取所有未过期期权合约
options = api.query_quotes(ins_class="OPTION", expired=False)

# 获取合约行情（含到期日）
quote = api.get_quote("SHFE.cu2609")

# 期货到期日
quote.expire_datetime          # float: timestamp 秒
quote.delivery_year             # int
quote.delivery_month            # int
quote.expired                   # bool

# 期权最后行权日
quote.last_exercise_datetime    # float: timestamp 秒
quote.exercise_year             # int
quote.exercise_month            # int
quote.option_class              # "CALL" / "PUT"
quote.strike_price              # float
quote.underlying_symbol         # str: 标的合约
```

### 4.3 注意事项

> **代理设置**：天勤必须直连，不走公司代理
> ```python
> import os
> os.environ["NO_PROXY"] = "shinnytech.com,.shinnytech.com"
> ```

> **免费账号限制**：
> - 无频率上限，但 get_quote 逐个调用会累积 wait_update 延时
> - 批量订阅推荐使用 `get_quote_list(symbols)` 一次拉取
> - 过期合约必须设置 `expired=False` 过滤

## 5. 项目结构

```
项目根目录/
├── main.py                  # Flask 主入口 + 路由
├── sync_options_fix.py      # 期权同步脚本（批量订阅版）
├── init_db.sql              # 建库建表 SQL
├── requirements.txt         # pip 依赖
├── DEV.md                   # 本文件
├── start.bat                # Windows 启动脚本
├── services/
│   ├── __init__.py
│   ├── futures_db.py        # MySQL 数据访问层（增删改查 + 同步函数）
│   ├── option_expiry.py     # 到期表查询逻辑
│   ├── sync_service.py      # 同步状态管理 + 定时器
│   ├── market_data.py       # 实时行情服务
│   └── history_kline.py     # 历史K线服务
├── core/
│   ├── __init__.py
│   └── tqsdk_client.py      # 天勤 TqSdk 客户端封装
└── venv/                    # Python 虚拟环境
```

## 6. 启动服务

### 6.1 启动命令

```bash
# Windows
venv\Scripts\activate
python main.py

# Linux/Mac
source venv/bin/activate
python main.py
```

### 6.2 访问地址

| 页面 | URL |
|------|-----|
| 行情看板 | `http://127.0.0.1:5088/` |
| 期货到期表 | `http://127.0.0.1:5088/option-expiry` |
| 期权行权日 | `http://127.0.0.1:5088/option-exercise` |

## 7. 数据同步

### 7.1 自动同步

服务启动后自动开启 12 小时定时同步（后台线程，不阻塞请求）。

### 7.2 手动同步

各页面提供"同步数据"按钮，点击后：
- 后台线程启动同步
- 显示进度："已拉取 X/Y"
- 同步中禁止重复触发
- 每 3 秒自动刷新进度

### 7.3 API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/sync/futures` | 手动触发期货同步 |
| POST | `/api/sync/options` | 手动触发期权同步 |
| GET | `/api/sync/status` | 查询同步进度 |

### 7.4 单独同步脚本

```bash
# 同步期货（当前+未来6个月）
python -c "import sys; sys.path.insert(0,'.'); from services.futures_db import sync_from_tqsdk; sync_from_tqsdk()"

# 同步期权（当前+未来3个月）
python -c "import sys; sys.path.insert(0,'.'); from services.futures_db import sync_options_from_tqsdk; sync_options_from_tqsdk()"
```

## 8. 注意事项

### 8.1 数据库切换（MySQL 5.5 → 8.0）

| 差异点 | 5.5 | 8.0 |
|--------|-----|-----|
| 字符集 | `utf8` (= utf8mb3) | `utf8mb4`（推荐） |
| 多个 CURRENT_TIMESTAMP | 仅第一列可用 | 支持多个列 |
| 端口 | `3306` | `3304` |
| 身份验证 | `mysql_native_password` | `caching_sha2_password` |

迁移时需修改 `services/futures_db.py` 中 `DB_CONFIG` 的 `port` 为 `3304`。

### 8.2 天勤连接

- **必须设置 `NO_PROXY`** 环境变量，否则在公司网络下无法连接
- **免费账号不支持同时多连接**，同步和行情轮询不要同时进行
- **`get_quote` 逐个调用很慢**，大量合约建议批量订阅

### 8.3 期权同步

- 期权数量巨大（26708 个），**必须分批注册**（每批200-500个）
- 不要一次性 `get_quote_list(全部)` ，不存在合约会导致整批失败
- 使用 `get_quote()` + `try/except` + 统一 `wait_update()` 的方式

### 8.4 多开发者协作

- 共享同一数据库 `d_lnx_tqhq`
- 表命名建议加模块前缀区分（如 `t_futures_`、`t_option_`）
- 路由前缀按功能命名（如 `/futures/`、`/analytics/`），不按人名

### 8.5 虚拟环境

- 不要提交 `venv/` 到 Git
- 统一使用 `requirements.txt` 管理依赖
- 各开发者自行创建虚拟环境并安装依赖