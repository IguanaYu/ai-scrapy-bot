# Jiaomei Scrapy 爬虫集合

## 项目简介
Jiaomei 爬虫项目是一组围绕商务部价格网、CPCA 汽车数据以及安居客等垂直站点的采集脚本。项目基于 Scrapy，结合自定义 Selenium + CDP 中间件，用于模拟真实浏览器行为并抓取动态接口，适合在数据监测与行情分析场景下复用。

爬虫的默认输出为 PostgreSQL，提供完善的字段映射与批量写入控制；同时也可以按需导出 JSON 文件或关闭数据库管道，以便调试与离线分析。

## 主要特性
- **Scrapy + Selenium CDP**：通过 `SeleniumCdpMiddleware` 捕获动态页面和 XHR 响应，可执行自定义动作序列并自动保存调试快照。
- **PostgreSQL 管道**：`PostgresPipeline` 支持字段映射、静态列、批量插入、冲突更新、使用既有表及 `_pg_table`、`_pg_skip_pg` 等动态控制。
- **参数化 Spider**：核心爬虫均支持 `-a` 参数覆盖检索条件（如时间范围、seqno、页大小等），便于批量任务调度。
- **调试资产**：启用 `SELENIUM_DEBUG_ARTIFACTS` 后会在 `debug_artifacts/` 留存截图与 HTML，排查定位更轻松。

## 仓库结构
```
.
|-- scrapy.cfg                   # Scrapy 项目入口
|-- jiaomei/
|   |-- settings.py              # 全局配置（限速、Selenium、PG 等）
|   |-- middlewares.py           # Selenium CDP 中间件与辅助工具
|   |-- pg_pipeline.py           # PostgreSQL 写入管道
|   |-- pipelines.py             # 占位管道（未启用）
|   `-- spiders/                 # 站点 Spider 集合
|-- debug_artifacts/             # Selenium 调试输出（按需生成）
|-- outputs/                     # 部分 Spider 的本地导出目录
|-- *.json                       # 运行样例数据
`-- README.md
```

## 环境准备
### 1. Python 与依赖
- 推荐 Python 3.10 及以上版本。
- 建议在项目根目录创建虚拟环境：
  ```powershell
  python -m venv .venv
  .\.venv\Scripts\Activate.ps1
  ```
- 安装基础依赖：
  ```powershell
  pip install --upgrade pip
  pip install scrapy selenium webdriver-manager psycopg[binary] itemadapter
  ```
  如需导出到 JSON/CSV，可额外安装 `pandas`、`orjson` 等库。

### 2. 浏览器与驱动
- 默认使用本地 Chrome/Chromium，需保证浏览器可执行文件存在于系统 PATH。
- 中间件默认启用 `webdriver-manager` 自动下载驱动；如需离线运行，可在 `settings.py` 中将 `SELENIUM_USE_WDM=False` 并自行配置 `chromedriver`。

## 配置说明
### PostgreSQL 输出
- 默认 DSN：`postgresql://myj_user:123456@10.7.14.201:5432/myj_db`（请在实际部署前通过环境变量或 `-s PG_DSN=...` 覆盖）。
- 也可分别设置 `PG_HOST`、`PG_PORT`、`PG_DB`、`PG_USER`、`PG_PASSWORD`。
- 重要开关：
  - `PG_USE_EXISTING_TABLE=True`：要求目标表已存在，并只写入既有列。
  - `PG_STRICT_COLUMNS=True`：忽略未出现在表中的字段。
  - `PG_BATCH_SIZE`：批量提交大小（默认 50）。
  - `PG_UPSERT_KEYS`：冲突键集合，开启后自动生成 `ON CONFLICT` 语句。
- Spider 层可通过 `pg_pipeline` 字典或同名属性覆盖：`pg_table`、`pg_field_map`、`pg_static_fields`、`pg_upsert_keys` 等。
- Item 级别控制：
  - `item["_pg_table"]`：将当前记录指向新的表名。
  - `item["_pg_skip_pg"]`（或 `_pg_skip`）：跳过数据库写入。

### Selenium 与调试
- 核心配置位于 `settings.py`：
  - `SELENIUM_HEADLESS`：是否开启无头模式（开发期可设为 `False` 观察交互）。
  - `SELENIUM_WAIT`：页面加载与动作默认等待秒数。
  - `SELENIUM_XHR_KEYWORD`：用于匹配感兴趣的接口路径。
  - `SELENIUM_DEBUG_ARTIFACTS`：保存截图与 HTML 到 `debug_artifacts/`。
- Request 元信息常用键：
  - `selenium=True` 触发 Selenium 渲染。
  - `xhr_keyword`、`xhr_keywords`：覆盖关键字匹配。
  - `preheat_root=True`：先访问站点首页以加载 Cookie/Referer。
  - `selenium_actions=[...]`：执行等待、脚本注入、滚动等动作。
- 渲染后的响应包含：
  - `response.meta["xhr_payloads"]`：捕获到的 XHR 列表（`url` + `body`）。
  - `debug_artifacts/` 中的截图与 HTML 便于回放页面状态。

## 运行爬虫
基础格式：
```
scrapy crawl <spider_name> [options]
```
常用参数包括 `-a key=value`（传入 spider 参数）、`-O file.json`（导出 JSON）、`-s NAME=VALUE`（覆盖设置）。

### 主要 Spider 列表
| Spider | 数据来源 | 功能简介 | 常用参数 / 输出 |
| --- | --- | --- | --- |
| `aluminium_price` | price.mofcom.gov.cn | 直连官方接口抓取指定 `seqno` 的铝价历史，自动去重并写入 `zonal_crawler_aluminium_price`。 | `-a seqno=289 -a start=2025-01-01 -a end=2025-03-31`；可用 `-O outputs/aluminium_price.json` 导出。 |
| `iron_ore_page` | price.mofcom.gov.cn | Selenium 渲染 + XHR 合并，适合需要对照页面表格的铁矿石行情。 | 支持 `-a use_selenium=0` 改为纯请求；默认写入 `zonal_crawler_iron_ore_price`。 |
| `iron_ore_api` / `mei_api` | price.mofcom.gov.cn | 直接分页 POST 接口，适合全量拉取（可通过 `-a pro_name=焦煤` 等切换品类）。 | `-a startTime=2024-01-01 -a endTime=2024-12-31 -a page_size=50`。 |
| `price2` | price.mofcom.gov.cn | 焦煤价格页的增强版：将页面表格与接口 JSON 合并，补齐详情链接。 | 需启用 `selenium=True`，默认映射至 `zonal_crawler_coking_coal_price`。 |
| `magnesium_mofcom` | price.mofcom.gov.cn | 氧化镁数据接口抓取，默认输出 `outputs/magnesium_mofcom.json` 并写入 `zonal_crawler_magnesium_price`。 | `-a seqno=350 -a start_time=2024-01-01 -a end_time=2024-12-31 -a page_size=100`。 |
| `thermal_coal_mofcom` | price.mofcom.gov.cn | 热煤行情采集，逻辑与铁矿石类似，支持 API/页面双策略。 | 可用 `-O outputs/thermal_coal.json` 导出调试。 |
| `car_total_market` | data.cpcadata.com | CPCA 全市场图表接口解析，拆分产量/批发/零售/出口指标，支持按指标映射不同 PG 表。 | 默认写入 `car_total_market.json`；在 item 中写入 `_pg_table`/`_pg_skip_pg` 控制落库。 |
| `anjuke_shanxi_price` | mobile.anjuke.com | 安居客山西省城市房价月度数据，支持多年份批量抓取。 | 可通过 `-a latest_year=2025 -a city_limit=5` 控制范围；默认落表 `zonal_crawler_house_price`。 |

> 其他试验/备份脚本（如 `jiaomei1.py`, `jiaomei222.py`）保留在 `spiders/` 中，可参考其写法扩展新的品类。

### 示例命令
```powershell
# 导出铝价数据到本地 JSON，并关闭 PG 管道
scrapy crawl aluminium_price -a seqno=289 -O outputs/aluminium_price.json -s ITEM_PIPELINES={}

# 拉取铁矿石 API 数据到自建数据库
scrapy crawl iron_ore_api -a startTime=2024-01-01 -a endTime=2024-12-31 -s PG_DSN="postgresql://user:pass@host:5432/db"

# 捕获 CPCA 全市场数据（保留 Selenium 调试工件）
scrapy crawl car_total_market -s SELENIUM_HEADLESS=False -s SELENIUM_DEBUG_ARTIFACTS=True
```

## 调试与实践建议
- 充分利用 `scrapy shell <url>` 复现选择器或接口响应。
- 检查 `response.meta["xhr_payloads"]`；必要时用 `json.loads()` 找到真正的业务列表键。
- 若远端 PG 不可用，可用 `-s ITEM_PIPELINES={}` 或临时修改 `settings.py` 禁用数据库写入。
- 批量任务建议调整 `CONCURRENT_REQUESTS`、`DOWNLOAD_DELAY`、`AUTOTHROTTLE_*` 以平衡速度与稳定性。
- `debug_artifacts/` 产生的文件较大，定期清理或设置 `SELENIUM_DEBUG_ARTIFACTS=False`。

## 数据产出与后续处理
- 默认写入的字段编码为 UTF-8，数值字段在管道中自动转换为 `float`/`Json` 类型。
- 表结构需提前在 PG 中创建；也可以将 `PG_USE_EXISTING_TABLE` 设为 `False` 让管道根据首批样本推断列并自动建表。
- 若需与其他系统联动，可在 spider 中补充 `pg_static_fields`（例如 `source`, `region`）或自行扩展 item。

欢迎根据业务需要扩展新的 Spider，如沿用 `pg_pipeline` 配置即可快速接入更多行情数据源。
