import os

BOT_NAME = "jiaomei"

SPIDER_MODULES = ["jiaomei.spiders"]
NEWSPIDER_MODULE = "jiaomei.spiders"

ROBOTSTXT_OBEY = False
COOKIES_ENABLED = True

# 降速更像真�?
CONCURRENT_REQUESTS = 2
DOWNLOAD_DELAY = 1.2
RANDOMIZE_DOWNLOAD_DELAY = True
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.5
AUTOTHROTTLE_MAX_DELAY = 10

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

RETRY_ENABLED = True
RETRY_TIMES = 2
RETRY_HTTP_CODES = [403, 429, 503]

DOWNLOADER_MIDDLEWARES = {
    "jiaomei.middlewares.SeleniumCdpMiddleware": 543,
}

# Selenium 相关参数（可按需调）
SELENIUM_HEADLESS = False           # 调试期建议可见浏览器；稳定后�?True
SELENIUM_WAIT = 3                   # 页面加载等待秒数
SELENIUM_XHR_KEYWORD = "pricequotation/priceQuery"  # 用于匹配接口URL片段
SELENIUM_DEBUG_ARTIFACTS = True     # 保存截图与HTML快照，便于排�?

LOG_LEVEL = "INFO"
FEED_EXPORT_ENCODING = "utf-8"



# pgsql配置
ITEM_PIPELINES = {
    "jiaomei.pg_pipeline.PostgresPipeline": 300,
}

PG_DSN = os.getenv("PG_DSN", "postgresql://myj_user:123456@10.7.14.201:5432/myj_db")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")  # 默认�?public，可按需覆盖
PG_TABLE = None  # 交由 spider 定义表名，默认使�?spider.name
PG_USE_EXISTING_TABLE = True
PG_STRICT_COLUMNS = True

# 将字段映射、静态列等留空，由具�?spider 覆盖
PG_FIELD_MAP = {}
PG_STATIC_FIELDS = {}

PG_UPSERT_KEYS = []
PG_CREATE_INDEX_ON_UPSERT_KEYS = False
PG_BATCH_SIZE = 50
