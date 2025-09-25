# thermal_coal_mofcom.py
# 用途：
# - 动力煤（seqno=387）的两种抓取方式：
#   1) ThermalCoalPageSpider：打开详情页 + 你的 selenium/xhr 中间件抓 XHR(JSON)。
#   2) ThermalCoalApiSpider：直连 priceQueryList 接口，自动翻页直到 maxPageNum。
# - 字段：商品名称 / 交易时间 / 规格 / 地区 / 单位名称 / 价格 / 详情链接 / _source / _page
# - 严格不使用 emoji。

import json
from urllib.parse import urlencode

import scrapy


THERMAL_COAL_PG_FIELD_MAP = {
    "交易时间": "date",
    "价格": "price",
    "详情链接": "datasourcelink",
}

# ---------------------------
# 通用工具
# ---------------------------

def _guess_rows_from_json(data):
    if not isinstance(data, dict):
        return []
    # 常见容器字段
    for k in ["rows", "list", "records", "items", "datas", "dataList"]:
        v = data.get(k)
        if isinstance(v, list):
            return v
    # 内层包装
    for k in ["data", "result", "page", "content", "payload", "body"]:
        v = data.get(k)
        if isinstance(v, dict):
            for kk in ["rows", "list", "records", "items", "datas", "dataList"]:
                vv = v.get(kk)
                if isinstance(vv, list):
                    return vv
    # 兜底：返回第一个 list 值
    for v in data.values():
        if isinstance(v, list):
            return v
    return []


def _pick(d, keys, default=""):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


# ---------------------------
# 路线 1：详情页 + XHR
# ---------------------------

class ThermalCoalPageSpider(scrapy.Spider):
    name = "thermal_coal_page"
    allowed_domains = ["price.mofcom.gov.cn"]

    pg_pipeline = {
        "pg_table": "zonal_crawler_thermal_coal_price",
        "pg_field_map": THERMAL_COAL_PG_FIELD_MAP,
        "pg_static_fields": {"source": "thermal_coal_page"},
    }

    def __init__(self, seqno=387, use_selenium=1, *args, **kwargs):
        """
        seqno: 详情页的序号，默认 387（动力煤）
        use_selenium: 是否启用你已有的 selenium / xhr 中间件（1/0）
        """
        super().__init__(*args, **kwargs)
        self.seqno = str(seqno)
        self.detail_page_url = (
            "https://price.mofcom.gov.cn/price_2021/pricequotation/pricequotationdetail.shtml?"
            + urlencode({"seqno": self.seqno})
        )
        self.use_selenium = str(use_selenium) in ("1", "true", "True")

    def start_requests(self):
        meta = {}
        if self.use_selenium:
            # 让中间件捕获 XHR：/pricequotation/priceQueryList
            meta.update({"selenium": True, "xhr_keyword": "pricequotation/priceQueryList"})
        yield scrapy.Request(
            self.detail_page_url,
            callback=self.parse,
            meta=meta,
            headers={
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
            dont_filter=True,
        )

    def parse(self, response):
        """
        首选使用中间件注入的 xhr_json；若未捕获到，则可在此处补充 HTML fallback（详情页结构变动时再适配）。
        """
        data = response.meta.get("xhr_json")
        if not data:
            self.logger.warning("No xhr_json captured. Consider enabling selenium/xhr middleware or use API spider.")
            return

        rows = _guess_rows_from_json(data)
        page_number = int(data.get("pageNumber") or 1)

        for r in rows:
            yyyy = str(r.get("yyyy") or "").strip()
            mm = str(r.get("mm") or "").zfill(2)
            dd = str(r.get("dd") or "").zfill(2)
            date_str = "-".join([yyyy, mm, dd]) if yyyy and mm and dd else ""

            item = {
                "商品名称": str(r.get("prod_name") or "动力煤").strip(),
                "交易时间": date_str,
                "规格": str(r.get("prod_spec") or "").strip(),
                "地区": str(r.get("region") or "").strip(),
                "单位名称": str(r.get("unit") or "").strip(),
                "价格": str(r.get("price") or "").replace(",", "").strip(),
                "详情链接": self.detail_page_url,
                "_page": page_number,
                "_source": "xhr",
            }
            yield item


# ---------------------------
# 路线 2：直连 API 自动翻页
# ---------------------------

class ThermalCoalApiSpider(scrapy.Spider):
    name = "thermal_coal_api"
    allowed_domains = ["price.mofcom.gov.cn"]

    pg_pipeline = {
        "pg_table": "zonal_crawler_thermal_coal_price",
        "pg_field_map": THERMAL_COAL_PG_FIELD_MAP,
        "pg_static_fields": {"source": "thermal_coal_api"},
    }

    # 默认参数（可用 -a 覆盖）
    seqno = "387"     # 动力煤详情页对应 seqno
    startTime = ""    # 例："2025-09-01"
    endTime = ""      # 例："2025-09-12"
    page_size = 15

    api_url = "https://price.mofcom.gov.cn/datamofcom/front/price/pricequotation/priceQueryList"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k in ["seqno", "startTime", "endTime", "page_size"]:
            if k in kwargs and kwargs[k] is not None and str(kwargs[k]) != "":
                setattr(self, k, kwargs[k] if k != "page_size" else int(kwargs[k]))

        # 用详情页作 Referer，提升成功率
        self.detail_page_url = (
            "https://price.mofcom.gov.cn/price_2021/pricequotation/pricequotationdetail.shtml?"
            + urlencode({"seqno": self.seqno})
        )

        # 防御性上限，避免异常循环
        self._max_guard = 2000

    def start_requests(self):
        # 先落地一次详情页，拿 Cookie
        yield scrapy.Request(
            self.detail_page_url,
            callback=self.after_landing,
            headers={
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
            dont_filter=True,
        )

    def after_landing(self, response):
        yield self._make_api_request(page_number=1)

    def _make_api_request(self, page_number: int):
        form = {
            "seqno": str(self.seqno),
            "startTime": self.startTime,
            "endTime": self.endTime,
            "pageNumber": str(page_number),
            "pageSize": str(self.page_size),
        }
        return scrapy.FormRequest(
            url=self.api_url,
            method="POST",
            formdata=form,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://price.mofcom.gov.cn",
                "Referer": self.detail_page_url,
            },
            callback=self.parse_api,
            meta={"page": page_number},
            dont_filter=True,
        )

    def parse_api(self, response):
        req_page = int(response.meta.get("page", 1))
        try:
            data = json.loads(response.text)
        except Exception as e:
            self.logger.error("JSON parse error on page %s: %s", req_page, e)
            return

        cur_page = int(data.get("pageNumber") or req_page)

        # 翻页信息
        next_page = data.get("nextPage")
        try:
            next_page = int(next_page) if next_page is not None else None
        except Exception:
            next_page = None

        max_pages = data.get("maxPageNum") or data.get("totalPages") or data.get("pages") or 1
        try:
            max_pages = int(max_pages)
            if max_pages <= 0:
                max_pages = 1
        except Exception:
            max_pages = 1

        rows = data.get("rows") or []

        # 产出当前页
        for r in rows:
            yyyy = str(r.get("yyyy") or "").strip()
            mm = str(r.get("mm") or "").strip().zfill(2)
            dd = str(r.get("dd") or "").strip().zfill(2)
            date_str = "-".join([yyyy, mm, dd]) if yyyy and mm and dd else ""

            item = {
                "商品名称": str(r.get("prod_name") or "动力煤").strip(),
                "交易时间": date_str,
                "规格": str(r.get("prod_spec") or "").strip(),
                "地区": str(r.get("region") or "").strip(),
                "单位名称": str(r.get("unit") or "").strip(),
                "价格": str(r.get("price") or "").replace(",", "").strip(),
                "详情链接": self.detail_page_url,
                "_page": cur_page,
                "_source": "api",
            }
            yield item

        # 停止条件
        if not rows:
            return
        if cur_page >= max_pages:
            return
        if isinstance(next_page, int) and next_page <= cur_page:
            return
        if cur_page >= self._max_guard:
            self.logger.warning("Hit hard guard %s, stopping.", self._max_guard)
            return

        # 下一页
        yield self._make_api_request(page_number=cur_page + 1)
