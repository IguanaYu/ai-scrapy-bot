import json

import scrapy


FIELD_PRODUCT_NAME = "商品名称"
FIELD_TRADE_DATE = "交易时间"
FIELD_SPEC = "规格"
FIELD_REGION = "地区"
FIELD_UNIT = "单位名称"
FIELD_PRICE = "价格"
FIELD_DETAIL_LINK = "详情链接"

MAGNESIUM_PG_FIELD_MAP = {
    FIELD_PRICE: "price",
    FIELD_TRADE_DATE: "date",
    FIELD_DETAIL_LINK: "datasourcelink",
}


class MagnesiumMofcomSpider(scrapy.Spider):
    name = "magnesium_mofcom"
    allowed_domains = ["price.mofcom.gov.cn"]

    api_url = "https://price.mofcom.gov.cn/datamofcom/front/price/pricequotation/priceQueryList"
    detail_url = "https://price.mofcom.gov.cn/price_2021/pricequotation/pricequotationdetail.shtml"

    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            "jiaomei.middlewares.SeleniumCdpMiddleware": None,
        },
        "FEEDS": {
            "outputs/magnesium_mofcom.json": {
                "format": "json",
                "encoding": "utf-8",
                "indent": 2,
                "overwrite": True,
                "ensure_ascii": False,
            }
        },
    }

    pg_pipeline = {
        "pg_table": "zonal_crawler_magnesium_price",
        "pg_field_map": MAGNESIUM_PG_FIELD_MAP,
        "pg_static_fields": {"source": "magnesium_api"},
    }

    def __init__(self, seqno="350", start_time="", end_time="", page_size=50, max_pages=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seqno = str(seqno).strip() or "350"
        self.start_time = str(start_time or "").strip()
        self.end_time = str(end_time or "").strip()
        try:
            self.page_size = int(page_size)
        except (TypeError, ValueError):
            self.page_size = 50
        try:
            self.max_pages = int(max_pages) if max_pages is not None else None
        except (TypeError, ValueError):
            self.max_pages = None

    async def start(self):
        # Warm up cookies and referer by loading the public detail page first.
        url = f"{self.detail_url}?seqno={self.seqno}"
        yield scrapy.Request(
            url,
            callback=self.after_landing,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
            dont_filter=True,
        )

    def after_landing(self, response):
        yield self._fetch_page(1)

    def _fetch_page(self, page_number: int):
        form = {
            "seqno": self.seqno,
            "startTime": self.start_time,
            "endTime": self.end_time,
            "pageNumber": str(page_number),
            "pageSize": str(self.page_size),
        }
        return scrapy.FormRequest(
            self.api_url,
            method="POST",
            formdata=form,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://price.mofcom.gov.cn",
                "Referer": f"{self.detail_url}?seqno={self.seqno}",
            },
            callback=self.parse_api,
            meta={"page": page_number},
            dont_filter=True,
        )

    def parse_api(self, response):
        page_number = int(response.meta.get("page", 1))
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Failed to decode JSON on page %s", page_number)
            return

        rows = data.get("rows") or []
        for row in rows:
            yyyy = str(row.get("yyyy") or "").strip()
            mm = str(row.get("mm") or "").strip().zfill(2)
            dd = str(row.get("dd") or "").strip().zfill(2)
            date_parts = [yyyy, mm, dd]
            date_str = "-".join(part for part in date_parts if part)
            date_str = date_str if date_str.count("-") == 2 else ""

            price_value = (row.get("price") or "").replace(",", "").strip()
            seqno_value = str(row.get("seqno") or self.seqno).strip()
            detail_link = f"{self.detail_url}?seqno={seqno_value}" if seqno_value else f"{self.detail_url}?seqno={self.seqno}"

            yield {
                FIELD_PRODUCT_NAME: (row.get("prod_name") or "").strip() or "镁",
                FIELD_TRADE_DATE: date_str,
                FIELD_SPEC: (row.get("prod_spec") or "").strip(),
                FIELD_REGION: (row.get("region") or "").strip(),
                FIELD_UNIT: (row.get("unit") or "").strip(),
                FIELD_PRICE: price_value,
                FIELD_DETAIL_LINK: detail_link,
                "_page": page_number,
                "_source": "magnesium_api",
            }

        next_page = data.get("nextPage")
        max_pages = data.get("maxPageNum")
        try:
            next_page = int(next_page) if next_page is not None else None
        except (TypeError, ValueError):
            next_page = None
        try:
            max_pages = int(max_pages) if max_pages else page_number
        except (TypeError, ValueError):
            max_pages = page_number

        if self.max_pages is not None and page_number >= self.max_pages:
            return
        if not rows:
            return
        if next_page and next_page > page_number:
            if self.max_pages is None or next_page <= self.max_pages:
                yield self._fetch_page(next_page)
            return
        if page_number < max_pages:
            next_num = page_number + 1
            if self.max_pages is None or next_num <= self.max_pages:
                yield self._fetch_page(next_num)
