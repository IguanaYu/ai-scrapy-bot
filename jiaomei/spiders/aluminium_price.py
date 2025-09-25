import json
from datetime import datetime, timezone
from typing import Any, Dict

import scrapy


class AluminiumPriceSpider(scrapy.Spider):
    name = "aluminium_price"
    allowed_domains = ["price.mofcom.gov.cn"]

    seqno = "289"
    start_date = ""
    end_date = ""
    page_size = 15

    api_url = "https://price.mofcom.gov.cn/datamofcom/front/price/pricequotation/priceQueryList"

    pg_pipeline = {
        "pg_table": "zonal_crawler_aluminium_price",
        "pg_field_map": {
            "price": "price",
            "date": "date",
            "datasourcelink": "datasourcelink",
            "datasource": "datasource",
            "created": "created",
            "updated": "updated",
        },
    }

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
        }
    }

    def __init__(self, seqno: str = None, start: str = None, end: str = None, page_size: int = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if seqno:
            self.seqno = str(seqno)
        if start is not None:
            self.start_date = start
        if end is not None:
            self.end_date = end
        if page_size:
            self.page_size = int(page_size)
        self.detail_page_url = (
            "https://price.mofcom.gov.cn/price_2021/pricequotation/pricequotationdetail.shtml?seqno="
            + self.seqno
        )
        self._max_guard = 2000
        self._seen_keys = set()

    def start_requests(self):
        yield scrapy.Request(
            url=self.detail_page_url,
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
        yield self.make_api_request(page_number=1)

    def make_api_request(self, page_number: int):
        form = {
            "seqno": self.seqno,
            "startTime": self.start_date or "",
            "endTime": self.end_date or "",
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
        current_page = int(response.meta.get("page", 1))
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as exc:
            self.logger.error("JSON decode error on page %s: %s", current_page, exc)
            return

        rows = data.get("rows") or []
        self.logger.info(
            "page=%s rows=%s max=%s next=%s",
            current_page,
            len(rows),
            data.get("maxPageNum"),
            data.get("nextPage"),
        )

        for row in rows:
            item = self.build_item(row, current_page)
            if item is None:
                continue
            yield item

        if not rows:
            return

        try:
            max_pages = int(data.get("maxPageNum") or 1)
        except Exception:
            max_pages = 1
        if max_pages <= 0:
            max_pages = 1

        next_page = data.get("nextPage")
        try:
            next_page = int(next_page) if next_page is not None else None
        except Exception:
            next_page = None

        if current_page >= max_pages:
            return
        if isinstance(next_page, int) and next_page <= current_page:
            return
        if current_page >= self._max_guard:
            self.logger.warning("Reached guard page limit %s", self._max_guard)
            return

        yield self.make_api_request(page_number=current_page + 1)

    def build_item(self, row: Dict[str, Any], page: int):
        yyyy = str(row.get("yyyy") or "").strip()
        mm = str(row.get("mm") or "").strip().zfill(2)
        dd = str(row.get("dd") or "").strip().zfill(2)
        date_str = "-".join(part for part in (yyyy, mm, dd) if part)

        price_raw = str(row.get("price") or "").replace(",", "").strip()
        if not price_raw and not date_str:
            return None

        unique_key = (yyyy, mm, dd, price_raw)
        if unique_key in self._seen_keys:
            return None
        self._seen_keys.add(unique_key)

        try:
            price_value = float(price_raw) if price_raw else None
        except ValueError:
            price_value = price_raw

        now = datetime.now(timezone.utc)

        item = {
            "price": price_value,
            "date": date_str,
            "datasource": "price.mofcom.gov.cn",
            "datasourcelink": self.detail_page_url,
            "created": now,
            "updated": now,
            "product": str(row.get("prod_name") or "").strip(),
            "unit": str(row.get("unit") or "").strip(),
            "region": str(row.get("region") or "").strip(),
            "spec": str(row.get("prod_spec") or "").strip(),
            "_page": page,
        }
        return item
