import json
import re
from typing import Any, Dict, Iterable, List, Sequence, Tuple
from urllib.parse import urlencode
from datetime import datetime, timezone

import scrapy

CAR_FIELD_MAP: Dict[str, str] = {
    "price": "price",
    "date": "date",
    "datasourcelink": "datasourcelink",
    "created": "created",
    "updated": "updated",
}

METRIC_LABELS: Sequence[Tuple[str, str]] = (
    ("产量", "production"),
    ("批发", "wholesale"),
    ("零售", "retail"),
    ("出口", "export"),
)

METRIC_TO_TYPE_PARAM: Dict[str, int] = {
    "production": 1,
    "wholesale": 2,
    "retail": 3,
    "export": 4,
}

SCOPE_LABELS: Sequence[str] = ("狭义乘用车", "广义乘用车")

_MONTH_RE = re.compile(r"(\d+)")



def _as_float(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip()
    except Exception:
        return value
    if not text:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return value



def _build_date(year: str, month_label: Any) -> str:
    if not year:
        return ""
    try:
        year_num = int(year)
    except ValueError:
        digits = "".join(ch for ch in year if ch.isdigit())
        if not digits:
            return ""
        year_num = int(digits)
    match = _MONTH_RE.search(str(month_label or ""))
    if not match:
        return f"{year_num:04d}-01-01"
    month = int(match.group(1))
    if month <= 0 or month > 12:
        return f"{year_num:04d}-01-01"
    return f"{year_num:04d}-{month:02d}-01"



def _coerce_payload(data: Any) -> List[Dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, str):
        data = data.strip()
        if not data:
            return []
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            return []
        else:
            return _coerce_payload(parsed)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, (list, tuple)):
        result: List[Dict[str, Any]] = []
        for elem in data:
            if isinstance(elem, dict):
                result.append(elem)
        return result
    return []


class CarTotalMarketSpider(scrapy.Spider):
    name = "car_total_market"
    allowed_domains = ["cpcadata.com", "cpcaauto.com"]
    start_urls = ["http://data.cpcadata.com/TotalMarket"]

    # TODO: 将两个表名替换成实际表名
    production_table: str = ""
    retail_table: str = "zonal_crawler_auto_sales"

    api_url = "http://data.cpcadata.com/api/chartlist"
    charttype = "1"

    pg_pipeline = {
        "pg_field_map": CAR_FIELD_MAP,
    }

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
            "Referer": "http://data.cpcadata.com/",
            "Upgrade-Insecure-Requests": "1",
        },
        "FEEDS": {
            "car_total_market.json": {
                "format": "json",
                "encoding": "utf-8",
                "overwrite": True,
            }
        },
    }

    api_headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "http://data.cpcadata.com",
        "Referer": "http://data.cpcadata.com/TotalMarket",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._seen: set = set()
        self.metric_table_map = {
            "production": (self.production_table or "").strip(),
            "retail": (self.retail_table or "").strip(),
        }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={"selenium": True, "xhr_keyword": "api/chartlist"},
                dont_filter=True,
            )

    def parse(self, response):
        data = response.meta.get("xhr_json")
        if data:
            yield from self._handle_payload(data, origin="xhr")
        else:
            yield self._build_api_request(origin="fallback")

    def _build_api_request(self, origin: str) -> scrapy.Request:
        query = urlencode({"charttype": self.charttype})
        url = f"{self.api_url}?{query}"
        return scrapy.Request(
            url=url,
            method="GET",
            headers=self.api_headers,
            callback=self.parse_api,
            cb_kwargs={"origin": origin},
            dont_filter=True,
            meta={"origin": origin},
        )

    def parse_api(self, response, origin: str = "manual"):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as exc:
            self.logger.error("JSON decode error: %s", exc)
            return
        yield from self._handle_payload(data, origin=origin or "api")

    def _handle_payload(self, payload: Any, origin: str) -> Iterable[Dict[str, Any]]:
        blocks = _coerce_payload(payload)
        if not blocks:
            self.logger.warning("No chart data extracted from origin=%s", origin)
            return []
        count = 0
        for block_index, block in enumerate(blocks):
            for item in self._iter_chart_items(block, origin, block_index):
                count += 1
                yield item
        self.logger.info("yielded %s records from origin=%s", count, origin)


    def _iter_chart_items(
        self,
        block: Dict[str, Any],
        origin: str,
        block_index: int,
    ) -> Iterable[Dict[str, Any]]:
        category = str(block.get("category") or "").strip()
        if 0 <= block_index < len(SCOPE_LABELS):
            scope_label = SCOPE_LABELS[block_index]
        else:
            scope_label = category or f"block_{block_index}"
        if not category:
            category = scope_label
        data_list = block.get("dataList") or []
        if not isinstance(data_list, list):
            return
        for entry in data_list:
            if not isinstance(entry, dict):
                continue
            month_label = entry.get("month") or ""
            for year_key, values in entry.items():
                if not isinstance(values, list):
                    continue
                if not isinstance(year_key, str) or not year_key.endswith("年"):
                    continue
                year_digits = "".join(ch for ch in year_key if ch.isdigit())
                if not year_digits:
                    continue
                date_str = _build_date(year_digits, month_label)
                for idx, (cn_label, metric_key) in enumerate(METRIC_LABELS):
                    if idx >= len(values):
                        continue
                    price_value = _as_float(values[idx])
                    unique = (scope_label, metric_key, year_digits, date_str, price_value)
                    if unique in self._seen:
                        continue
                    self._seen.add(unique)
                    now = datetime.now(timezone.utc)
                    item = {
                        "category": category,
                        "vehicle_scope": scope_label,
                        "scope_index": block_index,
                        "metric": metric_key,
                        "metric_cn": cn_label,
                        "year": year_digits,
                        "month_label": month_label,
                        "price": price_value,
                        "date": date_str,
                        "datasourcelink": self._build_datasourcelink(metric_key),
                        "created": now,
                        "updated": now,
                        "_origin": origin,
                    }

                    table_name = self.metric_table_map.get(metric_key, "")
                    if table_name and block_index == 0:
                        item["_pg_table"] = table_name
                    else:
                        item["_pg_skip_pg"] = True
                    yield item


    def _build_datasourcelink(self, metric_key: str) -> str:
        type_param = METRIC_TO_TYPE_PARAM.get(metric_key)
        if type_param is None:
            return f"{self.api_url}?charttype={self.charttype}"
        return f"{self.api_url}?charttype={self.charttype}&type={type_param}"
