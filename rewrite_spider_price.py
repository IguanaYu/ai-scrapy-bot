from pathlib import Path

code = '''import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import scrapy


class AnjukeShanxiPriceSpider(scrapy.Spider):
    """Collect monthly price metadata for Shanxi cities from Anjuke mobile pages."""

    name = "anjuke_shanxi_price"
    allowed_domains = ["anjuke.com"]

    province_slug = "shanxi"
    mobile_base = "https://mobile.anjuke.com"
    latest_year = 2025
    target_years = list(range(2020, latest_year + 1))
    city_limit = 10
    mobile_user_agent = (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 "
        "Mobile/15E148 Safari/604.1"
    )

    pg_pipeline = {
        "pg_table": "zonal_crawler_house_price",
        "pg_field_map": {
            "city": "city",
            "date": "date",
            "datasourcelink": "datasourcelink",
            "price": "price",
        },
        "pg_upsert_keys": [],
        "pg_use_existing_table": True,
        "pg_strict_columns": True,
        "pg_static_fields": {"source": "anjuke_mobile"},
    }

    def start_requests(self) -> Iterable[scrapy.Request]:
        province_url = f"{self.mobile_base}/fangjia/{self.province_slug}{self.latest_year}/"
        yield scrapy.Request(
            province_url,
            headers={"User-Agent": self.mobile_user_agent},
            callback=self.parse_province,
        )

    async def start(self):
        for request in self.start_requests():
            yield request

    def parse_province(self, response: scrapy.http.Response, **kwargs: Any):
        page = self._extract_page_props(response)
        province_prices = page.get("provinceAvgPriceRes") or []
        city_entries = self._collect_city_entries(province_prices)
        if not city_entries:
            self.logger.error("No city price entries found at %s", response.url)
            return

        selected: List[Tuple[int, str, str]] = []
        for idx, entry in enumerate(city_entries[: self.city_limit], start=1):
            slug = self._city_slug_from_url(entry.get("actionUrl"))
            if not slug:
                self.logger.warning("Skip city entry without slug: %s", entry)
                continue
            city_name = self._city_name_from_title(entry.get("title")) or slug
            selected.append((idx, slug, city_name))

            for year in self.target_years:
                city_year_url = f"{self.mobile_base}/fangjia/{slug}{year}/"
                yield scrapy.Request(
                    city_year_url,
                    headers={"User-Agent": self.mobile_user_agent},
                    callback=self.parse_city_year,
                    meta={
                        "city": city_name,
                        "target_year": year,
                    },
                )

        self.logger.info(
            "Selected cities: %s",
            ", ".join(
                f"#{rank} {name} ({slug})" for rank, slug, name in selected
            ),
        )

    def parse_city_year(self, response: scrapy.http.Response):
        target_year = int(response.meta.get("target_year"))

        page = self._extract_page_props(response)
        breadcrumb = page.get("breadCrumbInfo") or {}
        city_name = breadcrumb.get("cityName") or breadcrumb.get("name") or response.meta.get("city")

        price_info = page.get("provinceAvgPriceRes") or {}
        if isinstance(price_info, list):
            price_info = price_info[0] if price_info else {}
        if not isinstance(price_info, dict):
            self.logger.warning("No price info for %s", response.url)
            return

        year_list = price_info.get("yearList") or []
        if not year_list:
            self.logger.warning("No yearList data for %s", response.url)
            return

        by_month: Dict[int, Optional[float]] = {}
        for entry in year_list:
            if not isinstance(entry, dict):
                continue
            year, month = self._parse_year_month(entry.get("title") or "")
            if year is None or month is None or year != target_year:
                continue
            price = self._to_float(entry.get("avgPrice"))
            by_month[month] = price

        if not by_month:
            self.logger.warning(
                "No monthly records for %s (year %s)", response.url, target_year
            )
            return

        for month in sorted(by_month):
            yield {
                "city": city_name,
                "date": f"{target_year}-{month:02d}-01",
                "datasourcelink": response.url,
                "price": by_month[month],
            }

    # ---------- helpers ----------

    def _extract_page_props(self, response: scrapy.http.Response) -> Dict[str, Any]:
        raw = response.xpath("//script[@id='__NEXT_DATA__']/text()").get()
        if not raw:
            self.logger.error("Missing __NEXT_DATA__ on %s", response.url)
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            self.logger.error("Failed to decode JSON on %s: %s", response.url, exc)
            return {}
        props = data.get("props", {}).get("pageProps", {})
        if not props:
            self.logger.warning("Empty pageProps on %s", response.url)
        return props

    def _collect_city_entries(self, sections: Any) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        if isinstance(sections, list):
            for block in sections:
                if isinstance(block, dict):
                    price_list = block.get("priceVOList")
                    if isinstance(price_list, list):
                        entries.extend(price_list)
        elif isinstance(sections, dict):
            price_list = sections.get("priceVOList")
            if isinstance(price_list, list):
                entries.extend(price_list)
        return entries

    def _city_slug_from_url(self, url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        match = re.search(r"/fangjia/([^/\\s]+?)(?:\\d{4})/", url)
        if not match:
            return None
        return match.group(1)

    def _city_name_from_title(self, title: Optional[str]) -> Optional[str]:
        if not title:
            return None
        match = re.search(r"\\d{4}([^\\s]+?)房价", title)
        if match:
            return match.group(1)
        return title.strip()

    def _parse_year_month(self, title: str) -> Tuple[Optional[int], Optional[int]]:
        match = re.search(r"(\\d{4})年(\\d{1,2})月", title)
        if not match:
            return None, None
        try:
            year = int(match.group(1))
            month = int(match.group(2))
            return year, month
        except ValueError:
            return None, None

    def _to_float(self, value: Any) -> Optional[float]:
        if value in (None, "", "--"):
            return None
        try:
            return float(str(value).replace(",", ""))
        except (TypeError, ValueError):
            return None
'''

Path('jiaomei/spiders/anjuke_shanxi_price.py').write_text(code, encoding='utf-8')
