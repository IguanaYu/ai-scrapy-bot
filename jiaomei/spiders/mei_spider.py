# iron_ore_mofcom.py
# 说明：
# - 提供两种抓取策略：
#   1) IronOrePageSpider：先打开检索页，配合你现有的 selenium/xhr 中间件，从页面表格与 XHR JSON 合并出结果。
#   2) IronOreApiSpider：直接 POST 调用官方接口，自动翻页直到 maxPageNum。
# - 输出字段与原脚本保持一致：商品名称 / 交易时间 / 规格 / 单位名称 / 价格 / 详情链接 / _source

import json
from urllib.parse import urlencode

import scrapy

IRON_ORE_PG_FIELD_MAP = {
    "商品名称": "prod_name",
    "交易时间": "date",
    "单位名称": "unit",
    "价格": "price",
    "详情链接": "datasourcelink",
}

# ---------------------------
# 通用小工具（保持与原脚本一致的风格）
# ---------------------------

def _guess_rows_from_json(data):
    if not isinstance(data, dict):
        return []
    for k in ["list", "rows", "records", "items", "datas", "dataList"]:
        v = data.get(k)
        if isinstance(v, list):
            return v
    for k in ["data", "result", "page", "content", "payload", "body"]:
        v = data.get(k)
        if isinstance(v, dict):
            for kk in ["list", "rows", "records", "items", "datas", "dataList"]:
                vv = v.get(kk)
                if isinstance(vv, list):
                    return vv
    for v in data.values():
        if isinstance(v, list):
            return v
    return []


def _pick(d, keys, default=""):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def _parse_html_table(response):
    """
    从 HTML 表格提取每一行，保持与页面显示相同的行序。
    """
    rows = []
    trs = response.xpath('//table[@id="price_price_table_01"]//tr[position()>1]')
    for tr in trs:
        rows.append({
            "name": tr.xpath('normalize-space(td[1])').get(default=""),
            "date": tr.xpath('normalize-space(td[2])').get(default=""),
            "spec": tr.xpath('normalize-space(string(td[3]))').get(default=""),
            "unit": tr.xpath('normalize-space(td[4])').get(default=""),
            "price": tr.xpath('normalize-space(td[5])').get(default=""),
            "detail_url": response.urljoin(tr.xpath('td[3]//a/@href').get() or "")
        })
    return rows


# ---------------------------
# 路线 2：直连 API 自动翻页
# ---------------------------

class IronOreApiSpider(scrapy.Spider):
    name = "mei_api"
    allowed_domains = ["price.mofcom.gov.cn"]

    pg_pipeline = {
        "pg_table": "zonal_crawler_iron_ore_price",
        "pg_field_map": IRON_ORE_PG_FIELD_MAP,
        "pg_static_fields": {"source": "iron_ore_api"},
    }

    # 默认参数（可用 -a 覆盖）
    pro_name = "铁矿石"
    pro_trade = ""
    pro_region = ""
    startTime = ""   # 例："2025-09-01"
    endTime = ""     # 例："2025-09-12"
    pro_type = ""
    page_size = 20

    api_url = "https://price.mofcom.gov.cn/datamofcom/front/price/pricequotation/priceQuery"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 支持命令行 -a 覆盖检索条件
        for k in ["pro_name", "pro_trade", "pro_region", "startTime", "endTime", "pro_type", "page_size"]:
            if k in kwargs and kwargs[k] is not None and str(kwargs[k]) != "":
                setattr(self, k, kwargs[k] if k != "page_size" else int(kwargs[k]))

        # 搜索页用于 Referer，提高成功率
        self.search_page_url = (
            "https://price.mofcom.gov.cn/price_2021/pricequotation/priceSearchdetail.shtml?"
            + urlencode({
                "pro_name": self.pro_name,
                "pro_trade": self.pro_trade,
                "pro_region": self.pro_region,
                "startTime": self.startTime,
                "endTime": self.endTime,
                "pro_type": self.pro_type,
            })
        )

        self._max_guard = 2000

    def start_requests(self):
        # 先落地一次页面，拿 Cookie
        yield scrapy.Request(
            self.search_page_url,
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
            "startTime": self.startTime,
            "endTime": self.endTime,
            "pageNumber": str(page_number),
            "pageSize": str(self.page_size),
            "pro_name": self.pro_name,
            "pro_trade": self.pro_trade,
            "pro_region": self.pro_region,
            "pro_type": self.pro_type,
        }
        return scrapy.FormRequest(
            url=self.api_url,
            method="POST",
            formdata=form,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://price.mofcom.gov.cn",
                "Referer": self.search_page_url,
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
                "商品名称": str(r.get("prod_name") or "").strip(),
                "交易时间": date_str,
                "规格": str(r.get("prod_spec") or "").strip(),
                "单位名称": str(r.get("unit") or "").strip(),
                "价格": str(r.get("price") or "").replace(",", "").strip(),
                "_page": cur_page,
                "_source": "api",
            }
            if r.get("seqno") is not None:
                item["详情链接"] = response.urljoin(
                    f"/price_2021/pricequotation/pricequotationdetail.shtml?seqno={r['seqno']}"
                )
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

        # 派发下一页
        yield self._make_api_request(page_number=cur_page + 1)
