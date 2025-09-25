import scrapy
import logging

PRICE_FIELD_MAP = {
    "商品名称": "prod_name",
    "交易时间": "date",
    "单位名称": "unit",
    "价格": "price",
    "详情链接": "datasourcelink",
}

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

class PriceSpiderxx2(scrapy.Spider):
    name = "price2"
    allowed_domains = ["price.mofcom.gov.cn"]  # 按你的目标域名
    pg_pipeline = {
        "pg_table": "zonal_crawler_coking_coal_price",
        "pg_field_map": PRICE_FIELD_MAP,
        "pg_static_fields": {"source": "coking_coal_page"},
    }

    start_urls = [
        # 按你的实际 URL
        "https://price.mofcom.gov.cn/price_2021/pricequotation/priceSearchdetail.shtml?pro_name=%E7%84%A6%E7%85%A4&pro_trade=&pro_region=&startTime=&endTime=&pro_type="
    ]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={"selenium": True, "xhr_keyword": "pricequotation/priceQuery"},
            )

    def parse(self, response):
        logger = self.logger

        # 1) 优先拿中间件捕获的 XHR JSON
        data = response.meta.get("xhr_json")
        rows_html = _parse_html_table(response)  # 无论如何都解析一遍 HTML，便于合并/兜底

        if data:
            rows_json = _guess_rows_from_json(data)

            # 打一条调试日志，帮助你确认真实字段名（执行时可在控制台看到）
            if rows_json:
                logger.info("XHR first row keys: %s", list(rows_json[0].keys()))

            # 提供更广的字段候选名（如果接口确实带这些字段，会被识别出来）
            name_keys = ["pro_name", "proName", "productName", "goodsName", "goods_name", "name"]
            date_keys = ["trade_time", "tradeTime", "date", "time", "publishDate", "quotationDate", "priceDate", "dealTime", "dateStr"]
            spec_keys = ["spec", "specification", "standard", "specName", "standardName", "specs", "remark"]
            unit_keys = ["unit_name", "unitName", "unit"]
            price_keys = ["price", "avgPrice", "closingPrice", "value"]

            merged_any = False
            n = max(len(rows_json), len(rows_html))
            for i in range(n):
                rj = rows_json[i] if i < len(rows_json) else {}
                rh = rows_html[i] if i < len(rows_html) else {}

                # 优先用 XHR 的价格、单位；如果字段不存在，用 HTML 的
                price = _pick(rj, price_keys, rh.get("price", ""))
                unit = _pick(rj, unit_keys, rh.get("unit", ""))

                # 名称、日期、规格优先尝试 XHR；拿不到就用 HTML 补齐
                name = _pick(rj, name_keys, rh.get("name", ""))
                date = _pick(rj, date_keys, rh.get("date", ""))
                spec = _pick(rj, spec_keys, rh.get("spec", ""))

                item = {
                    "商品名称": str(name).strip(),
                    "交易时间": str(date).strip(),
                    "规格": str(spec).strip(),
                    "单位名称": str(unit).strip(),
                    "价格": str(price).replace(",", "").strip(),
                    "_source": "xhr+html" if rj else "html_only",
                }
                # 如果有详情链接，顺带给上
                if rh.get("detail_url"):
                    item["详情链接"] = rh["detail_url"]

                # 至少价格或名称不为空才产出，避免全空脏数据
                if any(item.get(k) for k in ["商品名称", "交易时间", "规格", "价格"]):
                    merged_any = True
                    yield item

            # 如果 XHR 存在但完全没产出（极端情况），退化为纯 HTML
            if not merged_any and rows_html:
                for rh in rows_html:
                    yield {
                        "商品名称": rh["name"],
                        "交易时间": rh["date"],
                        "规格": rh["spec"],
                        "单位名称": rh["unit"],
                        "价格": rh["price"],
                        "详情链接": rh["detail_url"],
                        "_source": "html_fallback",
                    }
            return

        # 2) 没有 XHR 的情况：纯 HTML 解析
        for rh in rows_html:
            yield {
                "商品名称": rh["name"],
                "交易时间": rh["date"],
                "规格": rh["spec"],
                "单位名称": rh["unit"],
                "价格": rh["price"],
                "详情链接": rh["detail_url"],
                "_source": "html",
            }




import json
from urllib.parse import urlencode

import scrapy


class PriceApiSpider(scrapy.Spider):
    name = "price_api"
    allowed_domains = ["price.mofcom.gov.cn"]

    # 1) 可按需替换检索条件（保持与站内检索一致）
    pro_name = "焦煤"      # 商品名
    pro_trade = ""         # 交易所
    pro_region = ""        # 市场/地区
    startTime = ""         # 开始日期，格式如 "2025-08-01"
    endTime = ""           # 结束日期
    pro_type = ""          # 行业/类型
    page_size = 20         # 每页条数：与你给的 JSON 一致

    # 2) 页面与接口地址
    search_page_url = (
        "https://price.mofcom.gov.cn/price_2021/pricequotation/priceSearchdetail.shtml?"
        + urlencode({
            "pro_name": pro_name,
            "pro_trade": pro_trade,
            "pro_region": pro_region,
            "startTime": startTime,
            "endTime": endTime,
            "pro_type": pro_type,
        })
    )
    api_url = "https://price.mofcom.gov.cn/datamofcom/front/price/pricequotation/priceQuery"

    pg_pipeline = {
        "pg_table": "zonal_crawler_coking_coal_price",
        "pg_field_map": PRICE_FIELD_MAP,
        "pg_static_fields": {"source": "coking_coal_api"},
    }

    def start_requests(self):
        """
        第一步：先访问一次检索页，拿到站点 Cookie/会话。
        站点若有 Referer/防盗链校验，这一步能明显提高成功率。
        """
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
        """
        第二步：带着上一步的 Cookie，对接口发 Page 1 的 JSON 请求。
        """
        yield self.make_api_request(page_number=1)

    def make_api_request(self, page_number: int):
        form = {
            "pro_name": self.pro_name,
            "pro_trade": self.pro_trade,
            "pro_region": self.pro_region,
            "startTime": self.startTime,
            "endTime": self.endTime,
            "pro_type": self.pro_type,
            "pageNumber": str(page_number),  # 用字符串更保险
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
                "Referer": self.search_page_url,
            },
            callback=self.parse_api,
            meta={"page": page_number},
            dont_filter=True,  # 同一URL不同页码，仍需允许重复
        )


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._seen_keys = set()        # 去重哨兵：跨页记录唯一键
        self._max_guard = 2000         # 硬上限，保险丝，避免意外无限循环

    def parse_api(self, response):
        req_page = int(response.meta.get("page", 1))
        try:
            data = json.loads(response.text)
        except Exception as e:
            self.logger.error("JSON parse error on page %s: %s", req_page, e)
            return

        # 读分页字段
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
        self.logger.info("page=%s rows=%s max=%s next=%s", cur_page, len(rows), max_pages, next_page)

        # 产出本页
        for r in rows:
            yyyy = str(r.get("yyyy") or "").strip()
            mm = str(r.get("mm") or "").strip().zfill(2)
            dd = str(r.get("dd") or "").strip().zfill(2)
            date_str = "-".join([yyyy, mm, dd]) if yyyy and mm and dd else ""

            unique_key = (yyyy, mm, dd, str(r.get("seqno")), str(r.get("price")))
            self._seen_keys.add(unique_key)

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

        # ==== 停止条件（任一满足即停止派发下一页）====
        # 1) 空页
        if not rows:
            return
        # 2) 已到最后一页
        if cur_page >= max_pages:
            return
        # 3) 服务端不再推进（有 nextPage 字段且不大于当前页）
        if isinstance(next_page, int) and next_page <= cur_page:
            return
        # 4) 保险丝：超出合理范围（防误循环）
        if cur_page >= self._max_guard:
            self.logger.warning("Hit hard guard %s, stopping.", self._max_guard)
            return

        # 5) 软去重：若下一页返回的全是已见记录（如下实现需要读取下一页后判断；
        #    若你想做严格去重，可在 parse 下一页时比对并据此停止。这里直接派发下一页。)

        # 派发下一页
        yield self.make_api_request(page_number=cur_page + 1)


    # 备用：如果服务端不接受 JSON POST（返回 415/400），可改用表单：
    # def make_api_request(self, page_number: int):
    #     form = {
    #         "pro_name": self.pro_name,
    #         "pro_trade": self.pro_trade,
    #         "pro_region": self.pro_region,
    #         "startTime": self.startTime,
    #         "endTime": self.endTime,
    #         "pro_type": self.pro_type,
    #         "pageNumber": page_number,
    #         "pageSize": self.page_size,
    #     }
    #     return scrapy.FormRequest(
    #         url=self.api_url,
    #         formdata={k: str(v) for k, v in form.items()},
    #         method="POST",
    #         headers={
    #             "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    #             "Accept": "application/json, text/plain, */*",
    #             "Origin": "https://price.mofcom.gov.cn",
    #             "Referer": self.search_page_url,
    #         },
    #         callback=self.parse_api,
    #         meta={"page": page_number},
    #         dont_filter=True,
    #     )
