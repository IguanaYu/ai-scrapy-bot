import scrapy

def _guess_rows_from_json(data):
    """
    兼容常见字段，把第一页的“记录列表”提出来。
    你可以根据实际接口返回结构进一步固定字段名。
    """
    if not isinstance(data, dict):
        return []
    # 直取
    for k in ["list", "rows", "records", "items", "datas", "dataList"]:
        v = data.get(k)
        if isinstance(v, list):
            return v
    # 二层 dict
    for k in ["data", "result", "page", "content", "payload", "body"]:
        v = data.get(k)
        if isinstance(v, dict):
            for kk in ["list", "rows", "records", "items", "datas", "dataList"]:
                vv = v.get(kk)
                if isinstance(vv, list):
                    return vv
    # 兜底：找任意 list
    for v in data.values():
        if isinstance(v, list):
            return v
    return []

def _pick(d, keys, default=""):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default

class PriceSpider(scrapy.Spider):
    name = "price"
    allowed_domains = ["price.mofcom.gov.cn"]  # 替换成目标域名
    start_urls = [
        # 替换成你的检索结果页
        "https://price.mofcom.gov.cn/price_2021/pricequotation/priceSearchdetail.shtml?pro_name=%E7%84%A6%E7%85%A4&pro_trade=&pro_region=&startTime=&endTime=&pro_type="
    ]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={
                    "selenium": True,
                    # 如果接口路径片段不同，改这里或在 settings.py 改 SELENIUM_XHR_KEYWORD
                    "xhr_keyword": "pricequotation/priceQuery",
                },
            )

    def parse(self, response):
        # 1) 优先用中间件抓到的第一页 JSON
        data = response.meta.get("xhr_json")
        if data:
            rows = _guess_rows_from_json(data)
            for r in rows:
                name = _pick(r, ["pro_name", "productName", "name", "goodsName", "proName"])
                trade_date = _pick(r, ["trade_time", "tradeTime", "date", "time", "publishDate"])
                spec = _pick(r, ["spec", "specification", "standard", "specName"])
                unit = _pick(r, ["unit_name", "unitName", "unit"])
                price = _pick(r, ["price", "avgPrice", "closingPrice", "value"])
                yield {
                    "商品名称": str(name).strip(),
                    "交易时间": str(trade_date).strip(),
                    "规格": str(spec).strip(),
                    "单位名称": str(unit).strip(),
                    "价格": str(price).replace(",", "").strip(),
                    "_source": "xhr",
                }
            return  # 只要拿到第一页 JSON，就不再解析 HTML

        # 2) 没拿到 JSON 时，退化为解析第一页表格 HTML（按你给的结构）
        rows = response.xpath('//table[@id="price_price_table_01"]//tr[position()>1]')
        for row in rows:
            yield {
                "商品名称": row.xpath('normalize-space(td[1])').get(default=""),
                "交易时间": row.xpath('normalize-space(td[2])').get(default=""),
                "规格": row.xpath('normalize-space(string(td[3]))').get(default=""),
                "详情链接": response.urljoin(row.xpath('td[3]//a/@href').get() or ""),
                "单位名称": row.xpath('normalize-space(td[4])').get(default=""),
                "价格": row.xpath('normalize-space(td[5])').get(default=""),
                "_source": "html",
            }
