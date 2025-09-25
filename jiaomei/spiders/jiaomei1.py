import scrapy


class JiaomeiSpider1(scrapy.Spider):
    name = "jiaomei_spider"
    start_urls = [
        "https://price.mofcom.gov.cn/price_2021/pricequotation/priceSearchdetail.shtml?pro_name=%E7%84%A6%E7%85%A4&pro_trade=&pro_region=&startTime=&endTime=&pro_type="]  # 这个页面需要JS渲染


    def parse(self, response):

        
        print("*"*100)
        print(response.text)

        # 只抓第一页：按照你给的结构直接解析表格
        rows = response.xpath('//table[@id="price_price_table_01"]/tbody/tr[position()>1]')

        print(response.text)
        for row in rows:
            item = {
                "商品名称": row.xpath("td[1]/text()").get(default="").strip(),
                "交易时间": row.xpath("td[2]/text()").get(default="").strip(),
                "规格": row.xpath("td[3]//a/text()").get(default="").strip(),
                "详情链接": response.urljoin(row.xpath("td[3]//a/@href").get(default="")),
                "单位名称": row.xpath("td[4]/text()").get(default="").strip(),
                "价格": row.xpath("td[5]/text()").get(default="").strip(),
            }
            yield item
        # # 找到下一页的链接
        # next_page = response.css("li.next a::attr(href)").get()
        # if next_page:
        #     # 让 Scrapy 自动跟进下一页
        #     yield response.follow(next_page, self.parse)




        
class JiaomeiSpider(scrapy.Spider):
    name = "baidu_test"
    start_urls = ["https://www.baidu.com/?tn=88093251_140_hao_pg"]  # 这个页面需要JS渲染



    def parse(self, response):

        print(response.text)
        for quote in response.css("div.s-hotsearch-content li"):
            yield {
                "text": quote.css("span.title-content-title::text").get(),
            }



class QuotesSpider(scrapy.Spider):
    name = "quotes"
    start_urls = ["https://quotes.toscrape.com/page/1/"]

    def parse(self, response):
        print("*"*100)
        print(response.text)

        for quote in response.css("div.quote"):
            yield {
                "text": quote.css("span.text::text").get(),
                "author": quote.css("small.author::text").get()
            }
        