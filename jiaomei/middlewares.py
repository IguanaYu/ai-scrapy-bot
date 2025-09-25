# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class JiaomeiSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    async def process_start(self, start):
        # Called with an async iterator over the spider start() method or the
        # maching method of an earlier spider middleware.
        async for item_or_request in start:
            yield item_or_request

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class JiaomeiDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)



import os
import time
import json
import logging
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit

from scrapy import signals
from scrapy.http import HtmlResponse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)

def _site_root(url: str) -> str:
    pr = urlsplit(url)
    return urlunsplit((pr.scheme, pr.netloc, "/", "", ""))

class SeleniumCdpMiddleware:
    """
    通用 Downloader Middleware：
    - 当 Request(meta["selenium"]=True) 时，用 Selenium 打开页面。
    - 执行 Request(meta["selenium_actions"]) 传入的“动作序列”，支持：
        clear_perf_logs / sleep / script / wait_css / wait_xpath
    - 采集本次导航过程中的所有 XHR 响应（Network.responseReceived），
      以 [{"url":..., "body":...}, ...] 放到 response.meta["xhr_payloads"]。
    - 不包含任何站点私有逻辑（如翻页 JS、关键词筛选、字段解析）。
    """

    def __init__(self, headless=True, wait=2, debug_artifacts=False, use_wdm=True):
        self.headless = headless
        self.wait = wait
        self.debug_artifacts = debug_artifacts
        self.use_wdm = use_wdm
        self.driver = None
        self.debug_dir = os.path.abspath("debug_artifacts")
        os.makedirs(self.debug_dir, exist_ok=True)

    @classmethod
    def from_crawler(cls, crawler):
        headless = crawler.settings.getbool("SELENIUM_HEADLESS", True)
        wait = crawler.settings.getint("SELENIUM_WAIT", 2)
        debug_artifacts = crawler.settings.getbool("SELENIUM_DEBUG_ARTIFACTS", False)
        # 为了加速启动，默认使用 webdriver-manager；如你环境不需要，可在 settings 里关掉
        use_wdm = crawler.settings.getbool("SELENIUM_USE_WDM", True)
        mw = cls(headless=headless, wait=wait, debug_artifacts=debug_artifacts, use_wdm=use_wdm)
        crawler.signals.connect(mw.spider_closed, signal=signals.spider_closed)
        mw._init_driver()
        return mw

    def _init_driver(self):
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        # 打开 performance 日志，便于抓 XHR
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

        if self.use_wdm:
            # 首次会下载驱动并缓存，后续启动更快
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                service = Service(ChromeDriverManager(cache_valid_range=7).install())
                self.driver = webdriver.Chrome(service=service, options=options)
            except Exception as e:
                logger.warning("webdriver_manager init failed, fallback to Selenium Manager: %s", e)
                self.driver = webdriver.Chrome(options=options)
        else:
            self.driver = webdriver.Chrome(options=options)

        # 开启 CDP Network，注入通用请求头
        try:
            self.driver.execute_cdp_cmd("Network.enable", {})
            self.driver.execute_cdp_cmd(
                "Network.setExtraHTTPHeaders",
                {"headers": {"Accept-Language": "zh-CN,zh;q=0.9"}}
            )
        except Exception as e:
            logger.debug("CDP init failed: %s", e)

        logger.info("Selenium WebDriver initialized (headless=%s)", self.headless)

    def _wait_body(self):
        try:
            WebDriverWait(self.driver, max(self.wait, 1)).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except Exception:
            time.sleep(0.3)

    def _clear_perf_logs(self):
        try:
            _ = self.driver.get_log("performance")
        except Exception:
            pass

    def _save_artifacts(self, spider_name: str, tag: str, html: str):
        if not self.debug_artifacts:
            return
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        base = f"{spider_name}_{tag}_{ts}"
        try:
            self.driver.save_screenshot(os.path.join(self.debug_dir, base + ".png"))
        except Exception:
            pass
        try:
            with open(os.path.join(self.debug_dir, base + ".html"), "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            pass

    def _run_actions(self, actions):
        """
        依次执行传入的动作序列。支持动作：
          - {"type": "clear_perf_logs"}
          - {"type": "sleep", "seconds": 1.2}
          - {"type": "script", "code": "...", "args": [..]}
          - {"type": "wait_css", "selector": "css", "timeout": 5}
          - {"type": "wait_xpath", "expr": "//div", "timeout": 5}
        """
        if not actions:
            return
        for act in actions:
            t = (act.get("type") or "").lower()
            if t == "clear_perf_logs":
                self._clear_perf_logs()
            elif t == "sleep":
                secs = float(act.get("seconds", 0.5))
                time.sleep(secs)
            elif t == "script":
                code = act.get("code") or ""
                args = act.get("args") or []
                try:
                    self.driver.execute_script(code, *args)
                except Exception as e:
                    logger.debug("execute_script failed: %s", e)
            elif t == "wait_css":
                sel = act.get("selector") or ""
                timeout = int(act.get("timeout", max(self.wait, 1)))
                try:
                    WebDriverWait(self.driver, timeout).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                    )
                except Exception:
                    pass
            elif t == "wait_xpath":
                expr = act.get("expr") or ""
                timeout = int(act.get("timeout", max(self.wait, 1)))
                try:
                    WebDriverWait(self.driver, timeout).until(
                        EC.presence_of_element_located((By.XPATH, expr))
                    )
                except Exception:
                    pass
            else:
                logger.debug("unknown action: %s", t)

    def _collect_xhr_payloads(self):
        """
        采集当前 performance 日志中的所有 XHR 响应体，返回列表：
        [{"url": <str>, "body": <str>}, ...]
        """
        payloads = []
        try:
            logs = self.driver.get_log("performance")
        except Exception as e:
            logger.warning("get_log(performance) failed: %s", e)
            return payloads

        for entry in logs:
            try:
                msg = json.loads(entry.get("message", "{}"))
                message = msg.get("message", {})
            except Exception:
                continue
            if message.get("method") != "Network.responseReceived":
                continue

            params = message.get("params", {})
            response = params.get("response", {})
            url = response.get("url", "")
            req_id = params.get("requestId")

            # 只抓 text/json、text/html 之类的文本响应
            mime = (response.get("mimeType") or "").lower()
            if "json" not in mime and "text" not in mime:
                continue

            try:
                body = self.driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": req_id})
                text = body.get("body", "")
            except Exception:
                text = ""

            if url and text:
                payloads.append({"url": url, "body": text})

        return payloads

    def process_request(self, request, spider):
        if not request.meta.get("selenium", False):
            return None

        url = request.url
        logger.info("[Selenium] %s", url)

        # 可选预热（Spider 决定是否传 preheat_root=True）
        if request.meta.get("preheat_root"):
            try:
                root = _site_root(url)
                self.driver.get(root)
                self._wait_body()
                try:
                    self.driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": {"Referer": root}})
                except Exception:
                    pass
            except Exception:
                pass

        # 打开目标页
        self.driver.get(url)
        self._wait_body()

        # 执行动作序列（例如跳到第 N 页）
        actions = request.meta.get("selenium_actions", [])
        self._run_actions(actions)

        # 收集本次的 XHR 响应
        xhr_payloads = self._collect_xhr_payloads()
        if xhr_payloads:
            request.meta["xhr_payloads"] = xhr_payloads

        html = self.driver.page_source
        self._save_artifacts(getattr(spider, "name", "spider"), request.meta.get("tag") or "page", html)

        return HtmlResponse(
            url=self.driver.current_url,
            body=html.encode("utf-8"),
            encoding="utf-8",
            request=request,
        )

    def spider_closed(self, spider):
        if self.driver:
            try:
                self.driver.quit()
            finally:
                logger.info("Selenium WebDriver quit.")
