from linkCrawler import Throttle
from urllib.error import URLError, HTTPError, ContentTooShortError
from random import choice
import urllib
import string
from urllib.parse import urljoin, urlsplit, quote


class Downloader:
    def __init__(self, delay=10, proxies=None):
        self.throttle = Throttle(delay)  # 限速移到内部
        self.proxies = proxies
        self.num_retries = None  # 每次请求时设置， 基于每个url设置请求重试

    def __call__(self, url, num_retries=2):
        self.num_retries = num_retries
        try:
            result = self.cache[url]
            print("Loaded from cache:", url)
        except KeyError:
            result = None
        if result and self.num_retries and 500 <= result['code'] < 600:
            # server error so ignore result from cache
            # and re-download
            result = None
        if result is None:
            # result was not loaded from cache
            # so still need to download
            self.throttle.wait(url)
            proxies = choice(self.proxies) if self.proxies else None
            headers = {'User-Agengt': self.user_agent}
            result = self.download(url, headers, proxies, self.num_retries)
            if self.cache:
                # save to cache
                self.cache[url] = result
        return result['html']

    def download(self, url, headers, proxies, num_retries):  # rewrtie download in linkCrawler.py
        print('downloading:', url)
        request = urllib.request.Request(quote(url, safe=string.printable))
        request.add_header('User-agent', self.user_agent)  # 设置用户代理
        try:
            if proxies:
                proxy_support = urllib.request.ProxyHandler({'http': proxies})
                opener = urllib.request.build_opener(proxy_support)
                urllib.request.install_opener(opener)
            resp = urllib.request.urlopen(request)
            cs = resp.headers.get_content_charset()
            if not cs:
                cs = "utf-8"
            html = resp.read().decode(cs)
        except (URLError, HTTPError, ContentTooShortError) as e:  # 避免下载时遇到的无法控制错误
            print('Downlaod error:', e.reason)
            html = None
            if e.code == 400 and e.reason == "Bad Request":
                return {'html': html, 'code': 404}
            if num_retries > 0:
                if hasattr(e, 'code') and 500 <= e.code <= 600:  # 5XX错误，服务器端存在问题，下载重试
                    return self.download(url, headers, num_retries - 1)
        return {'html': html, 'code': resp.status}
