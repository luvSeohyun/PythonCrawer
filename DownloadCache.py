from linkCrawler import Throttle, get_robots_parser, get_links
from random import choice
import urllib
import re
from urllib.parse import urljoin, urlsplit
from urllib.error import URLError, HTTPError, ContentTooShortError
import os


class Downloader:
    def __init__(self, delay=5, user_agent="wswp", proxies=None, cache={}):
        self.throttle = Throttle(delay)
        self.user_agent = user_agent
        self.proxies = proxies
        self.num_retries = None  # 每次请求时设置
        self.cache = cache

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
            result = self.download(url, headers, proxies)
            if self.cache:
                # save to cache
                self.cache[url] = result
        return result['html']

    def download(self, url, headers, proxies):  #rewrtie download in linkCrawler.py
        print('downloading:', url)
        request = urllib.request.Request(url)
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
            if self.num_retries > 0:
                if hasattr(e, 'code') and 500 <= e.code <= 600:  # 5XX错误，服务器端存在问题，下载重试
                    return self.download(url, headers, self.num_retries - 1)
        return {'html': html, 'code': resp.status_code}


def link_crawler(start_url, link_regex, robots_url=None, user_agent='wswp', scrape_callback=None, max_depth=4, delay=1,
                 proxies=None, num_retries=2, cache={}):
    """传入要爬取的网站URL和匹配想跟踪的链接的正则表达式
    如果要禁用深度判断(爬虫陷阱——动态生成的页面)——max_depth改为负数
    Crawl from the given start URL following links matched by link_regex"""
    crawl_queue = [start_url]
    seen = {start_url: 0}  # 修改为字典，而不是set。不再只记录访问过的网页链接。 增加已发现链接的深度记录
    if not robots_url:
        robots_url = '{}/robots.txt'.format(start_url)
    rp = get_robots_parser(robots_url)
    D = Downloader(delay=delay, user_agent=user_agent, proxies=proxies, cache=cache)
    while crawl_queue:
        url = crawl_queue.pop()
        # check url passes robots.txt restrictions
        if rp.can_fetch(user_agent, url):
            depth = seen.get(url, 0)
            if depth > max_depth:
                # print('Skipping %s due to depth' % url)
                continue
            html = D(url, num_retries=num_retries)
            if not html:
                continue
            data = []
            if scrape_callback:
                data.extend(scrape_callback(url, html) or [])
            for link in get_links(html):
                if re.search(link_regex, link):  # match匹配以link_regex开头的link, search匹配任意
                    abs_link = urljoin(start_url, link)  # 取得绝对路径
                    if abs_link not in seen:
                        seen[abs_link] = depth + 1
                        crawl_queue.append(abs_link)
        else:
            print('blocked by robots.txt:', url)


class DiskCache:
    def __init__(self, cache_dir='cache', max_len=255):
        self.cache_dir = cache_dir
        self.max_len = max_len

    def url_to_path(self, url):  # 磁盘缓存边界情况处理
        """return file system path string for givin URL"""
        components = urlsplit(url)
        path = components.path
        if not path:
            path = '/index.html'
        elif path.endswith('/'):
            path += 'index.html'
        filename = components.netloc + path + components.query
        filename = re.sub('[^/0-9a-zA-Z\-.,;_]', '_', url)
        filename = '/'.join(segment[:255] for segment in url.split('/'))
        return os.path.join(self.cache_dir, filename)

if __name__ == "__main__":
    """如果执行一个大型爬虫工作，缓存可以无需重新爬取可能已抓取的页面，并能离线访问页面"""