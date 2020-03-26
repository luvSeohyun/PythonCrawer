import urllib.request
import requests
from urllib.error import URLError, HTTPError, ContentTooShortError
from requests.exceptions import ConnectionError
import re
import itertools
from urllib.parse import urljoin, urlparse
from urllib import robotparser
import time


def download(url, user_agent='wswp', num_retries=2, charset='utf-8', proxy=None):  # proxy设置代理， 可能不支持https代理
    print('downloading:', url)
    request = urllib.request.Request(url)
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                 "Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134"
    request.add_header('User-agent', user_agent)  # 设置用户代理
    """Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 
    Safari/537.36 Edge/17.17134"""
    try:
        if proxy:  # 支持代理
            proxy_support = urllib.request.ProxyHandler({'http': proxy})
            opener = urllib.request.build_opener(proxy_support)
            urllib.request.install_opener(opener)
        resp = urllib.request.urlopen(request)  # 下载网页
        cs = resp.headers.get_content_charset()  # 处理编码转换
        if not cs:
            cs = charset
        html = resp.read().decode(cs)
    except (URLError, HTTPError, ContentTooShortError) as e:  # 稳定版本，避免下载时遇到的无法控制错误
        print('Downlaod error:', e.reason)
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code <= 600:  # 5XX错误，服务器端存在问题，下载重试
                return download(url, user_agent, num_retries - 1)
    return html


class Throttle:  # 记录每个域名上次访问的时间，若距离上次访问时间小于制定延时，执行睡眠操作——下载限速
    """Add a delay between downloads to the same domain"""
    def __init__(self, delay):
        self.delay = delay
        self.domains = {}

    def wait(self, url):
        domain = urlparse(url).netloc
        last_accessed = self.domains.get(domain)

        if self.delay > 0 and last_accessed is not None:
            sleep_secs = self.delay - (time.time() - last_accessed)
            if sleep_secs > 0:
                # print('start sleep')
                time.sleep(sleep_secs)
        self.domains[domain] = time.time()


def get_links(html):
    """Return a list of links from html"""
    webpage_regex = re.compile("""(href|src)=["'](.*?)["']""", re.IGNORECASE)  # re.I不区分大小写的匹配
    # webpage_regex = re.compile(""".""", re.IGNORECASE)  # re.I不区分大小写的匹配
    return webpage_regex.findall(html)


def link_crawler(start_url, robots_url=None, user_agent='wswp', scrape_callback=None, max_depth=4, delay=1):
    """跟踪每个链接，更容易下载整个网站页面。
    传入：要爬取的网站URL和匹配想跟踪的链接的正则表达式
    如果要禁用深度判断(爬虫陷阱——动态生成的页面)——max_depth改为负数
    Crawl from the given start URL following links matched by link_regex"""
    seen = {}  # 修改为字典，而不是set。不再只记录访问过的网页链接。 增加已发现链接的深度记录
    throttle = Throttle(delay)
    crawl_queue = [start_url]
    while crawl_queue:
        url = crawl_queue.pop()
        depth = seen.get(url, 0)
        if depth > max_depth:
            print('Skipping %s due to depth' % url)
            continue
        throttle.wait(url)
        html = download(url)
        if not html:
            continue
        data = []
        if scrape_callback:  # 为链接爬虫添加抓取回调——第二章
            data.extend(scrape_callback(url, html) or [])
        for link in get_links(html):
            if "javascript" in link[1] or "#" in link[1]:
                continue
            if "www" in link[1]:
                tmp = urljoin(start_url,link[1])
            elif link[1].startswith("//"):
                tmp = link[1][2:]
            if tmp not in seen:
                seen[tmp] = depth + 1
                print("found:{}".format(tmp))
            # abs_link = urljoin(start_url, link)  # 取得绝对路径，爬虫才能正常执行
            # if abs_link not in seen:  # 确保新的网站没被爬取过
            #     seen[abs_link] = depth + 1
            #     crawl_queue.append(abs_link)


if __name__ == "__main__":
    url = "http://www.quanjingke.com/dest/scenic_linggusi"
    link_crawler(url, max_depth=1, delay=0)
