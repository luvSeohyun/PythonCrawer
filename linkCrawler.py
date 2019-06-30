import whois
import urllib.request
from urllib.error import URLError, HTTPError, ContentTooShortError
import re
import itertools
from urllib.parse import urljoin, urlparse
from urllib import robotparser
import time


def get_sitemaps(url):  # 起点用，从robot开始网站地图爬虫，之获取网页版
    sitemaps = download(url)
    maps = sitemaps.split('Sitemap:')
    for map in maps:
        if 'https://www' in map:
            crawl_sitemap(map)


def download(url, user_agent='wswp', num_retries=2, charset='utf-8', proxy=None):  # proxy设置代理， 可能不支持https代理
    print('downloading:', url)
    request = urllib.request.Request(url)
    request.add_header('User-agent', user_agent)  # 设置用户代理
    try:
        if proxy:
            proxy_support = urllib.request.ProxyHandler({'http': proxy})
            opener = urllib.request.build_opener(proxy_support)
            urllib.request.install_opener(opener)
        resp = urllib.request.urlopen(request)
        cs = resp.headers.get_content_charset()
        if not cs:
            cs = charset
        html = resp.read().decode(cs)
    except (URLError, HTTPError, ContentTooShortError) as e:  # 避免下载时遇到的无法控制错误
        print('Downlaod error:', e.reason)
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code <= 600:  # 5XX错误，服务器端存在问题，下载重试
                return download(url, user_agent, num_retries - 1)
    return html


def crawl_sitemap(url):  # 无法依靠sitemap文件提供每个网页的链接
    sitemap = download(url)
    links = re.findall('<loc>(.*?)</loc>', sitemap)
    for link in links:
        html = download(link)


def crawl_site(url, max_errors=5):  # 只利用ID来下载所有国家或地区的页面， 数据库ID不一定连续
    for page in itertools.count(1):
        pg_url = '{}{}'.format(url, page)
        html = download(pg_url)
        if html is None:
            num_errors += 1
            if num_errors == max_errors:
                break
            else:
                num_errors = 0


def link_crawler(start_url, link_regex, robots_url=None, user_agent='wswp', scrape_callback=None, max_depth=4, delay=1):
    """传入要爬取的网站URL和匹配想跟踪的链接的正则表达式
    如果要禁用深度判断(爬虫陷阱——动态生成的页面)——max_depth改为负数
    Crawl from the given start URL following links matched by link_regex"""
    seen = {}  # 修改为字典，而不是set。不再只记录访问过的网页链接。 增加已发现链接的深度记录
    if not robots_url:
        robots_url = '{}/robots.txt'.format(start_url)
    rp = get_robots_parser(robots_url)
    throttle = Throttle(delay)
    crawl_queue = [start_url]
    while crawl_queue:
        url = crawl_queue.pop()
        if rp.can_fetch(user_agent, url):
            depth = seen.get(url, 0)
            if depth > max_depth:
                # print('Skipping %s due to depth' % url)
                continue
            throttle.wait(url)
            html = download(url)
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


def get_links(html):
    """Return a list of links from html"""
    webpage_regex = re.compile("""<a[^>]+href=["'](.*?)["']""", re.IGNORECASE)  # re.I不区分大小写的匹配
    return webpage_regex.findall(html)


def get_robots_parser(robots_url):
    rp = robotparser.RobotFileParser()
    rp.set_url(robots_url)
    rp.read()
    return rp


class Throttle:  # 记录每个域名上次访问的时间，若距离上次访问时间小于制定延时，执行睡眠操作
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


if __name__ == '__main__':
    if 0:
        # download('http://httpstat.us/500')  # 恒定500错误
        # crawl_sitemap('http://example.python-scraping.com/sitemap.xml')  # xml文件中的网址都是loc中的
        # crawl_site('http://example.python-scraping.com/places/default/view/-')
        link_crawler('http://example.python-scraping.com', '/(index|view)/', max_depth=1, delay=0)
    else:
        """目标：自用查找并获取起点韩娱相关书籍
        chapter: 各个章节内容， info:书籍封页"""
        link_crawler('https://www.qidian.com', 'info', max_depth=10)
