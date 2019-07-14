from linkCrawler import Throttle, get_robots_parser, get_links
from random import choice
import urllib
import re
from urllib.parse import urljoin, urlsplit, quote
from urllib.error import URLError, HTTPError, ContentTooShortError
import os
import json
import zlib
from datetime import datetime, timedelta
from redis import StrictRedis
import string
from lxml.html import fromstring


FIELDS = ('//div[@class="book-info "]/h1/em', '//p[@class="intro"]', '//div[@class="book-intro"]/p')


class Downloader:
    def __init__(self, delay=10, user_agent="wswp", proxies=None, cache={}):
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


def link_crawler(start_url, link_regex, robots_url=None, user_agent='wswp', scrape_callback=None, max_depth=4, delay=1,
                 proxies=None, num_retries=10, cache={}):
    """传入要爬取的网站URL和匹配想跟踪的链接的正则表达式
    如果要禁用深度判断(爬虫陷阱——动态生成的页面)——max_depth改为负数
    Crawl from the given start URL following links matched by link_regex"""
    if isinstance(start_url, list):
        crawl_queue = start_url
    else:
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
    def __init__(self, cache_dir='cache', max_len=255, compress=True, encoding='utf-8', expires=timedelta(days=30)):
        self.cache_dir = cache_dir
        self.max_len = max_len
        self.compress = compress
        self.encoding = encoding
        self.expires = expires

    def url_to_path(self, url):  # 磁盘缓存边界情况处理
        """return file system path string for givin URL"""
        components = urlsplit(url)
        path = components.path
        if not path:
            path = '/index.html'
        elif path.endswith('/'):
            path += 'index.html'
        filename = components.netloc + path + components.query
        filename = re.sub('[^/0-9a-zA-Z\-.,;_]', '_', filename)
        filename = '/'.join(segment[:255] for segment in filename.split('/'))
        return os.path.join(self.cache_dir, filename)

    def __getitem__(self, url):
        """Load data from disk for given URL"""
        path = self.url_to_path(url)
        mode = ('rb' if self.compress else 'r')
        if os.path.exists(path):
            with open(path, mode) as fp:  # 最小化缓存所需空间
                if self.compress:
                    data = zlib.decompress(fp.read()).decode(self.encoding)
                    return json.loads(data)
                else:
                    data = json.load(fp)
                exp_date = data.get('expires')
                if exp_date and datetime.strftime(exp_date, '%Y-%m-%dT%H:%M:%S') <= datetime.utcnow():
                    print('cache expired!', exp_date)
                    raise KeyError(url + 'has expired.')
                return data
        else:
            # URL has not yet been cached
            raise KeyError(url + 'not exist')

    def __setitem__(self, url, result):
        """Save data to disk for given url"""
        result['expires'] = (datetime.utcnow() + self.expires).isoformat(timespec='seconds')
        path = self.url_to_path(url)  # 映射为安全文件名
        folder = os.path.dirname(path)
        if not os.path.exists(folder):
            os.makedirs(folder)
        mode = ('wb' if self.compress else 'w')
        with open(path, mode) as fp:
            if self.compress:
                data = bytes(json.dumps(result), self.encoding)
                fp.write(zlib.compress(data))
            else:
                json.dump(result, path)  # 序列化处理，然后保存到磁盘


class RedisCache:
    def __init__(self, client=None, expires=timedelta(days=30), encoding='utf-8', compress=True):
        # if a client object is not passed then try
        # connecting to redis at the default localhost port
        self.client = StrictRedis(host='localhost', port=6379, db=0, charset=encoding,
                                  errors='ignore') \
            if client is None else client
        self.expires = expires
        self.encoding = encoding
        self.compress = compress

    def __getitem__(self, url):
        """Load value from redis for given URL"""
        record = self.client.get(url)
        if record:
            if self.compress:
                record = zlib.decompress(record)
            return json.loads(record.decode(self.encoding))
        else:
            # URL has not yet been cached
            raise KeyError(url + 'not exist')

    def __setitem__(self, url, result):
        """Save value in redis for given url"""
        if re.search('/info/', url) and result['code'] != 404:
            tree = fromstring(result['html'])
            result['saved'] = [tree.xpath('%s' % field)[i].text_content() for field in FIELDS
                           for i in range(len(tree.xpath('%s' % field)))]
            print(result['saved'])
        data = bytes(json.dumps(result), self.encoding)
        if self.compress:
            data = zlib.compress(data)
        self.client.setex(url, self.expires, data)


if __name__ == "__main__":
    """如果执行一个大型爬虫工作，缓存可以无需重新爬取可能已抓取的页面，并能离线访问页面"""
    if 0:
        # link_crawler('http://example.python-scraping.com/', '/(index|view)', cache=DiskCache())
        link_crawler('http://example.python-scraping.com/', '/(index|view)', cache=RedisCache())
    else:
        link_crawler('https://www.qidian.com', 'info', cache=RedisCache())
    # redis test
    # r = StrictRedis(host='localhost', port=6379, db=0)
    # r.set('test', 'answer')
    # print(r.get('test'))
