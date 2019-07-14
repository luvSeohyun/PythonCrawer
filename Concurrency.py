import csv
from zipfile import ZipFile
from io import BytesIO, TextIOWrapper
import requests
import time
import threading
import urlparse2
from DownloadCache import Downloader
from redis import StrictRedis
import re
import socket
from urllib import robotparser
from urllib.parse import urljoin, urlparse
import multiprocessing


socket.setdefaulttimeout(60)
SLEEP_TIME = 1


def get_robots_parser(robots_url):
    """return the robots parser object using the robots_url"""
    try:
        rp = robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        return rp
    except Exception as e:
        print("Error finding robots url:", robots_url, e)


def get_links(html, link_regex="."):
    """return a list of links (using simple regex matching) from the html content"""
    # a regular expression to extract all links from the webpage
    webpage_regex = re.compile("""<a[^>]+href=["'](.*?)["']""", re.IGNORECASE)
    # list of all links from the webpage
    links = webpage_regex.findall(html)
    links = (link for link in links if re.match(link_regex, link))
    return links


def link_crawler(start_url, link_regex, robots_url=None, user_agent='wswp',
                 proxies=None, delay=3, max_depth=4, num_retries=2, cache={}, scraper_callback=None):
    """ Crawl from the given start URL following links matched by link_regex. In the current
        implementation, we do not actually scrapy any information.

        args:
            start_url (str or list of strs): web site(s) to start crawl
            link_regex (str): regex to match for links
        kwargs:
            robots_url (str): url of the site's robots.txt (default: start_url + /robots.txt)
            user_agent (str): user agent (default: wswp)
            proxies (list of dicts): a list of possible dicts for http / https proxies
                For formatting, see the requests library
            delay (int): seconds to throttle between requests to one domain (default: 3)
            max_depth (int): maximum crawl depth (to avoid traps) (default: 4)
            num_retries (int): # of retries when 5xx error (default: 2)
            cache (dict): cache dict with urls as keys and dicts for responses (default: {})
            scraper_callback: function to be called on url and html content
    """
    if isinstance(start_url, list):
        crawl_queue = start_url
    else:
        crawl_queue = [start_url]
    # keep track which URL's have seen before
    # 使用字典来存储每个域名的解析器
    seen, robots = {}, {}
    D = Downloader(delay=delay, user_agent=user_agent, proxies=proxies, cache=cache)
    while crawl_queue:
        url = crawl_queue.pop()
        # 可能部分网站没有robots.txt文件
        no_robots = False
        if 'http' not in url:
            continue
        domain = '{}://{}'.format(urlparse(url).scheme, urlparse(url).netloc)
        rp = robots.get(domain)
        if not rp and domain not in robots:
            robots_url = '{}/robots.txt'.format(domain)
            rp = get_robots_parser(robots_url)
            if not rp:
                # issue finding robots.txt, still crawl
                no_robots = True
            robots[domain] = rp
        elif domain in robots:
            no_robots = True
        # check url passes robots.txt restrictions
        if no_robots or rp.can_fetch(user_agent, url):
            depth = seen.get(url, 0)
            if depth == max_depth:
                print('Skipping %s due to depth' % url)
                continue
            html = D(url, num_retries=num_retries)
            if not html:
                continue
            if scraper_callback:
                links = scraper_callback(url, html) or []
            else:
                links = []
            # filter for links matching our regular expression
            for link in get_links(html) + links:
                if re.match(link_regex, link):
                    if 'http' not in link:
                        if link.startswith('//'):
                            link = '{}:{}'.format(urlparse(url).scheme, link)
                        elif link.startswith('://'):
                            link = '{}{}'.format(urlparse(url).scheme, link)
                        else:
                            link = urljoin(domain, link)

                    if link not in seen:
                        seen[link] = depth + 1
                        crawl_queue.append(link)
        else:
            print('Blocked by robots.txt:', url)


def threaded_crawler(start_url, link_regex, delay=5, cache={}, scrape_callback=None, user_agent='wswp', proxies=None,
                     num_retries=2, max_depth=4, max_threads=10):
    """ Crawl from the given start URLs following links matched by link_regex. In this
            implementation, we do not actually scrape any information.

            args:
                start_url (str or list of strs): web site(s) to start crawl
                link_regex (str): regex to match for links
            kwargs:
                user_agent (str): user agent (default: wswp)
                proxies (list of dicts): a list of possible dicts for http / https proxies
                    For formatting, see the requests library
                delay (int): seconds to throttle between requests to one domain (default: 3)
                max_depth (int): maximum crawl depth (to avoid traps) (default: 4)
                num_retries (int): # of retries when 5xx error (default: 2)
                cache (dict): cache dict with urls as keys and dicts for responses (default: {})
                scraper_callback: function to be called on url and html content
        """
    if isinstance(start_url, list):
        crawl_queue = start_url
    else:
        crawl_queue = [start_url]
    # keep track which URL's have seen before
    seen, robots = {}, {}
    D = Downloader(cache=cache, delay=delay, user_agent=user_agent, proxies=proxies)

    def process_queue():
        while crawl_queue:
            url = crawl_queue.pop()
            no_robots = False
            if not url or "http" not in url:
                continue
            domain = "{}://{}".format(urlparse(url).scheme, urlparse(url).netloc)
            rp = robots.get(domain)
            if not rp and domain not in robots:
                robots_url = "{}/robots.txt".format(domain)
                rp = get_robots_parser(robots_url)
                if not rp:
                    # issue finding robots.txt, still crawl
                    no_robots = True
                robots[domain] = rp
            elif domain in robots:
                no_robots = True
            # check url passes robots.txt resrictions
            if no_robots or rp.can_fetch(user_agent, url):
                depth = seen.get(url, 0)
                if depth == max_depth:
                    print("skip %s due to max depth" % url)
                    continue
                html = D(url, num_retries=num_retries)
                if not html:
                    continue
                if scrape_callback:
                    links = scrape_callback(url, html) or []
                else:
                    links = []
                # filter for links matching our regular expression
                for link in get_links(html) + links:
                    if re.match(link_regex, link):
                        if "http" not in link:
                            if link.startswith("//"):
                                link = "{};{}".format(urlparse(url).scheme, link)
                            elif link.startswith("://"):
                                link = "{}{}".format(urlparse(url).scheme, link)
                            else:
                                link = urljoin(domain, link)
                        if link not in seen:
                            seen[link] = depth + 1
                            crawl_queue.append(link)
            else:
                print("Blocked by robots.txt:", url)

    # wait for all download threads to finish
    threads = []
    print(max_threads)
    while threads or crawl_queue:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        while len(threads) < max_threads and crawl_queue:
            # can start some more threads
            thread = threading.Thread(target=process_queue)
            # set daemon so main thread can exit when receives ctrl-c
            thread .setDaemon(True)
            thread.start()
            threads.append(thread)
        print(threads)
        for thread in threads:
            thread.join()

        time.sleep(SLEEP_TIME)


def clean_link(url, domain, link):
    if link.startswith("//"):
        link = "{};{}".format(urlparse(url).scheme, link)
    elif link.startswith("://"):
        link = "{}{}".format(urlparse(url).scheme, link)
    else:
        link = urljoin(domain, link)
    return link


def threaded_crawler_rq(start_url, link_regex, delay=5, cache={}, scrape_callback=None, user_agent='wswp', proxies=None,
                     num_retries=2, max_depth=4, max_threads=10):
    """ Crawl from the given start URLs following links matched by link_regex. In this
            implementation, we do not actually scrape any information.

            args:
                start_url (str or list of strs): web site(s) to start crawl
                link_regex (str): regex to match for links
            kwargs:
                user_agent (str): user agent (default: wswp)
                proxies (list of dicts): a list of possible dicts for http / https proxies
                    For formatting, see the requests library
                delay (int): seconds to throttle between requests to one domain (default: 3)
                max_depth (int): maximum crawl depth (to avoid traps) (default: 4)
                num_retries (int): # of retries when 5xx error (default: 2)
                cache (dict): cache dict with urls as keys and dicts for responses (default: {})
                scraper_callback: function to be called on url and html content
        """
    crawl_queue = RedisQueue
    crawl_queue.push(start_url)
    # keep track which URL's have seen before
    robots = {}
    D = Downloader(cache=cache, delay=delay, user_agent=user_agent, proxies=proxies)

    def process_queue():
        while len(crawl_queue):
            url = crawl_queue.pop()
            no_robots = False
            if not url or "http" not in url:
                continue
            domain = "{}://{}".format(urlparse(url).scheme, urlparse(url).netloc)
            rp = robots.get(domain)
            if not rp and domain not in robots:
                robots_url = "{}/robots.txt".format(domain)
                rp = get_robots_parser(robots_url)
                if not rp:
                    # issue finding robots.txt, still crawl
                    no_robots = True
                robots[domain] = rp
            elif domain in robots:
                no_robots = True
            # check url passes robots.txt resrictions
            if no_robots or rp.can_fetch(user_agent, url):
                depth = crawl_queue.get_depth(url) or 0
                if depth == max_depth:
                    print("skip %s due to max depth" % url)
                    continue
                html = D(url, num_retries=num_retries)
                if not html:
                    continue
                if scrape_callback:
                    links = scrape_callback(url, html) or []
                else:
                    links = []
                # filter for links matching our regular expression
                for link in get_links(html, link_regex) + links:
                    if re.match(link_regex, link):
                        if "http" not in link:
                            link = clean_link(url, domain, link)
                        crawl_queue.push(link)
                        crawl_queue.set_depth(link, depth + 1)
            else:
                print("Blocked by robots.txt:", url)

    # wait for all download threads to finish
    threads = []
    print(max_threads)
    while threads or len(crawl_queue):
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        while len(threads) < max_threads and crawl_queue:
            # can start some more threads
            thread = threading.Thread(target=process_queue)
            # set daemon so main thread can exit when receives ctrl-c
            thread .setDaemon(True)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        time.sleep(SLEEP_TIME)


def mp_threaded_crawler(*args, **kwargs):
    """create a multiprocessing threaded crawler"""
    processes = []
    num_procs = kwargs.pop('num_procs')
    if not num_procs:
        num_procs = multiprocessing.cpu_count()
        print("cpu count:", num_procs)
    for _ in range(num_procs):
        proc = multiprocessing.Process(target=threaded_crawler_rq, args=args, kwargs=kwargs)
        proc.start()
        processes.append(proc)
    # wait for process to complete
    for proc in processes:
        proc.join()


class AlexaCallback:
    def __init__(self, max_urls=500):
        self.max_urls = max_urls
        self.seed_url = "http://s3.amazonaws.com/alexa-static/top-1m.csv.zip"
        self.urls = []

    def __call__(self):
        resp = requests.get(self.seed_url, stream=True)
        with ZipFile(BytesIO(resp.content)) as zf:
            csv_filename = zf.namelist()[0]  # zip压缩文件中只有一个文件， 直接取0即可
            with zf.open(csv_filename) as csv_file:
                for _, website in csv.reader(TextIOWrapper(csv_file)):
                    self.urls.append('http://' + website)
                    if len(self.urls) == self.max_urls:
                        break


class RedisQueue:
    """RedisQueue helps store urls to crawl to Redis
    Initialization componets:
    client: a Redis client connected to the key-value database for the web crawling cache(if not set, a localhost:6379
        default connection is used).
    db (int): which database to use for Redis
    queue_name(str): name for queue(default: wswp)"""

    # TODO set last time about access url
    def __init__(self, client=None, db=0, queue_name='wswp'):
        self.client = (StrictRedis(host='localhost', port=6379, db=db)) if client else client  # redis列表类型
        self.name = "queue:%s" % queue_name  # 存队列名
        self.seen_set = "seen:%s" % queue_name  # 存集合名
        self.depth = "depth:%s" % queue_name

    def __len__(self):
        return self.client.llen(self.name)

    def push(self, element):
        """push an element to the tail of the queue"""
        if isinstance(element, list):
            element = [e for e in element if not self.already_seen(e)]
            self.client.lpush(self.name, *element)
            self.client.sadd(self.seen_set, *element)  # 添加新键
        elif not self.client.already_seen(element):
            self.client.lpush(self.name, element)
            self.client.sadd(self.seen_set, element)

    def pop(self):
        """pop an element from the head of the queue"""
        return self.client.rpop(self.name).decode('utf-8')

    def already_seen(self, element):
        """determine if an element has already been seen"""
        return self.client.sismember(self.seen_set, element)  # 测试成员

    def set_depth(self, element, depth):
        """set the seen hash and depth"""
        self.client.hset(self.depth, element, depth)

    def get_depth(self, element):
        """get the seen hash and depth"""
        return (lambda dep: int(dep) if dep else 0)(self.client.hget(self.depth, element))


if __name__ == "__main__":
    """Alexa统计的最受欢迎的100万网站列表"""
    choose = 4
    from DownloadCache import RedisCache
    if choose == 0:
        AC = AlexaCallback()
        AC()
        start_time = time.time()
        link_crawler(AC.urls, '$^', cache=RedisCache())
        print("Total time:", time.time() - start_time)
    elif choose == 1:
        AC = AlexaCallback()
        AC()
        import argparse
        parser = argparse.ArgumentParser(description="Threaded link crawler")
        parser.add_argument('max_threads', type=int, help="maximum number of threads", nargs="?", default=5)
        parser.add_argument('url_pattern', type=str, help="regex pattern for url matching", nargs="?", default="$^")
        par_args = parser.parse_args()
        start_time = time.time()
        threaded_crawler(AC.urls, par_args.url_pattern, cache=RedisCache(), max_threads=par_args.max_threads)
        print("Total time:", time.time() - start_time)
    elif choose == 2:
        AC = AlexaCallback()
        AC()
        import argparse
        parser = argparse.ArgumentParser(description="Multiprocessing threaded link crawler")
        parser.add_argument('max_threads', type=int, help="maximum number of threads", nargs="?", default=5)
        parser.add_argument('num_procs', type=int, help="number of processes", nargs="?", default=None)
        parser.add_argument('url_pattern', type=str, help="regex pattern for url matching", nargs="?", default="$^")
        par_args = parser.parse_args()
        start_time = time.time()
        mp_threaded_crawler(AC.urls, par_args.url_pattern, cache=RedisCache(), num_procs=par_args.num_procs,
                            max_threads=par_args.max_threads)
        print("Total time:", time.time() - start_time)
    else:
        # qidian download
        import argparse
        parser = argparse.ArgumentParser(description="Multiprocessing threaded link crawler")
        parser.add_argument('max_threads', type=int, help="maximum number of threads", nargs="?", default=5)
        parser.add_argument('num_procs', type=int, help="number of processes", nargs="?", default=None)
        parser.add_argument('url_pattern', type=str, help="regex pattern for url matching", nargs="?", default="info")
        par_args = parser.parse_args()
        # start_time = time.time()
        mp_threaded_crawler("https://www.qidian.com", par_args.url_pattern, cache=RedisCache(),
                            num_procs=par_args.num_procs, max_threads=par_args.max_threads)
        # print("Total time:", time.time() - start_time)

    # print(urls)
