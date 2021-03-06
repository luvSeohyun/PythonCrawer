import itertools
import random
import requests
from requests.exceptions import ConnectionError
import threading
import time
import urllib.request
from urllib.error import URLError, HTTPError, ContentTooShortError
import pymongo


def load_user_agent(url):
    uass = []
    with open(url, 'rb') as uaf:
        for ua in uaf.readlines():
            if ua:
                uass.append(ua.strip()[:-1])
    random.shuffle(uass)
    return uass


def get_ids(start, count):
    idss = []
    if start < 1:
        start = 1
    for ii in range(1, count + 1):
        tmp = start + 600000*(ii - 1)
        idss.append(tmp)
    return idss


uas = load_user_agent("userAgents.txt")
ids = []
indexs = 0


def download(url, num_retries=2, charset='utf-8', proxy=None):  # proxy设置代理， 可能不支持https代理
    print('downloading:', url)
    request = urllib.request.Request(url)
    user_agent = random.choice(uas)
    request.add_header('User-agent', user_agent)  # 设置用户代理
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
                return download(url, num_retries - 1)
    return html


def crawl_site(url, mid, scrape_callback=None, max_errors=10):  # 只利用ID来下载所有国家或地区的页面， 数据库ID不一定连续
    global ids
    # 改进版， 连续发生多次错误后才推出程序
    errors = 0
    # 69216
    # 151963
    start = 0
    if scrape_callback:
        start = scrape_callback.num - mid + 1  # 存储的为page+mid
    for page in itertools.count(start):
        if page >= 655360:
            ids.remove(mid)
            break
        """请求 URL: https://api.bilibili.com/x/space/notice?mid=14100618&jsonp=jsonp"""
        pg_url = '{}/x/space/notice?mid={}&jsonp=jsonp'.format(url, page + mid)
        acc = '{}/x/space/acc/info?mid={}&jsonp=jsonp'.format(url, page + mid)
        space_url = "https://space.bilibili.com/{}".format(page + mid)
        time.sleep(1)
        headers = {
            "Host": "api.bilibili.com",
            "Origin": "https://space.bilibili.com",
            "Referer": "https: // space.bilibili.com / " + str(page + mid),
            "User - Agent": random.choice(uas)
        }
        repeat = 3
        res = 0
        while repeat > 0:  # 处理云端强制关闭
            try:
                res = requests.get(pg_url, headers, timeout=60)
                break
            except ConnectionError:
                print("Connection Error")
                time.sleep(600)
                repeat -= 1
        repeat = 3
        info = 0
        while repeat > 0:
            try:
                info = requests.get(acc, headers, timeout=60)
                break
            except ConnectionError:
                print("Connection Error")
                time.sleep(600)
                repeat -= 1
        if res.status_code != 200 and info.status_code != 200:
            if info.json().get('data') is None:
                print('requests false. {}'.format(info.json()))
                exit(-1)
            html = download(space_url)
            flag = False
            if not html or "404" in html:
                flag = True
            if errors > max_errors and flag:
                print(res)
                return
            else:
                if flag:
                    print("res error:{}, errors:{}".format(res, errors))
                    errors += 1
                else:
                    errors = 0
                continue
        if res.json().get("data"):
            infos = None
            if info.json().get("data"):
                infos = info.json().get("data")
            else:
                print('info is None: {}'.format(info.json()))
            data = res.json().get("data")
            if scrape_callback:
                surl = "https://space.bilibili.com/" + str(page + mid)
                scrape_callback(surl, data, infos, page + mid)
        elif info.json().get("data"):
            data = None
            if res.json().get("data"):
                data = res.json().get("data")
            infos = info.json().get("data")
            if scrape_callback:
                surl = "https://space.bilibili.com/" + str(page + mid)
                scrape_callback(surl, data, infos, page + mid)


class MongoBiliCallback:
    fileName = "biliInfo"

    def __init__(self):
        self.flags = indexs
        self.num = 0 + self.flags * 655360
        self.fileName += str(indexs)
        client = pymongo.MongoClient("localhost", 27017)
        database = client["biliLists"]
        table_name = "bili{}".format(self.flags)
        self.db = database[table_name]
        if table_name in database.list_collection_names():  # 找到已存储的最大mid
            max_item = self.db.find().sort("_id", -1)
            self.num = max_item[0]["_id"]

    def __call__(self, url, data, info, idss):
        bname = info["name"] if ("name" in info.keys()) else "None"
        bsex = info["sex"] if ("sex" in info.keys()) else "None"
        bsign = info["sign"] if ("sign" in info.keys()) else "None"
        all_rows = ["url", "notice", "name", "sex", "sign"]
        datas = {"_id": idss, all_rows[0]: url, all_rows[1]: data, all_rows[2]: bname, all_rows[3]: bsex,
                 all_rows[4]: bsign}
        print("{}:[{}, {}, {}, {}, {}]".format(self.num, datas[all_rows[0]], datas[all_rows[1]], datas[all_rows[2]],
                                               datas[all_rows[3]], datas[all_rows[4]]))
        self.num += 1
        self.db.insert_one(datas)


if __name__ == "__main__":
    counts = 10
    ids = get_ids(1, counts)
    threads = []
    while threads or ids:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
                indexs -= 1
        while len(threads) < counts and ids:
            # can start some more threads
            thread = threading.Thread(target=crawl_site, args=('https://api.bilibili.com', ids[indexs],
                                                               MongoBiliCallback()))
            # set daemon so main thread can exit when receives ctrl-c
            thread.setDaemon(True)
            thread.start()
            threads.append(thread)
            if indexs < counts:
                indexs += 1
        print(threads)
        for thread in threads:
            thread.join()

        time.sleep(1)
