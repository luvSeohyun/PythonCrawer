import itertools
import random
import requests
from requests.exceptions import ConnectionError
import threading
import time
import urllib.request
from urllib.error import URLError, HTTPError, ContentTooShortError
import xlrd
import xlwt
from xlutils.copy import copy


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
        tmp = start + 65536*(ii - 1)
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
    # 改进版， 连续发生多次错误后才推出程序
    errors = 0
    # 69216
    # 151963
    for page in itertools.count(0):
        if page > 65535:
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
            data = res.json().get("data")
            if scrape_callback:
                surl = "https://space.bilibili.com/" + str(page + mid)
                scrape_callback(surl, data, infos)
        elif info.json().get("data"):
            data = None
            if res.json().get("data"):
                data = res.json().get("data")
            infos = info.json().get("data")
            if scrape_callback:
                surl = "https://space.bilibili.com/" + str(page + mid)
                scrape_callback(surl, data, infos)


class CsvBiliCallback:
    fileName = "biliInfo"

    def __init__(self):
        self.num = 0
        if 1:
            self.fileName += str(indexs)
            book = xlwt.Workbook(encoding="utf-8")
            sheet = book.add_sheet("notice&info")
            all_rows = ["url", "notice", "name", "sex", "sign"]
            print(all_rows)
        # all_rows = [tree.xpath('%s' % field)[i].text_content() for field in self.fields
        #             for i in range(len(tree.xpath('%s' % field)))]
            for ii in range(len(all_rows)):
                sheet.write(self.num, ii, all_rows[ii])
            self.num += 1
            book.save("bili/saved/{}.xls".format(self.fileName))
        # self.fields = ('//div[@class="i-ann-content"]', '//div[@class="h-basic-spacing"]/h4', '//span[@id="h-name"]')

    def __call__(self, url, data, info):
        # num = 0
        # max 65536
        book = xlrd.open_workbook("bili/saved/{}.xls".format(self.fileName))
        sheet = book.sheet_by_index(0)
        newbook = copy(book)
        newsheet = newbook.get_sheet(0)
        bname = info["name"] if "name" in info.keys() else ""
        bsex = info["sex"] if "sex" in info.keys() else ""
        bsign = info["sign"] if "sign" in info.keys() else ""
        all_rows = [url, data, bname, bsex, bsign]
        print("{}:{}".format(self.num, all_rows))
        name = url.split("/")
        for ii in range(len(all_rows)):
            newsheet.write(self.num, ii, all_rows[ii])
        self.num += 1

        newbook.save("bili/saved/{}.xls".format(self.fileName))
        self.save()

    def save(self):
        with open("bili/saved/indexs.txt", "w+") as fl:
            alllines = fl.readlines()
            nowline = int(self.fileName[-1])
            alllines[nowline] = self.num
            fl.writelines(alllines)


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
            thread = threading.Thread(target=crawl_site, args=('https://api.bilibili.com', ids[indexs], CsvBiliCallback()))
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
