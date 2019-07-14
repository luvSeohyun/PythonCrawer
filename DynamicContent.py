import requests
import string
from csv import DictWriter
from PyQt5.QtGui import *
from PyQt5.QtCore import *
from PyQt5.QtWebEngineWidgets import *
from PyQt5.QtWidgets import *
import lxml.html


PAGE_SIZE = 100
template_url = "http://example.python-scraping.com/places/ajax/search.json?&search_term=.&page_size={}&page=0"
countries = set()


def json_scraper():
    """由于依赖AJAX， 促使数据和表现层分离， 因此抽取数据更容易"""
    if 0:
        for letter in string.ascii_lowercase:
            print('Searching with %s' % letter)
            page = 0
            while True:
                resp = requests.get(template_url.format(letter, PAGE_SIZE, page))
                data = resp.json()
                print('adding %d more records from page %d' % (len(data.get('records')), page))
                for record in data.get('records'):
                    countries.add(record['country_or_district'])
                page += 1
                if page >= data['num_pages']:
                 break
        with open('data/countries_or_districts.txt', 'w') as countries_or_districts_file:
            countries_or_districts_file.write('n'.join(sorted(countries)))
    else:
        resp = requests.get(template_url.format(PAGE_SIZE))
        data = resp.json()
        records = data.get("records")
        with open('data/countries.csv', 'w') as countries_or_districts_file:
            wrtr = DictWriter(countries_or_districts_file, fieldnames=records[0].keys())
            wrtr.writeheader()
            wrtr.writerows(records)


def webkitTest():
    url = "http://example.python-scraping.com/dynamic"
    app = QApplication([])  # 其他qt对象初始化前，需要先有qt框架
    webengine = QWebEngineView()  # 创建对象，为web文档的构件
    loop = QEventLoop()  # 用于创建本地事件循环
    webengine.loadFinished.connect(loop.quit)  # loadFinished链接quit方法，可以在网页加载完成后停止事件循环
    webengine.load(QUrl(url))  # pyqt需要封装到QUrl中， pyside是可选
    loop.exec_()  # 喜欢等待网页加载完成
    page = webengine.page()
    webengine.show()
    app.exec_()
    html = page.mainFrame().toHtml()  # 抽取加载完成网页的html, 失败：无相关方法
    tree = lxml.html.fromstring(html)
    print(tree.cssselect('#result')[0].text_content())


def webkit_search():
    app = QApplication([])  # 其他qt对象初始化前，需要先有qt框架
    webengine = QWebEngineView()  # 创建对象，为web文档的构件
    loop = QEventLoop()  # 用于创建本地事件循环
    webengine.loadFinished.connect(loop.quit)  # loadFinished链接quit方法，可以在网页加载完成后停止事件循环
    webengine.load(QUrl('http://example.python-scraping.com/search'))  # pyqt需要封装到QUrl中， pyside是可选
    loop.exec_()  # 等待网页加载完成
    webengine.show()
    # webengine.page().("#search_term")
    app.exec_()


if __name__ == "__main__":
    """使用JavaScript时，不再是加载后立即下载页面全部内容。这让许多网页在浏览器中展示的内容可能不会出现在HTML源代码中。
    之前的抓取技术也就无法抽取网站的重要信息"""
    if 1:
        # D = Downloader()
        # html = D(u'http://example.python-scraping.com/search')
        # tree = fromstring(html)
        # print(tree.cssselect('div#results a'))  # return None, can't get search result)
        # json_scraper()
        webkitTest()
