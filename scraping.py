import re
from linkCrawler import download, link_crawler
from bs4 import BeautifulSoup
from lxml.html import fromstring, tostring
import time
import csv


FIELDS = ('area', 'population', 'iso', 'country_or_district', 'capital', 'continent', 'tld', 'currency_code',
          'currency_name', 'phone', 'postal_code_format', 'postal_code_regex', 'languages', 'neighbours')


class CsvCallback:
    def __init__(self):
        self.writer = csv.writer(open('data/countries_or_districts.csv', 'w'))
        self.fields = ('area', 'population', 'iso', 'country_or_district', 'capital', 'continent', 'tld',
                       'currency_code', 'currency_name', 'phone', 'postal_code_format',
                       'postal_code_regex', 'languages', 'neighbours')

    def __call__(self, url, html):
        if re.search('/view/', url):
            tree = fromstring(html)
            all_rows = [tree.xpath('//tr[@id="places_%s__row"]/td[@class="w2p_fw"]' % field)[0].text_content()
                        for field in self.fields]
            self.writer.writerow(all_rows)


class CsvQidianCallback:
    def __init__(self):
        self.writer = csv.writer(open('qidian/books.csv', 'w', errors='ignore'))
        self.fields = ('//div[@class="book-info "]/h1/em', '//p[@class="intro"]', '//div[@class="book-intro"]/p')

    def __call__(self, url, html):
        if re.search('/info/', url):
            tree = fromstring(html)
            all_rows = [tree.xpath('%s' % field)[i].text_content() for field in self.fields
                        for i in range(len(tree.xpath('%s' % field)))]
            self.writer.writerow(all_rows)



def re_scraper(html):  # 3.2s
    results = {}
    for field in FIELDS:
        results[field] = re.search('<tr id="places_%s__row">.*?<td class="w2p_fw">(.*?)</td>' % field, html).groups()[0]
    return results


def bs_scraper(html):  # 31.42s
    soup = BeautifulSoup(html, 'html5lib')
    results = {}
    for field in FIELDS:
        results[field] = soup.find('table').find('tr', id='places_%s__row' % field).find('td', class_='w2p_fw').text
    return results


def lxml_scraper(html):  # 4.23s， lxml+cssselect, $('tr')
    tree = fromstring(html)
    results = {}
    for field in FIELDS:
        results[field] = tree.cssselect('table > tr#places_%s__row > td.w2p_fw' % field)[0].text_content()
    return results


def lxml_xpath_scraper(html):  # 1.07s  lxml+xpath $x('//tr')
    tree = fromstring(html)
    results = {}
    for field in FIELDS:
        results[field] = tree.xpath('//tr[@id="places_%s__row"]/td[@class="w2p_fw"]' % field)[0].text_content()
    return results


def scrape_callback(url, html):
    if 0:
        fields = ('area', 'population', 'iso', 'country_or_district', 'capital', 'continent', 'tld', 'currency_code',
                  'currency_name', 'phone', 'postal_code_format', 'postal_code_regex', 'languages', 'neighbours')
        if re.search('/view/', url):
            tree = fromstring(html)
            all_rows = [tree.xpath('//tr[@id="places_%s__row"]/td[@class="w2p_fw"]' % field)[0].text_content()
                        for field in fields]
            print(url, all_rows)
    else:
        fields = ('//div[@class="book-info "]/h1/em', '//p[@class="intro"]', '//div[@class="book-intro"]/p')
        if re.search('/info/', url):
            tree = fromstring(html)
            all_rows = [tree.xpath('%s' % field)[i].text_content() for field in fields
                        for i in range(len(tree.xpath('%s' % field)))]
            print(url, all_rows)


def three_scraping(html, case):
    broken_html = '<ul class=country_or_district><li>Area<li>Population</ul'
    if case == 1:
        # 正则表达式抓取数据。 缺点：过于脆弱，网页更新后容易出问题
        print(re.findall(r'<td class="w2p_fw">(.*?)</td>', html)[1])  # 正则表达式
    elif case == 2:
        # 使用Beautiful Soup
        soup = BeautifulSoup(broken_html, 'html5lib')  # 默认的html.parser解析错误，改为使用html5lib
        fixed_html = soup.prettify()
        ul = soup.find('ul', attrs={'class': 'country_or_district'})
        print(ul.find('li'))
        print(ul.find_all('li'))
        soup = BeautifulSoup(html)  # 会提示warning
        tr = soup.find(attrs={'id': 'places_area__row'})
        td = tr.find(attrs={'class': 'w2p_fw'})
        area = td.text
        print(area)
    else:
        tree = fromstring(broken_html)
        fixed_html = tostring(tree, pretty_print=True)  # 相比Beautiful不添加html和body标签， 更快
        print(fixed_html)
        tree = fromstring(html)
        td = tree.cssselect('tr#places_area__row > td.w2p_fw')[0]  # #表示根据id， 。表示根据class
        area = td.text_content()
        print(area)


def xpaths(html):
    tree = fromstring(html)
    area = tree.xpath('//tr[@id="places_area__row"]/td[@class="w2p_fw"]/text()')[0]
    print(area)
    table = tree.xpath('//table')[0]
    print(table.getchildren())


if __name__ == '__main__':
    """能用xpath就用xpath， 时间最快"""
    if 0:
        # case = 3
        # url = 'http://example.python-scraping.com/places/default/view/Afghanistan-1'
        # htmls = download(url)
        # three_scraping(htmls, case)
        # xpaths(htmls)
        """ 性能测试
        NUM_ITERATIONS = 1000  # number of times to test each scraper
        html = download('http://example.python-scraping.com/places/default/view/United-Kingdom-233')
        scrapers = [('Regular expressions', re_scraper), ('BeautifulSoup', bs_scraper), ('Lxml', lxml_scraper),
                    ('Xpath', lxml_xpath_scraper)]
        for name, scraper in scrapers:
            # record start time of scrape
            start = time.time()
            for i in range(NUM_ITERATIONS):
                if scraper == re_scraper:
                    re.purge()
                result = scraper(html)
                # check scraped result is as expected
                # print(result['area'])
                assert result['area'] == '244,820 square kilometres'
            # recode end time of scrape and output the total
            end = time.time()
            print('%s: %.2f seconds' % (name, end - start))"""
        # link_crawler('http://example.python-scraping.com', '/(view)/', max_depth=1,
        #             scrape_callback=scrape_callback)
        link_crawler('http://example.python-scraping.com', '/(view)/', max_depth=1,
                     scrape_callback=CsvCallback())
    else:
        # url = 'https://book.qidian.com/info/1013839308'  # 起点书籍封页
        # htmls = download(url)
        # trees = fromstring(htmls)
        # intros = trees.xpath('//div[@class="book-intro"]/p/text()')
        # print(intros)
        link_crawler('https://www.qidian.com', '/(info)/', max_depth=10,
                    scrape_callback=CsvQidianCallback())
