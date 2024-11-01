import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

from crawlingnews.sources.ApnewsParser import ApnewsParser
from crawlingnews.sources.AprParser import AprParser
from crawlingnews.sources.YorkpressParser import YorkpressParser


def parse_yorkpress():
    url = 'https://www.yorkpress.co.uk/news/24109733.damen-metcalfe-attacked-homeless-man-outside-york-station/'
    PROXIES = {
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890'
    }
    HEADERS = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
    }

    sess = requests.Session()
    sess.mount('http://', HTTPAdapter(max_retries=10))
    sess.mount('https://', HTTPAdapter(max_retries=10))
    sess.keep_alive = False
    requests.packages.urllib3.disable_warnings()
    req = requests.get(url, proxies=PROXIES, headers=HEADERS, timeout=10, verify=False)
    req.close()

    soup = BeautifulSoup(req.text, features='lxml')
    parser = YorkpressParser()
    title = parser.get_title(soup)
    author = parser.get_authors(soup)
    date = parser.get_date(soup)
    ps = parser.get_paragraphs(soup)

    print(ps)


def parse_apnews():
    url = 'https://apnews.com/article/brazil-indigenous-land-rights-deadline-marco-temporal-55bab0520b77c189959f4b21564435cb'
    PROXIES = {
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890'
    }
    HEADERS = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
    }

    sess = requests.Session()
    sess.mount('http://', HTTPAdapter(max_retries=10))
    sess.mount('https://', HTTPAdapter(max_retries=10))
    sess.keep_alive = False
    requests.packages.urllib3.disable_warnings()
    req = requests.get(url, proxies=PROXIES, headers=HEADERS, timeout=10, verify=False)
    req.close()

    soup = BeautifulSoup(req.text, features='lxml')
    parser = ApnewsParser()
    title = parser.get_title(soup)
    author = parser.get_authors(soup)
    date = parser.get_date(soup)
    ps = parser.get_paragraphs(soup)

    print(ps)


def parse_apr():
    # url = 'https://www.apr.org/politics-government/2024-10-30/supreme-court-allows-virginia-to-purge-individuals-from-voter-rolls'
    url = 'https://www.apr.org/news/2024-10-30/scotus-allows-virginia-to-purge-voters-how-does-that-change-alabamas-situation'
    PROXIES = {
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890'
    }
    HEADERS = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
    }

    sess = requests.Session()
    sess.mount('http://', HTTPAdapter(max_retries=10))
    sess.mount('https://', HTTPAdapter(max_retries=10))
    sess.keep_alive = False
    requests.packages.urllib3.disable_warnings()
    req = requests.get(url, proxies=PROXIES, headers=HEADERS, timeout=10, verify=False)
    req.close()

    soup = BeautifulSoup(req.text, features='lxml')
    parser = AprParser()
    title = parser.get_title(soup)
    author = parser.get_authors(soup)
    date = parser.get_date(soup)
    ps = parser.get_paragraphs(soup)

    print(ps[0])


def parse_bbc():
    # url = 'https://www.apr.org/politics-government/2024-10-30/supreme-court-allows-virginia-to-purge-individuals-from-voter-rolls'
    # url = 'https://www.bbc.com/news/articles/c8rllx0xxlro'
    url = 'https://www.bbc.co.uk/news/articles/c8rllx0xxlro'
    PROXIES = {
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890'
    }
    HEADERS = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
    }

    sess = requests.Session()
    sess.mount('http://', HTTPAdapter(max_retries=10))
    sess.mount('https://', HTTPAdapter(max_retries=10))
    sess.keep_alive = False
    requests.packages.urllib3.disable_warnings()
    req = requests.get(url, proxies=PROXIES, headers=HEADERS, timeout=10, verify=False)
    req.close()

    soup = BeautifulSoup(req.text, features='lxml')

    # todo
    pass

if __name__ == '__main__':
    # parse_yorkpress()
    # parse_apnews()
    # parse_apr()
    parse_bbc()
