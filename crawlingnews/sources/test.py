import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
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

    soup = BeautifulSoup(req.text)
    parser = YorkpressParser()
    title = parser.get_title(soup)
    author = parser.get_author(soup)
    date = parser.get_date(soup)
    ps = parser.get_paragraphs(soup)

    print(ps)



if __name__ == '__main__':
    parse_yorkpress()
