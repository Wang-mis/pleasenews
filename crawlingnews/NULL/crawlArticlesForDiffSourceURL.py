"""
爬取不同域名下相关URL的新闻文章
"""
import os
import random
import re

from bs4 import BeautifulSoup

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from utils import TIME_RANGE, PROCESS_GDELT_PATH, create_date_range, json_to_dict, DAYS

PROXIES = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890'
}

HEADERS = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
}

DAY = str(TIME_RANGE[0])
FILTER = "MentionSourceName"  # FILTER = "SOURCEURL"
SAVE_TXT = "crawlingnews/NULL/txt/" + DAY + "/"
SAVE_ARTICLE = "crawlingnews/NULL/articles/" + DAY + "/"


def make_dirs():
    if not os.path.exists(SAVE_TXT):
        os.makedirs(SAVE_TXT)

    if not os.path.exists(SAVE_ARTICLE):
        os.makedirs(SAVE_ARTICLE)


def get_url_domain(domain) -> set[str]:
    """
    查询一个媒体在一天中发布的所有新闻的链接，
    生成unique_url_about_文件，
    文件每一行记录该媒体的一条新闻的链接+事件发生时间+UniqueID
    """
    unique_path = SAVE_TXT + "unique_url_about_" + domain + ".txt"
    if os.path.exists(unique_path):
        print(unique_path, "已经出现")
        fr = open(unique_path)
        lines = fr.readlines()
        fr.close()
        return set([line.strip() for line in lines])

    domain_urls_list = []
    data = pd.read_csv(PROCESS_GDELT_PATH)
    for row in data.itertuples():
        if FILTER == 'MentionSourceName':
            source_url_domain = getattr(row, 'MentionSourceName')
            if domain == source_url_domain:
                source_url = getattr(row, 'MentionIdentifier')
                day = getattr(row, 'Day')
                unique_id = getattr(row, 'UniqueID')
                domain_urls_list.append(source_url + "+" + str(day) + "+" + unique_id)
        elif FILTER == 'SOURCEURL':
            source_url_domain = getattr(row, 'SOURCEURL')
            if domain in source_url_domain.split("//")[-1].split("/")[0]:
                day = getattr(row, 'Day')
                unique_id = getattr(row, 'UniqueID')
                domain_urls_list.append(source_url_domain + "+" + str(day) + "+" + unique_id)

    # 去重
    domain_urls_set = set(domain_urls_list)
    print(f"{domain}域名下新闻文章链接个数为:{len(domain_urls_set)}")
    # 保存
    f = open(unique_path, "w")
    f.writelines([el + '\n' for el in domain_urls_set])
    f.close()

    return domain_urls_set


def get_error_url_domain(domain) -> set[str]:
    """ 查询一个媒体的爬取失败的新闻链接 """
    unique_path = SAVE_TXT + 'unique_url_about_' + domain + '.txt'
    error_path = SAVE_TXT + 'error_url_' + domain + '.txt'

    fr = open(unique_path)
    lines = fr.readlines()
    fr.close()

    url_data = {}
    for line in lines:
        url_data[line.strip().split('+')[0]] = line.strip().split('+')[1] + "+" + line.strip().split('+')[2]

    fr = open(error_path)
    lines = fr.readlines()
    fr.close()

    tmp_domain_urls_set = set([line.strip() + "+" + url_data[line.strip()] for line in lines])
    return tmp_domain_urls_set


def craw_articles(domain_urls, domain, div_article, div_attrs, use_sub, h_in,
                  h_x, h_attrs, a_x, a_attrs, t_x, t_attrs, find_p, p_attrs):  # use_sub是否创建子文件夹
    error_urls = []  # 保存出错的URL
    max_count_domain = 500
    len_domain = len(domain_urls)
    for i, url_data in enumerate(domain_urls):
        try:
            # 限制每个域名每天新闻数量，避免爬取时间过长
            if i > max_count_domain:
                break

            url, day, unique_id = url_data.split('+')
            print(f"{domain}下的第{i}/{len_domain}篇新闻")
            print("新闻URL：", url)
            print("事件发生日期：", day)
            print("Unique ID：", unique_id)
            if use_sub:
                sub_domain = url.split(domain + "/")[-1].split("/")[0]
                sub_path = SAVE_ARTICLE + domain + "/" + sub_domain + "/"
            else:
                sub_path = SAVE_ARTICLE + domain + "/"

            if not os.path.exists(sub_path):
                os.makedirs(sub_path)

            article_path = sub_path + unique_id + ".txt"
            if os.path.exists(article_path):
                continue

            # 获取网页
            sess = requests.Session()
            sess.mount('http://', HTTPAdapter(max_retries=10))
            sess.mount('https://', HTTPAdapter(max_retries=10))
            sess.keep_alive = False

            req = requests.get(url, proxies=PROXIES, headers=HEADERS, timeout=30)
            req.close()

            # 保存出错的URL
            if req.status_code != 200:
                print(f"爬取文章出错！URL={url}, State Code={req.status_code}")
                error_urls.append(url + '\n')
                continue

            # 解析网页
            content = req.text
            soup = BeautifulSoup(content, 'lxml')
            content_div = soup.find(name=div_article, attrs=div_attrs)

            # 获取标题、作者、时间的Tag
            tag = content_div if h_in else soup
            title_tag = [t for t in tag.find_all(h_x, attrs=h_attrs) if t.text.strip() != '']
            title_tag = None if len(title_tag) == 0 else title_tag[0]
            author_tag = [t for t in tag.find_all(a_x, attrs=a_attrs) if t.text.strip() != '']
            author_tag = None if len(author_tag) == 0 else author_tag[0]
            release_date_tag = [t for t in tag.find_all(t_x, attrs=t_attrs) if t.text.strip() != '']
            release_date_tag = None if len(release_date_tag) == 0 else release_date_tag[0]

            # 获取文章所有段落的Tag
            paragraph_tags = []
            for ele_p in find_p:
                paragraph_tags += content_div.find_all(ele_p, attrs=p_attrs)

            # 保存
            article = []

            # 获取新闻标题
            if title_tag:
                title = title_tag.text.strip().replace('\n', ' ').replace('\r', ' ')
                title = re.sub(r"\s+", " ", title)  # 将标题中的\f \n \t \r \v换成空格
                article.append(title + '\n')  # 将标题写入文件
                print("新闻标题：", title)
            else:
                article.append('_\n')
                print("获取新闻标题失败。")

            # 获取新闻作者
            if author_tag:
                author = author_tag.text.strip().replace('\n', ' ').replace('\r', ' ')
                author = re.sub(r"\s+", " ", author)
                print("新闻作者：", author)
                article.append(author + '\n')
            else:
                article.append('_\n')
                print("获取新闻作者失败。")

            # 获取发布时间
            if release_date_tag:
                release_date = release_date_tag.text.strip().replace('\n', ' ').replace('\r', ' ')
                release_date = re.sub(r"\s+", " ", release_date)
                print("新闻发布日期：", release_date)
                article.append(release_date + '\n')
            else:
                article.append('_\n')
                print("获取新闻发布日期失败。")

            article.append(day + "\n")  # 事件发生日期
            article.append(url + "\n")  # 新闻URL

            # 获取文章内容
            for p in paragraph_tags:
                paragraph = p.text
                paragraph = paragraph.encode("utf8", 'ignore').decode("utf8", "ignore")  # gbk编码报错
                paragraph = paragraph.replace('\n', ' ').replace('\r', ' ').strip()
                article.append(paragraph + '\n')

                f = open(article_path, "w", encoding='utf8')
                f.writelines(article)
                f.close()
        except Exception:
            print("爬取新闻失败！")
            continue

    # 保存出错的URL
    f = open(SAVE_TXT + "error_url_" + domain + ".txt", "w")
    f.writelines(error_urls)
    f.close()


def craw():
    make_dirs()
    domains = [
        "yorkpress.co.uk",  ##
        # "yahoo.com", # 8000+ ##
        # "washingtonpost.com",  # 0
        # "theguardian.com",  # 526 !!
        # "telegraphindia.com",  # 0
        "tass.com",  # 49 ##
        # "nytimes.com",  # 232 !!
        "apnews.com",  # 172 ##
        "apr.org",  # 55 ##
        # "bbc.com",  # 318 !!
        # "bbc.co.uk",  # 238 ##
        # "bignewsnetwork.com",  # 1138 ##
        "cnn.com",  # 459 ##
        "dailymail.co.uk",  # 1254 ##
        "foxnews.com",  # 343 ##
        "globalsecurity.org",  # 409 ##
        # "indiatimes.com",  # 2586 ##
        # "indiablooms.com",  # 86 --
        # "indianexpress.com",  # 747 !!
        "jpost.com",  # 307 ##
        # "mirror.co.uk",  # 641 --
    ]

    for domain in domains:
        # 获取爬虫参数
        configs = json_to_dict("crawlingnews/" + domain + ".config.json")
        div_article = configs["div_article"]
        div_attrs = configs["div_attrs"]
        if 'class' in div_attrs.keys():
            div_attrs['class'] = div_attrs['class'].split(' ')
        h_in = True if configs["h_in"] == "True" else False
        h_x = configs["h_x"]
        h_attrs = configs["h_attrs"]
        if 'class' in h_attrs.keys():
            h_attrs['class'] = h_attrs['class'].split(' ')
        a_x = configs["a_x"]
        a_attrs = configs["a_attrs"]
        if 'class' in a_attrs.keys():
            a_attrs['class'] = a_attrs['class'].split(' ')
        t_x = configs["t_x"]
        t_attrs = configs["t_attrs"]
        if 'class' in t_attrs.keys():
            t_attrs['class'] = t_attrs['class'].split(' ')
        find_p = configs["find_p"]
        p_attrs = configs["p_attrs"]
        if 'class' in p_attrs.keys():
            p_attrs['class'] = p_attrs['class'].split(' ')

        # 第一次爬取 没有出现error_url_tmp_domain.txt
        error_url_txt = SAVE_TXT + "error_url_" + domain + ".txt"
        if not os.path.exists(error_url_txt):
            domain_urls = get_url_domain(domain)
            craw_articles(domain_urls, domain, div_article=div_article, div_attrs=div_attrs, use_sub=False,
                          h_x=h_x, h_in=h_in, h_attrs=h_attrs, a_x=a_x, a_attrs=a_attrs, t_x=t_x, t_attrs=t_attrs,
                          find_p=find_p, p_attrs=p_attrs)
        else:
            # 再次获取出错的链接文章
            domain_urls = get_error_url_domain(domain)
            craw_articles(domain_urls, domain, div_article=div_article, div_attrs=div_attrs, use_sub=False,
                          h_x=h_x, h_in=h_in, h_attrs=h_attrs, a_x=a_x, a_attrs=a_attrs, t_x=t_x, t_attrs=t_attrs,
                          find_p=find_p, p_attrs=p_attrs)


def crawl_days():
    global DAY, SAVE_TXT, SAVE_ARTICLE

    for day in DAYS:
        DAY = str(day)
        SAVE_TXT = "crawlingnews/NULL/txt/" + DAY + "/"
        SAVE_ARTICLE = "crawlingnews/NULL/articles/" + DAY + "/"
        craw()


if __name__ == "__main__":
    crawl_days()
