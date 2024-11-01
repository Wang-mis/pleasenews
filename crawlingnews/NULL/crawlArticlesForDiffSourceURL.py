"""
爬取不同域名下相关URL的新闻文章
"""
import os
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm

from utils import json_to_dict

txts_dir = "crawlingnews/NULL/txt/"
articles_dir = "crawlingnews/NULL/articles/"
# configs_dir = "crawlingnews/"
configs_dir = "crawlingnews/configs/"

PROXIES = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890'
}

HEADERS = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
    # 'User-Agent': "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
    #               "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Mobile Safari/537.36 Edg/129.0.0.0"
}

NYTIMES_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7,en-GB;q=0.6,en-US;q=0.5',
    'Cache-Control': 'max-age=0',
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
    'Cookie': r'nyt-a=yslptueUyh390lUuMUB1CE; _cb=BkXLeVB27HwDlDI6c; _gcl_au=1.1.1788262337.1728382037; nyt-gdpr=0; nyt-counter=1; nyt-tos-viewed=true; purr-pref-agent=<G_<C_<T0<Tp1_<Tp2_<Tp3_<Tp4_<Tp7_<a12; nyt-purr=cfhhcfhhhckfhcfhhgah2; g_state={"i_p":1730095522390,"i_l":1}; nyt-us=0; nyt-geo=HK; _ga=GA1.1.966910138.1730088415; checkout_entry_point=direct; ath_content_edition=uk; ath_anonymous_user_id=17300884149345618251; iter_id=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhaWQiOiI2NzA1MDQ1NTQxMGY1MTA4YmM3OTZhNTYiLCJhaWRfZXh0Ijp0cnVlLCJjb21wYW55X2lkIjoiNWMwOThiM2QxNjU0YzEwMDAxMmM2OGY5IiwiaWF0IjoxNzMwMDg4NjIxfQ.r0T5DgIYPdMRhpECkxKGiCw-OcKZH7KlL2R--CEaFow; _ga_EKN0VKJGMQ=GS1.1.1730088414.1.0.1730088926.60.0.0; _v__chartbeat3=Br6dRDCSChD0Uc0kx; purr-cache=<G_<C_<T0<Tp1_<Tp2_<Tp3_<Tp4_<Tp7_<a0_<K0<S0<r<ur; NYT-MPS=0000000cc73d0055888cea3b169f54474f1553c42e627584a2146320d12c00141bd52940d349a3ce212756f08a422c2b5d0943a40f323c7321a7b54b412b8f; nyt-auth-method=username; RT="z=1&dm=nytimes.com&si=0b558ac3-cdc8-4efa-b8b9-f5c481535dfc&ss=m2shy3hu&sl=2&tt=26z&bcn=%2F%2F684d0d47.akstat.io%2F&nu=169tn99b1&cl=nb3g&ul=o0ca&hd=o0m7"; _fbp=fb.1.1730089495109.401457472732908316; _scid=QnjD7HmtfCr3-qBZqlnCiSqNj2ZXOWoR; _sctr=1%7C1730044800000; regi_cookie=; _scid_r=XvjD7HmtfCr3-qBZqlnCiSqNj2ZXOWoRZUHfyw; _rdt_uuid=1730089495175.a402bd5b-c34b-4928-8c97-69a444ac4748; _ScCbts=%5B%5D; nyt-b3-traceid=f9bfe13194d74360803995e2ca305dbc; nyt-jkidd=uid=263822989&lastRequest=1730090108623&activeDays=%5B0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C1%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C1%5D&adv=2&a7dv=1&a14dv=1&a21dv=2&lastKnownType=sub&newsStartDate=1730075363&entitlements=AAA+ATH+AUD+CKG+MM+MOW+MSD+MTD+WC+XWD; _chartbeat2=.1728382036627.1730090108595.0000000000000001.C8A38XCGVETwD7__ySBUltmOCI-A8A.1; _cb_svref=external; datadome=jjv8EeLoN3EfaLaqxUSJcQlWn_FRR7O~TiX7fEWTm7IxcvLxFq5yd5ukmsi6Gn1fM~vzqjKVS_uR22jzKitjRI7OXDpIS5sab1nqyaEzqhBTf1D6Gn~sfbWSp9nI3IFt; NYT-S=0^CBkSMQiUpPy4BhCNqvy4BhoSMS2PQdKz962L8eP-6N_f8GTLII295n0qAh53OJSk_LgGQgAaQLxaZMG-BqXbxqek9LEH0C73JexF8sq-EaMMRu5jm4eWYlfVQCdW2Sya2OyeZ3tVkEGW3XjYrbmmucEVLNlfUg4=; SIDNY=CBkSMQiUpPy4BhCNqvy4BhoSMS2PQdKz962L8eP-6N_f8GTLII295n0qAh53OJSk_LgGQgAaQLxaZMG-BqXbxqek9LEH0C73JexF8sq-EaMMRu5jm4eWYlfVQCdW2Sya2OyeZ3tVkEGW3XjYrbmmucEVLNlfUg4=; _dd_s=rum=0&expire=1730091070791&logs=1&id=0ec2553c-f9b9-47dd-b43d-07fed0e6c979&created=1730088414846',
}


def get_url_domain(day, domain) -> set[str]:
    """
    查询一个媒体在一天中发布的所有新闻的链接，
    生成unique_url_about_文件，
    文件每一行记录该媒体的一条新闻的链接+事件发生时间+UniqueID
    """
    txt_dir = txts_dir + day + "/"
    unique_path = txt_dir + "unique_url_about_" + domain + ".txt"
    if os.path.exists(unique_path):
        print(unique_path, "已经出现")
        fr = open(unique_path)
        lines = fr.readlines()
        fr.close()
        return set([line.strip() for line in lines])

    domain_urls_list = []
    data = pd.read_csv("merge/" + day + ".media.merge.csv")
    for row in data.itertuples():
        source_url_domain = getattr(row, 'MentionSourceName')
        if domain == source_url_domain:
            source_url = getattr(row, 'MentionIdentifier')
            event_day = getattr(row, 'Day')
            unique_id = getattr(row, 'UniqueID')
            domain_urls_list.append(source_url + "+" + str(event_day) + "+" + unique_id)

    # 去重
    domain_urls_set = set(domain_urls_list)
    print(f"{domain}域名下新闻文章链接个数为:{len(domain_urls_set)}")
    # 保存
    f = open(unique_path, "w")
    f.writelines([el + '\n' for el in domain_urls_set])
    f.close()

    return domain_urls_set


def get_error_url_domain(day, domain) -> set[str]:
    """ 查询一个媒体的爬取失败的新闻链接 """
    txt_dir = txts_dir + day + "/"

    unique_path = txt_dir + 'unique_url_about_' + domain + '.txt'
    error_path = txt_dir + 'error_url_' + domain + '.txt'

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


def craw_articles(day, domain_urls, domain, configs):  # use_sub是否创建子文件夹
    div_article = configs['div_article']
    div_attrs = configs['div_attrs']
    h_in = configs['h_in']
    h_x = configs['h_x']
    h_attrs = configs['h_attrs']
    a_x = configs['a_x']
    a_attrs = configs['a_attrs']
    t_x = configs['t_x']
    t_attrs = configs['t_attrs']
    find_p = configs['find_p']
    p_attrs = configs['p_attrs']

    article_dir = articles_dir + day + "/"
    txt_dir = txts_dir + day + "/"

    # 爬取新闻，并将爬取失败的文章的URL DATA写入error文件中
    error_file_path = txt_dir + "error_url_" + domain + ".txt"
    with open(error_file_path, 'w') as error_file:
        process_bar = tqdm(total=len(domain_urls), desc="")
        for url_data in domain_urls:
            try:
                # 输出当前要爬取的文章的信息
                url, date, unique_id = url_data.split('+')
                print()
                process_bar.set_description(f"\nURL={url}\nDate={date}\nID={unique_id}\n")
                process_bar.update(1)
                print()
                # 创建文件夹和文件
                sub_path = article_dir + domain + "/"
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
                requests.packages.urllib3.disable_warnings()
                headers = NYTIMES_HEADERS if domain == "nytimes.com" else HEADERS
                req = requests.get(url, proxies=PROXIES, headers=headers, timeout=10, verify=False)
                req.close()

                # 爬取失败，保存出错的URL
                if req.status_code != 200:
                    print(f"爬取新闻出错！URL={url}, State Code={req.status_code}")
                    error_file.write(url_data + '\n')
                    continue

                # 解析网页
                content = req.text
                soup = BeautifulSoup(content, 'lxml')
                content_div = soup.find(name=div_article, attrs=div_attrs)

                # 解析网页内容错误，保存出错的URL
                if content_div is None:
                    print(f"解析网页内容错误！URL={url} \n")
                    error_file.write(url_data + '\n')
                    continue

                # 获取标题、作者、时间的Tag
                tag = content_div if h_in else soup
                title_tag, author_tag, release_date_tag = None, None, None
                if tag:
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

                article.append(date + "\n")  # 事件发生日期
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
            except Exception as e:
                print(f"爬取新闻出错！URL_DATA={url_data} \n", e)
                error_file.write(url_data + '\n')
                continue


def craw_day(day, my_domains=None):
    txt_dir = txts_dir + day + "/"
    article_dir = articles_dir + day + "/"

    if not os.path.exists(article_dir):
        os.makedirs(article_dir)

    if not os.path.exists(txt_dir):
        os.makedirs(txt_dir)

    domains = [
        "yorkpress.co.uk",  ##
        "dw.com",
        "yahoo.com",  # 8000+ ##
        "washingtonpost.com",  # 0
        "theguardian.com",  # 526 !!
        "telegraphindia.com",  # 0
        "tass.com",  # 49 ##
        "nytimes.com",  # 232 !!
        "apnews.com",  # 172 ##
        "apr.org",  # 55 ##
        "bbc.com",  # 318
        # "bbc.co.uk",  # 238 ##
        "bignewsnetwork.com",  # 1138 ##
        "cnn.com",  # 459 ##
        "dailymail.co.uk",  # 1254 ##
        "foxnews.com",  # 343 ##
        "globalsecurity.org",  # 409 ##
        # "indiatimes.com",  # 2586 ##
        "indiablooms.com",  # 86 --
        "indianexpress.com",  # 747 !!
        "jpost.com",  # 307 ##
        "mirror.co.uk",  # 641 --
        "africa.com",
        "france24.com",
        "nbcnews.com",
        "abcnews.go.com",
        "thesun.co.uk",
        "economist.com",
    ]

    if my_domains is not None:
        domains = my_domains

    for domain in domains:
        # region 处理爬虫参数
        configs = json_to_dict(configs_dir + domain + ".config.json")
        if 'class' in configs["div_attrs"].keys():
            configs["div_attrs"]['class'] = configs["div_attrs"]['class'].split(' ')
        configs["h_in"] = True if configs["h_in"] == "True" else False
        if 'class' in configs["h_attrs"].keys():
            configs["h_attrs"]['class'] = configs["h_attrs"]['class'].split(' ')
        if 'class' in configs["a_attrs"].keys():
            configs["a_attrs"]['class'] = configs["a_attrs"]['class'].split(' ')
        if 'class' in configs["t_attrs"].keys():
            configs["t_attrs"]['class'] = configs["t_attrs"]['class'].split(' ')
        if 'class' in configs["p_attrs"].keys():
            configs["p_attrs"]['class'] = configs["p_attrs"]['class'].split(' ')
        # endregion

        # 第一次爬取 没有出现error_url_tmp_domain.txt
        error_url_txt = txt_dir + "error_url_" + domain + ".txt"
        if not os.path.exists(error_url_txt):
            domain_urls = get_url_domain(day, domain)
            craw_articles(day, domain_urls, domain, configs)
        else:
            # 再次获取出错的链接文章
            domain_urls = get_error_url_domain(day, domain)
            craw_articles(day, domain_urls, domain, configs)
