'''
爬取不同域名下相关URL的新闻文章
'''
import pandas as pd
import os
import json
import operator
import random
import requests
from bs4 import BeautifulSoup
import json
import zipfile
import os
import csv
import pandas as pd
import urllib.request
import urllib.parse
from lxml import etree
import re
from requests.adapters import HTTPAdapter


PROXIES= {
    'http':'http://127.0.0.1:1080',
    'https':'http://127.0.0.1:1080'
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
}

# MERGE Mention 数据集位置
PROCESS_GDELT_PATH = '../../merge/NULL/'
FILTER = "MentionSourceName"
# FILTER = "SOURCEURL"

# 查询相关域名下新闻文章链接
def unique_url_about_source_domain(tmp_domian):
    unique_path = "./txt/unique_url_about_" + tmp_domian + ".txt"
    if os.path.exists(unique_path):
        print(unique_path, "已经出现")
        fr = open(unique_path)
        lines = fr.readlines()
        fr.close()
        return set([line.strip() for line in lines])

    tmp_domain_urls = []
    for i in os.listdir(PROCESS_GDELT_PATH):
        tmp = pd.read_csv(PROCESS_GDELT_PATH + i)
        for row in tmp.itertuples():
            if FILTER == 'MentionSourceName':
                source_url_domain = getattr(row, 'MentionSourceName')
                if tmp_domian == source_url_domain:
                    tmp_source_url = getattr(row, 'MentionIdentifier')
                    day = getattr(row, 'Day')
                    tmp_domain_urls.append(tmp_source_url+"+"+str(day))
            if FILTER == 'SOURCEURL':
                source_url_domain = getattr(row, 'SOURCEURL')
                if tmp_domian in source_url_domain.split("//")[-1].split("/")[0]:
                    day = getattr(row, 'Day')
                    tmp_domain_urls.append(source_url_domain+"+"+str(day))

    # 去重
    tmp_domain_urls_set = set(tmp_domain_urls)
    tmp_domain_urls_list = [el+'\n' for el in tmp_domain_urls_set]
    print("{}域名下新闻文章链接个数为:{}".format(tmp_domian, len(tmp_domain_urls_list)))
    # 保存
    f=open(unique_path, "w")
    f.writelines(tmp_domain_urls_list)
    f.close()

    return tmp_domain_urls_set


# 读取出错的url继续爬取文章
def regen_tmp_domain_urls_set(tmp_domain):
    unique_path = './txt/unique_url_about_' + tmp_domain +'.txt'
    error_path = './txt/error_url_' + tmp_domain +'.txt'
    
    fr = open(unique_path)
    lines = fr.readlines()
    fr.close()
    
    url_data = {}
    for line in lines:
        url_data[line.strip().split('+')[0]] = line.strip().split('+')[1]

    fr = open(error_path)
    lines = fr.readlines()
    fr.close()
    
    tmp_domain_urls_set = set([line.strip()+"+"+url_data[line.strip()] for line in lines])
    return tmp_domain_urls_set



def generate_random_str(randomlength=16):
    random_str =''
    base_str ='ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz0123456789'
    length =len(base_str) -1
    for i in range(randomlength):
        random_str +=base_str[random.randint(0, length)]
    return random_str




def craw_articles(tmp_domain_urls_set,tmp_domain, div_article, div_attrs, use_sub=False, h_x='h1', h_in=False, h_attrs={}, find_p=['p'], p_attrs={}): # use_sub是否创建子文件夹
    error_url = [] # 保存出错的URL
    for urli in tmp_domain_urls_set:
        try:
            sess = requests.Session()
            sess.mount('http://', HTTPAdapter(max_retries=10))
            sess.mount('https://', HTTPAdapter(max_retries=10))
            sess.keep_alive = False
            
            # 获取链接与时间
            day = urli.split("+")[-1]
            url = urli.split("+")[0]
            sub_path = "./articles/" + tmp_domain+"/"
            if use_sub:
                # 获取子域 创建子域文件夹
                sub_tmp_domain = urli.split(tmp_domain + "/")[-1].split("/")[0]
                sub_path = "./articles/" + tmp_domain+"/" + sub_tmp_domain+"/"
            
            if not os.path.exists(sub_path):
                os.makedirs(sub_path)
            
            # 获取网页
            test_request = requests.get(url, proxies=PROXIES, headers=headers, timeout=30) #, verify=False)
            print("state code: ", test_request.status_code)
            
            # 保存出错的URL
            if test_request.status_code != 200:
                error_url.append(url+'\n')
            
            # 解析网页
            content = test_request.text
            soup = BeautifulSoup(content, 'html.parser')

            content_div = soup.find(name=div_article, attrs=div_attrs)

            if len(h_attrs) != 0:
                if h_in:
                    new_title = content_div.find_all(h_x, attrs=h_attrs)
                else:
                    new_title = soup.find_all(h_x, attrs=h_attrs)
            else:
                if h_in:
                    new_title = content_div.find_all(h_x)
                else:
                    new_title = soup.find_all(h_x)
            
            print("new_title: ", new_title, "len(new_title): ", len(new_title))

            new_paragraph_list = []
            for ele_p in find_p:
                new_paragraph_list += content_div.find_all(ele_p, attrs=p_attrs) # paragraphs
            
            # 保存
            article = []
            title = '_'
            for h in new_title:
                if h.text.strip() == '':
                    print("title列表中当前title为空")
                    continue
                
                simple_punctuation = '[!"‘’“”#$%&\'()*+,-/:;<=>?@[\\]^_`{|}~，。,]'
                title = re.sub('[\u4e00-\u9fa5]','',h.text)
                title = re.sub(simple_punctuation, '', title)
                print('title:',title)
                title = title.replace('\n','').replace('\r','')
                print("title=============", title)
                title = title.strip()
                article.append(title+'\n')
                
                break # 找到一个标题不为空行

            if title == '_':
                title = day + "+" + generate_random_str()
                raise Exception("random title !!!!")
            else:
                title = day + title[:25]
            article.append(day+"\n")
            article.append(url+"\n")
            
            # new_paragraph_list = content_div.find('div',attrs={'class':'article-text ds-container svelte-1efvmlf'})
            # print(new_paragraph_list)

            for p in new_paragraph_list:
                paragraph = p.text
                paragraph = paragraph.encode("gbk", 'ignore').decode("gbk", "ignore") # gbk编码报错
                paragraph=re.sub('[\u4e00-\u9fa5]', '', paragraph)
                simple_punctuation = '[‘’“”–，。]'
                paragraph = re.sub(simple_punctuation, '', paragraph)
                article.append(paragraph+'\n')

            f=open(sub_path + title + ".txt","w", encoding='utf8')
            f.writelines(article)
            f.close()
        
        except Exception as e:
            print("Exception: ",e)
            print("URL: ",url)
            error_url.append(url+'\n')
    
    # 保存出错的URL
    f=open("./txt/error_url_" + tmp_domain + ".txt","w") 
    f.writelines(error_url)
    f.close()

if __name__ == "__main__":

    print(PROCESS_GDELT_PATH)

    same_struct_domain_list = [
        "yahoo.com"
    ]

    for tmp_domain in same_struct_domain_list:
        
        print(tmp_domain)

        error_url_txt = "./txt/error_url_" + tmp_domain + ".txt"
        # ==================================================================== #
        div_article = 'article'
        # div_article = 'div'
        div_attrs = {
            # "id": "article-content"
            # "data-testid" : "article-body"
            # 'id': 'artikelTextPuffer'
            # 'itemprop': "description"
            # "itemprop" : "articleBody"
            # "class" : re.compile("default__ArticleBody")
            # "class" : "single-article-text"
            # "class" : "tdb-block-inner"
            # "id" : "ContentPlaceHolder1_divContent"
            "class" : "caas-container"
            # "data-test-id" : "article-content"
        }
        # ==================================================================== #
        h_x = 'h1'
        h_in = False
        h_in = True
        h_attrs = {
            # "class" : re.compile('Component-heading')
            # "class" : "widget__headline h1"
            # "class" : "wp-block-post-title"
            # "id" : "ContentPlaceHolder1_hd" 
            # 'itemprop': "name"
            # "itemprop" : "headline name"
            # "data-test-id" : "post-title"
            # "data-testid" : "article-header-heading"
        }
        
        find_p = 'p'
        # find_p = 'span'
        p_attrs = {
            # "data-text" : "true"
        }





        # 第一次爬取 没有出现error_url_tmp_domian.txt
        if not os.path.exists(error_url_txt):
            tmp_domain_urls_set = unique_url_about_source_domain(tmp_domain)
            craw_articles(tmp_domain_urls_set,tmp_domain, div_article=div_article, div_attrs=div_attrs, use_sub=False, h_x=h_x, h_in=h_in, h_attrs=h_attrs,find_p=find_p, p_attrs=p_attrs)
        else:
            print("已经出现")
            # 再次获取出错的链接文章
            tmp_domain_urls_set = regen_tmp_domain_urls_set(tmp_domain)
            craw_articles(tmp_domain_urls_set,tmp_domain, div_article=div_article, div_attrs=div_attrs, use_sub=False, h_x=h_x, h_in=h_in, h_attrs=h_attrs,find_p=find_p, p_attrs=p_attrs)

