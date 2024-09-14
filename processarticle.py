import os
import pandas as pd
from tqdm import tqdm


def read_article(article_path):
    fr = open(article_path, encoding="utf-8")
    lines = fr.readlines()
    fr.close()
    title = lines[0].strip()
    author = lines[1].strip()
    ptime = lines[2].strip()
    dtime = lines[3].strip()
    url = lines[4].strip()
    content = "".join(lines[5:]).strip()
    return title, author, ptime, dtime, url, content


def merge_articles(day):
    articles_dir = "crawlingnews/NULL/articles/" + day + "/"

    medias = os.listdir(articles_dir)

    dfs_media = []
    for media in medias:  # 遍历每一个媒体
        articles = os.listdir(articles_dir + media + "/")

        data_media = {
            "UniqueID": [],
            "Title": [],
            "Author": [],
            "PTime": [],
            "DTime": [],
            "MentionSourceName": [],
            "MentionIdentifier": [],
            "Content": [],
        }

        articles_count = len(articles)
        process_bar = tqdm(total=articles_count, desc=day + f" 处理{media}的文章中：")
        for article in articles:
            title, author, ptime, dtime, url, content = read_article(articles_dir + media + "/" + article)

            data_media["UniqueID"].append(article.split(".")[0])  # txt文本文件的名称就是UniqueID
            data_media["Title"].append(title)
            data_media["Author"].append(author)
            data_media["PTime"].append(ptime)
            data_media["DTime"].append(dtime)
            data_media["MentionSourceName"].append(media)
            data_media["MentionIdentifier"].append(url)
            data_media["Content"].append(content)

            process_bar.update(1)

        dfs_media.append(pd.DataFrame(data_media))

    # 合并爬取到的当天的所有新闻
    merge_articles_dir = "pnews/" + day + "/"
    if not os.path.exists(merge_articles_dir):
        os.makedirs(merge_articles_dir)
    pd.concat(dfs_media).to_csv(merge_articles_dir + "MentionSourceNames.csv", index=False, sep='\\')
