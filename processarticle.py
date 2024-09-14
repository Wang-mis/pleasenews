import os
import pandas as pd
from tqdm import tqdm
from utils import generate_random_string, create_date_range, TIME_RANGE

DAY = "20240901"
ARTICLES_PATH = "crawlingnews/NULL/articles/" + DAY + "/"
SAVE_NEWS = "pnews/" + DAY + "/"


def make_dirs():
    if not os.path.exists(SAVE_NEWS):
        os.makedirs(SAVE_NEWS)


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


def merge_articles():
    medium_list = os.listdir(ARTICLES_PATH)

    dfs_medium = []

    for medium in medium_list:  # 遍历每一个媒体
        articles = os.listdir(ARTICLES_PATH + medium + "/")

        data_medium = {
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
        process_bar = tqdm(total=articles_count, desc=DAY + f" 处理{medium}的文章中：")
        for article in articles:
            title, author, ptime, dtime, url, content = read_article(ARTICLES_PATH + medium + "/" + article)

            data_medium["UniqueID"].append(article.split(".")[0])  # txt文本文件的名称就是UniqueID
            data_medium["Title"].append(title)
            data_medium["Author"].append(author)
            data_medium["PTime"].append(ptime)
            data_medium["DTime"].append(dtime)
            data_medium["MentionSourceName"].append(medium)
            data_medium["MentionIdentifier"].append(url)
            data_medium["Content"].append(content)

            process_bar.update(1)

        dfs_medium.append(pd.DataFrame(data_medium))

    # 合并所有的merge.csv
    pd.concat(dfs_medium).to_csv(SAVE_NEWS + "MentionSourceNames.csv", index=False)


def process_articles():
    global DAY, ARTICLES_PATH, SAVE_NEWS

    days = create_date_range(TIME_RANGE)
    for day in days:
        DAY = str(day)
        ARTICLES_PATH = "crawlingnews/NULL/articles/" + DAY + "/"
        SAVE_NEWS = "pnews/" + DAY + "/"
        make_dirs()
        merge_articles()


if __name__ == "__main__":
    process_articles()
