
import os
import pandas as pd
from utils import generate_random_string


DAY = "20240126"
ARTICLES_PATH = "./crawlingnews/NULL/articles/"+ DAY +"/"
SAVE_NEWS = "./pnews/"+ DAY +"/"

def make_dirs():
    if not os.path.exists(SAVE_NEWS):
        os.makedirs(SAVE_NEWS)

def readArticle(article_path):
    fr = open(article_path, encoding="utf-8")
    lines = fr.readlines()
    fr.close()
    title = lines[0].strip()
    author = lines[1].strip()
    ptime = lines[2].strip()
    dtime = lines[3].strip()
    url = lines[4].strip()
    content = "".join(lines[5:]).strip()
    return title, author, ptime, dtime, url, content, len(lines[5:])


def mergeArticles():
    mediumList = os.listdir(ARTICLES_PATH)

    sub_merge_dfs = []

    for medium in mediumList: # 遍历每一个媒体
        articles = os.listdir(ARTICLES_PATH + medium + "/")

        data = {
            "UniqueID": [],
            "Title": [],
            "Author": [],
            "PTime": [],
            "DTime": [],
            "MentionSourceName":[],
            "MentionIdentifier": [],
            "Content": [],
        }
        print(medium)
        m_len = 0
        for index, article in enumerate(articles):
            title, author, ptime, dtime, url, content, len = readArticle(ARTICLES_PATH + medium + "/" + article)
            
            data["UniqueID"].append(article.split(".")[0]) # txt文本文件的名称就是UniqueID
            data["Title"].append(title)
            data["Author"].append(author)
            data["PTime"].append(ptime)
            data["DTime"].append(dtime)
            data["MentionSourceName"].append(medium)
            data["MentionIdentifier"].append(url)
            data["Content"].append(content)

            m_len += len

        df = pd.DataFrame(data)
        df.to_csv(SAVE_NEWS + medium + ".csv", index=False)
        
        print(m_len)

        sub_merge_dfs.append(df)
    # 合并所有的merge.csv
    all_merge_dfs = pd.concat(sub_merge_dfs)
    all_merge_dfs.to_csv(SAVE_NEWS + "MentionSourceNames.csv", index=False)


if __name__ == "__main__":
    make_dirs()
    
    mergeArticles()