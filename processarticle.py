
import os
import pandas as pd
from utils import generate_random_string


ARTICLES_PATH = "./crawlingnews/NULL/articles/"


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
    for medium in mediumList: # 遍历每一个媒体
        articles = os.listdir(ARTICLES_PATH + medium + "/")

        data = {
            "newid": [],
            "title": [],
            "author": [],
            "ptime": [],
            "dtime": [],
            "url": [],
            "content": [],
        }
        print(medium)
        m_len = 0
        for index, article in enumerate(articles):
            title, author, ptime, dtime, url, content, len = readArticle(ARTICLES_PATH + medium + "/" + article)
            
            data["newid"].append(generate_random_string() + "-" + str(index))
            data["title"].append(title)
            data["author"].append(author)
            data["ptime"].append(ptime)
            data["dtime"].append(dtime)
            data["url"].append(url)
            data["content"].append(content)

            m_len += len

        df = pd.DataFrame(data)
        df.to_csv("./pnews/" + medium + ".csv", index=False)
        
        print(m_len)


if __name__ == "__main__":

    mergeArticles()