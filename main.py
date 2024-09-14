import time

from crawlingnews.NULL.crawlArticlesForDiffSourceURL import craw
from downcsv import download_csv
from helper.SQLALchemyUtil import to_sql
from processarticle import merge_articles
from processcsv import process_csv
from utils import create_date_range
from zlog.ztest import get_keywords


def run(day):
    t1 = time.time()
    download_csv(day)
    process_csv(day)
    craw(day)
    merge_articles(day)
    get_keywords(day)
    to_sql(day)
    t2 = time.time()
    print(f"爬取 {day} 新闻耗时：{(t2 - t1) / 3600} h。")


if __name__ == '__main__':
    days = create_date_range(['20240813', '20240913'])
    for day in days:
        run(day)
