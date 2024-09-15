import time

from crawlingnews.NULL.crawlArticlesForDiffSourceURL import craw_day
from downcsv import download_csv
from helper.SQLALchemyUtil import to_sql
from processarticle import merge_articles
from processcsv import process_csv
from utils import create_date_range
from zlog.ztest import get_keywords

from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import date, timedelta


def pre_process(day):
    res_export, _ = download_csv(day)
    if not res_export:
        print(f"下载{day}.export.CSV文件失败，跳过后续步骤！")
        return
    process_csv(day)


def post_process(day):
    merge_articles(day)
    get_keywords(day)
    to_sql(day)


def run(day):
    t1 = time.time()

    pre_process(day)

    craw_count = 2
    for _ in range(craw_count):
        craw_day(day)

    post_process(day)

    t2 = time.time()
    print(f"爬取 {day} 新闻耗时：{(t2 - t1) / 3600} h。")


def start():
    today = date.today()
    yesterday = today - timedelta(days=1)
    yesterday = yesterday.strftime('%Y%m%d')

    scheduler = BlockingScheduler()
    scheduler.add_job(run, 'cron', hour=22, minute=0, args=[yesterday])
    scheduler.start()


if __name__ == '__main__':
    start()

    # days = create_date_range(['20240813', '20240913'])
    # for day in days:
    #     run(day)
