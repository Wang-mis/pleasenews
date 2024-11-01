import os
import time
import sys
import getopt
import requests
from datetime import date, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from crawlingnews.NULL.crawlArticlesForDiffSourceURL import craw_day
from helper.SQLALchemyUtil import to_sql
from downcsv import download_csv
from processarticle import merge_articles
from processcsv import process_csv
from utils import create_date_range
from zlog.ztest import get_keywords


def run(day, download=True, process=True, craw=True, getkeys=True, upload=True, domains=None):
    t1 = time.time()
    # 下载文件
    if download:
        res_export, _ = download_csv(day)
        if not res_export:
            print(f"下载{day}.export.CSV文件失败，跳过后续步骤！")
            return

    if process:
        process_csv(day, domains)

    # 爬取文章
    if craw:
        craw_count = 1
        for _ in range(craw_count):
            craw_day(day, domains)

        merge_articles(day)

    if getkeys:
        get_keywords(day)

    if upload:
        to_sql(day)
        response = requests.get("http://123.57.216.53:14451/dataset/update")
        if response.status_code == 200:
            print("已通知后端服务器更新数据。")
        else:
            print("通知后端服务器更新数据失败！")

    t2 = time.time()
    print(f"{day}  耗时：{(t2 - t1) / 3600} h。")


def start(hour, minute, download=True, process=True, craw=True, getkeys=True, upload=True, domains=None):
    print(f"开始运行定时爬取新闻脚本，每晚{str(hour).zfill(2)}:{str(minute).zfill(2)}爬取新闻。pid={os.getpid()}")

    def task():
        today = date.today()
        yesterday = today - timedelta(days=1)
        yesterday = yesterday.strftime('%Y%m%d')
        print(f"开始运行脚本，day={yesterday}")
        run(yesterday, download, process, craw, getkeys, upload, domains)

    scheduler = BlockingScheduler()
    scheduler.add_job(task, 'cron', hour=hour, minute=minute, misfire_grace_time=3600)
    scheduler.start()


if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], '-d:-s:',
                               ['date=', 'nodownload', 'nocraw', 'noupload', 'nogetkeys', 'start=', 'domains=',
                                'noprocess'])
    download, process, craw, upload, getkeys = True, True, True, True, True
    start_date, end_date = "", ""
    hour, minute = -1, -1
    domains = None
    for opt_name, opt_value in opts:
        if opt_name in ('-d', '--date'):
            start_date = opt_value[:8]
            end_date = opt_value[9:]
        elif opt_name in ('--nodownload',):
            download = False
        elif opt_name in ('--nocraw',):
            craw = False
        elif opt_name in ('--noupload',):
            upload = False
        elif opt_name in ('--nogetkeys',):
            getkeys = False
        elif opt_name in ('--noprocess',):
            process = False
        elif opt_name in ('-s', '--start'):
            hour = int(opt_value[:2])
            minute = int(opt_value[3:])
        elif opt_name in ('--domains',):
            domains = opt_value.split(' ')

    # 启动定时任务
    if hour >= 0 and minute >= 0:
        start(hour, minute, download, process, craw, getkeys, upload, domains)

    # 爬取一段时间的新闻
    elif start_date != "":
        if end_date == "":
            end_date = start_date
        dates = create_date_range([start_date, end_date])
        print(f"pid={os.getpid()}")
        for run_date in dates:
            run(run_date, download, process, craw, getkeys, upload, domains)
