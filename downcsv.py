"""
- Download GDELT Data.
- timescope: 20210601 - 20220831
"""
from typing import Union

import requests
import zipfile
import os
import csv
from requests.adapters import HTTPAdapter
from utils import export_head, mentions_head, create_date_range, TIME_RANGE
import warnings

os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
os.environ["HTTP_PROXY"] = 'http://127.0.0.1:7890'

mention_times = [
    '000000',
    '001500',
    '003000',
    '004500',
    '010000',
    '011500',
    '013000',
    '014500',
    '020000',
    '021500',
    '023000',
    '024500',
    '030000',
    '031500',
    '033000',
    '034500',
    '040000',
    '041500',
    '043000',
    '044500',
    '050000',
    '051500',
    '053000',
    '054500',
    '060000',
    '061500',
    '063000',
    '064500',
    '070000',
    '071500',
    '073000',
    '074500',
    '080000',
    '081500',
    '083000',
    '084500',
    '090000',
    '091500',
    '093000',
    '094500',
    '100000',
    '101500',
    '103000',
    '104500',
    '110000',
    '111500',
    '113000',
    '114500',
    '120000',
    '121500',
    '123000',
    '124500',
    '130000',
    '131500',
    '133000',
    '134500',
    '140000',
    '141500',
    '143000',
    '144500',
    '150000',
    '151500',
    '153000',
    '154500',
    '160000',
    '161500',
    '163000',
    '164500',
    '170000',
    '171500',
    '173000',
    '174500',
    '180000',
    '181500',
    '183000',
    '184500',
    '190000',
    '191500',
    '193000',
    '194500',
    '200000',
    '201500',
    '203000',
    '204500',
    '210000',
    '211500',
    '213000',
    '214500',
    '220000',
    '221500',
    '223000',
    '224500',
    '230000',
    '231500',
    '233000',
    '234500'
]


def download_day(daytime, filedir, download) -> bool:
    """
    下载某一天的CSV数据，并且为其添加Header。
    如果download="export"，下载GDELT1.0中的exports文件；
    如果download="mentions"，下载GDELT2.0中的mentions文件。
    """

    # 设置并创建下载目录
    if download == "mentions":
        filedir = filedir + daytime[:8] + "/"

    if not os.path.exists(filedir):
        os.makedirs(filedir)

    try:
        headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/105.0.0.0 Safari/537.36 Edg/105.0.1343.33"
        }
        sess = requests.Session()
        sess.mount('http://', HTTPAdapter(max_retries=3))
        sess.mount('https://', HTTPAdapter(max_retries=3))
        sess.keep_alive = False

        # 如果download=="export"，url为GDELT1.0中的exports文件
        url = "http://data.gdeltproject.org/events/" + daytime + ".export.CSV.zip"

        # 如果download=="mentions"，url为GDELT2.0中的mentions文件
        if download == "mentions":
            url = "http://data.gdeltproject.org/gdeltv2/" + daytime + ".mentions.CSV.zip"

        print("开始下载：", url)

        # 下载zip文件
        test_request = requests.get(url=url, headers=headers, stream=True, verify=False, timeout=(5, 5))

        if test_request.status_code != 200:
            print(f"下载文件URL={url}失败！")
            return False

        # 保存zip文件
        with open(filedir + "data.zip", "wb") as data:
            data.write(test_request.content)

        # 从zip文件中提取CSV文件
        with zipfile.ZipFile(filedir + 'data.zip', 'r') as zf:
            if download == "export":
                zf.extract(daytime + '.export.CSV', path=filedir)
            if download == "mentions":
                zf.extract(daytime + '.mentions.CSV', path=filedir)

        # 删除zip文件
        if os.path.exists(filedir + "data.zip"):
            os.remove(filedir + "data.zip")

        # CSV文件添加head
        if download == "mentions":
            file_path = filedir + daytime + '.mentions.CSV'
            head = mentions_head
        else:
            file_path = filedir + daytime + '.export.CSV'
            head = export_head

        datas = []
        with open(file_path, "r") as f:
            reader = csv.reader(f)
            reader = list(reader)
            for line in reader:
                datas.append(line)

        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(head)  # 写入header
            writer = csv.writer(f)
            writer.writerows(datas)  # 写入数据

        return True

    except Exception as e:
        print(f"下载文件失败！\n", e)
        return False


def download_export(day, filedir) -> bool:
    return download_day(daytime=day, filedir=filedir, download="export")


def download_mentions(day, filedir) -> list[bool]:
    res = []
    for mention_time in mention_times:
        res.append(download_day(daytime=day + mention_time, filedir=filedir, download="mentions"))

    return res


def check_download(day, export_dir, mention_dir) -> tuple[bool, list[[bool]]]:
    res_export = False
    res_mentions = []
    # 重新下载export
    export_path = export_dir + day + ".export.CSV"
    max_try_count = 3
    for i in range(max_try_count):
        if not os.path.exists(export_path):
            print(f"第{i}/{max_try_count}次尝试重新下载{day}.export.CSV文件")
            download_day(daytime=day, filedir=export_dir, download="export")
        else:
            break

    if not os.path.exists(export_path):
        print(f"下载{day}.export.CSV文件失败！")
    else:
        res_export = True

    # 重新下载mentions
    mention_day_dir = mention_dir + day + "/"
    for i in range(max_try_count):
        mentions = os.listdir(mention_day_dir)

        # GEDLT2.0中，每15分钟记录一次mentions文件，每天记录96个mentions文件
        # 如果len(mentions_csv) != 96，说明当天的文件未全部下载
        if len(mentions) != 96:
            mention_day_times = [ele.split(".")[0][-6:] for ele in mentions]  # 获取mentions文件对应的时间段
            print(f"第{i}/{max_try_count}次尝试重新下载{day}*.mentions.CSV文件")
            for time in mention_times:
                res_mentions = []
                if time not in mention_day_times:  # 如果某一时间段不存在mentions文件，则下载
                    print(f"\t重新下载{day + time}")
                    download_day(daytime=day + time, filedir=mention_dir, download="mentions")
                    res_mentions.append(os.path.exists(mention_dir + day + time + ".mentions.CSV"))
        else:
            break

    mentions = os.listdir(mention_day_dir)
    if len(mentions) != 96:
        print(f"仍有{96 - len(mentions)}个文件下载失败。")

    return res_export, res_mentions


def download_csv(day) -> tuple[bool, list[[bool]]]:
    # 下载一天的数据
    download_export(day, filedir='export/')
    download_mentions(day, filedir='mentions/')
    return check_download(day, export_dir='export/', mention_dir='mentions/')
