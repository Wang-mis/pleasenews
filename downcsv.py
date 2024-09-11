"""
- Download GDELT Data.
- timescope: 20210601 - 20220831
"""
import requests
import zipfile
import os
import csv
from requests.adapters import HTTPAdapter
from utils import export_head, mentions_head, times, create_date_range, TIME_RANGE
import warnings

warnings.filterwarnings("ignore")

# os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:1080'
# os.environ["HTTP_PROXY"] = 'http://127.0.0.1:1080'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
os.environ["HTTP_PROXY"] = 'http://127.0.0.1:7890'


# download a single data from gdelt.
def specificDay(string, withhead=False, filedir="./csv/", download="export"):
    """
    下载某一天的CSV数据，并且为其添加Header。
    如果download="export"，下载GDELT1.0中的exports文件；
    如果download="mentions"，下载GDELT2.0中的mentions文件。
    """
    print(string)

    # 设置并创建下载目录
    if download == "mentions":
        print(filedir + string[:8])
        filedir = filedir + string[:8] + "/"
        print(filedir)

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
        url = "http://data.gdeltproject.org/events/" + string + ".export.CSV.zip"

        # 如果download=="mentions"，url为GDELT2.0中的mentions文件
        if download == "mentions":
            url = "http://data.gdeltproject.org/gdeltv2/" + string + ".mentions.CSV.zip"

        print(url)

        # 下载zip文件
        test_request = requests.get(url=url, headers=headers, stream=True, verify=False, timeout=(5, 5))

        if test_request.status_code != 200:
            print("error")
            print("error-url: ", url)
            return

        # 保存zip文件
        with open(filedir + "test.zip", "wb") as data:
            data.write(test_request.content)

        # 从zip文件中提取CSV文件
        with zipfile.ZipFile(filedir + 'test.zip', 'r') as zf:
            if download == "export":
                zf.extract(string + '.export.CSV', path=filedir)
            if download == "mentions":
                zf.extract(string + '.mentions.CSV', path=filedir)

        # 删除zip文件
        if os.path.exists(filedir + "test.zip"):
            os.remove(filedir + "test.zip")

        # CSV文件添加head
        if withhead:
            datas = []
            tmp_path = filedir + string + '.export.CSV'
            tmp_head = export_head
            if download == "mentions":
                tmp_path = filedir + string + '.mentions.CSV'
                tmp_head = mentions_head

            with open(tmp_path, "r") as csvfile:
                reader = csv.reader(csvfile)
                reader = list(reader)
                for line in reader:
                    datas.append(line)
            with open(tmp_path, 'w', newline='') as csvfile2:
                writer = csv.writer(csvfile2, delimiter='\t')
                writer.writerow(tmp_head)  # 写入header
                writer = csv.writer(csvfile2)
                writer.writerows(datas)  # 写入数据

    except Exception as e:
        print("Exception: ", e)


def downloadExport(day, withhead=True, filedir='./csv/', download="export"):
    specificDay(string=day, withhead=withhead, filedir=filedir, download=download)


def downloadMentions(day, withhead=True, filedir='./mentions/', download="mentions"):
    for time in times:
        specificDay(string=day + time, withhead=withhead, filedir=filedir, download=download)


def checkMentions(filedir="./mentions/", day="20240121"):
    # mentions 表下载 复检 （下载时有些会忽略）
    mfiledir = filedir + day + "/"
    mentions_csv = os.listdir(mfiledir)

    # GEDLT2.0中，每15分钟记录一次mentions文件，每天记录96个mentions文件
    # 如果len(mentions_csv) != 96，说明当天的文件未全部下载
    if len(mentions_csv) != 96:
        haved = [ele.split(".")[0][-6:] for ele in mentions_csv]  # 获取mentions文件对应的时间段
        print("待获取: ", (96 - len(haved)))
        for time in times:
            if time not in haved:  # 如果某一时间段不存在mentions文件，则下载
                print("复检: ", day + time)
                specificDay(string=day + time, withhead=True, filedir=filedir, download="mentions")

    print("待获取: 0")


def download_csv():
    for day in create_date_range(TIME_RANGE):
        print(day)
        downloadExport(day, filedir='./export/')
        downloadMentions(day, filedir='./mentions/')
        checkMentions(day=day)


if __name__ == "__main__":
    download_csv()
