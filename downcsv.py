
'''
- Download GDELT Data.
- timescope: 20210601 - 20220831
'''
import requests
import zipfile
import os
import csv
import pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from utils import export_head, mentions_head, times
import warnings
warnings.filterwarnings("ignore")

os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:1080'
os.environ["HTTP_PROXY"] = 'http://127.0.0.1:1080'


def create_date_range(inp:list):
    result_list = []
    sta_day = datetime.strptime(str(inp[0]), '%Y%m%d')
    end_day = datetime.strptime(str(inp[1]), '%Y%m%d')
    dlt_day = (end_day - sta_day).days + 1

    for i in range(dlt_day):
        tmp_day = sta_day + timedelta(days=i)
        tmp_day_txt = tmp_day.strftime('%Y%m%d')
        result_list.append(tmp_day_txt)

    return result_list

# download a single data form gdelt.
def specificDay(string, withhead=False, filedir="./csv/", download="export"):
    print(string)
    
    if download == "mentions":
        print(filedir + string[:8])
        filedir = filedir + string[:8] + "/"
        print(filedir)
    
    if not os.path.exists(filedir):
        os.makedirs(filedir)
    
    try:
        headers = {
            'User-Agent' : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 Edg/105.0.1343.33"
        }
        sess = requests.Session()
        sess.mount('http://', HTTPAdapter(max_retries=3))
        sess.mount('https://', HTTPAdapter(max_retries=3))
        sess.keep_alive = False
        
        url = "http://data.gdeltproject.org/events/"+string+".export.CSV.zip"
        
        if download == "mentions":
            url = "http://data.gdeltproject.org/gdeltv2/" + string + ".mentions.CSV.zip"
        
        print(url)

        test_request = requests.get(url=url,\
            headers=headers, stream=True, verify= False, timeout=(5,5))
        
        if test_request.status_code != 200:
            print("error")
            print("error-url: ", url)
            return
        
        with open(filedir+"test.zip", "wb") as data:
            data.write(test_request.content)
        
        with zipfile.ZipFile(filedir+'test.zip','r') as zf:
            if download == "export":
                zf.extract(string+'.export.CSV', path=filedir)
            if download == "mentions":
                zf.extract(string+'.mentions.CSV', path=filedir)
        
        if os.path.exists(filedir+"test.zip"):
            os.remove(filedir+"test.zip")
        
        if withhead:
            
            datas = []
            tmp_path = filedir + string + '.export.CSV'
            tmp_head = export_head
            if download == "mentions":
                tmp_path = filedir + string+'.mentions.CSV'
                tmp_head = mentions_head

            with open(tmp_path, "r") as csvfile:
                reader = csv.reader(csvfile)
                reader = list(reader)
                for line in reader:
                    datas.append(line)
            with open(tmp_path, 'w', newline='') as csvfile2:
                writer = csv.writer(csvfile2, delimiter='\t')
                writer.writerow(tmp_head)
                writer = csv.writer(csvfile2)
                writer.writerows(datas)
    
    except Exception as e:
        print("Exception: ",e)

def process(tmp, filter=['RUS', 'UKR']):

    print("|".join(filter))
    
    actor1_country_code_rus_ukr_bool = tmp.Actor1CountryCode.str.contains("|".join(filter), na=False)
    Actor1CountryCode_RUS_UKR = tmp[actor1_country_code_rus_ukr_bool]
    Actor1CountryCode_RUS_UKR = Actor1CountryCode_RUS_UKR.reset_index(drop=True)

    actor2_country_code_rus_ukr_bool = tmp.Actor2CountryCode.str.contains("|".join(filter), na=False)
    Actor2CountryCode_RUS_UKR = tmp[actor2_country_code_rus_ukr_bool]
    Actor2CountryCode_RUS_UKR = Actor2CountryCode_RUS_UKR.reset_index(drop=True)

    concat_df = pd.concat([Actor1CountryCode_RUS_UKR,Actor2CountryCode_RUS_UKR]).reset_index(drop=True)
    concat_df = concat_df.drop_duplicates(subset=export_head[5:-2], keep='first')
    concat_df = concat_df.dropna(axis=0, subset=["Actor1Code", "Actor2Code", "Actor1CountryCode", "Actor2CountryCode"])
    concat_df = concat_df.dropna(axis=0, subset=["ActionGeo_Fullname", "ActionGeo_CountryCode"])

    return concat_df


def mergeMentions(mention_path="./mentions/", export_path = "./export/", merge_save= "./merge/", filter=["RUS","UKR"]):
    merge_save += "_".join(filter) +"/"

    if not os.path.exists(merge_save):
        os.makedirs(merge_save)

    mention_dir = os.listdir(mention_path)

    for ele_dir in mention_dir:
        ele_dir_path = mention_path + ele_dir + "/"
        sub_dfs = []
        for csv in os.listdir(ele_dir_path):
            if csv.split(".")[-1].lower() != 'csv':
                print(csv)
                continue
            sub_dfs.append(pd.read_csv(ele_dir_path + csv, delimiter='\t'))
        
        result = pd.concat(sub_dfs)
        
        process_df = pd.read_csv(export_path + ele_dir + ".export.CSV", delimiter='\t')
        
        process_df = process(process_df, filter=filter)

        data = pd.merge(process_df, result, how='inner', on='GlobalEventID')
        # data = pd.merge(process_df,result,how='left',on='GlobalEventID')

        # 排序规则：MentionIdentifier升序, SentenceID升序, Confidence 降序
        data.sort_values(['MentionIdentifier', 'SentenceID', 'Confidence'], ascending=[1, 1, 0], inplace=True)
        grouped = data.groupby(['MentionIdentifier']).head(1)
        grouped = grouped[grouped["Confidence"]>=75]
        grouped = grouped[grouped["SentenceID"]==1]

        # 只看yahoo.com的
        grouped = grouped[grouped["MentionSourceName"]=='yahoo.com']
        # grouped.to_csv("grouped.csv", index=False)
        
        grouped.to_csv(merge_save + ele_dir + ".merge.csv", index=False)



def downloadExport(day, withhead=True, filedir='./csv/', download="export"):
    specificDay(string=day, withhead=withhead, filedir=filedir, download=download)


def downloadMentions(day, withhead=True, filedir='./csv/', download="mentions"):
    for time in times:
        specificDay(string=day + time, withhead=withhead, filedir=filedir, download=download)


if __name__ == "__main__":
    TIME_RANGE = [20240101,20240109]

    for i in create_date_range(TIME_RANGE):
        downloadExport(i, filedir='./export/')
        downloadMentions(i, filedir='./mentions/')
    
    mergeMentions(mention_path="./mentions/", export_path = "./export/", merge_save= "./merge/", filter=["CHN","CHN"])