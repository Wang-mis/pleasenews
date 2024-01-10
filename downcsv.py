
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


def process(string, source='./csv/', target='./process/', filter=['RUS', 'UKR']):
    print("|".join(filter))
    
    if not os.path.exists(target):
        os.makedirs(target)

    tmp = pd.read_csv(source + string+".export.CSV", delimiter='\t')
    
    actor1_country_code_rus_ukr_bool = tmp.Actor1CountryCode.str.contains("|".join(filter), na=False)
    Actor1CountryCode_RUS_UKR = tmp[actor1_country_code_rus_ukr_bool]
    Actor1CountryCode_RUS_UKR = Actor1CountryCode_RUS_UKR.reset_index(drop=True)

    actor2_country_code_rus_ukr_bool = tmp.Actor2CountryCode.str.contains("|".join(filter), na=False)
    Actor2CountryCode_RUS_UKR = tmp[actor2_country_code_rus_ukr_bool]
    Actor2CountryCode_RUS_UKR = Actor2CountryCode_RUS_UKR.reset_index(drop=True)

    concat_df = pd.concat([Actor1CountryCode_RUS_UKR,Actor2CountryCode_RUS_UKR]).reset_index(drop=True)
    concat_df = concat_df.drop_duplicates(subset=export_head[5:-2],keep='first')
    concat_df = concat_df.dropna(axis=0,subset = ["Actor1Code", "Actor2Code", "Actor1CountryCode", "Actor2CountryCode"])
    concat_df = concat_df.dropna(axis=0,subset = ["ActionGeo_Fullname", "ActionGeo_CountryCode"])
    
    concat_df.to_csv(target + "_".join(filter) + '_' + string+ '.csv', index=False)



if __name__ == "__main__":
    TIME_RANGE = [20240108,20240109]
    source='./csv/'
    # download="export"
    download="mentions"
    for i in create_date_range(TIME_RANGE):
        for time in times:
            specificDay(string=i+time, withhead=True, filedir =source, download=download)