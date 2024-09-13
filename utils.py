"""
- GDELT DATA FORMAT CODEBOOK V 1.03.
- http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf
"""

import random
import string
import time
import json

from datetime import datetime, timedelta

export_head = [
    'GlobalEventID',
    'Day',
    'MonthYear',
    'Year',
    'FractionDate',
    'Actor1Code',
    'Actor1Name',
    'Actor1CountryCode',
    'Actor1KnownGroupCode',
    'Actor1EthnicCode',
    'Actor1Religion1Code',
    'Actor1Religion2Code',
    'Actor1Type1Code',
    'Actor1Type2Code',
    'Actor1Type3Code',
    'Actor2Code',
    'Actor2Name',
    'Actor2CountryCode',
    'Actor2KnownGroupCode',
    'Actor2EthnicCode',
    'Actor2Religion1Code',
    'Actor2Religion2Code',
    'Actor2Type1Code',
    'Actor2Type2Code',
    'Actor2Type3Code',
    'IsRootEvent',
    'EventCode',
    'EventBaseCode',
    'EventRootCode',
    'QuadClass',
    'GoldsteinScale',
    'NumMentions',
    'NumSources',
    'NumArticles',
    'AvgTone',
    'Actor1Geo_Type',
    'Actor1Geo_Fullname',
    'Actor1Geo_CountryCode',
    'Actor1Geo_ADM1Code',
    'Actor1Geo_Lat',
    'Actor1Geo_Long',
    'Actor1Geo_FeatureID',
    'Actor2Geo_Type',
    'Actor2Geo_Fullname',
    'Actor2Geo_CountryCode',
    'Actor2Geo_ADM1Code',
    'Actor2Geo_Lat',
    'Actor2Geo_Long',
    'Actor2Geo_FeatureID',
    'ActionGeo_Type',
    'ActionGeo_Fullname',
    'ActionGeo_CountryCode',
    'ActionGeo_ADM1Code',
    'ActionGeo_Lat',
    'ActionGeo_Long',
    'ActionGeo_FeatureID',
    'DATEADDED',
    'SOURCEURL'
]

mentions_head = [
    'GlobalEventID',
    'EventTimeDate',
    'MentionTimeDate',
    'MentionType',
    'MentionSourceName',
    'MentionIdentifier',
    'SentenceID',
    'Actor1CharOffset',
    'Actor2CharOffset',
    'ActionCharOffset',
    'InRawText',
    'Confidence',
    'MentionDocLen',
    'MentionDocTone',
    'MentionDocTranslationInfo',
    'Extras'
]

times = [
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

MEDIUMLIST = [
    'apnews.com',
    'apr.org',
    'bbc.com',
    'bbc.co.uk',
    'bignewsnetwork.com',
    'cnn.com',
    'dailymail.co.uk',
    'foxnews.com',
    'globalsecurity.org',
    'indiatimes.com',
    'indiablooms.com',
    'indianexpress.com',
    'jpost.com',
    'mirror.co.uk',
    'nytimes.com',
    'tass.com',
    'theguardian.com',
    'telegraphindia.com',
    'washingtonpost.com',
    'yorkpress.co.uk',
    'yahoo.com',
]


def generate_random_string(length=10):
    # 大写字母加数字
    characters = string.ascii_uppercase + string.digits
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return random_string


def curr_time():
    t = time.time()
    return str(int(round(t * 1000000)))  # 微秒级时间戳


def create_date_range(inp: list):
    result_list = []
    sta_day = datetime.strptime(str(inp[0]), '%Y%m%d')
    end_day = datetime.strptime(str(inp[1]), '%Y%m%d')
    dlt_day = (end_day - sta_day).days + 1

    for i in range(dlt_day):
        tmp_day = sta_day + timedelta(days=i)
        tmp_day_txt = tmp_day.strftime('%Y%m%d')
        result_list.append(tmp_day_txt)

    return result_list


def dict_to_json(outfile, data):
    with open(outfile, 'w') as f:
        json.dump(data, f)


def json_to_dict(file):
    with open(file, 'r') as f:
        ans = json.load(f)
    return ans


TIME_RANGE = [20240813, 20240911]
DAYS = create_date_range(TIME_RANGE)
PROCESS_GDELT_PATH = 'merge/NULL/' + str(TIME_RANGE[0]) + '_' + str(TIME_RANGE[1]) + '.media.merge.csv'

if __name__ == "__main__":
    # 测试生成一个长度为10的随机字符串
    random_string = generate_random_string(10)
    print(random_string)
