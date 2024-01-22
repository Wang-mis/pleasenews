
import pandas as pd
from utils import export_head, mentions_head, times, MEDIUMLIST, generate_random_string, create_date_range, TIME_RANGE
import os
import warnings
warnings.filterwarnings("ignore")


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

    if len(filter) == 0:
        merge_save += "NULL/"

    if not os.path.exists(merge_save):
        os.makedirs(merge_save)

    mention_dir = os.listdir(mention_path)
    
    sub_merge_dfs = []

    for ele_dir in mention_dir:

        if ele_dir not in create_date_range(TIME_RANGE):
            print("跳过: ", ele_dir)
            continue

        print("不跳过: ", ele_dir)

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

        # 排序规则：MentionIdentifier升序, SentenceID升序, Confidence 降序, NumMentions 降序, NumArticles 降序, NumSources 降序
        data.sort_values(['MentionIdentifier', 'SentenceID', 'Confidence', 'NumMentions', 'NumArticles', 'NumSources'], ascending=[1, 1, 0, 0, 0, 0], inplace=True)

        # 具体网址URL编组去重
        grouped = data.groupby(['MentionIdentifier']).head(1)
        # 核心事件信服度占比
        grouped = grouped[grouped["Confidence"]>=75]
        # 核心事件出现在文章开头第一句
        grouped = grouped[grouped["SentenceID"]==1]


        # 按照媒体列表MEDIUMLIST筛选
        # 目前给定的媒体列表筛选完一天大概70条文章，全球媒体大概700条文章
        # grouped = grouped[grouped["MentionSourceName"].isin(MEDIUMLIST)]

        # 添加唯一标识的列（随机函数生成，也可能不是唯一）
        grouped['UniqueID'] = grouped.apply(lambda x : generate_random_string(10) + "_" + generate_random_string(15), axis = 1)
        
        grouped.to_csv(merge_save + ele_dir + ".merge.csv", index=False)
        sub_merge_dfs.append(grouped)
    
    # 合并所有的merge.csv
    all_merge_dfs = pd.concat(sub_merge_dfs)
    ele_dir = str(TIME_RANGE[0]) + '_' + str(TIME_RANGE[-1])
    all_merge_dfs.to_csv(merge_save + ele_dir + ".merge.csv", index=False)


if __name__ == "__main__":
    # filter = ['RUS', 'UKR']
    # filter = ["CHN", "CHN"]
    # filter = ["CHN", "TWN"]

    filter = [] # 当filter为[]，检索媒体列表每天报道的核心事件

    mergeMentions(mention_path="./mentions/", export_path = "./export/", merge_save= "./merge/", filter=filter)