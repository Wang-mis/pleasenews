
import pandas as pd
from utils import export_head, mentions_head, times
import os

MEDIUM = 'yahoo.com'


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

        # 具体媒体MEDIUM
        grouped = grouped[grouped["MentionSourceName"]==MEDIUM]
        # grouped.to_csv("grouped.csv", index=False)
        
        grouped.to_csv(merge_save + ele_dir + ".merge.csv", index=False)


if __name__ == "__main__":
        
    mergeMentions(mention_path="./mentions/", export_path = "./export/", merge_save= "./merge/", filter=["CHN","CHN"])