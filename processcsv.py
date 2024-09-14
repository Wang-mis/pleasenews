import os
import pandas as pd
from utils import MEDIUMLIST, generate_random_string, create_date_range, TIME_RANGE


def process(data):
    """
    排序、去重、添加唯一标识UniqueID
    """
    # 对原来的数据进行排序，按MentionIdentifier, SentenceID升序、Confidence, NumMentions, NumArticles, NumSources降序
    data.sort_values(['MentionIdentifier', 'SentenceID', 'Confidence', 'NumMentions', 'NumArticles', 'NumSources'],
                     ascending=[1, 1, 0, 0, 0, 0], inplace=True)
    # 具体网址URL编组去重
    data = data.groupby(['MentionIdentifier']).head(1)
    # 添加唯一标识的列
    data['UniqueID'] = data.apply(lambda x: generate_random_string(10) + "_" + generate_random_string(15), axis=1)
    return data


def merge_mentions(day, mention_path, export_path, merge_path):
    """
    遍历mentions和export文件夹中的所有文件，按日期合并，生成.merge.csv
    """
    # 设置merge文件存储路径
    if not os.path.exists(merge_path):
        os.makedirs(merge_path)

    mention_day_path = mention_path + day + "/"
    mention_day_dfs = []
    # 遍历mentions/DAY/目录下的所有mentions文件
    for mention_file in os.listdir(mention_day_path):
        # 如果当前文件不是CSV文件，直接跳过
        if mention_file.split(".")[-1].lower() != 'csv':
            continue

        # 读取mentions.CSV
        mention_day_dfs.append(pd.read_csv(mention_day_path + mention_file, delimiter='\t'))

    # 合并一天中的所有mentions.CSV
    mention_day_merged_df = pd.concat(mention_day_dfs)
    # 读取export.CSV
    export_day_df = pd.read_csv(export_path + day + ".export.CSV", delimiter='\t')
    # 合并mentions.CSV和export.CSV
    data_day_df = pd.merge(export_day_df, mention_day_merged_df, how='inner', on='GlobalEventID')
    # 对合并后的DataFrame进行处理
    data_day_df = process(data_day_df)
    data_day_df.to_csv(merge_path + day + ".merge.csv", index=False)


def merge_medialist(day, merge_path):
    merge_file = day + ".merge.csv"
    df = pd.read_csv(merge_path + merge_file)
    df = df[df["MentionSourceName"].isin(MEDIUMLIST)]
    df.to_csv(merge_path + day + ".media.merge.csv", index=False)


def process_csv(day):
    merge_mentions(day, mention_path="mentions/", export_path="export/", merge_path="merge/")
    merge_medialist(day, merge_path="merge/")
