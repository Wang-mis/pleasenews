import os
import pandas as pd
from keybert import KeyBERT
from tqdm import tqdm
from utils import create_date_range, TIME_RANGE

os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
os.environ["HTTP_PROXY"] = 'http://127.0.0.1:7890'

DAY = "20240901"
SAVE_NEWS = "pnews/" + DAY + "/"
MEIDA_CONTENT = SAVE_NEWS + "MentionSourceNames.csv"
MEDIA_KEYWORD = SAVE_NEWS + "Keywords.csv"
MEDIA_KEYWORD_CHECK = SAVE_NEWS + "Keywords_check.csv"

def generate_keywords_by_KeyBERT():
    df = pd.read_csv(MEIDA_CONTENT)
    UniqueIDList = df["UniqueID"].to_list()
    ContentList = df["Content"].to_list()
    KeywordList = []

    kw_model = KeyBERT()

    process_bar = tqdm(total=len(UniqueIDList), desc=DAY + " 生成关键词中")
    for index, content in enumerate(ContentList):
        try:
            keywords = kw_model.extract_keywords(content, keyphrase_ngram_range=(1, 1), stop_words='english', top_n=15)
            keywords = "|".join([ele[0] for ele in keywords])
            KeywordList.append(keywords)
            process_bar.update(1)
        except Exception as e:
            print(e)
            print(index)
            print(content)
            KeywordList.append("error...")

    # 创建一个字典，包含列名和相应的数据
    data = {
        'UniqueID': UniqueIDList,
        'Keyword': KeywordList,
    }

    # 使用字典创建数据框
    df_key = pd.DataFrame(data)
    df_key.to_csv(MEDIA_KEYWORD, index=False, encoding='utf-8', errors='ignore')
    pass


def process_keywords():
    def parse_keywords(x):
        if x == "error..." or "'gbk' codec can't encode character" in x:
            return ""

        key_list = [ele.strip() for ele in x.split("|")]
        key_list1 = []
        for ele in key_list:
            key_list1 += [i.strip("\'\"") for i in ele.split(",")]

        ans = "|".join(key_list1)
        return ans

    df = pd.read_csv(MEDIA_KEYWORD).fillna("")
    df['Keyword'] = df['Keyword'].apply(lambda x: parse_keywords(x))
    df.to_csv(MEDIA_KEYWORD_CHECK, index=False)
    return


def get_keywords():
    global DAY, SAVE_NEWS, MEIDA_CONTENT, MEDIA_KEYWORD, MEDIA_KEYWORD_CHECK

    days = create_date_range(TIME_RANGE)
    for day in days:
        DAY = str(day)
        SAVE_NEWS = "pnews/" + DAY + "/"
        MEIDA_CONTENT = SAVE_NEWS + "MentionSourceNames.csv"
        MEDIA_KEYWORD = SAVE_NEWS + "Keywords.csv"
        MEDIA_KEYWORD_CHECK = SAVE_NEWS + "Keywords_check.csv"
        generate_keywords_by_KeyBERT()
        process_keywords()


if __name__ == "__main__":
    get_keywords()
