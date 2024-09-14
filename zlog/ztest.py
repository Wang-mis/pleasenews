import os

import pandas as pd
import spacy
from keybert import KeyBERT
from tqdm import tqdm
from utils import create_date_range, TIME_RANGE

os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
os.environ["HTTP_PROXY"] = 'http://127.0.0.1:7890'

DAY = "20240901"
SAVE_NEWS = "pnews/" + DAY + "/"
CONTENT_PATH = SAVE_NEWS + "MentionSourceNames.csv"
KEYWORD_PATH = SAVE_NEWS + "Keywords.csv"
KEYWORD_CHECK_PATH = SAVE_NEWS + "Keywords_check.csv"

lemmatize = False

if lemmatize:
    nlp = spacy.load('en_core_web_sm')
else:
    nlp = None


def generate_keywords_by_keybert():
    df = pd.read_csv(CONTENT_PATH)
    unique_id_list = df["UniqueID"].to_list()
    content_list = df["Content"].to_list()
    keyword_list = []

    kw_model = KeyBERT()

    process_bar = tqdm(total=len(unique_id_list), desc=DAY + " 生成关键词中")
    for content in content_list:
        try:
            if lemmatize:
                doc = nlp(content)
                words = [t.lemma_ for t in doc]
                content = " ".join(words)

            keywords = kw_model.extract_keywords(content, keyphrase_ngram_range=(1, 1), stop_words='english', top_n=15)
            keywords = "|".join([ele[0] for ele in keywords])
            keyword_list.append(keywords)
            process_bar.update(1)
        except Exception as e:
            print(e)
            print(content)
            keyword_list.append("error...")

    # 创建一个字典，包含列名和相应的数据
    data = {
        'UniqueID': unique_id_list,
        'Keyword': keyword_list,
    }

    # 生成csv
    pd.DataFrame(data).to_csv(KEYWORD_PATH, index=False, encoding='utf-8', errors='ignore')


def process_keywords():
    def parse_keywords(x):
        if x == "error..." or "'gbk' codec can't encode character" in x:
            return ""

        keywords = [ele.strip() for ele in x.split("|")]
        keywords_processed = []
        for ele in keywords:
            keywords_processed += [i.strip("\'\"") for i in ele.split(",")]

        keywords_str = "|".join(keywords_processed)
        return keywords_str

    df = pd.read_csv(KEYWORD_PATH).fillna("")
    df['Keyword'] = df['Keyword'].apply(lambda x: parse_keywords(x))
    df.to_csv(KEYWORD_CHECK_PATH, index=False)


def get_keywords():
    global DAY, SAVE_NEWS, CONTENT_PATH, KEYWORD_PATH, KEYWORD_CHECK_PATH

    days = create_date_range(TIME_RANGE)
    for day in days:
        if int(day) < 20240901:
            continue
        DAY = str(day)
        SAVE_NEWS = "pnews/" + DAY + "/"
        CONTENT_PATH = SAVE_NEWS + "MentionSourceNames.csv"
        KEYWORD_PATH = SAVE_NEWS + "Keywords.csv"
        KEYWORD_CHECK_PATH = SAVE_NEWS + "Keywords_check.csv"
        generate_keywords_by_keybert()
        process_keywords()


if __name__ == "__main__":
    get_keywords()
