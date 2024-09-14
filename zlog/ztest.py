import os

import pandas as pd
import spacy
from keybert import KeyBERT
from tqdm import tqdm

os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
os.environ["HTTP_PROXY"] = 'http://127.0.0.1:7890'

lemmatize = False

if lemmatize:
    nlp = spacy.load('en_core_web_sm')
else:
    nlp = None


def generate_keywords_by_keybert(day):
    content_path = "pnews/" + day + "/MentionSourceNames.csv"
    keyword_path = "pnews/" + day + "/Keywords.csv"

    df = pd.read_csv(content_path, sep='\\')
    unique_id_list = df["UniqueID"].to_list()
    content_list = df["Content"].to_list()
    keyword_list = []

    kw_model = KeyBERT()

    process_bar = tqdm(total=len(unique_id_list), desc=day + " 生成关键词中")
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
    pd.DataFrame(data).to_csv(keyword_path, index=False, encoding='utf-8', errors='ignore')


def process_keywords(day):
    keyword_path = "pnews/" + day + "/Keywords.csv"
    keyword_check_path = "pnews/" + day + "/Keywords_check.csv"

    def parse_keywords(x):
        if x == "error..." or "'gbk' codec can't encode character" in x:
            return ""

        keywords = [ele.strip() for ele in x.split("|")]
        keywords_processed = []
        for ele in keywords:
            keywords_processed += [i.strip("\'\"") for i in ele.split(",")]

        keywords_str = "|".join(keywords_processed)
        return keywords_str

    df = pd.read_csv(keyword_path).fillna("")
    df['Keyword'] = df['Keyword'].apply(lambda x: parse_keywords(x))
    df.to_csv(keyword_check_path, index=False)


def get_keywords(day):
    generate_keywords_by_keybert(day)
    process_keywords(day)
