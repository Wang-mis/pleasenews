import os
import openai
import pandas as pd
import helper
import time
from keybert import KeyBERT
from tqdm import tqdm

from utils import create_date_range, TIME_RANGE

# os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:1080'
# os.environ["HTTP_PROXY"] = 'http://127.0.0.1:1080'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
os.environ["HTTP_PROXY"] = 'http://127.0.0.1:7890'

# SAVE_NEWS = "../pnews/20240131/"
SAVE_NEWS = "../pnews/20240901/"
MEIDA_CONTENT = SAVE_NEWS + "MentionSourceNames.csv"
MEDIA_KEYWORD = SAVE_NEWS + "Keywords.csv"

OPENAI_API_KEY = "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"  # gpt-4
openai.api_key = OPENAI_API_KEY


def get_openai_completion(prompt, model="gpt-4", temperature=0):
    """
    prompt: question
    model:  gpt-3.5-turbo(ChatGPT) / text-davinci-003 , gpt-4
    """
    try:
        messages = [{"role": "user", "content": prompt}]
        response = openai.ChatCompletion.create(
            model=model,
            messages=messages,
            # max_tokens=256,
            n=1,
            temperature=temperature,  # 模型输出的温度系数，控制输出的随机程度
        )
        # 调用 OpenAI 的 ChatCompletion 接口
        print(response.choices[0].message["content"])
    except Exception as e:
        print(e)
        return "error..."

    return response.choices[0].message["content"]


def gainKeywordByKeyBERT():
    print(MEIDA_CONTENT)
    df = pd.read_csv(MEIDA_CONTENT)
    UniqueIDList = df["UniqueID"].to_list()
    ContentList = df["Content"].to_list()
    KeywordList = []
    print(len(UniqueIDList))

    kw_model = KeyBERT()

    for index, content in tqdm(enumerate(ContentList)):
        try:
            keywords = kw_model.extract_keywords(content, keyphrase_ngram_range=(1, 1), stop_words='english', top_n=15)
            keywords = "|".join([ele[0] for ele in keywords])
            KeywordList.append(keywords)
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
    df_key.to_csv(SAVE_NEWS + "Keywords.csv", index=False, encoding='utf-8', errors='ignore')
    pass


# 为爬取的新闻文章提取关键词
def gainKeywordByGPT4():
    print(MEIDA_CONTENT)
    df = pd.read_csv(MEIDA_CONTENT)
    UniqueIDList = df["UniqueID"].to_list()
    ContentList = df["Content"].to_list()
    KeywordList = []

    print(len(UniqueIDList))

    for index, content in tqdm(enumerate(ContentList)):
        time.sleep(2)
        prompt = "Please understand the following text and extract keywords from it. \
            The number of keywords should not be less than 3 and should not exceed 15. \
            The return value should be of type str in Python, separated by a '|' symbol between each keyword. \
            This passage is: '{}'".format(content)

        print(index)

        keywords = get_openai_completion(prompt=prompt)
        KeywordList.append(keywords)

    # print(KeywordList)

    # 创建一个字典，包含列名和相应的数据
    data = {
        'UniqueID': UniqueIDList,
        'Keyword': KeywordList,
    }

    # 使用字典创建数据框
    df_key = pd.DataFrame(data)
    df_key.to_csv(SAVE_NEWS + "Keywords.csv", index=False)

    return


# 自定义函数
def parse_keyword(x):
    if x == "error..." or "'gbk' codec can't encode character" in x:
        return ""

    key_list = [ele.strip() for ele in x.split("|")]
    key_list1 = []
    for ele in key_list:
        key_list1 += [i.strip("'\"") for i in ele.split(",")]

    ans = "|".join(key_list1)
    return ans


def processKeyword():
    df = pd.read_csv(MEDIA_KEYWORD).fillna("")
    df['Keyword'] = df['Keyword'].apply(lambda x: parse_keyword(x))
    df.to_csv(SAVE_NEWS + "Keywords_check.csv", index=False)
    return


def oneDayMediaListFun():
    # 查看全球媒体 20240121 报道文章数量
    MERGE_FILE = "../merge/NULL/20240121.merge.csv"
    df = pd.read_csv(MERGE_FILE)
    result = df.groupby("MentionSourceName").size().to_dict()
    media_list_today = helper.sortCustomDict(result)

    helper.Dict2Json("./media_list_today.json", media_list_today)
    return


def otfunc():
    import pandas as pd
    # 创建两个数据框
    df1 = pd.DataFrame({'ID': [1, 2, 3, 3], 'Name': ['Alice', 'Bob', 'Charlie', 'AA']})
    df2 = pd.DataFrame({'ID': [2, 3, 3, 4], 'Age': [25, 30, 40, 35]})
    # 内连接两个数据框，基于 'ID' 列进行匹配
    result_df = pd.merge(df1, df2, on='ID', how='inner')
    print(result_df)


def get_keywords():
    global SAVE_NEWS, MEIDA_CONTENT, MEDIA_KEYWORD

    # oneDayMediaListFun()
    days = create_date_range(TIME_RANGE)
    for day in days:
        SAVE_NEWS = "../pnews/" + str(day) + "/"
        MEIDA_CONTENT = SAVE_NEWS + "MentionSourceNames.csv"
        MEDIA_KEYWORD = SAVE_NEWS + "Keywords.csv"

        # gainKeywordByGPT4()
        gainKeywordByKeyBERT()
        processKeyword()

        # re = get_openai_completion("hi")
        # print(re)
        pass


if __name__ == "__main__":
    get_keywords()
