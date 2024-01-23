
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, DECIMAL, Float, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd


# engine = create_engine('sqlite:///:memory:', echo=True)
engine = create_engine('sqlite:///SQLiteTest.db?check_same_thread=False', echo=True)

Base = declarative_base()

class MergeItem(Base):
    __tablename__ = 'merge_table'

    # 系统自带id 可自增
    AutoId = Column(Integer, primary_key=True)

    GlobalEventID = Column(Integer)
    Day = Column(Integer)
    MonthYear = Column(Integer)
    Year = Column(Integer)
    FractionDate = Column(Float)

    Actor1Code = Column(String)
    Actor1Name = Column(String)
    Actor1CountryCode = Column(String)
    Actor1KnownGroupCode = Column(String)
    Actor1EthnicCode = Column(String)
    Actor1Religion1Code = Column(String)
    Actor1Religion2Code = Column(String)
    Actor1Type1Code = Column(String)
    Actor1Type2Code = Column(String)
    Actor1Type3Code = Column(String)

    Actor2Code = Column(String)
    Actor2Name = Column(String)
    Actor2CountryCode = Column(String)
    Actor2KnownGroupCode = Column(String)
    Actor2EthnicCode = Column(String)
    Actor2Religion1Code = Column(String)
    Actor2Religion2Code = Column(String)
    Actor2Type1Code = Column(String)
    Actor2Type2Code = Column(String)
    Actor2Type3Code = Column(String)

    IsRootEvent = Column(Integer)
    EventCode = Column(Integer)
    EventBaseCode = Column(Integer)
    EventRootCode = Column(Integer)
    QuadClass = Column(Integer)

    GoldsteinScale = Column(Float)

    NumMentions = Column(Integer)
    NumSources = Column(Integer)
    NumArticles = Column(Integer)

    AvgTone = Column(Float)

    Actor1Geo_Type = Column(Integer)
    Actor1Geo_Fullname = Column(String)
    Actor1Geo_CountryCode = Column(String)
    Actor1Geo_ADM1Code = Column(String)
    Actor1Geo_Lat = Column(String)
    Actor1Geo_Long = Column(String)
    Actor1Geo_FeatureID = Column(String)

    Actor2Geo_Type = Column(Integer)
    Actor2Geo_Fullname = Column(String)
    Actor2Geo_CountryCode = Column(String)
    Actor2Geo_ADM1Code = Column(String)
    Actor2Geo_Lat = Column(String)
    Actor2Geo_Long = Column(String)
    Actor2Geo_FeatureID = Column(String)

    ActionGeo_Type = Column(Integer)
    ActionGeo_Fullname = Column(String)
    ActionGeo_CountryCode = Column(String)
    ActionGeo_ADM1Code = Column(String)
    ActionGeo_Lat = Column(String)
    ActionGeo_Long = Column(String)
    ActionGeo_FeatureID = Column(String)

    DATEADDED = Column(Integer)
    SOURCEURL = Column(String)

    EventTimeDate = Column(Integer)
    MentionTimeDate = Column(Integer)
    MentionType = Column(Integer)

    MentionSourceName = Column(String)
    MentionIdentifier = Column(String)

    SentenceID = Column(Integer)
    Actor1CharOffset = Column(Integer)
    Actor2CharOffset = Column(Integer)
    ActionCharOffset = Column(Integer)
    InRawText = Column(Integer)
    Confidence = Column(Integer)
    MentionDocLen = Column(Integer)

    MentionDocTone = Column(Float)
    MentionDocTranslationInfo = Column(String)
    Extras = Column(String)

    # 为了唯一标记一篇新闻文章
    UniqueID = Column(String)

def gainMergeItem(row):
    return MergeItem(
        GlobalEventID = row[0],
        Day = row[1],
        MonthYear = row[2],
        Year = row[3],
        FractionDate = row[4],
        Actor1Code = row[5],
        Actor1Name = row[6],
        Actor1CountryCode = row[7],
        Actor1KnownGroupCode = row[8],
        Actor1EthnicCode = row[9],
        Actor1Religion1Code = row[10],
        Actor1Religion2Code = row[11],
        Actor1Type1Code = row[12],
        Actor1Type2Code = row[13],
        Actor1Type3Code = row[14],
        Actor2Code = row[15],
        Actor2Name = row[16],
        Actor2CountryCode = row[17],
        Actor2KnownGroupCode = row[18],
        Actor2EthnicCode = row[19],
        Actor2Religion1Code = row[20],
        Actor2Religion2Code = row[21],
        Actor2Type1Code = row[22],
        Actor2Type2Code = row[23],
        Actor2Type3Code = row[24],
        IsRootEvent = row[25],
        EventCode = row[26],
        EventBaseCode = row[27],
        EventRootCode = row[28],
        QuadClass = row[29],
        GoldsteinScale = row[30],
        NumMentions = row[31],
        NumSources = row[32],
        NumArticles = row[33],
        AvgTone = row[34],
        Actor1Geo_Type = row[35],
        Actor1Geo_Fullname = row[36],
        Actor1Geo_CountryCode = row[37],
        Actor1Geo_ADM1Code = row[38],
        Actor1Geo_Lat = row[39],
        Actor1Geo_Long = row[40],
        Actor1Geo_FeatureID = row[41],
        Actor2Geo_Type = row[42],
        Actor2Geo_Fullname = row[43],
        Actor2Geo_CountryCode = row[44],
        Actor2Geo_ADM1Code = row[45],
        Actor2Geo_Lat = row[46],
        Actor2Geo_Long = row[47],
        Actor2Geo_FeatureID = row[48],
        ActionGeo_Type = row[49],
        ActionGeo_Fullname = row[50],
        ActionGeo_CountryCode = row[51],
        ActionGeo_ADM1Code = row[52],
        ActionGeo_Lat = row[53],
        ActionGeo_Long = row[54],
        ActionGeo_FeatureID = row[55],
        DATEADDED = row[56],
        SOURCEURL = row[57],
        EventTimeDate = row[58],
        MentionTimeDate = row[59],
        MentionType = row[60],
        MentionSourceName = row[61],
        MentionIdentifier = row[62],
        SentenceID = row[63],
        Actor1CharOffset = row[64],
        Actor2CharOffset = row[65],
        ActionCharOffset = row[66],
        InRawText = row[67],
        Confidence = row[68],
        MentionDocLen = row[69],
        MentionDocTone = row[70],
        MentionDocTranslationInfo = row[71],
        Extras = row[72],
        UniqueID = row[73]
    )


def writeMergeTable(file_path = "../merge/NULL/20240110_20240120.merge.csv"):
    Base.metadata.create_all(engine, checkfirst=True)
    Session = sessionmaker(bind=engine)
    session = Session() # 实例化会话

    data = pd.read_csv(file_path).fillna("")
    merge_list = []
    for index, row in data.iterrows():
        merge_list.append(gainMergeItem(row.tolist()))

    session.add_all(merge_list)
    session.commit() # 提交数据
    session.close()


# 新闻表
class NewItem(Base):
    __tablename__ = 'new_table'

    # 系统自带id 可自增
    AutoId = Column(Integer, primary_key=True)

    # 为了唯一标记一篇新闻文章
    UniqueID = Column(String)
    Title = Column(String)
    Author = Column(String)
    PTime = Column(String)
    DTime = Column(Integer)
    MentionSourceName = Column(String)
    MentionIdentifier = Column(String)
    Content = Column(Text)

def gainNewItem(row):
    return NewItem(
        UniqueID = row[0],
        Title = row[1],
        Author = row[2],
        PTime = row[3],
        DTime = row[4],
        MentionSourceName = row[5],
        MentionIdentifier = row[6],
        Content = row[7]
    )

def writeNewTable(file_path = "../pnews/20240120/MentionSourceNames.csv"):
    Base.metadata.create_all(engine, checkfirst=True)
    Session = sessionmaker(bind=engine)
    session = Session() # 实例化会话

    data = pd.read_csv(file_path).fillna("")
    merge_list = []
    for index, row in data.iterrows():
        merge_list.append(gainNewItem(row.tolist()))

    session.add_all(merge_list)
    session.commit() # 提交数据
    session.close()



# 关键词表
class KeywordItem(Base):
    __tablename__ = 'keyword_table'

    # 系统自带id 可自增
    AutoId = Column(Integer, primary_key=True)
    # 为了唯一标记一篇新闻文章
    UniqueID = Column(String)
    Keyword = Column(Text)

def gainKeywordItem(row):
    return KeywordItem(
        UniqueID = row[0],
        Keyword = row[1]
    )

def writeKeywordTable(file_path = "../pnews/20240109/Keywords_check.csv"):
    Base.metadata.create_all(engine, checkfirst=True)
    Session = sessionmaker(bind=engine)
    session = Session() # 实例化会话

    data = pd.read_csv(file_path).fillna("")
    merge_list = []
    for index, row in data.iterrows():
        merge_list.append(gainKeywordItem(row.tolist()))

    session.add_all(merge_list)
    session.commit() # 提交数据
    session.close()

def test():
    # 写一条sql
    sql = "SELECT COUNT(*) FROM keyword_table" # merge_table"
    #建立dataframe
    df = pd.read_sql_query(sql,engine)
    print(df)

if __name__ == '__main__':
    # writeMergeTable()
    # writeNewTable()
    # writeKeywordTable()
    test()
    pass