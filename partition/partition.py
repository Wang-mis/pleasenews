from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import Column, Integer, String, Text, select, and_, union_all, Float
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from utils import create_date_range

engine = create_engine(
    r'sqlite:///D:\Programs\github\pleasenews\helper\SQLiteTest_partition.db?check_same_thread=False',
    echo=False)
Base = declarative_base()
orms = dict()


def get_table_orm(table_name, gen_table_columns_func):
    """ 动态创建 ORM 模型，调用前确保table_columns中的Column没有被其它表绑定 """
    if table_name in orms:
        return orms[table_name]
    # 获取orm
    table_columns = gen_table_columns_func()
    orm = type(table_name, (Base,), {**table_columns, '__tablename__': table_name})
    # 创建表
    Base.metadata.create_all(engine, tables=[orm.__table__])

    orms[table_name] = orm
    return orm


def get_merge_table_orm(month: str):
    table_name = f"merge_table_{month}"

    def gen_table_columns_func():
        return {
            'GlobalEventID': Column(Integer),
            'Day': Column(Integer),
            'MonthYear': Column(Integer),
            'Year': Column(Integer),
            'FractionDate': Column(Float),
            'Actor1Code': Column(String),
            'Actor1Name': Column(String),
            'Actor1CountryCode': Column(String),
            'Actor1KnownGroupCode': Column(String),
            'Actor1EthnicCode': Column(String),
            'Actor1Religion1Code': Column(String),
            'Actor1Religion2Code': Column(String),
            'Actor1Type1Code': Column(String),
            'Actor1Type2Code': Column(String),
            'Actor1Type3Code': Column(String),
            'Actor2Code': Column(String),
            'Actor2Name': Column(String),
            'Actor2CountryCode': Column(String),
            'Actor2KnownGroupCode': Column(String),
            'Actor2EthnicCode': Column(String),
            'Actor2Religion1Code': Column(String),
            'Actor2Religion2Code': Column(String),
            'Actor2Type1Code': Column(String),
            'Actor2Type2Code': Column(String),
            'Actor2Type3Code': Column(String),
            'IsRootEvent': Column(Integer),
            'EventCode': Column(Integer),
            'EventBaseCode': Column(Integer),
            'EventRootCode': Column(Integer),
            'QuadClass': Column(Integer),
            'GoldsteinScale': Column(Float),
            'NumMentions': Column(Integer),
            'NumSources': Column(Integer),
            'NumArticles': Column(Integer),
            'AvgTone': Column(Float),
            'Actor1Geo_Type': Column(Integer),
            'Actor1Geo_Fullname': Column(String),
            'Actor1Geo_CountryCode': Column(String),
            'Actor1Geo_ADM1Code': Column(String),
            'Actor1Geo_Lat': Column(String),
            'Actor1Geo_Long': Column(String),
            'Actor1Geo_FeatureID': Column(String),
            'Actor2Geo_Type': Column(Integer),
            'Actor2Geo_Fullname': Column(String),
            'Actor2Geo_CountryCode': Column(String),
            'Actor2Geo_ADM1Code': Column(String),
            'Actor2Geo_Lat': Column(String),
            'Actor2Geo_Long': Column(String),
            'Actor2Geo_FeatureID': Column(String),
            'ActionGeo_Type': Column(Integer),
            'ActionGeo_Fullname': Column(String),
            'ActionGeo_CountryCode': Column(String),
            'ActionGeo_ADM1Code': Column(String),
            'ActionGeo_Lat': Column(String),
            'ActionGeo_Long': Column(String),
            'ActionGeo_FeatureID': Column(String),
            'DATEADDED': Column(Integer),
            'SOURCEURL': Column(String),
            'EventTimeDate': Column(Integer),
            'MentionTimeDate': Column(Integer),
            'MentionType': Column(Integer),
            'MentionSourceName': Column(String),
            'MentionIdentifier': Column(String),
            'SentenceID': Column(Integer),
            'Actor1CharOffset': Column(Integer),
            'Actor2CharOffset': Column(Integer),
            'ActionCharOffset': Column(Integer),
            'InRawText': Column(Integer),
            'Confidence': Column(Integer),
            'MentionDocLen': Column(Integer),
            'MentionDocTone': Column(Float),
            'MentionDocTranslationInfo': Column(String),
            'Extras': Column(String),
            'UniqueID': Column(String, primary_key=True),
        }

    return get_table_orm(table_name, gen_table_columns_func)


def get_new_table_orm(month: str):
    table_name = f"new_table_{month}"

    def gen_table_columns_func():
        return {
            'UniqueID': Column(String, primary_key=True),
            'Title': Column(String),
            'Author': Column(String),
            'PTime': Column(String),
            'DTime': Column(Integer),
            'MentionSourceName': Column(String),
            'MentionIdentifier': Column(String),
            'Content': Column(Text),
        }

    return get_table_orm(table_name, gen_table_columns_func)


def add_to_new_table(items: list[tuple]):
    item_orms = []
    for item in items:
        # 获取表名
        month = str(item[4])[:6]
        # 创建表
        table_orm = get_new_table_orm(month)
        # 将数据转化为orm对象
        item_orm = table_orm(
            UniqueID=str(item[0]),
            Title=str(item[1]),
            Author=str(item[2]),
            PTime=str(item[3]),
            DTime=int(item[4]),
            MentionSourceName=str(item[5]),
            MentionIdentifier=str(item[6]),
            Content=str(item[7])
        )
        item_orms.append(item_orm)
        # 添加数据
        Session = sessionmaker(bind=engine)
        with Session() as session:
            session.add_all(item_orms)
            session.commit()


def add_to_merge_table(rows: list[tuple]):
    item_orms = []
    for row in rows:
        # 获取表名
        month = str(row[2])
        # 创建表
        table_orm = get_merge_table_orm(month)
        # 将数据转化为orm对象
        item_orm = table_orm(
            GlobalEventID=row[0],
            Day=row[1],
            MonthYear=row[2],
            Year=row[3],
            FractionDate=row[4],
            Actor1Code=row[5],
            Actor1Name=row[6],
            Actor1CountryCode=row[7],
            Actor1KnownGroupCode=row[8],
            Actor1EthnicCode=row[9],
            Actor1Religion1Code=row[10],
            Actor1Religion2Code=row[11],
            Actor1Type1Code=row[12],
            Actor1Type2Code=row[13],
            Actor1Type3Code=row[14],
            Actor2Code=row[15],
            Actor2Name=row[16],
            Actor2CountryCode=row[17],
            Actor2KnownGroupCode=row[18],
            Actor2EthnicCode=row[19],
            Actor2Religion1Code=row[20],
            Actor2Religion2Code=row[21],
            Actor2Type1Code=row[22],
            Actor2Type2Code=row[23],
            Actor2Type3Code=row[24],
            IsRootEvent=row[25],
            EventCode=row[26],
            EventBaseCode=row[27],
            EventRootCode=row[28],
            QuadClass=row[29],
            GoldsteinScale=row[30],
            NumMentions=row[31],
            NumSources=row[32],
            NumArticles=row[33],
            AvgTone=row[34],
            Actor1Geo_Type=row[35],
            Actor1Geo_Fullname=row[36],
            Actor1Geo_CountryCode=row[37],
            Actor1Geo_ADM1Code=row[38],
            Actor1Geo_Lat=row[39],
            Actor1Geo_Long=row[40],
            Actor1Geo_FeatureID=row[41],
            Actor2Geo_Type=row[42],
            Actor2Geo_Fullname=row[43],
            Actor2Geo_CountryCode=row[44],
            Actor2Geo_ADM1Code=row[45],
            Actor2Geo_Lat=row[46],
            Actor2Geo_Long=row[47],
            Actor2Geo_FeatureID=row[48],
            ActionGeo_Type=row[49],
            ActionGeo_Fullname=row[50],
            ActionGeo_CountryCode=row[51],
            ActionGeo_ADM1Code=row[52],
            ActionGeo_Lat=row[53],
            ActionGeo_Long=row[54],
            ActionGeo_FeatureID=row[55],
            DATEADDED=row[56],
            SOURCEURL=row[57],
            EventTimeDate=row[58],
            MentionTimeDate=row[59],
            MentionType=row[60],
            MentionSourceName=row[61],
            MentionIdentifier=row[62],
            SentenceID=row[63],
            Actor1CharOffset=row[64],
            Actor2CharOffset=row[65],
            ActionCharOffset=row[66],
            InRawText=row[67],
            Confidence=row[68],
            MentionDocLen=row[69],
            MentionDocTone=row[70],
            MentionDocTranslationInfo=row[71],
            Extras=row[72],
            UniqueID=row[73]
        )
        item_orms.append(item_orm)
        # 添加数据
        Session = sessionmaker(bind=engine)
        with Session() as session:
            session.add_all(item_orms)
            session.commit()


def add_to_table_from_file(file_path, sep, add_to_table_func):
    """ 读取csv文件，并将数据插入表中 """
    csv = pd.read_csv(file_path, sep=sep).fillna("")
    items = list(csv.itertuples(index=False, name=None))
    add_to_table_func(items)


def get_new_subquery_filter_by_date_helper(month, start_date: int, end_date: int):
    orm = get_new_table_orm(month)
    return select(orm).where(and_(orm.DTime >= int(start_date), orm.DTime <= int(end_date)))


def get_merge_subquery_filter_by_date_helper(month, start_date, end_date):
    orm = get_merge_table_orm(month)
    return select(orm).where(and_(orm.Day >= int(start_date), orm.Day <= int(end_date)))


def get_subquery_filter_by_date(start_date: str | int, end_date: str | int, helper):
    """ 获取筛选时间段的查询语句 """
    start_date, end_date = str(start_date), str(end_date)
    # 获取中间所有的月份
    start_month, end_month = start_date[:6], end_date[:6]
    start = datetime.strptime(start_month, '%Y%m')
    end = datetime.strptime(end_month, '%Y%m')
    months = []
    current = start
    while current <= end:
        months.append(current.strftime('%Y%m'))
        current += relativedelta(months=1)
    # 将所有月份的表连接起来，并按照日期筛选
    querys = [helper(month, int(start_date), int(end_date)) for month in months]
    querys = union_all(*querys)
    return querys


if __name__ == '__main__':
    start_date = '20240915'
    end_date = '20241015'
    dates = create_date_range([start_date, end_date])
    for date in dates:
        merge_file_path = rf"D:\Programs\github\pleasenews\merge\{date}.media.merge.csv"
        new_file_path = rf"D:\Programs\github\pleasenews\pnews\{date}\MentionSourceNames.csv"
        add_to_table_from_file(merge_file_path, ',', add_to_merge_table)
        add_to_table_from_file(new_file_path, '\\', add_to_new_table)

    # query = get_subquery_filter_by_date(20231003, 20240719, get_new_subquery_filter_by_date_helper)
    # Session = sessionmaker(bind=engine)
    # with Session() as session:
    #     res = session.execute(query)
    #     for r in res:
    #         print(r)
