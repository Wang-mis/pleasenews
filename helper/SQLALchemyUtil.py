
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, DECIMAL, Float
from sqlalchemy.orm import declarative_base

import pandas as pd


# engine = create_engine('sqlite:///:memory:', echo=True)
engine = create_engine('sqlite:///SQLiteTest.db?check_same_thread=False', echo=True)

Base = declarative_base()

class MergeTable(Base):
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




class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    password = Column(String)

    def __repr__(self):
       return "<User(name='%s', fullname='%s', password='%s')>" % (
                            self.name, self.fullname, self.password)


    


if __name__ == '__main__':
    print(User.__table__)

    # 创建数据表。一方面通过engine来连接数据库，另一方面根据哪些类继承了Base来决定创建哪些表
    # checkfirst=True，表示创建表前先检查该表是否存在，如同名表已存在则不再创建。其实默认就是True
    # Base.metadata.create_all(engine, checkfirst=True)

    # from sqlalchemy.orm import sessionmaker
    # Session = sessionmaker(bind=engine)
    # session = Session() # 实例化会话

    # # 对象实例
    # user = User(name='ed', fullname='Ed Jones', password='edsnickname')
    # session.add(user)

    # # 一次插入多条记录形式
    # session.add_all([
    #     User(name='wendy', fullname='Wendy Williams', password='foobar'),
    #     User(name='mary', fullname='Mary Contrary', password='xxg527'),
    #     User(name='fred', fullname='Fred Flinstone', password='blah')
    # ])

    # # 当前更改只是在session中，需要使用commit确认更改才会写入数据库
    # session.commit() # 提交数据




    pass