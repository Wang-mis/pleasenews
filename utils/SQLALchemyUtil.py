
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

# engine = create_engine('sqlite:///:memory:', echo=True)
engine = create_engine('sqlite:///SQLiteTest.db?check_same_thread=False', echo=True)

Base = declarative_base()

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
    Base.metadata.create_all(engine, checkfirst=True)


    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session() # 实例化会话

    # 对象实例
    user = User(name='ed', fullname='Ed Jones', password='edsnickname')
    session.add(user)

    # 一次插入多条记录形式
    session.add_all([
        User(name='wendy', fullname='Wendy Williams', password='foobar'),
        User(name='mary', fullname='Mary Contrary', password='xxg527'),
        User(name='fred', fullname='Fred Flinstone', password='blah')
    ])

    # 当前更改只是在session中，需要使用commit确认更改才会写入数据库
    session.commit() # 提交数据


    pass