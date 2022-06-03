# db와 연결해주는 engine 코드
from sqlalchemy import create_engine

class DBController: #DB 연결 컨트롤러
    server = 'localhost'
    user = 'root'
    password = '02160216'
    db = 'sw1'

    def __init__(self): #DB와 연결
        self.engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.server}/{self.db}?charset=utf8')

    def create_engine(self): #DB와 연결한 engine 리턴
        self.engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.server}/{self.db}?charset=utf8')
        return self.engine

    def dispose_engine(self): #DB와 연결 해제
        self.engine.dispose()
