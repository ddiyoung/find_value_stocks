import FinanceDataReader as fdr
from datetime import datetime, timedelta
import pandas as pd
from conn_db import DBController
import sqlalchemy

class CorrectionStocks:
    table = 'kospi_adjusted1' #수정주가 테이블 이름

    def __init__(self):
        self.conn = DBController() #DB 연결
        self.stock_list = fdr.StockListing('KRX').dropna() #krx의 stock list 불러오기
        self.daily = pd.DataFrame() #daily 변수 초기화
        self.table_df = pd.DataFrame() # DB의 테이블 불러올 변수 초기화
        self._get_ohclv_data() #DB에서 table을 불러옴


    def init_save(self, start_date, end_date): #지정한 날짜로 부터 모든 데이터를 저장
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        date_list = [start + timedelta(days=x) for x in range(0, (end-start).days)]
        for date in date_list:
            self.daily = pd.DataFrame()  # daily 변수 초기화
            print("start get_daily_concat : " + date.strftime("%Y-%m-%d"))
            self._get_daily_concat(date.strftime("%Y-%m-%d"))
            self._save_daily()



    def _get_table_df(self): #table data 리턴
        return self.table_df


    def _get_ohclv_data(self): #DB로 부터 table에 있는 데이터를 갖고오는 함수
        try:
            self.table_df = pd.read_sql_table(
                self.table,
                con=self.conn.create_engine()
            )
            self.conn.dispose_engine()
        except:
            pass


    def _get_index_stock_date(self, stock_code, date): #stock code와 date를 기준으로 해당 데이터의 인덱스를 리턴하는 함수
        return self.table_df.index[(self.table_df['Code'] == stock_code) & (self.table_df['Date'] == date)].tolist()


    def _get_stock_date(self, stock_code, date): #stock code와 date를 기준으로 해당 dataframe을 리턴하는 함수
        return self.table_df.iloc[self._get_index_stock_date(stock_code, date)[0]]


    def _update_stocks(self, stock_code, start_date, end_date): #ohlcv 테이블을 업데이트 해주는 함수
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        date_list = [start + timedelta(days=x) for x in range(0, (end-start).days)]
        for date in date_list:
            self.daily = pd.DataFrame()
            date_str = date.strftime("%Y-%m-%d")
            if self._get_index_stock_date(stock_code, date_str): #해당 리스트가 존재한다면 이미 존재하는 데이터이기 때문에 pass
                print("Already Exists data || date : " + date_str + " || stock_code : " + stock_code)
                continue
            print("start get_daily_concat : " + date_str + "|| stock_code : " + stock_code) #아닌 경우 data 수집
            ohlcv = fdr.DataReader(stock_code, date, date)
            ohlcv['Code'] = stock_code
            stock_index = self.stock_list.index[self.stock_list['Code'] == stock_code].tolist()[0]
            ohlcv['Name'] = self.stock_list.iloc[stock_index]['Name']
            self.daily = pd.concat([self.daily, ohlcv])
            self.daily['Date'] = self.daily.index
            self._save_daily()  # data 저장



    def _get_daily_concat(self, date): #모든 stock_list의 수정주가 갖고오기
        for code, name in self.stock_list[['Symbol', 'Name']].values:
            ohlcv = fdr.DataReader(code, date, date)
            ohlcv['Code'] = code
            ohlcv['Name'] = name
            self.daily = pd.concat([self.daily, ohlcv])
            self.daily['Date'] = self.daily.index


    def _save_daily(self): #daily 변수를 DB에 저장
        try:
            self.daily.to_sql(name = self.table, con= self.conn.create_engine(), if_exists='append', index=False,
                              dtype={  # sql에 저장할 때, 데이터 유형도 설정할 수 있다.
                                  'Open': sqlalchemy.types.BIGINT(),
                                  'High': sqlalchemy.types.BIGINT(),
                                  'Low': sqlalchemy.types.BIGINT(),
                                  'Close': sqlalchemy.types.BIGINT(),
                                  'Volume': sqlalchemy.types.BIGINT(),
                                  'Change': sqlalchemy.types.FLOAT(),
                                  'Code': sqlalchemy.types.VARCHAR(10),
                                  'Name': sqlalchemy.types.TEXT(),
                                  'Date': sqlalchemy.types.DATE(),

                              })
        except:
            pass

        self.conn.dispose_engine()