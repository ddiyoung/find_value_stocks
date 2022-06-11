import FinanceDataReader as fdr
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from conn_db import DBController
from KrxShare import Krx_shares
import sqlalchemy

class CorrectionStocks:

    def __init__(self):
        self.conn = DBController() #DB 연결
        self.table = 'kospi_adjusted1' #수정주가 테이블 이름
        self.stock_list = fdr.StockListing('KRX').dropna() #krx의 stock list 불러오기
        self.daily = pd.DataFrame() #daily 변수 초기화
        self.table_df = pd.DataFrame() # DB의 테이블 불러올 변수 초기화
        self.ohlc_df = pd.DataFrame()


    def init_save(self, start_date, end_date): #지정한 날짜로 부터 모든 데이터를 저장
        self.daily = pd.DataFrame()
        self._get_daily_concat(start_date, end_date)
        self._save_daily()


    def _get_ohlcv_data(self, term): #DB로 부터 table에 있는 데이터를 갖고오는 함수
        # 분기별 시작날까/종료날짜 설정
        if term[5] == '1':  # 1분기 (1월~3월)
            start_date = term[0:4] + '-01-01'
            end_date = term[0:4] + '-03-31'
            period = term[0:4] + '/03'

        elif term[5] == '2':  # 2분기 (4월~6월)
            start_date = term[0:4] + '-04-01'
            end_date = term[0:4] + '-06-30'
            period = term[0:4] + '/06'

        elif term[5] == '3':  # 3분기 (7월~9월)
            start_date = term[0:4] + '-07-01'
            end_date = term[0:4] + '-09-30'
            period = term[0:4] + '/09'

        elif term[5] == '4':  # 4분기 (10월~12월)
            start_date = term[0:4] + '-10-01'
            end_date = term[0:4] + '-12-31'
            period = term[0:4] + '/12'

        sql = f"SELECT * FROM kospi_adjusted1 where Date(date) BETWEEN '{start_date}' AND '{end_date}'"
        try:
            self.table_df = pd.read_sql(
                sql,
                con=self.conn.create_engine()
            )
            self.conn.dispose_engine()
        except Exception as e:
            print(e)
            pass

        print('start_date({}) ~ end_date({})'.format(start_date, end_date))

        df = self.table_df

        df = df.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low' : 'low',
            'Close': 'close',
            'Volume': 'volume',
            'Change': 'change',
            'Code': 'stock_code',
            'Name': 'name',
            'Date': 'date'
        })

        df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        shares = Krx_shares()
        shares._get_share(term)
        df_share = shares.share_df

        df = pd.merge(df, df_share, how='right', on = ['stock_code', 'date'])

        df.set_index('date', inplace=True)

        df = df.sort_values(by=['stock_code', 'date'], axis=0)  # stock_code, date별로 정렬

        # 결측치 처리
        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].replace(0, np.nan)

        df['open'] = np.where(pd.notnull(df['open']) == True, df['open'], df['close'])
        df['high'] = np.where(pd.notnull(df['high']) == True, df['high'], df['close'])
        df['low'] = np.where(pd.notnull(df['low']) == True, df['low'], df['close'])
        df['close'] = np.where(pd.notnull(df['close']) == True, df['close'], df['close'])

        # stock_code 별로 통계 모으기    `
        groups = df.groupby('stock_code')

        df_ohlc = pd.DataFrame()
        df_ohlc['high'] = groups.max()['high']  # 분기별 고가
        df_ohlc['low'] = groups.min()['low']  # 분기별 저가
        df_ohlc['period'] = period  # 분기 이름 설정
        df_ohlc['open'], df_ohlc['close'], df_ohlc['volume'] = np.nan, np.nan, np.nan
        df_ohlc['시가총액'], df_ohlc['상장주식수'] = np.nan, np.nan

        df_ohlc['stock_code'] = df_ohlc.index
        df_ohlc = df_ohlc.reset_index(drop=True)

        for i in range(len(df_ohlc)):
            df_ohlc['open'][i] = float(df[df['stock_code'] == df_ohlc['stock_code'][i]].head(1)['open'])  # 분기별 시가
            df_ohlc['close'][i] = float(df[df['stock_code'] == df_ohlc['stock_code'][i]].tail(1)['close'])  # 분기별 저가
            df_ohlc['volume'][i] = float(df[df['stock_code'] == df_ohlc['stock_code'][i]].tail(1)['volume'])  # 분기별 거래량
            df_ohlc['시가총액'][i] = float(df[df['stock_code'] == df_ohlc['stock_code'][i]].tail(1)['MKTCAP'])  # 분기별 시가총액
            df_ohlc['상장주식수'][i] = float(
                df[df['stock_code'] == df_ohlc['stock_code'][i]].tail(1)['LIST_SHRS'])  # 분기별 상장주식수

        df_ohlc = df_ohlc[['stock_code', 'period', 'open', 'high', 'low', 'close', 'volume', '시가총액', '상장주식수']]

        self.ohlc_df = df_ohlc

        return df_ohlc, period


    def _get_ohlcv_stock(self, stock_code, term):
        # 분기별 시작날까/종료날짜 설정
        if term[5] == '1':  # 1분기 (작년 4월~3월)
            start_date = str(int(term[0:4]) - 1) + '-04-01'
            end_date = term[0:4] + '-03-31'

        elif term[5] == '2':  # 2분기 (작년 7월~6월)
            start_date = str(int(term[0:4]) - 1) + '-07-01'
            end_date = term[0:4] + '-06-30'

        elif term[5] == '3':  # 3분기 (작년 10월~9월)
            start_date = str(int(term[0:4]) - 1) + '-09-01'
            end_date = term[0:4] + '-09-30'

        elif term[5] == '4':  # 4분기 (1월~12월)
            start_date = term[0:4] + '-01-01'
            end_date = term[0:4] + '-12-31'

        sql = f"SELECT * FROM kospi_adjusted1 where Code = '{stock_code}' AND DATE(date) BETWEEN '{start_date} AND '{end_date}'"

        ohlcv_data = pd.DataFrame()
        try:
            ohlcv_data = pd.read_sql(
                sql,
                con=self.conn.create_engine()
                )
            self.conn.dispose_engine()
        except Exception as e:
            print(e)
            pass

        ohlcv_data = ohlcv_data[['stock_code', 'date', 'close']]  # 종가만 갖고오기

        return ohlcv_data



    def _get_index_stock_date(self, stock_code, date): #stock code와 date를 기준으로 해당 데이터의 인덱스를 리턴하는 함수
        return self.table_df.index[(self.table_df['Code'] == stock_code) & (self.table_df['Date'] == date)].tolist()


    def _get_stock_date(self, stock_code, date): #stock code와 date를 기준으로 해당 dataframe을 리턴하는 함수
        if self._get_index_stock_date(stock_code, date)[0]:
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



    def _get_daily_concat(self, start_date, end_date): #모든 stock_list의 수정주가 갖고오기
        for code, name in self.stock_list[['Symbol', 'Name']].values:
            print("Code : " + code + " | Name : " + name + " | get_daily_concat")
            ohlcv = fdr.DataReader(code, start_date, end_date)
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
        except Exception as e:
            print(e)
            pass

        self.conn.dispose_engine()