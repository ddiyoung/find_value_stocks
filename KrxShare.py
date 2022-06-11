from conn_db import DBController
import pandas as pd
from datetime import datetime
import requests

class Krx_shares:
    def __init__(self):
        self.table = 'shares'
        self.conn = DBController()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36'
        }
        self.url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        self.krx_list = pd.DataFrame()
        self.krx_shares = pd.DataFrame()
        self._get_krx_code()

    def init_save(self):
        start_date = '2000-01-01'
        end_date = '2022-06-01'

        for i in range(len(self.krx_list)):
            self._store_stock_shares(self.krx_list.iloc[i], start_date, end_date)

            try:
                self.krx_shares.to_sql(name= self.table, con=self.conn.create_engine(), if_exists='append', index=False, dtype=None)
            except Exception as e:
                print("Code : " + self.krx_list.iloc[i].short_code + " Name : " + self.krx_list.iloc[i].codeName )
                print(e)
                pass

    def _get_krx_code(self):
        data = {
            'locale': 'ko_KR',
            'mktsel': 'ALL',
            'typeNo': '0',
            'searchText': "",
            'bld': 'dbms/comm/finder/finder_stkisu'
        }
        req = requests.post(url=self.url, headers=self.headers, data=data)
        df = pd.read_json(req.text)['block1']
        df = pd.json_normalize(df)
        self.krx_list = df.set_index("short_code", drop= False)


    def _store_stock_shares(self, krx, start_date, end_date):

        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        start_date = start_date.strftime("%Y%m%d")

        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        end_date = end_date.strftime("%Y%m%d")


        data = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
            'locale': 'ko_KR',
            'tboxisuCd_finder_stkisu0_0': f"{krx.short_code}/{krx.codeName}",
            'isuCd': f'{krx.full_code}',
            'isuCd2': '',
            'codeNmisuCd_finder_stkisu0_0': f'{krx.codeName}',
            'param1isuCd_finder_stkisu0_0': 'ALL',
            'strtDd': f'{start_date}',
            'endDd': f'{end_date}',
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
        }

        req = requests.post(url=self.url, headers=self.headers, data=data)

        df = pd.read_json(req.text)['output']
        df = pd.json_normalize(df)
        print("Code : " + krx.short_code)
        try:
            df['TRD_DD'] = pd.to_datetime(df['TRD_DD'], format="%Y-%m-%d")
            df['Code'] = f"{krx.short_code}"
            self.krx_shares = df
        except Exception as e:
            print(e)
            pass