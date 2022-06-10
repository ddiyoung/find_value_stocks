from conn_db import DBController
import pandas as pd
import numpy as np
from bs4 import BeauifulSoup
from urllib.request import urlopen, Request
from KrxCode import Krx_code
import sqlalchemy


class Cash_Flow(Krx_code):
    def __init__(self):
        super().__init__()
        self.conn = DBController()
        self.table = 'cf_kr'
        self.cf_data = pd.DataFrame()

    def init_save(self):
        for code in self.krx_list.code:
            try:
                consolidated_A_data = self._store_bs(code, 'CONSOLIDATED', 'A')
                consolidated_A_data.to_sql(name=self.table, con=self.conn.create_engine(), if_exists='append',
                                           index=False,
                                           dtype=None)
                consolidated_Q_data = self._store_bs(code, 'CONSOLIDATED', 'Q')
                consolidated_Q_data.to_sql(name=self.table, con=self.conn.create_engine(), if_exists='append',
                                           index=False,
                                           dtype=None)
                unconsolidated_A_data = self._store_bs(code, 'UNCONSOLIDATED', 'A')
                unconsolidated_A_data.to_sql(name=self.table, con=self.conn.create_engine(), if_exists='append',
                                             index=False,
                                             dtype=None)
                unconsolidated_Q_data = self._store_bs(code, ' UNCONSOLIDATED', 'Q')
                unconsolidated_Q_data.to_sql(name=self.table, con=self.conn.create_engine(), if_exists='append',
                                             index=False,
                                             dtype=None)
            except Exception as e:
                print("fail: " + code)
                print(e)
                pass


    def _get_cf(self, stock_code, period):
        sql = f"SELECT * FROM {self.table} WHERE stock_code = {stock_code} AND rpt_type = 'Consolidated_Q'"

        try:
            self.cf_data = pd.read_sql(
                sql,
                con = self.conn.create_engine()
            )
            self.conn.dispose_engine()

            self.cf_data = self.cf_data.rename(columns={
                'CFO_Total': '영업활동으로인한현금흐름',
                'Net_Income_Total': '당기순손익',
                'Cont_Biz_Before_Tax': '법인세비용차감전계속사업이익',
                'Add_Exp_WO_CF_Out': '현금유출이없는비용등가산',
                'Ded_Rev_WO_CF_In': '(현금유입이없는수익등차감)',
                'Chg_Working_Capital': '영업활동으로인한자산부채변동(운전자본변동)',
                'CFO': '*영업에서창출된현금흐름', 'Other_CFO': '기타영업활동으로인한현금흐름',
                'CFI_Total': '투자활동으로인한현금흐름',
                'CFI_In': '투자활동으로인한현금유입액',
                'CFI_Out': '(투자활동으로인한현금유출액)',
                'Other_CFI': '기타투자활동으로인한현금흐름',
                'CFF_Total': '재무활동으로인한현금흐름',
                'CFF_In': '재무활동으로인한현금유입액',
                'CFF_Out': '(재무활동으로인한현금유출액)',
                'Other_CFF': '기타재무활동으로인한현금흐름',
                'Other_CF': '영업투자재무활동기타현금흐름',
                'Chg_CF_Consolidation': '연결범위변동으로인한현금의증가',
                'Forex_Effect': '환율변동효과',
                'Chg_Cash_and_Cash_Equivalents': '현금및현금성자산의증가',
                'Cash_and_Cash_Equivalents_Beg': '기초현금및현금성자산',
                'Cash_and_Cash_Equivalents_End': '기말현금및현금성자산'
            })

            self.cf_data = self.cf_data[['stock_code', 'period', 'rpt_type',

                           '영업활동으로인한현금흐름', '당기순손익', '법인세비용차감전계속사업이익', '현금유출이없는비용등가산',
                           '(현금유입이없는수익등차감)', '영업활동으로인한자산부채변동(운전자본변동)',
                           '*영업에서창출된현금흐름', '기타영업활동으로인한현금흐름',

                           '투자활동으로인한현금흐름', '투자활동으로인한현금유입액',
                           '(투자활동으로인한현금유출액)', '기타투자활동으로인한현금흐름',

                           '재무활동으로인한현금흐름', '재무활동으로인한현금유입액',
                           '(재무활동으로인한현금유출액)', '기타재무활동으로인한현금흐름',

                           '영업투자재무활동기타현금흐름', '연결범위변동으로인한현금의증가', '환율변동효과',
                           '현금및현금성자산의증가', '기초현금및현금성자산', '기말현금및현금성자산']]
        except Exception as e:
            print(e)
            pass

    def _store_cf(self, stock_code, rpt_type, freq):
        """
        [FnGuid] 공시기업의 최근 3개 연간 및 4개 분기 현금흐름표를 수집하는 함수
        :param stock_code: str, 종목코드
        :param rpt_type: str, 재무제표 종류
            'Consolidated' (연결), 'Unconsolidated' (별도)
        :param freq: str, 연간 및 분기보고서
            'A' (연간), 'Q' (분기)
        """

        items_en = ['cfo', 'cfo1', 'cfo2', 'cfo4', 'cfo5', 'cfo6', 'cfo7',
                    'cfi', 'cfi1', 'cfi2', 'cfi3', 'cff', 'cff1', 'cff2', 'cff3',
                    'cff4', 'cff5', 'cff6', 'cff7', 'cff8', 'cff9']

        if rpt_type.upper() == "CONSOLIDATED":
            url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701'
        else:
            url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701'

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36'
        }

        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')

        if freq.upper() == 'A':
            cf_a = soup.find(id = 'divCashY')
            num_col = 3
        else:
            cf_a = soup.find(id = 'divCashQ')
            num_col = 4

        cf_a = cf_a.find_all(['tr'])

        items_kr = [cf_a[m].find(['th']).get_text().replace('\n', '').replace('계산에 참여한 계정 펼치기', '')
                    for m in range(1, len(cf_a))]

        period = [cf_a[0].find_all('th')[n].get_text() for n in range(1, num_col + 1)]

        idx = [1, 2, 3, 4, 39, 70, 75, 76, 84, 85, 99, 113, 121, 122, 134, 145, 153, 154, 155, 156, 157, 158]

        for item, i in zip(items_en, idx):

            temps = []
            for j in range(0, num_col):
                temp = [float(cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '')) \
                            if cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                            else (0 if cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0' \
                                      else 0)]
                temps.append(temp[0])

                globals()[item] = temps

        cf_domestic = pd.DataFrame({
            "stock_Code": stock_code, "period": period,
            "CFO_Total": cfo, "Net_Income_Total": cfo1, "Cont_Biz_Before_Tax": cfo2,
            "Add_Exp_WO_CF_Out": cfo3, "Ded_Rev_WO_CF_In": cfo4, "Chg_Working_Capital": cfo5,
            "CFO": cfo6, "Other_CFO": cfo7,
            "CFI_Total": cfi, "CFI_In": cfi1, "CFI_Out": cfi2, "Other_CFI": cf3,
            "CFF_Total": cff, "CFF_In": cff1, "CFF_Out": cff2, "Other_CFF": cff3,
            "Other_CF": cff4, "Chg_CF_Consolidation": cff5, "Forex_Effect": cff6,
            "Chg_Cash_and_Cash_Equivalents": cff7, "Cash_and_Cash_Equivalents_Beg": cff8,
            "Cash_and_Cash_Equivalents_End": cf9
        })

        cf_domestic['rpt_type'] = rpt_type

        return cf_domestic