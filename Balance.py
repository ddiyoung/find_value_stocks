from conn_db import DBController
import pandas as pd
import numpy as np
from KrxCode import Krx_code
from urllib.request import urlopen, Request
import sqlalchemy

class Balance_sheet(Krx_code):

    def __init__(self):
        super().__init__()
        self.conn = DBController()
        self.table = 'bs_kr'
        self.bs_data = pd.DataFrame()

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

    def _get_bs(self, stock_code, period):
        sql = f"SELECT * FROM {self.table} WHERE stock_code = {stock_code} AND period = '{period}' AND rpt_type = 'Consolidated_Q'"

        try:
            self.bs_data = pd.read_sql(
                sql,
                con = self.conn.create_engine()
            )
            self.conn.dispose_engine()

            self.bs_data = self.bs_data.rename(columns={
                'Assets_Total': '자산',
                'Current_Assets_Total': '유동자산',
                'LT_Assets_Total': '비유동자산',
                'Other_Fin_Assets': '기타금융업자산',
                'Liabilities_Total': '부채',
                'Current_Liab_Total': '유동부채',
                'LT_Liab_Total': '비유동부채',
                'Other_Fin_Liab_Total': '기타금융업부채',
                'Equity_Total': '자본',
                'Paid_In_Capital': '자본금',
                'Contingent_Convertible_Bonds': '신종자본증권',
                'Capital_Surplus': '자본잉여금',
                'Other_Equity': '기타자본',
                'Accum_Other_Comprehensive_Income': '기타포괄손익누계액',
                'Retained_Earnings': '이익잉여금(결손금)'
            })

            self.bs_data = self.bs_data[['stock_code', 'period', 'rpt_type',
                   '자산', '유동자산', '비유동자산', '기타금융업자산',
                   '부채', '유동부채', '비유동부채', '기타금융업부채',
                   '자본', '자본금', '신종자본증권', '자본잉여금', '기타자본', '기타포괄손익누계액', '이익잉여금(결손금)']]
        except Exception as e:
            print(e)
            pass


    def _store_bs(self, stock_code, rpt_type, freq):
        """
        [FnGuide] 공시기업의 최근 3개 연간 및 4개 분기 재무상태표를 수집하는 함수

        :param stock_code: str, 종목코드
        :param rpt_type: str, 재무제표 종류
            'Consolidated' (연결), 'unconsolidated' (별도)
        :param freq: str, 연간 및 분기보고서
            'A'(연간), 'Q' (분기)
        """

        items_en = ['assets', 'curassets', 'curassets1', 'curassets2', 'curassets3', 'curassets4', 'curassets5', #7
                     'curassets6', 'curassets7', 'curassets8', 'curassets9', 'curassets10', 'curassets11', #8
                     'ltassets', 'ltassets1', 'ltassets2', 'ltassets3', 'ltassets4', 'ltassets5', 'ltassets6', 'ltassets7', #8
                     'ltassets8', 'ltassets9', 'ltassets10', 'ltassets11', 'ltassets12', 'ltassets13', 'finassets', #7
                     'liab', 'curliab', 'curliab1', 'curliab2', 'curliab3', 'curliab4', 'curliab5' #7
                     'curliab6', 'curliab7', 'curliab8', 'curliab9', 'curliab10', 'curliab11', 'curliab12', 'curliab13', #8
                     'ltliab', 'ltliab1', 'ltliab2', 'ltliab3', 'ltliab4', 'ltliab5', 'ltliab6', #7
                     'ltliab7', 'ltliab8', 'ltliab9', 'ltliab10', 'ltliab11', 'ltliab12', 'finliab', #7
                     'equity', 'equity1', 'equity2', 'equity3', 'equity4', 'equity5', 'equity6', 'equity7', 'equity8' ] #9

        if rpt_type.uppder() == 'CONSOLIDATED':
            url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701'

        else:
            url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701'
            items_en = [item for item in items_en if item not in ['equity1', 'equity8']]

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36'
        }

        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')

        if freq.upper() == 'A':
            bs_a = soup.find(id = 'divDaechaY')
            num_col = 3
        else:
            bs_a = soup.find(id = 'divDaechQ')
            num_col = 4

        bs_a = bs_a.find_all(['tr'])

        items_kr = [bs_a[m].find(['th']).get_text().replace('\n', '').replace('계산에 참여한 계정 펼치기', '')
                    for m in range(1, len(bs_a))]

        period = [bs_a[0].find_all('th')[n].get_text() for n in range(1, num_col + 1)]

        for item, i in zip(items_en, range(1, len(bs_a))):

            temps = []
            for j in range(0, num_col):
                temp = [float(bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '')) \
                            if bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                            else (0 if bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0' \
                                      else 0)]
                temps.append(temp[0])

                globals()[item] = temps

        if rpt_type.upper() == 'CONSOLIDATED':
            pass
        else:
            globals()['equity1'], globals()['equity8'] = [np.NaN] * num_col, [np.NaN] * num_col

        bs_domestic = pd.DataFrame({"stock_code": stock_code, "period": period, #2
                                    "Assets_Total": assets, "Current_Assets_Total": curassets,
                                    "Current_Inventory": curassets1, "Current_Bio_Assets": curassets2,
                                    "Current_Fin_Assets": curassets3, "Current_Receivables": curassets4,
                                    "Current_Tax_Assets": curassets5, "Current_Entrusted_Assets": curassets6,
                                    "Current_Returned_Goods_Assets": curassets7, "Current_Emissions_Allowance_Assets": curassets8,
                                    "Other_Current_Assets": curassets9, "Cash_and_Cash_Equivalents": curassets10,
                                    "Current_Assets_On_Assets_For_Sale": curassets11,
                                    "LT_Assets_Total": ltassets, "LT_Tangilble_Assets": ltassets1,
                                    "LT_Intangible_Assets": ltassets2, "LT_Bio_Assets": ltassets3,
                                    "LT_InveCurrent_Real_Estate": ltassets4, "LT_Fin_Assets":ltassets5,
                                    "LT_InveCurrent_Associates": ltassets6, "LT_Receivables": ltassets7,
                                    "LT_Def_Income_Tax_Assets": ltassets8, "LT_Tax_Assets": ltassets9,
                                    "LT_Entrusted_Assets": ltassets10, "LT_Returned_Goods": ltassets11,
                                    "LT_Emissions_Allowance": ltassets12, "Other_LT_Assets": ltassets13,
                                    "Other_Fin_Assets": finassets,
                                    "Liabilities_Total": liab, "Current_Liab_Total": curliab, "Current_Bonds": curliab1,
                                    "Current_Loans": curliab2, "Current_Portion_LT_Liab": curliab3,
                                    "Current_Fin_Liab": curliab4, "Current_Payables": curliab5,
                                    "Current_Emp_Benefits": curliab6, "Current_Provisions": curliab7,
                                    "Current_Tax_Liab": curliab8, "Current_Entrusted_Liab": curliab9,
                                    "Current_Returned_Goods_Liab": curliab10, "Current_Emissions_Allowance_Liab": curliab11,
                                    "Other_Current_Liab": curliab12, "Current_Liab_On_Assets_For_Sale": curliab13,
                                    "LT_Liab_Total": ltliab, "LT_Bonds": ltliab1, "LT_Loans": ltliab2,
                                    "LT_Fin_Liab": ltliab3, "LT_Payables" : ltliab4, "LT_Emp_Benefits": ltliab5,
                                    "LT_Provisions": ltliab6, "LT_Def_Income_Tax_Liab": ltliab7,
                                    "LT_Tax_Liab": ltliab8, "LT_Entrusted_Liab": ltliab9,
                                    "LT_Returned_Goods_Liab": ltliab10, "LT_Emissions_Allowance_Liab": ltliab11,
                                    "Other_LT_Liab": ltliab12, "Other_Fin_Liab_Total": finliab,
                                    "Equity_total": equity, "Controlling_Equity_Total": equity1, "Paid_In_Capital": equity2,
                                    "Contingent_Convertible_Bonds": equity3, "Capital_Surplus": equity4, "Other_Equity": equity5,
                                    "Accum_Other_Comprehensive_Income": equity6, "Retained_Earnings": equity7,
                                    "Non_Controlling_Equity_Total": equity8
        })

        bs_domestic['rpt_type'] = rpt_type + '_' + freq.upper()

        return bs_domestic