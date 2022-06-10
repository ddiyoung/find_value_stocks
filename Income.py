from conn_db import DBController
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from KrxCode import Krx_code
import sqlalchemy


class Income_stock(Krx_code):
    def __init__(self):
        super().__init__()
        self.conn = DBController()
        self.table = 'is_kr'
        self.is_data = pd.DataFrame()

    def init_save(self):
        for code in self.krx_list.code:
            try:
                consolidated_A_data = self.getIS(code, 'CONSOLIDATED', 'A')
                consolidated_A_data.to_sql(name=self.table, con=self.conn.create_engine(), if_exists='append', index=False,
                                           dtype=None)
                consolidated_Q_data = self.getIS(code, 'CONSOLIDATED', 'Q')
                consolidated_Q_data.to_sql(name=self.table, con=self.conn.create_engine(), if_exists='append', index=False,
                                           dtype=None)
                unconsolidated_A_data = self.getIS(code, 'UNCONSOLIDATED', 'A')
                unconsolidated_A_data.to_sql(name=self.table, con=self.conn.create_engine(), if_exists='append', index=False,
                                             dtype=None)
                unconsolidated_Q_data = self.getIS(code, ' UNCONSOLIDATED', 'Q')
                unconsolidated_Q_data.to_sql(name=self.table, con=self.conn.create_engine(), if_exists='append', index=False,
                                             dtype=None)
            except Exception as e:
                print("fail: " + code)
                print(e)
                pass

    def _get_is(self, stock_code, period):
        sql = f"SELECT * FROM is_kr WHERE stock_code = {stock_code} AND period = '{period}' AND rpt_type = 'Consolidated_Q'"

        try:
            self.is_data = pd.read_sql(
                sql,
                con = self.conn.create_engine()
            )
            self.conn.dispose_engine()

            self.is_data = self.is_data.rename(columns = {
                'Revenue': '매출액',
                'Cost_of_Goods_Sold': '매출원가',
                'Gross_Profit': '매출총이익',
                'Sales_General_Administrative_Exp_Total': '판매비와 관리비',
                'Operating_Profit_Total': '영업이익',
                'Financial_Income_Total': '금융이익',
                'Financial_Costs_Total': '금융원가',
                'Other_Income_Total': '기타수익',
                'Other_Costs_Total': '기타비용',
                'Subsidiaries_JointVentures_PL_Total': '종속기업,공동지배기업및관계기업관련손익',
                'EBIT': '세전계속사업이익',
                'Income_Taxes_Exp': '법인세비용',
                'Profit_Cont_Operation': '계속영업이익',
                'Profit_Discont_Operation': '중단영업이익',
                'Net_Income_Total': '당기순이익'
            })

            self.is_data = self.is_data[['stock_code', 'period', 'rpt_type',
                   '매출액', '매출원가', '매출총이익', '판매비와 관리비',
                   '영업이익', '금융이익', '금융원가', '기타수익', '기타비용', '종속기업,공동지배기업및관계기업관련손익',
                   '세전계속사업이익', '법인세비용', '계속영업이익', '중단영업이익',
                   '당기순이익']]

        except Exception as e:
            print(e)
            pass


    def getIS(self, stock_code, rpt_type, freq):
        """
        [FnGuide] 공시기업의 최근 3개 연간 및 4개 분기 손익계산서를 수집하는 함수

        Parameters
        ==========
        stock_code : str, 종목코드
        rpt_type   : str, 재무제표 종류
            'Consolidated'(연결), 'Unconsolidated'(별도)
        freq       : str, 연간 및 분기보고서
            'A'(연간), 'Q'(분기)
        """

        items_en = ['rev', 'cgs', 'gross', 'sga', 'sga1', 'sga2', 'sga3', 'sga4', 'sga5', 'sga6', 'sga7', 'sga8', 'opr',
                    'opr_',  # 14
                    'fininc', 'fininc1', 'fininc2', 'fininc3', 'fininc4', 'fininc5',  # 6
                    'fininc6', 'fininc7', 'fininc8', 'fininc9', 'fininc10', 'fininc11',  # 6
                    'fincost', 'fincost1', 'fincost2', 'fincost3', 'fincost4', 'fincost5',  # 6
                    'fincost6', 'fincost7', 'fincost8', 'fincost9', 'fincost10',  # 5
                    'otherrev', 'otherrev1', 'otherrev2', 'otherrev3', 'otherrev4', 'otherrev5', 'otherrev6',
                    'otherrev7', 'otherrev8',  # 9
                    'otherrev9', 'otherrev10', 'otherrev11', 'otherrev12', 'otherrev13', 'otherrev14', 'otherrev15',
                    'otherrev16',  # 8
                    'othercost', 'othercost1', 'othercost2', 'othercost3', 'othercost4', 'othercost5',  # 6
                    'othercost6', 'othercost7', 'othercost8', 'othercost9', 'othercost10', 'othercost11', 'othercost12',
                    # 7
                    'otherpl', 'otherpl1', 'otherpl2', 'otherpl3', 'otherpl4',  # 6
                    'ebit', 'tax', 'contop', 'discontop', 'netinc']

        if rpt_type.upper() == 'CONSOLIDATED':
            url = 'https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701'.format(
                stock_code)
            items_en = items_en + ['netinc1', 'netinc2']
        else:
            url = 'https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?' + f'pGB=1&gicode=A{stock_code}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701'

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36'
        }

        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')

        if freq.upper() == 'A':
            is_a = soup.find(id='divSonikY')
            num_col = 3
        else:
            is_a = soup.find(id='divSonikQ')
            num_col = 4

        if (not is_a.find_all(['tr'])):
            print(is_a)

        is_a = is_a.find_all(['tr'])

        items_kr = [is_a[m].find(['th']).get_text().replace('\n', '').replace('계산에 참여한 계정 펼치기', '')
                    for m in range(1, len(is_a))]

        period = [is_a[0].find_all('th')[n].get_text() for n in range(1, num_col + 1)]

        for item, i in zip(items_en, range(1, len(is_a))):

            temps = []
            for j in range(0, num_col):
                temp = [float(is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '')) \
                            if is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                            else (0 if is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') == '-0' \
                                      else 0)]
                temps.append(temp[0])

                globals()[item] = temps

        # 지배 / 비지배 항목 처리
        if rpt_type.upper() == 'CONSOLIDATED':
            pass
        else:
            globals()['netinc1'], globals()['netinc2'] = [np.NaN] * num_col, [np.NaN] * num_col

        is_domestic = pd.DataFrame({'stock_code': stock_code, 'period': period,
                                    'Revenue': rev, 'Cost_of_Goods_Sold': cgs, 'Gross_Profit': gross,
                                    'Sales_General_Administrative_Exp_Total': sga, 'Salaries_and_Wages_Exp': sga1,
                                    'Depreciation_and_Amortization_Exp': sga2, 'Research_Development_Exp': sga3,
                                    'Advertising_and_Promotion_Exp': sga4, 'Selling_Exp': sga5,
                                    'General_Admin_Exp': sga6, 'Other_Costs_Exp': sga7, 'Other_Exp': sga8,
                                    'Operating_Profit_Total': opr, 'Operating_Profit_Total_': opr_,
                                    'Financial_Income_Total': fininc, 'Fin_Interest_Income': fininc1,
                                    'Fin_Dividend_Income': fininc2, 'Fin_Forex_Income': fininc3,
                                    'Fin_Rev_Bad_Debt_Allowance_Income': fininc4, 'Fin_T/R_Income': fininc5,
                                    'Gains_FV_PL': fininc6, 'Gains_Disp_Fin_Assets': fininc7,
                                    'Gains_Val_Fin_Assets': fininc8, 'Rev_Impair_Fin_Assets': fininc9,
                                    'Fin_Derivatives_Income': fininc10, 'Other_Financial_Income': fininc11,
                                    'Financial_Costs_Total': fincost, 'Fin_Interest_Cost': fincost1,
                                    'Fin_Forex_Loss': fincost2, 'Fin_Dep_Bad_Debt_Allowance': fincost3,
                                    'Loss_FV_PL': fincost4, 'Fin_Loss_T/R': fincost5, 'Loss_Disp_Fin_Assets': fincost6,
                                    'Loss_Val_Fin_Assets': fincost7, 'Loss_Impair_Fin_Assets': fincost8,
                                    'Fin_Derivatives_Loss': fincost9, 'Other_Financial_Costs': fincost10,
                                    'Other_Income_Total': otherrev, 'Interest_Income': otherrev1,
                                    'Dividend_Income': otherrev2, 'Forex_Income': otherrev3,
                                    'Rev_Inventory_Obs': otherrev4, 'Gains_Disp_Inventory': otherrev5,
                                    'Gains_FV_OCI': otherrev6, 'Gains_Disp_Assets': otherrev7,
                                    'Gains_Val_Assets': otherrev8, 'Rev_Impair_Assets': otherrev9,
                                    'Derivatives_Income': otherrev10, 'Rent_Income': otherrev11,
                                    'Royalty_Income': otherrev12, 'Fees_Income': otherrev13,
                                    'Rev_Bad_Debt_Allowance_Income': otherrev14,
                                    'Rev_Credit_Allowance_Income': otherrev15, 'Other_Income': otherrev16,
                                    'Other_Costs_Total': othercost, 'Interest_Cost': othercost1,
                                    'Forex_Loss': othercost2,
                                    'Loss_Inventory_Obs': othercost3, 'Loss_Disp_Inventory': othercost4,
                                    'Loss_FV_OCI': othercost5, 'Loss_Disp_Assets': othercost6,
                                    'Loss_Val_Assets': othercost7, 'Loss_Impair_Assets': othercost8,
                                    'Derivatives_Loss': othercost9, 'Bad_Debt_Allowance_Exp': othercost10,
                                    'Credit_Allowance_Exp': othercost11, 'Other_Costs': othercost12,
                                    'Subsidiaries_JointVentures_PL_Total': otherpl, 'Equity_Method_PL': otherpl1,
                                    'Dist_Inv_Sec_PL': otherpl2, 'Allowance_Inv_Sec_PL': otherpl3, 'Other_PL': otherpl4,
                                    'EBIT': ebit, 'Income_Taxes_Exp': tax, 'Profit_Cont_Operation': contop,
                                    'Profit_Discont_Operation': discontop, 'Net_Income_Total': netinc,
                                    'Net_Income_Controlling': globals()['netinc1'],
                                    'Net_Income_Noncontrolling': globals()['netinc2']})

        is_domestic = is_domestic.drop(columns=['Operating_Profit_Total_'])

        is_domestic['rpt_type'] = rpt_type + '_' + freq.upper()

        return is_domestic
