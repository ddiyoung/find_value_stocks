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

        bs_domestic = pd.DataFrame({
            
        })