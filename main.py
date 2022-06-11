from conn_db import DBController
from correction_stock import CorrectionStocks
from Balance import Balance_sheet
from Income import Income_stock
from CashFlow import Cash_Flow
from KrxShare import Krx_shares
from trailing import Trailing
from Stock_select import stock_select
import pandas as pd
import numpy as np

def init_db():
    correct_stock = CorrectionStocks()
    income_stock = Income_stock()
    bs_stock = Balance_sheet()
    cf_stock = Cash_Flow()
    shares = Krx_shares()
    correct_stock.init_save("2001-01-01", "2020-06-01")
    print("finish adjust")
    income_stock.init_save()
    print("finish is")
    bs_stock.init_save()
    print("finish bs")
    cf_stock.init_save()
    print("finish cf")
    shares.init_save()
    print("finish krx_share")

def main():
    selector = stock_select()
    correct_stock = CorrectionStocks()
    print(correct_stock._get_ohlcv_data('2022Q1'))
    print("========================== 수정 주가 불러오기 완료 ==========================")
    Trail = Trailing()
    print(Trail._get_all_trailing('2022/03'))
    print("========================== 주식 별 트레일링 완료 ==========================")
    df_mrg = pd.merge(correct_stock.ohlc_df, Trail.trail_df_all, how = 'left', on = ['stock_code', 'period'])

    print(df_mrg)

    print("========================== 주가, 트레일링 데이터 합병 완료 ==========================")

    # 결측치 처리
    df_mrg = df_mrg.dropna()

    df_mrg = df_mrg[df_mrg['매출액'] != 0]
    df_mrg = df_mrg[df_mrg['당기순이익'] != 0]
    df_mrg = df_mrg[df_mrg['영업활동으로인한현금흐름'] != 0]
    df_mrg = df_mrg[df_mrg['영업활동으로인한자산부채변동(운전자본변동)'] != 0]

    """
        Factor 계산하기
    """
    df_factor = pd.DataFrame()
    df_factor['stock_code'] = df_mrg['stock_code']
    df_factor['period'] = df_mrg['period']
    df_factor['시가총액'] = df_mrg['시가총액']
    df_factor['EPS'] = df_mrg['당기순이익'] / df_mrg['상장주식수']
    df_factor['PER'] = df_mrg['close'] / (df_factor['EPS'])
    df_factor['BPS'] = df_mrg['자본'] / df_mrg['상장주식수']
    df_factor['PBR'] = df_mrg['close'] / df_factor['BPS']
    df_factor['SPS'] = df_mrg['매출액'] / df_mrg['상장주식수']
    df_factor['PSR'] = df_mrg['close'] / df_factor['SPS']
    df_factor['CPS'] = df_mrg['영업활동으로인한현금흐름'] / df_mrg['상장주식수']
    df_factor['PCR'] = df_mrg['close'] / df_factor['CPS']
    df_factor['EV'] = df_factor['시가총액'] + df_mrg['부채'] - df_mrg['기말현금및현금성자산']
    df_factor['EVITDA'] = df_mrg['영업활동으로인한자산부채변동(운전자본변동)']
    df_factor['EV/EVITDA'] = df_factor['EV'] / df_factor['EVITDA']
    df_factor['EV/Sales'] = df_factor['EV'] / df_mrg['매출액']
    df_factor['NCAV'] = df_mrg['유동자산'] - df_mrg['부채']
    df_factor['안전마진'] = df_factor['NCAV'] - df_factor['시가총액']
    df_factor['EPS Growth'] = (df_factor['EPS'] - df_factor['EPS'].shift(4)) / abs(df_factor['EPS'].shift(4))
    df_factor['PEG'] = (df_mrg['close'] / df_factor['EPS']) / df_factor['EPS Growth']
    df_factor['Avg Assets'] = (df_mrg['자산'] + df_mrg['자산'].shift(4)) / 2
    df_factor['ROA'] = df_mrg['당기순이익'] / df_factor['Avg Assets']
    df_factor['Avg Equity'] = (df_mrg['자본'] + df_mrg['자본'].shift(4)) / 2
    df_factor['ROE'] = df_mrg['당기순이익'] / df_factor['Avg Equity']
    df_factor['ROE3AVG'] = df_factor['ROE'].rolling(12).mean()
    df_factor['RIM'] = df_factor['BPS'] * df_factor['ROE3AVG'] / 0.1
    df_factor['GP/A'] = df_mrg['매출총이익'] / df_mrg['자산']
    df_factor['Liability/Equity'] = df_mrg['부채'] / df_mrg['자본']


    factor_list = ['PER', 'PBR', 'PSR', 'PCR']

    print(selector._get_select(df_factor, 0.2, 30, factor_list))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
