from Balance import Balance_sheet
from Income import Income_stock
from CashFlow import Cash_Flow
from KrxCode import Krx_code
import pandas as pd

class Trailing(Krx_code):
    def __init__(self):
        super().__init__()
        self.is_kr = Income_stock()
        self.bs_kr = Balance_sheet()
        self.cf_kr = Cash_Flow()
        self.trail_df_all = pd.DataFrame()

    def _get_all_trailing(self, period):
        krx_list = self.krx_list

        for code in krx_list.code:
            #print("트레일링 시작 ||  Code : " + code)
            trail_df = self._get_trailing(code, period)
            self.trail_df_all = pd.concat([self.trail_df_all, trail_df])

        return self.trail_df_all

    def _get_trailing(self, stock_code, period):
        df_quat = {}
        period_quat = period

        for i in range(4):
            self.is_kr._get_is(stock_code, period_quat)
            self.bs_kr._get_bs(stock_code, period_quat)
            self.cf_kr._get_cf(stock_code, period_quat)

            df_is = self.is_kr.is_data
            df_bs = self.bs_kr.bs_data
            df_cf = self.cf_kr.cf_data

            df_merge = pd.merge(df_is, df_bs, how='left', on=['stock_code', 'period', 'rpt_type'])
            df_merge = pd.merge(df_merge, df_cf, how='left', on=['stock_code', 'period', 'rpt_type'])

            df_quat[i] = df_merge

            if int(period_quat[5:7]) - 3 > 0:
                period_quat = period_quat[0:4] + '/' + str(int(period_quat[5:7]) - 3).zfill(2)

            else:
                period_quat = str(int(period_quat[0:4]) - 1) + '/12'

        df_trailing = pd.DataFrame(df_quat[0], columns=['stock_code', 'period', 'rpt_type'])
        df_trailing[df_quat[0].columns[3:]] = 0

        for i in range(len(df_quat)):
            df_trailing[df_quat[0].columns[3:]] += df_quat[i][df_quat[i].columns[3:]]

        return df_trailing