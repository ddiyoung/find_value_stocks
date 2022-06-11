

class stock_select:

    def _get_select(self, df_factor, MKTCAP_top, n, factor_list):
        basic_list = ['stock_code', 'period', '시가총액']

        basic_list.extend(factor_list)

        df_select = df_factor.copy()
        df_select = df_select[basic_list]
        df_select['score'] = 0



        # 시가총액 상위 MKTCAP_top% 산출
        # MKTCAP_top = 0.2
        df_select = df_select.sort_values(by=['시가총액'], ascending=False).head(int(len(df_select) * MKTCAP_top))
        df_select = df_select.dropna()

        # 팩터간의 점수 계산
        for i in range(len(factor_list)):
            df_select[factor_list[i] + '_score'] = (df_select[factor_list[i]] - max(df_select[factor_list[i]]))
            df_select[factor_list[i] + '_score'] = df_select[factor_list[i] + '_score'] / min(
                df_select[factor_list[i] + '_score'])

            df_select['score'] += (df_select[factor_list[i] + '_score'] / len(factor_list))

        # 상위 n개 종목 추출
        # n = 30
        df_select = df_select.sort_values(by=['score'], ascending=False).head(n)

        # 종목 선택
        stock_select_list = list(df_select['stock_code'])

        return stock_select_list