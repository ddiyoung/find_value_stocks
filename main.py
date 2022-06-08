from conn_db import DBController
from correction_stock import CorrectionStocks
import pandas as pd


def main():
    corrStock = CorrectionStocks()
    table_df = corrStock._get_ohlcv_stock('060310')
    print(table_df)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
