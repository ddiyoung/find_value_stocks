from conn_db import DBController
from correction_stock import CorrectionStocks
from Balance import Balance_sheet
import pandas as pd


def main():
    bs = Balance_sheet()
    print(bs.krx_list)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
