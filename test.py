from conn_db import DBController
from correction_stock import CorrectionStocks
from Income import Income_stock
import pandas as pd


def main():
    incomeStock = Income_stock()
    incomeStock.init_save()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
