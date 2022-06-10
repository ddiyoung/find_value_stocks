from conn_db import DBController
from correction_stock import CorrectionStocks
from Balance import Balance_sheet
from Income import Income_stock
from CashFlow import Cash_Flow
import pandas as pd

def init_db():
    correct_stock = CorrectionStocks()
    income_stock = Income_stock()
    bs_stock = Blance_sheet()
    cf_stock = Cash_Flow()
    correct_stock.init_save("2001-01-01", "2020-06-01")
    income_stock.init_save()
    bs_stock.init_save()
    cf_stock.init_save()

def main():
    init_db()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
