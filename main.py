from conn_db import DBController
from correction_stock import CorrectionStocks

def main():
    corrStock = CorrectionStocks()
    corrStock.init_save('2019-01-01', '2022-03-31')
    print("OLCHV 저장 끝")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
