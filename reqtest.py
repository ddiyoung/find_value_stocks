import requests
import pandas as pd
url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36'
}

data = {
    'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
    'locale': 'ko_KR',
    'tboxisuCd_finder_stkisu0_0': "005930/삼성전자",
    'isuCd': 'KR7005930003',
    'isuCd2': '',
    'codeNmisuCd_finder_stkisu0_0': '삼성전자',
    'param1isuCd_finder_stkisu0_0': 'ALL',
    'strtDd': '20220602',
    'endDd': '20220610',
    'share': '1',
    'money': '1',
    'csvxls_isNo': 'false',
}

req = requests.post(url=url, headers=headers, data=data)

df = pd.read_json(req.text)['output']

df = pd.json_normalize(df)

df['TRD_DD'] = pd.to_datetime(df['TRD_DD'], format = "%Y-%m-%d")

print(len(df))
