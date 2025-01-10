import datetime
import pandas as pd
from binance.client import Client
from airflow.models import Variable 

# Binance API 설정, Airflow Variable로 설정된 값 불러오기
api_key = Variable.get("API_KEY") 
api_secret = Variable.get("API_SECRET")  

# 추출하려는 'BTCUSDT' 심볼과, 추출 인터벌 정의
SYMBOL = 'BTCUSDT'
INTERVAL = Client.KLINE_INTERVAL_1DAY

def fetch_binance_batch(**kwargs):
    """바이낸스 API 사용해 'BTCUSDT' 가격 배치 데이터 추출, 
    메모리에 csv 형식으로 저장 후 경로를 XCom에 Push 하는 함수"""

    # 바이낸스 API 클라이언트 셋업
    client = Client(api_key, api_secret)

    # 시작, 어제 날짜 설정
    start_time = datetime.datetime(2018, 1, 1, 0, 0, 0)
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # 바이낸스 API에서 요구하는 시작, 종료 시점을 문자열 형식으로 변환
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = yesterday.strftime('%Y-%m-%d %H:%M:%S')

    # 바이낸스 API 호출
    klines = client.get_historical_klines(symbol=SYMBOL, interval=INTERVAL, start_str=start_str, end_str=end_str)

    # 호출결과를 데이터프레임으로 변환
    df_M = pd.DataFrame(klines, columns=[
        'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time',
        'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume',
        'Taker Buy Quote Asset Volume', 'Ignore'
    ])

    # 불필요 열 제거
    df_M = df_M.drop(columns=['Close Time', 'Ignore'])

    # numerical 데이터 열을 float type으로 변환
    df_M[['Open', 'High', 'Low', 'Close', 'Volume', 'Quote Asset Volume',
          'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume']] = \
    df_M[['Open', 'High', 'Low', 'Close', 'Volume', 'Quote Asset Volume',
              'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume']].astype(float)

    df_M['Open Time'] = pd.to_datetime(df_M['Open Time'], unit='ms')

    # 데이터프레임을 csv 형식으로 메모리에 저장
    OUTPUT_FILE = "/tmp/btc_usdt_data.csv"  # 로컬 저장 경로
    df_M.to_csv(OUTPUT_FILE, index=False)
    
    # csv 저장 경로를 XCom 에 푸시해 이후 Task에서 사용
    kwargs['ti'].xcom_push(key='binance_batch', value=OUTPUT_FILE)