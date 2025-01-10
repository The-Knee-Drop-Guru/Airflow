from datetime import timedelta
import pandas as pd
from binance.client import Client
from airflow.models import Variable  

# Binance API 설정, Airflow Variable로 설정된 값 불러오기
api_key = Variable.get("API_KEY") 
api_secret = Variable.get("API_SECRET")  

# 추출하려는 'BTCUSDT' 심볼과, 추출 인터벌 정의
SYMBOL = 'BTCUSDT'
INTERVAL = Client.KLINE_INTERVAL_1MINUTE

def fetch_binance_hourly(**context):
    """바이낸스 API 사용해 매시간 지난 시간의 60분치 'BTCUSDT' 가격 데이터 추출, 
    메모리에 csv 형식으로 저장 후 경로를 XCom에 Push 하는 함수"""

    # 바이낸스 API 클라이언트 셋업
    client = Client(api_key, api_secret)

    logical_date = context["logical_date"]  # Airflow가 제공하는 datetime 객체

    # 1시간 전으로 이동
    start_time = logical_date - timedelta(hours=1)
    # "start_of('hour')" 대체: 시간을 시각적으로 00분 00초로 정리
    start_time = start_time.replace(minute=0, second=0, microsecond=0)

    # "end_of('hour')" 대체: 해당 시간의 마지막 59분 59초로 설정
    end_time = start_time + timedelta(hours=1) - timedelta(seconds=1)

    logical_date_kst = logical_date.in_timezone('Asia/Seoul')
    start_time_kst = start_time.in_timezone('Asia/Seoul')
    end_time_kst = end_time.in_timezone('Asia/Seoul')

    print(f"logical_date_kst: {logical_date_kst}")
    print(f"start_time_kst: {start_time_kst}, end_time_kst: {end_time_kst}")

    # 바이낸스 API에서 요구하는 시작, 종료 시점을 문자열 형식으로 변환
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

    # 바이낸스 API 호출
    klines = client.get_historical_klines(symbol=SYMBOL, interval=INTERVAL, start_str=start_str, end_str=end_str)

    # 호출결과를 데이터프레임으로 변환
    df_M = pd.DataFrame(klines, columns=[
        'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time',
        'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume',
        'Taker Buy Quote Asset Volume', 'Ignore'
    ])

    df_M = df_M[['Open Time', 'Close']]

    # 'Close' 열을 float 타입으로 변환
    df_M['Close'] = df_M['Close'].astype(float)

    df_M['Open Time'] = pd.to_datetime(df_M['Open Time'], unit='ms')

    # UTC에서 9시간 추가하여 KST로 변환
    df_M['Open Time'] = df_M['Open Time'] + timedelta(hours=9)  

    # 데이터프레임을 csv 형식으로 메모리에 저장
    OUTPUT_FILE = "/tmp/btc_usdt_hourly_data.csv"  # 로컬 저장 경로
    df_M.to_csv(OUTPUT_FILE, index=False)
    
    # csv 저장 경로를 XCom 에 푸시해 이후 Task에서 사용
    context['ti'].xcom_push(key='binance_hourly', value=OUTPUT_FILE)