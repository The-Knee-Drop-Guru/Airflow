from datetime import datetime, timedelta
import pendulum
import pandas as pd
from binance.client import Client
from airflow.models import Variable  # Variable 임포트

# Binance API 설정, Airflow Variable로 설정된 값 불러오기
api_key = Variable.get("API_KEY")  # 'API_KEY'라는 변수 불러오기
api_secret = Variable.get("API_SECRET")  # 'API_SECRET'라는 변수 불러오기

# Function to fetch Binance data and return it as a pandas DataFrame
def fetch_binance_hourly(**context):
    # Set up Binance API client
    client = Client(api_key, api_secret)

    # Define the symbol and interval
    symbol = 'BTCUSDT'
    interval = Client.KLINE_INTERVAL_1MINUTE

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

    # Convert start and end time to string format required by Binance API
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

    # Fetch historical kline (candlestick) data from Binance API
    klines = client.get_historical_klines(symbol=symbol, interval=interval, start_str=start_str, end_str=end_str)

    # Convert the data into a pandas dataframe for easier manipulation
    df_M = pd.DataFrame(klines, columns=[
        'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time',
        'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume',
        'Taker Buy Quote Asset Volume', 'Ignore'
    ])

    df_M = df_M[['Open Time', 'Close']]

    # 'Close' 열을 float 타입으로 변환
    df_M['Close'] = df_M['Close'].astype(float)

    df_M['Open Time'] = pd.to_datetime(df_M['Open Time'], unit='ms')

    df_M['Open Time'] = df_M['Open Time'] + timedelta(hours=9)  # UTC에서 9시간 추가하여 KST로 변환

    # Save dataframe as CSV in memory
    OUTPUT_FILE = "/tmp/btc_usdt_hourly_data.csv"  # 로컬 저장 경로
    df_M.to_csv(OUTPUT_FILE, index=False)
    
    # Push the CSV string to XCom
    context['ti'].xcom_push(key='binance_hourly', value=OUTPUT_FILE)