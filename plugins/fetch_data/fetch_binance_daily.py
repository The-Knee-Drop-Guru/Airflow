import datetime
import pandas as pd
from binance.client import Client
from airflow.models import Variable  # Variable 임포트

# Binance API 설정, Airflow Variable로 설정된 값 불러오기
api_key = Variable.get("API_KEY")  # 'API_KEY'라는 변수 불러오기
api_secret = Variable.get("API_SECRET")  # 'API_SECRET'라는 변수 불러오기

# Function to fetch Binance data and return it as a pandas DataFrame
def fetch_binance_daily(**kwargs):
    # Set up Binance API client
    client = Client(api_key, api_secret)

    # Define the symbol for BTC/USDT pair
    symbol = 'BTCUSDT'

    # Define custom start and end time
    yesterday_start = (datetime.datetime.now() - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = (datetime.datetime.now() - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)

    # Convert start and end time to string format required by Binance API
    start_str = yesterday_start.strftime('%Y-%m-%d %H:%M:%S')
    end_str = yesterday_end.strftime('%Y-%m-%d %H:%M:%S')

    # Fetch historical kline (candlestick) data from Binance API
    klines = client.get_historical_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1DAY, start_str=start_str, end_str=end_str)

    # Convert the data into a pandas dataframe for easier manipulation
    df_M = pd.DataFrame(klines, columns=[
        'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time',
        'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume',
        'Taker Buy Quote Asset Volume', 'Ignore'
    ])

    # Drop unnecessary columns
    df_M = df_M.drop(columns=['Close Time', 'Ignore'])

    # Convert numerical columns to float
    df_M[['Open', 'High', 'Low', 'Close', 'Volume', 'Quote Asset Volume',
          'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume']] = \
    df_M[['Open', 'High', 'Low', 'Close', 'Volume', 'Quote Asset Volume',
              'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume']].astype(float)
    
    df_M['Open Time'] = pd.to_datetime(df_M['Open Time'], unit='ms')

    # Save dataframe as CSV in memory
    OUTPUT_FILE = "/tmp/btc_usdt_data.csv"  # 로컬 저장 경로
    df_M.to_csv(OUTPUT_FILE, index=False)
    
    # Push the CSV string to XCom
    kwargs['ti'].xcom_push(key='binance_daily', value=OUTPUT_FILE)