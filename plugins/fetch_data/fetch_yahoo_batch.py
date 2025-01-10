import yfinance as yf
import pandas as pd
import logging

def fetch_yahoo_batch(symbol, **kwargs):
    """야후 파이낸스 배치 데이터를 불러와 csv 파일로 메모리에 저장하고, 
    이후 Task에서 사용 할 수 있도록 파일 저장 경로를 XCom에 Push 하는 함수"""
    logging.info(f"Fetching historical data for {symbol}")
    ticker = yf.Ticker(symbol)
    data = ticker.history(start="2018-01-01", interval="1d")  
    data.reset_index(inplace=True)
    data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
    df = pd.DataFrame(data)

    # 야후 파이낸스 배치 데이터 데이터프레임을 csv파일로 메모리에 저장
    OUTPUT_FILE = f"/tmp/{symbol}_data.csv"  
    df.to_csv(OUTPUT_FILE, index=False)
    
    # 저장한 파일 경로를 이후 Task 에서 사용위해 XCom에 Push
    kwargs['ti'].xcom_push(key=f'{symbol}_batch', value=OUTPUT_FILE)
    logging.info(f"Data for {symbol} successfully pushed to XCom")