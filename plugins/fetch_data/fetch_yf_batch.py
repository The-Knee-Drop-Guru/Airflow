from airflow import DAG
from datetime import datetime
import yfinance as yf
import pandas as pd
import logging
import csv
import os
import io

def fetch_yf_batch(symbol, **kwargs):
    """Fetch all historical data for a given symbol and save to CSV."""
    logging.info(f"Fetching historical data for {symbol}")
    ticker = yf.Ticker(symbol)
    data = ticker.history(start="2018-01-01", interval="1d")  # 이러면 어제까지 불러옴
    data.reset_index(inplace=True)
    data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
    df = pd.DataFrame(data)

    # Save dataframe as CSV in memory
    OUTPUT_FILE = f"/tmp/{symbol}_data.csv"  # 로컬 저장 경로
    df.to_csv(OUTPUT_FILE, index=False)
    
    # Push the data to XCom
    kwargs['ti'].xcom_push(key=f'{symbol}_batch', value=OUTPUT_FILE)
    logging.info(f"Data for {symbol} successfully pushed to XCom")