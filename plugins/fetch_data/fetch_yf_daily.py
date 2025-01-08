from airflow import DAG
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import logging



def fetch_yf_daily_data(symbol, **kwargs):
    """Fetch incremental data for a given symbol."""
    # 어제 날짜 계산
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")
    logging.info(f"Fetching data for {symbol} from {yesterday} to {today}")
    
    ticker = yf.Ticker(symbol)
    data = ticker.history(start=yesterday, end=today, interval="1d")
    if data.empty:
        logging.info(f"No new data for {symbol}")
        return []
    data.reset_index(inplace=True)
    data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
    # Push the data to XCom
    kwargs['ti'].xcom_push(key=f'{symbol}_daily_data', value=data.to_json(orient='split'))
    logging.info(f"Data for {symbol} successfully pushed to XCom")
