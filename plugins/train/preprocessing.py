import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from sklearn.preprocessing import MinMaxScaler, RobustScaler

def delete_columns(df, columns):
    df.drop(columns=columns, inplace=True)
    return df

def add_technical_features(data, sma_window=20, rsi_period=14):
    # Simple Moving Average (SMA)
    data['SMA'] = data['BTC_Close'].rolling(window=sma_window).mean() #20일

    # 볼린저 밴드 (상단, 하단)
    rolling_std = data['BTC_Close'].rolling(window=sma_window).std()
    data['Bollinger Upper Band'] = data['SMA'] + (rolling_std * 2)
    data['Bollinger Lower Band'] = data['SMA'] - (rolling_std * 2)

    # 상대 강도 지수 (RSI)
    delta = data['BTC_Close'].diff(1) # 간격 1간 차이값
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=rsi_period).mean()
    avg_loss = pd.Series(loss).rolling(window=rsi_period).mean()
    rs = avg_gain / avg_loss
    data['RSI'] = 100 - (100 / (1 + rs))

    # 2020년 부터 데이터 넣기
    data = data[data['Date'].dt.year > 2018] # 2012 ~ 2021년
    return data

def preprocessing(ti):
    # dict 통해 각 데이터 저장
    btc_usdt_df = pd.read_csv("/tmp/btc_usdt.csv")
    gold_df = pd.read_csv("/tmp/gold.csv")
    nasdaq_df = pd.read_csv("/tmp/nasdaq.csv")
    sp500_df = pd.read_csv("/tmp/sp500.csv")
    dollar_df = pd.read_csv("/tmp/dollar.csv")
    vix_df = pd.read_csv("/tmp/vix.csv")

    ## yf 컬럼 삭제
    dfs = [gold_df, nasdaq_df, sp500_df, dollar_df, vix_df]
    columns_drop = ['Open', 'High', 'Low', 'Volume', 'Dividends', 'Stock Splits'] # volumne data는 평일에만 존재하여 주말 data의 null값 대처 불가
    dfs = [delete_columns(df, columns_drop) for df in dfs]

    # 컬럼 이름 변경
    btc_usdt_df.rename(columns={'Open Time': 'Date', 'Open': 'BTC_Open', 'High':'BTC_High',
                                'Low':'BTC_Low','Close':'BTC_Close','Volume':'BTC_Volume',
                                'Quote Asset Volume':'BTC_QAV','Number of Trades':'BTC_NoT', # Quote Asset Volume은 전체 거래량 × 평균 가격
                                'Taker Buy Base Asset Volume':'BTC_TBAV','Taker Buy Quote Asset Volume':'BTC_TBQAV'}, inplace=True) # 시장가 주문자가 매수한 BTC 총량 × 매수 시점의 가격

    gold_df.rename(columns={'Close': 'Gold_Close'}, inplace=True)
    nasdaq_df.rename(columns={'Close': 'Nasdaq_Close'}, inplace=True)
    sp500_df.rename(columns={'Close': 'SP500_Close'}, inplace=True)
    dollar_df.rename(columns={'Close': 'Dollar_Close'}, inplace=True)
    vix_df.rename(columns={'Close': 'VIX_Close'}, inplace=True)

    ## 데이터 병합
    merged_data = pd.merge(btc_usdt_df, nasdaq_df, on='Date', how='left')
    merged_data = pd.merge(merged_data, sp500_df, on='Date', how='left')
    merged_data = pd.merge(merged_data, gold_df, on='Date', how='left')
    merged_data = pd.merge(merged_data, dollar_df, on='Date', how='left')
    merged_data = pd.merge(merged_data, vix_df, on='Date', how='left')

    # Date열 Datetime type으로 변경
    merged_data['Date'] = pd.to_datetime(merged_data['Date'])

    # null 값 이전 데이터 값으로 fill
    merged_data = merged_data.fillna(method='ffill')

    # 상관분석 및 다중공선성 해결을 위한 변수 제거 
    merged_data = merged_data.drop(columns=['BTC_Open', 'BTC_High', 'BTC_Low', 'BTC_TBAV', 'BTC_TBQAV'])
    
    # 보조지표 추가 
    merged_data_added = add_technical_features(merged_data)
    merged_data.reset_index(drop=True, inplace=True)

    # 최종 데이터 파일 저장
    merged_data.to_csv("/tmp/merged_data.csv", index=False)
