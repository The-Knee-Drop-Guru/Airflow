import pandas as pd
import numpy as np
import joblib
import pendulum
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

def scale_data(train_data, valid_data, test_data, target='BTC_Close'):
    """
    Train, Validation, Test 데이터에 로그 변환 및 스케일링 적용하는 함수
    """

    # 로그 변환 및 타겟 RobustScaler 스케일링
    for data in [train_data, valid_data, test_data]:
        data.loc[:, target] = np.log1p(data[target])

    target_scaler = RobustScaler()
    scaled_train_target = target_scaler.fit_transform(train_data[[target]])
    scaled_valid_target = target_scaler.transform(valid_data[[target]])
    scaled_test_target = target_scaler.transform(test_data[[target]])

    # Min-Max Scaling 적용 변수
    features = [col for col in train_data.columns if col != target]  # 나머지 변수 minmax만 적용
    feature_scaler = MinMaxScaler()

    # 올바른 순서로 fit 및 transform 적용
    scaled_train_features = feature_scaler.fit_transform(train_data[features])
    scaled_valid_features = feature_scaler.transform(valid_data[features])  # 학습 데이터에서 fit한 스케일러 사용
    scaled_test_features = feature_scaler.transform(test_data[features])   # 학습 데이터에서 fit한 스케일러 사용

    scaled_train = pd.DataFrame(scaled_train_features, columns=features, index=train_data.index)
    scaled_train[target] = scaled_train_target

    scaled_valid = pd.DataFrame(scaled_valid_features, columns=features, index=valid_data.index)
    scaled_valid[target] = scaled_valid_target

    scaled_test = pd.DataFrame(scaled_test_features, columns=features, index=test_data.index)
    scaled_test[target] = scaled_test_target

    return scaled_train, scaled_valid, scaled_test, {
        'minmax_scaler': feature_scaler,
        'target_scaler': target_scaler
    }

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

    # 1월 1일 데이터는 얻을 수 없다. 12월 31일 데이터를 ffill해야하는데 못해서
    merged_data = merged_data.iloc[1:]
    merged_data.reset_index(drop=True, inplace=True)

    merged_data = merged_data.drop(columns=['BTC_Open', 'BTC_High', 'BTC_Low', 'BTC_TBAV', 'BTC_TBQAV'])

    merged_data_added = add_technical_features(merged_data)

    today = pendulum.instance(ti.execution_date).date()

    target_date = today.strftime("%Y-%m-%d")
    ############################
    ###### 데이터셋 나누기 ######     
    ############################
    # print("target_date: ",target_date)
    # start_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=365) + timedelta(days=1)).strftime("%Y-%m-%d")
    
    train_df_start_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=9+365*5-1))
    train_df_end_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=9))
    train_period = (train_df_end_date-train_df_start_date).days + 1
    # print("train_df 시작일: ", train_df_start_date.strftime("%Y-%m-%d"))
    # print("train_df 종료일: ", train_df_end_date.strftime("%Y-%m-%d"))
    train_df = merged_data_added[(merged_data_added['Date'] >= train_df_start_date) & (merged_data_added['Date'] <= train_df_end_date)]

    val_df_start_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=15))
    val_df_end_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=2))
    val_period = (val_df_end_date-val_df_start_date).days + 1
    # print("val_df 시작일: ", val_df_start_date.strftime("%Y-%m-%d"))
    # print("val_df 종료일: ", val_df_end_date.strftime("%Y-%m-%d"))
    val_df = merged_data_added[(merged_data_added['Date'] >= val_df_start_date) & (merged_data_added['Date'] <= val_df_end_date)]

    test_df_start_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=7))
    test_df_end_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=1))
    test_period = (test_df_end_date-test_df_start_date).days + 1
    # print("test_df 시작일: ", test_df_start_date.strftime("%Y-%m-%d"))
    # print("test_df 종료일: ", test_df_end_date.strftime("%Y-%m-%d"))
    test_df = merged_data_added[(merged_data_added['Date'] >= test_df_start_date) & (merged_data_added['Date'] <= test_df_end_date)]

    train_df.drop(columns=['Date'], inplace=True)
    val_df.drop(columns=['Date'], inplace=True)
    test_df.drop(columns=['Date'], inplace=True)
    train_df = train_df.reset_index(drop=True)
    val_df = val_df.reset_index(drop=True)
    test_df = test_df.reset_index(drop=True)
    
    ############################
    ####### 스케일링 실행 #######
    ############################
    scaled_train, scaled_valid, scaled_test, scalers = scale_data(train_df, val_df, test_df)

    scaled_train.to_csv("/tmp/scaled_train.csv", index=False)
    scaled_valid.to_csv("/tmp/scaled_valid.csv", index=False)
    scaled_test.to_csv("/tmp/scaled_test.csv", index=False)
    joblib.dump(scalers, '/tmp/scalers.joblib')
