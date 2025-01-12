import pandas as pd
import numpy as np
import logging
from sklearn.preprocessing import MinMaxScaler, RobustScaler
from time import time
import joblib
from tensorflow.keras import Sequential
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.losses import MeanSquaredError
from tensorflow.keras.metrics import MeanAbsoluteError
from tensorflow.keras.layers import Dense, LSTM, Dropout

# Train, Valid, Test Split    
def data_split(org_data):
    
    # 날짜, 데이터 분리 
    date = org_data.iloc[0:,0]
    data = org_data.iloc[0:,1:] # 'Date' 컬럼 제외

    # 예측 타겟 컬럼 및 변수 컬럼 설정
    target_column = "BTC_Close"
    feature_columns = [col for col in data.columns if col != target_column]

    # Train end index
    train_end_index = 1461 # 여기 수정

    pred_size = 1  # LSTM의 시계열 길이
    lookback = 10
    valid_size = 10  # 검증 데이터 크기

    current_train_end = train_end_index
    iterations = []
    sliding = 0

    for current_train_end in range(train_end_index, len(data) - pred_size - valid_size, pred_size):
        # 훈련 데이터 20 ~ 23년까지
        train_data = data.iloc[sliding:current_train_end]  # [0:1461] -> 1430 ~ 1459 -> 1460 예측

        # 검증 데이터 (10개)
        valid_data_start = current_train_end  # 1461
        valid_data_end = valid_data_start + valid_size  # 1471
        valid_data = data.iloc[valid_data_start - lookback + 1 : valid_data_end+1]  # [1452:1472]  -> 1452 ~ 1461 -> 1462 예측 , 1461~1470 -> 1471예측 // 10개

        # 테스트 데이터 (1일)
        test_data_start = valid_data_end # 1471
        test_data_end = test_data_start + pred_size  # 1472
        test_data = data.iloc[test_data_start - lookback + 1: test_data_end+1]  # [1462:1473] -> 1462~1471 -> 1472

        # Iteration 저장
        iterations.append((train_data.copy(), valid_data.copy(), test_data.copy()))


        # 슬라이딩 적용
        sliding += pred_size

    if not iterations:
        raise ValueError("The data_split function returned an empty list. Check the splitting logic.")
    
    return iterations



def scale_data(train_data, valid_data, test_data, target='BTC_Close'):
    """
    Train, Validation, Test 데이터에 로그 변환 및 스케일링을 적용합니다.
    원본 데이터를 다시 불러와서 동일한 스케일링을 진행하면 둘이 같다.
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

    scalers = {
        'minmax_scaler': feature_scaler,
        'target_scaler': target_scaler
    }

    return scaled_train, scaled_valid, scaled_test, scalers

# lstm 안에 넣을 데이터 생성
def create_lstm_data(data, target, lookback):

    # 타겟 열 제외한 피처 자동 설정
    features = [col for col in data.columns if col != target]

    x, y = [], []
    for i in range(lookback, len(data)):
        x.append(data[features].iloc[i - lookback:i].values)  # 시퀀스 데이터
        y.append(data[target].iloc[i])  # 타깃 값
    return np.array(x), np.array(y)

# LSTM 모델
def lstm_model(input_shape):
    model = Sequential()
    model.add(LSTM(100, return_sequences=True, input_shape=input_shape))
    model.add(Dropout(0.5))
    model.add(LSTM(100))
    model.add(Dropout(0.5))
    model.add(Dense(1))
    model.compile(optimizer=Adam(learning_rate=0.001), loss='mae')
    return model

# 모델 학습 함수
def run_modeling(x_train, y_train, x_valid, y_valid, x_input, scalers, target='BTC_Close', lookback=10, epochs=30, batch_size=32):

    # x_train 데이터가 비었는지 확인
    if len(x_train) == 0:
        raise ValueError("x_train is empty. Check your data preprocessing or parameters.")

    # 모델 생성
    input_shape = (x_train.shape[1], x_train.shape[2])
    model = lstm_model(input_shape)

    # EarlyStopping 설정
    early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

    # 모델 학습
    start_time = time()
    history = model.fit(
        x_train, y_train,
        validation_data=(x_valid, y_valid),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[early_stopping],
        verbose=1
    )
    end_time = time()

    # 테스트 데이터 예측 및 복원
    y_pred = model.predict(x_input)

    # Step 1: RobustScaler 복원
    y_pred_scaled_back = scalers['target_scaler'].inverse_transform(y_pred.reshape(-1, 1)).flatten()
    #y_test_scaled_back = scalers['target_scaler'].inverse_transform(y_test.reshape(-1, 1)).flatten()

    # Step 2: 로그 복원
    y_pred_actual = np.expm1(y_pred_scaled_back)  # 로그 변환된 값을 원래 값으로 복원
    #y_test_actual = np.expm1(y_test_scaled_back)

    # 결과 저장
    results = {
        'history': history.history,
        'y_pred': y_pred_actual,
        #'y_test': y_test_actual,
        'time_elapsed': end_time - start_time,
        'model': model,  # 학습된 모델 반환
        'x_train': x_train,
        'x_input': x_input,
        'scalers': scalers  # scalers 반환
    }
    return results

# 학습이 이루어져야함 
def predict_next_day(ti, target='BTC_Close', lookback=10):
    # 이 안에서 마지막 데이터에 대해 학습이 이루어져야해 
    
    # 데이터 로드 
    data = pd.read_csv('/tmp/merged_data.csv')
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')  # 변환 실패 시 NaT 처리
    if data['Date'].isna().sum() > 0:
        raise ValueError("Date column contains invalid or missing entries after conversion.")

    train_size = 1461 
    valid_size = 10

    # 마지막 lookback 길이만큼 데이터 준비
    last_data = data.iloc[-(lookback + valid_size + train_size):] # -1481
    print("Last data length:", len(last_data))

    # data split
    try:
        last_data_split = data_split(last_data)
        if not last_data_split:
            raise ValueError("Data split returned an empty list. Check your data and split logic.")
    except Exception as e:
        raise RuntimeError(f"Error during data split: {e}")

    # data split
    last_data_split = data_split(last_data)
    train_data = last_data_split[-1][0]
    valid_data = last_data_split[-1][1]
    input_data = last_data_split[-1][2] # 마지막 10일 데이터  

    # data scaling 
    scaled_train, scaled_valid, scaled_input, scalers = scale_data(train_data, valid_data, input_data, target=target) # 다 되었고 

    # data transform 
    x_train, y_train = create_lstm_data(scaled_train, target=target, lookback=lookback)
    x_valid, y_valid = create_lstm_data(scaled_valid, target=target, lookback=lookback)

    x_input= []
    
    feature_columns = [col for col in last_data.columns if col not in ['BTC_Close', 'Date']]
    
    for i in range(lookback, len(scaled_input)+1):
        x_input.append(scaled_input[feature_columns].iloc[i - lookback:i].values)  # 시퀀스 데이터
    x_input = np.array(x_input)
    
    # 모델 지속적 학습
    results = run_modeling(x_train, y_train, x_valid, y_valid, x_input, scalers, target='BTC_Close', lookback=10, epochs=30, batch_size=32)
    
    # 피쳐 중요도를 위한 데이터 저장
    results_shap = {
    'model': results['model'],  # 학습된 LSTM 모델
    'x_input': results['x_input']  # 다음날 예측에 사용되는 입력 데이터
    }   
    joblib.dump(results_shap, '/tmp/results_shap.joblib')


    # 다음날 예측값
    y_pred = results['y_pred'][-1]
    
    # xcom에 예측값 저장 
    OUTPUT_FILE = f"/tmp/pred.joblib"  # 로컬 저장 경로
    ti.xcom_push(key="pred", value= float(y_pred)) 

# next_day = predict_next_day(all_results[-1]['scalers'], merged_data, all_results[-1]['model'], target='BTC_Close', lookback=10)
# next_day # 날짜 표시
