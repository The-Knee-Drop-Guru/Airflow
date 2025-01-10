import joblib
import pandas as pd
import numpy as np
import setuptools.dist
from tensorflow.keras import Sequential
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.losses import MeanSquaredError
from tensorflow.keras.metrics import MeanAbsoluteError
from tensorflow.keras.layers import Dense, LSTM, Dropout
from utils.data_window import DataWindow

def train_n_predict(ti):
    scaled_train = pd.read_csv("/tmp/scaled_train.csv")
    scaled_valid = pd.read_csv("/tmp/scaled_valid.csv")
    scaled_test = pd.read_csv("/tmp/scaled_test.csv")
    scalers = joblib.load('/tmp/scalers.joblib')
 
    lstm_model = Sequential([
        LSTM(100, return_sequences=True),
        Dropout(0.5),  # Dropout 레이어 추가
        LSTM(100),
        Dropout(0.5),  # Dropout 레이어 추가
        Dense(1),
    ])
    
    window = DataWindow(input_width=7, label_width=1, shift=1, train_df=scaled_train, val_df=scaled_valid, test_df=scaled_test, label_columns=['BTC_Close'])

    early_stopping = EarlyStopping(monitor='val_loss', patience=3, mode='min')
    
    lstm_model.compile(loss=MeanSquaredError(), optimizer=Adam(learning_rate=0.001), metrics=[MeanAbsoluteError()])
    
    lstm_model.fit(window.train, epochs=50, validation_data=window.val, callbacks=[early_stopping])    # input, label = window.test
    predict = lstm_model.predict(window.test)
    
    # 로버스트 스케일러로 역변환 적용
    pred_lstm_inv_scaled = scalers['target_scaler'].inverse_transform(np.array([predict[0][0]]).reshape(-1, 1))

    # 로그 변환의 역변환 적용
    pred_lstm_final = np.expm1(pred_lstm_inv_scaled[0][0])
    print(pred_lstm_final)

    ti.xcom_push(key="pred", value= float(pred_lstm_final) )
