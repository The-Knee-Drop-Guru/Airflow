import joblib
import pandas as pd
import numpy as np
import shap
import logging

def feature_importance(ti):
    
    # data load
    results_shap = joblib.load('/tmp/results_shap.joblib')
    data = pd.read_csv('/tmp/merged_data.csv')
    
    # 모델과 입력 데이터
    model = results_shap['model']
    x_input = results_shap['x_input']  # 현재 예측에 사용된 입력 데이터
    print(x_input.shape)

    # 데이터 차원 축소 (시퀀스 평균 계산)
    x_input_reshaped = x_input.mean(axis=1)  # (samples, features)
    print("x_input_reshaped:", x_input_reshaped.shape)


    # 데이터 차원 확인
    if x_input_reshaped.ndim != 2:
        raise ValueError(f"x_input_reshaped must be 2D (samples, features). Current shape: {x_input_reshaped.shape}")

    # 모델 예측 함수 정의
    def model_predict(data):
        # 데이터가 2차원인 경우, LSTM 입력 형식으로 변환
        reshaped_data = data[:, np.newaxis, :]  # (samples, 1, features)
        return model.predict(reshaped_data).flatten()

    # SHAP KernelExplainer 초기화
    explainer = shap.KernelExplainer(model_predict, shap.sample(x_input_reshaped, 100))  # x_input 일부 샘플링

    # SHAP 값 계산 (축소된 데이터 사용)
    shap_values = explainer.shap_values(x_input_reshaped)

    # 피쳐 컬럼 명시
    feature_columns = [col for col in data.columns if col not in ['BTC_Close', 'Date']]
    
    # SHAP 값의 절대값 계산
    shap_abs_values = np.abs(shap_values).mean(axis=0)  # 각 피처의 평균 SHAP 값(절대값)

    # 피처 중요도의 비율 계산
    feature_importance = shap_abs_values / shap_abs_values.sum() \
    
    # 피처 중요도를 데이터프레임으로 정리
    importance_df = pd.DataFrame({
        'Feature': feature_columns,  # 피처 이름
        'Importance': feature_importance
    }).sort_values(by='Importance', ascending=False)

    # 메모리에 데이터프레임 저장
    OUTPUT_FILE = f"/tmp/shap.csv"  # 로컬 저장 경로
    importance_df.to_csv(OUTPUT_FILE, index=False)
    
    # xcom에 투입
    ti.xcom_push(key="shap", value= OUTPUT_FILE )
    logging.info(f"Data for shap successfully pushed to XCom")
    
    