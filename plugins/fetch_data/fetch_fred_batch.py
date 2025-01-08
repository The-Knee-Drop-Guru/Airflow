import os
import pandas as pd
from datetime import datetime, timedelta
import requests
from airflow.models import Variable
from airflow.exceptions import AirflowException

def fetch_fred_batch(series_id, xcom_key, **kwargs):
    # FRED API 키 가져오기
    api_key = Variable.get("FRED_API_KEY")
    
    # API 요청 매개변수 설정
    url = "https://api.stlouisfed.org/fred/series/observations"
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": "2020-01-01",
        "observation_end": end_date,
    }

    # FRED API에서 데이터 가져오기
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"FRED API 호출 실패: {response.text}")

    data = response.json().get("observations", [])
    if not data:
        raise Exception(f"{series_id} 데이터가 비어 있습니다.")

    # Pandas 데이터프레임으로 변환
    df = pd.DataFrame(data)

    # 데이터 저장
    OUTPUT_FILE = f"/tmp/{series_id}_data.csv"
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"{series_id} 데이터를 파일로 저장: {OUTPUT_FILE}")

    # XCom에 경로 저장
    kwargs['ti'].xcom_push(key=xcom_key, value=OUTPUT_FILE)
    print(f"{series_id} 파일 경로를 XCom에 저장했습니다. 키: {xcom_key}")