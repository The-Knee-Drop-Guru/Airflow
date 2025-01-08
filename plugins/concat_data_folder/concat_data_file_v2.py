from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
from io import StringIO
import pandas as pd
import logging

def concat_data(data, s3_key, ti, symbol='binance'):
    """
    Extract data from S3, concatenate with daily data, and save as CSV.
    """

    try:
        # daily 추출한 데이터 가져오기
        df_new = pd.read_csv(data)
        logging.info(f"Loaded new data for concatenation. New data shape: {df_new.shape}")

        # airflow connection으로부터 버킷이름 가져오기
        connection = BaseHook.get_connection('aws_s3')
        extra = connection.extra_dejson
        bucket_name = extra.get('bucket_name')  # Extra 필드에서 bucket_name 가져오기

        if not bucket_name:
            raise AirflowException("Bucket name not found in Airflow connection's extra field.")
        logging.info(f"Connecting to S3. Bucket: {bucket_name}, Key: {s3_key}")

        # S3Hook 초기화
        s3_hook = S3Hook(aws_conn_id='aws_s3')  # conn id -> 이거 아마 코드 수정해야할꺼임 -> 두번 S3 불러와서 문제 조금 있음

        try:
            # 기존 파일 다운로드
            existing_file = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
            df_existing = pd.read_csv(StringIO(existing_file))
            logging.info(f"Successfully loaded existing data from S3. Existing data shape: {df_existing.shape}")
        except Exception as e:
            logging.warning(f"Failed to load existing data from S3: {str(e)}")
            df_existing = pd.DataFrame()  # Create an empty DataFrame if no existing file is found

        # 기존 데이터와 새로운 데이터 병합
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        logging.info(f"Concatenated data. Combined data shape: {df_combined.shape}")

        # Save dataframe as CSV in memory
        OUTPUT_FILE = f"/tmp/{symbol}_data.csv"  # 로컬 저장 경로
        df_combined.to_csv(OUTPUT_FILE, index=False)
        logging.info(f"Saved concatenated data to local file: {OUTPUT_FILE}")

        if ti:
            ti.xcom_push(key=f'{symbol}_daily', value=OUTPUT_FILE)
            logging.info(f"Pushed concatenated data file path to XCom with key '{symbol}_daily'.")
        else:
            logging.error("TaskInstance (ti) is not passed properly.")
            raise AirflowException("Failed to push XCom value.")
        
    except Exception as e:
        logging.error(f"An error occurred during data concatenation: {str(e)}")
        raise AirflowException("Failed to concatenate data.")