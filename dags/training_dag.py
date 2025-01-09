import datetime
import pendulum
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

from train.preprocessing import preprocessing
from train.train_n_predict import train_n_predict
from upload_data.load_from_s3 import load_from_s3
from upload_data.insert_to_db_daily import insert_to_db_daily

LOCAL_TIMEZONE = pendulum.timezone("Asia/Seoul")

# airflow connection으로부터 버킷이름 가져오기
connection = BaseHook.get_connection('aws_s3')
extra = connection.extra_dejson
BUCKE_NAME = extra.get('bucket_name')  # Extra 필드에서 bucket_name 가져오기

if not BUCKE_NAME:
    raise AirflowException("Bucket name not found in Airflow connection's extra field.")
logging.info(f"Connecting to S3. Bucket: {BUCKE_NAME}")

# S3Hook 초기화
S3_HOOK = S3Hook(aws_conn_id='aws_s3')  

# DAG 정의
with DAG(
    dag_id = 'training_dag',
    default_args = {
    'owner': 'airflow',
    'retries': 1,  # retries는 여전히 default_args에서 설정 가능
    },
    description='A DAG to train and make daily prediction',
    schedule_interval=None,
    start_date=datetime.datetime(2023, 10, 1, tzinfo=LOCAL_TIMEZONE),
    catchup=False,
    tags=['training','triggered'],
) as dag:

    # S3 파일 이름과 변수 이름 매핑
    files_to_download = {
        "raw/btc_usdt_data.csv": "btc_usdt",
        "raw/gold.csv": "gold",
        "raw/nasdaq.csv": "nasdaq",
        "raw/s&p500.csv": "sp500",
        "raw/dollar.csv": "dollar",
        "raw/vix.csv": "vix",
    }

    # S3로부터 RAW 데이터 로드 Task
    load_from_s3_task = PythonOperator(
        task_id='load_from_s3',
        python_callable=load_from_s3,
        op_kwargs={
        "bucket_name": BUCKE_NAME,  # 버킷 이름 전달
        "s3_hook": S3_HOOK,     # Hook (S3 connection 전달달)
        "files_to_download": files_to_download,  # 파일 매핑 전달
        },
    )

    # 전처리 Task
    preprocessing_task = PythonOperator(
        task_id='preprocessing',
        python_callable= preprocessing,
    )

    # 훈련 및 예측측 Task
    train_n_predict_task = PythonOperator(
        task_id='train_n_predict',
        python_callable= train_n_predict,
    )

    # RDS에 예측값 로드 Task
    insert_to_db_daily_task = PythonOperator(
        task_id='insert_to_db_daily',
        python_callable= insert_to_db_daily,
    )

    # Task 종속성
    load_from_s3_task >> preprocessing_task >> train_n_predict_task >> insert_to_db_daily_task