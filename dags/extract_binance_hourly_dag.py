import time
import datetime
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from fetch_data.fetch_binance_hourly import fetch_binance_hourly
from upload_data.upload_to_s3 import upload_to_s3
from upload_data.insert_to_db import insert_to_db

local_timezone = pendulum.timezone("Asia/Seoul")

def upload_binance_hourly_task(**context):
    # execution_date를 한국 시간으로 변환
    execution_date_kst = pendulum.instance(context['logical_date']).in_timezone('Asia/Seoul')
    print("logical_date_kst: ", execution_date_kst)
    # 시간 및 날짜를 기반으로 s3_key 생성 (한시간 전으로 조정)
    one_hour_ago = execution_date_kst.subtract(hours=1)
    date_str = one_hour_ago.format("YYYYMMDD")
    hour_str = one_hour_ago.format("HH")
    
    # S3의 경로를 동적으로 생성
    s3_key = f"btc_price_hist/{date_str}/{hour_str}.csv"

    upload_to_s3(
        data=context['ti'].xcom_pull(task_ids='fetch_binance_hourly', key='binance_hourly'),
        s3_key=s3_key
    )
    print(f"uploaded to:", s3_key)

# DAG 정의
with DAG(
    dag_id = 'extract_binance_hourly_dag',
    default_args = {
    'owner': 'airflow',
    'retries': 1,  # retries는 여전히 default_args에서 설정 가능
    },
    description='A DAG to fetch binance data hourly and upload to S3',
    schedule_interval='10 * * * *',
    start_date=datetime.datetime(2023, 10, 1, tzinfo=local_timezone),
    catchup=False,
    tags=['hourly','production_data', 'S3'],
) as dag:

    # 바이낸스 BTC_USDT 가격 데이터 추출 Task
    fetch_binance_hourly_task = PythonOperator(
        task_id='fetch_binance_hourly',
        python_callable=fetch_binance_hourly,
    )

    # prod_db 에 insert
    insert_to_db_task = PythonOperator(
    task_id="insert_to_db",
    python_callable=insert_to_db,
    )

    # 바이낸스 BTC_USDT 가격 데이터 S3에 업로드 Task
    upload_binance_houly_task = PythonOperator(
        task_id='upload_binance_batch',
        python_callable=upload_binance_hourly_task,
    )

    # Task 종속성
    fetch_binance_hourly_task >> insert_to_db_task >> upload_binance_houly_task