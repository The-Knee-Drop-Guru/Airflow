import time
import datetime
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from fetch_data.fetch_binance_batch import fetch_binance_batch
from fetch_data.fetch_yf_batch import fetch_yf_batch
from upload_data.upload_to_s3 import upload_to_s3

# 심볼과 S3 키 정의
yf_symbols_and_keys = [
    ('^GSPC', 'raw/s&p500.csv'),
    ('^IXIC', 'raw/nasdaq.csv'),
    ('GC=F', 'raw/gold.csv'),
    ('DX-Y.NYB', 'raw/dollar.csv'),
    ('^VIX', 'raw/vix.csv'),
]

fred_symbols_and_keys = [
    ("SOFR", "raw/sofr_data.csv"),
    ("DTB3", "raw/tbill_data.csv"),
]

# DAG 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 12, 23),
    'retries': 1,
}

def upload_yf_task(ti, yf_symbol, s3_key):
    upload_to_s3(
        data=ti.xcom_pull(
            task_ids=f"yf.fetch_yf_batch_{yf_symbol.replace('^', '').replace('=', '')}",
            key=f'{yf_symbol}_batch'
        ),
        s3_key=s3_key
    )

def upload_binance_task(ti):
    upload_to_s3(
        data=ti.xcom_pull(task_ids='binance.fetch_binance_batch', key='binance_batch'),
        s3_key="raw/btc_usdt_data.csv"
    )

# DAG 정의
with DAG(
    dag_id = 'extract_batch_dag',
    default_args=default_args,
    description='A DAG to fetch batch data and upload to S3',
    schedule_interval='0 10 * * *',
    catchup=False,
    tags=['batch', 'S3'],
) as dag:

    # DummyOperator로 start와 end 정의
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    @task_group
    def binance():
        # 바이낸스 BTC_USDT 가격 데이터 추출 Task
        fetch_binance_batch_task = PythonOperator(
            task_id='fetch_binance_batch',
            python_callable=fetch_binance_batch,
        )

        # 바이낸스 BTC_USDT 가격 데이터 S3에 업로드 Task
        upload_binance_batch_task = PythonOperator(
            task_id='upload_binance_batch',
            python_callable=upload_binance_task,
        )

        fetch_binance_batch_task >> upload_binance_batch_task

    @task_group
    def yf():
        for yf_symbol, yf_s3_key in yf_symbols_and_keys:
            sanitized_symbol = yf_symbol.replace("^", "").replace("=", "")
            fetch_yf_task_id = f'fetch_yf_batch_{sanitized_symbol}'
            upload_yf_task_id = f'upload_yf_batch_{sanitized_symbol}'

            fetch_yf_batch_task = PythonOperator(
                task_id=fetch_yf_task_id,
                python_callable=fetch_yf_batch,
                op_kwargs={'symbol': yf_symbol},
            )

            upload_yf_batch_task = PythonOperator(
                task_id=upload_yf_task_id,
                python_callable=upload_yf_task,
                op_kwargs={'yf_symbol': yf_symbol, 's3_key': yf_s3_key},
            )

            fetch_yf_batch_task >> upload_yf_batch_task

    trigger_training_task = TriggerDagRunOperator(
        task_id='trigger_training',
        trigger_dag_id='training_dag',  # 트리거할 DAG ID
    )

    # 전체 Task 종속성
    start >> [binance(), yf()] >> trigger_training_task >> end