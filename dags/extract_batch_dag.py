import datetime
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from fetch_data.fetch_binance_batch import fetch_binance_batch
from fetch_data.fetch_yf_batch import fetch_yf_batch
from upload_data.upload_to_s3 import upload_to_s3

################################################
### 바이낸스, 야후 파이낸스 API 활용해 가격, 지수 
### 배치 데이터 추출해 S3에 업로드 하는 DAG
### 매일 10시에 실행 
################################################

# 야후 파이낸스 API 추출용 심볼 및 S3 저장 경로 정의
YAHOO_FINANCE_SYMBOLS_N_KEYS = [
    ('^GSPC', 'raw/s&p500.csv'),
    ('^IXIC', 'raw/nasdaq.csv'),
    ('GC=F', 'raw/gold.csv'),
    ('DX-Y.NYB', 'raw/dollar.csv'),
    ('^VIX', 'raw/vix.csv'),
]

# 바이낸스 데이터 추출 후 업로드 시키는 함수
def upload_binance_batch_data(ti):
    upload_to_s3(
        data=ti.xcom_pull(task_ids='binance.fetch_binance_batch', key='binance_batch'),
        s3_key="raw/btc_usdt_data.csv"
    )

# 야후 파이낸스 데이터 추출 후 업로드 시키는 함수
def upload_yahoo_batch_data(ti, yf_symbol, s3_key):
    upload_to_s3(
        data=ti.xcom_pull(
            task_ids=f"yf.fetch_yf_batch_{yf_symbol.replace('^', '').replace('=', '')}",
            key=f'{yf_symbol}_batch'    # 복수의 심볼 사용으로 상단에 정의한 딕셔너리 참조
        ),
        s3_key=s3_key
    )

# DAG 정의
with DAG(
    dag_id = 'extract_batch_dag',
    default_args={
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 12, 23),
    'retries': 1,
    },
    description='A DAG to fetch batch data and upload to S3',
    schedule_interval='0 10 * * *',
    catchup=False,
    tags=['batch', 'S3'],
) as dag:

    # DummyOperator로 DAG의 start와 end 정의
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # 바이낸스 데이터 추출 -> 업로드 Task Group
    @task_group
    def binance_batch_data_task_group():
        fetch_binance_batch_task = PythonOperator(
            task_id='fetch_binance_batch',
            python_callable=fetch_binance_batch,
        )

        upload_binance_batch_task = PythonOperator(
            task_id='upload_binance_batch',
            python_callable=upload_binance_batch_data,
        )

        fetch_binance_batch_task >> upload_binance_batch_task

    # 야후 파이낸스 데이터 추출 -> 업로드 Task Group
    @task_group
    def yahoo_batch_data_task_group():
        for YAHOO_SYMBOL, YAHOO_KEY in YAHOO_FINANCE_SYMBOLS_N_KEYS:
            sanitized_symbol = YAHOO_SYMBOL.replace("^", "").replace("=", "")
            fetch_yahoo_task_id = f'fetch_yf_batch_{sanitized_symbol}'
            upload_yahoo_task_id = f'upload_yf_batch_{sanitized_symbol}'

            fetch_yahoo_batch_task = PythonOperator(
                task_id=fetch_yahoo_task_id,
                python_callable=fetch_yf_batch,
                op_kwargs={'symbol': YAHOO_SYMBOL},
            )

            upload_yahoo_batch_task = PythonOperator(
                task_id=upload_yahoo_task_id,
                python_callable=upload_yahoo_batch_data,
                op_kwargs={'yf_symbol': YAHOO_SYMBOL, 's3_key': YAHOO_KEY},
            )

            fetch_yahoo_batch_task >> upload_yahoo_batch_task

    # 배치 데이터 로드 작업 완료시 training DAG를 Trigger
    trigger_training_task = TriggerDagRunOperator(
        task_id='trigger_training',
        trigger_dag_id='training_dag',
    )

    # 전체 Task Flow
    start >> [binance_batch_data_task_group(), yahoo_batch_data_task_group()] >> trigger_training_task >> end