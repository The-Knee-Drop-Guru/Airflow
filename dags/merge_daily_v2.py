import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from fetch_data.fetch_binance_daily import fetch_binance_daily
from upload_data.upload_to_s3 import upload_to_s3
from concat_data_folder.concat_data_file import concat_data

# DAG 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 12, 23),
    'retries': 1,
}

with DAG(
    dag_id = 'btc_usdt_daily',
    default_args=default_args,
    description='A DAG to fetch daily data and upload to S3',
    schedule_interval='0 10 * * *',  # 매일 10시에 실행
    catchup=False,
) as dag:

    # 바이낸스 BTC_USDT daily 가격 데이터 추출 Task
    fetch_binance_daily_task = PythonOperator(
        task_id='fetch_binance_daily',
        python_callable=fetch_binance_daily,
    )

    # S3 데이터 불러와 daily 데이터와 concat Task
    concat_binance_daily_task = PythonOperator(
        task_id='concat_binance_data',
        python_callable = lambda ti: concat_data(
                    data=ti.xcom_pull(task_ids='fetch_binance_daily',  key='binance_daily'),
                    s3_key="raw/btc_usdt_data.csv",
                    ti=ti  # ti 객체를 명시적으로 전달
            )
    )

    # 바이낸스 BTC_USDT daily 가격 데이터 S3에 업로드 Task
    upload_binance_daily_task = PythonOperator(
        task_id='upload_binance_daily',
        python_callable = lambda ti: upload_to_s3(
                    data=ti.xcom_pull(task_ids='concat_binance_data',  key='binance_daily'),
                    s3_key="raw/btc_usdt_data.csv"
            ),
    )

    # 바이낸스 데이터 추출 -> 업로드 작업 종속성
    fetch_binance_daily_task >> concat_binance_daily_task >> upload_binance_daily_task