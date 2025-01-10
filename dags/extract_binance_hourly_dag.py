import datetime
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from fetch_data.fetch_binance_hourly import fetch_binance_hourly
from upload_data.upload_to_s3 import upload_to_s3
from upload_data.insert_to_db_hourly import insert_to_db_hourly

LOCAL_TIMEZONE = pendulum.timezone("Asia/Seoul")

def upload_binance_hourly_data(**context):
    # execution_date를 한국 시간으로 변환
    execution_date_kst = pendulum.instance(context['logical_date']).in_timezone('Asia/Seoul')
    print("logical_date_kst: ", execution_date_kst)

    # 시간 및 날짜를 기반으로 s3_key 생성 (한시간 전으로 조정)
    one_hour_ago = execution_date_kst.subtract(hours=1)
    date_str = one_hour_ago.format("YYYYMMDD")
    hour_str = one_hour_ago.format("HH")
    
    # 파일명을 실행 한시간 전 값으로 지정해 S3의 경로를 동적으로 생성
    s3_key = f"btc_price_hist/{date_str}/{hour_str}.csv"

    # S3에 업로드하는 함수 실행
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
    'retries': 1,  
    },
    description='A DAG to fetch binance data hourly and upload to S3',
    schedule_interval='10 * * * *',
    start_date=datetime.datetime(2023, 10, 1, tzinfo=LOCAL_TIMEZONE),
    catchup=False,
    tags=['hourly','production_data', 'S3'],
) as dag:

    # 60분간의 바이낸스 BTC_USDT 가격 데이터를 추출하는 Task
    fetch_binance_hourly_task = PythonOperator(
        task_id='fetch_binance_hourly',
        python_callable=fetch_binance_hourly,
    )

    # Django 웹페이지 그래프에 실가격 표현 목적으로 prod_db에 추출 데이터 insert하는 Task
    insert_to_db_hourly_task = PythonOperator(
    task_id="insert_to_db_hourly",
    python_callable=insert_to_db_hourly,
    )

    # 영구저장 목적의 추출데이터 S3로 업로드하는 Task
    upload_binance_houly_task = PythonOperator(
        task_id='upload_binance_batch',
        python_callable=upload_binance_hourly_data,
    )

    # 전체 Task Flow
    fetch_binance_hourly_task >> insert_to_db_hourly_task >> upload_binance_houly_task