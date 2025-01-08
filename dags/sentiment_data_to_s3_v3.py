# dags/sentiment_data_to_s3_V2.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from upload_data.upload_to_s3 import upload_to_s3
from fetch_data.fetch_news_daily import fetch_news_daily
from fetch_data.fetch_reddit_daily import fetch_reddit_daily

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 23),  # 시작 날짜 설정
    'retries': 1,
}

with DAG(
    'fetch_sentiment_data',
    default_args=default_args,
    schedule_interval=None,  # 테스트용
    # schedule_interval='@daily',  # 매일 자정에 실행
    catchup=False,
) as dag:

    # 뉴스 데이터를 로컬에 수집 및 저장
    fetch_news_daily_task = PythonOperator(
        task_id='fetch_news_daily',
        python_callable=fetch_news_daily,
    )

    # 로컬 뉴스 데이터를 S3로 업로드
    upload_news_daily_task = PythonOperator(
        task_id='upload_news_daily',
        python_callable=lambda ti: upload_to_s3(
            data = ti.xcom_pull(task_ids='fetch_news_daily', key='news_daily'),
            s3_key = "raw/news_data.csv"
        ),
    )

    # Reddit 데이터를 로컬에 수집 및 저장
    fetch_reddit_daily_task = PythonOperator(
        task_id='fetch_reddit_daily',
        python_callable=fetch_reddit_daily,
    )

    # 로컬 Reddit 데이터를 S3로 업로드
    upload_reddit_daily_task = PythonOperator(
        task_id='upload_reddit_daily',
        python_callable=lambda ti: upload_to_s3(
            data = ti.xcom_pull(task_ids='fetch_reddit_daily', key='reddit_daily'),
            s3_key = "raw/reddit_data.csv"
        ),
    )

    # 작업 순서 지정
    fetch_news_daily_task >> upload_news_daily_task >> fetch_reddit_daily_task >> upload_reddit_daily_task
