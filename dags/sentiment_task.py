from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from upload_data.download_from_s3 import download_from_s3  # plugins 디렉토리에서 import
from datetime import datetime, timedelta
import pandas as pd
import requests
import time
import logging
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Fetch data from S3 using the load_from_s3 plugin function
def fetch_data_from_s3(**kwargs):
    ti = kwargs['ti']
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    connection = BaseHook.get_connection('aws_s3')
    extra = connection.extra_dejson
    bucket_name = extra.get('bucket_name')

    # Define files to download and their corresponding variable names
    files_to_download = {
        'raw/news_data.csv': 'news_data',
        'raw/reddit_data.csv': 'reddit_data'
    }

    # Use the load_from_s3 function to download files
    download_from_s3(bucket_name, s3_hook, files_to_download, ti)

    # Push file paths to XCom
    for file_name, variable_name in files_to_download.items():
        file_path = f"/tmp/{variable_name}.csv"
        ti.xcom_push(key=f"{variable_name}_path", value=file_path)


# Preprocess news data
def preprocess_news_data(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='news_data_path', task_ids='fetch_data')
    logging.info(f"Pulled file path from XCom: {file_path}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File does not exist: {file_path}")

    data = pd.read_csv(file_path)

    def combine_text(row):
        title = row["title"] if pd.notna(row["title"]) else ""
        description = row["description"] if pd.notna(row["description"]) else ""
        return f"{title} {description}".strip()

    data["text"] = data.apply(combine_text, axis=1)
    preprocessed_path = '/tmp/preprocessed_news_data.csv'
    data.to_csv(preprocessed_path, index=False)
    ti.xcom_push(key='news_preprocessed_path', value=preprocessed_path)

# Preprocess reddit data
def preprocess_reddit_data(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='reddit_data_path', task_ids='fetch_data')
    data = pd.read_csv(file_path)

    def combine_text(row):
        title = row["title"] if pd.notna(row["title"] )else ""
        description = (
            row["description"]
            if pd.notna(row["description"]) and row["description"] != "본문없음"
            else ""
        )
        comment = row["comment"] if pd.notna(row["comment"]) else ""
        return f"{title} {description} {comment}".strip()

    data["text"] = data.apply(combine_text, axis=1)
    data["text"] = data["text"].apply(lambda x: x[:500] if len(x) > 500 else x)  # 텍스트 길이 제한
    preprocessed_path = '/tmp/preprocessed_reddit_data.csv'
    data.to_csv(preprocessed_path, index=False)
    ti.xcom_push(key='reddit_preprocessed_path', value=preprocessed_path)

# Perform sentiment analysis
def analyze_sentiment(**kwargs):
    ti = kwargs['ti']
    source = kwargs['source']
    file_path = ti.xcom_pull(key=f'{source}_preprocessed_path', task_ids=f'preprocess_{source}_data')
    API_URL = "https://api-inference.huggingface.co/models/mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis"
    api_key = Variable.get("huggingface_api_key")  # API key from Airflow Variables
    headers = {"Authorization": f"Bearer {api_key}"}

    def query(payload):
        response = requests.post(API_URL, headers=headers, json=payload)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API request failed: {response.status_code}, {response.text}")

    data = pd.read_csv(file_path)
    labels = []
    scores = []

    for i, text in enumerate(data["text"]):
        if pd.notna(text):
            print(f"Processing row {i+1}/{len(data['text'])}: {text}")
            output = query({"inputs": text})  # API 요청
            if output and isinstance(output, list) and isinstance(output[0], list):  # 중첩 리스트 처리
                # 가장 높은 score를 가진 label 추출
                top_sentiment = max(output[0], key=lambda x: x["score"])
                labels.append(top_sentiment["label"])
                scores.append(top_sentiment["score"])
            else:
                raise Exception(f"API 응답 형식 오류: {output}")
            time.sleep(5)  # 5초 대기
        else:
            labels.append(None)
            scores.append(None)

    data = data[["title", "text"]]  # 필요한 열만 유지
    data["result"] = labels
    data["score"] = scores
    sentiment_path = f'/tmp/sentiment_{source}_analysis_results.csv'
    data.to_csv(sentiment_path, index=False)
    ti.xcom_push(key=f'{source}_sentiment_path', value=sentiment_path)

# Upload results to S3
def upload_results_to_s3(**kwargs):
    ti = kwargs['ti']
    source = kwargs['source']
    file_path = ti.xcom_pull(key=f'{source}_sentiment_path', task_ids=f'analyze_{source}_sentiment')
    logging.info(f"Starting upload to S3. File: {file_path}")

    try:
        # AWS 연결 정보에서 추가 속성(extra) 가져오기
        connection = BaseHook.get_connection('aws_s3')
        extra = connection.extra_dejson
        bucket_name = extra.get('bucket_name')  # Extra 필드에서 bucket_name 가져오기

        if not bucket_name:
            raise AirflowException("No bucket_name found in connection's extra field.")

        # S3 업로드
        s3_hook = S3Hook(aws_conn_id='aws_s3')
        key = f'sentiment_hist/{source}_sentiment.csv'
        s3_hook.load_file(
            filename=file_path,  # 업로드할 파일
            key=key,
            bucket_name=bucket_name,
            replace=True  # 기존 파일 덮어쓰기
        )
        logging.info(f"File {file_path} successfully uploaded to S3 key: {key}.")
    except Exception as e:
        logging.error(f"Error uploading file {file_path} to S3: {str(e)}")
        raise AirflowException(f"Error uploading file {file_path} to S3: {str(e)}")

# DAG definition
with DAG(
    'sentiment_analysis',
    default_args=default_args,
    description='A DAG for news and reddit sentiment analysis',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_s3
    )

    preprocess_news_task = PythonOperator(
        task_id='preprocess_news_data',
        python_callable=preprocess_news_data
    )

    analyze_news_task = PythonOperator(
        task_id='analyze_news_sentiment',
        python_callable=analyze_sentiment,
        op_kwargs={'source': 'news'}
    )

    upload_news_task = PythonOperator(
        task_id='upload_news_results',
        python_callable=upload_results_to_s3,
        op_kwargs={'source': 'news'}
    )

    delay_task = PythonOperator(
        task_id='delay',
        python_callable=lambda: time.sleep(600),  # 10분 대기
    )

    preprocess_reddit_task = PythonOperator(
        task_id='preprocess_reddit_data',
        python_callable=preprocess_reddit_data
    )

    analyze_reddit_task = PythonOperator(
        task_id='analyze_reddit_sentiment',
        python_callable=analyze_sentiment,
        op_kwargs={'source': 'reddit'}
    )

    upload_reddit_task = PythonOperator(
        task_id='upload_reddit_results',
        python_callable=upload_results_to_s3,
        op_kwargs={'source': 'reddit'}
    )

    fetch_task >> preprocess_news_task >> analyze_news_task >> upload_news_task >> delay_task >> preprocess_reddit_task >> analyze_reddit_task >> upload_reddit_task
