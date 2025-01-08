from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from io import StringIO
import json

def fetch_data_from_s3(**kwargs):
    """
    Task 1: S3에서 데이터를 가져옵니다.
    """
    try:
        s3_hook = S3Hook(aws_conn_id='aws_s3')
        connection = BaseHook.get_connection('aws_s3')
        extra = connection.extra_dejson
        bucket_name = extra.get('bucket_name')  # Extra 필드에서 bucket_name 가져오기

        # 파일 키
        news_key = 'sentiment_hist/news_sentiment.csv'
        reddit_key = 'sentiment_hist/reddit_sentiment.csv'

        # S3에서 news_sentiment.csv 가져오기
        news_object = s3_hook.get_key(news_key, bucket_name)
        news_data = news_object.get()['Body'].read().decode('utf-8')

        # S3에서 reddit_sentiment.csv 가져오기
        reddit_object = s3_hook.get_key(reddit_key, bucket_name)
        reddit_data = reddit_object.get()['Body'].read().decode('utf-8')

        # XCom에 데이터 저장
        ti = kwargs['ti']
        ti.xcom_push(key='news_data', value=news_data)
        ti.xcom_push(key='reddit_data', value=reddit_data)
        print("S3 데이터를 성공적으로 가져왔습니다.")

    except Exception as e:
        print(f"fetch_data_from_s3 에러: {e}")
        raise

def merge_and_transform_data(**kwargs):
    """
    Task 2: 데이터를 합치고 변환합니다.
    """
    try:
        # XCom에서 데이터 가져오기
        ti = kwargs['ti']
        news_data = ti.xcom_pull(key='news_data', task_ids='fetch_data_from_s3')
        reddit_data = ti.xcom_pull(key='reddit_data', task_ids='fetch_data_from_s3')

        # S3에서 가져온 데이터를 DataFrame으로 변환
        news_df = pd.read_csv(StringIO(news_data))
        reddit_df = pd.read_csv(StringIO(reddit_data))

        # 클래스 ID 추가
        news_df['class_id'] = 'news'
        reddit_df['class_id'] = 'reddit'

        # 두 데이터를 합치기
        combined_df = pd.concat([news_df, reddit_df])

        # 인덱스 재설정
        combined_df.reset_index(drop=True, inplace=True)

        # 컬럼명 매핑
        combined_df = combined_df.rename(
            columns={
                'title': 'title',
                'result': 'sentiment_value'
            }
        )

        # XCom에 변환된 데이터 저장
        ti.xcom_push(key='combined_data', value=combined_df.to_json(orient='records'))
        print("데이터를 성공적으로 합치고 변환했습니다.")

    except Exception as e:
        print(f"merge_and_transform_data 에러: {e}")
        raise


def insert_data_into_postgresql(**kwargs):
    """
    Task 3: 기존 데이터를 삭제하고 새로운 데이터를 PostgreSQL에 삽입합니다.
    """
    try:
        # XCom에서 데이터 가져오기
        ti = kwargs['ti']
        combined_data = ti.xcom_pull(key='combined_data', task_ids='merge_and_transform_data')
        combined_df = pd.read_json(combined_data)

        postgres_hook = PostgresHook(postgres_conn_id='prod_db')

        # 삭제 쿼리 작성 (기존 데이터를 삭제)
        delete_query = """
        DELETE FROM public.sentiment_table
        WHERE class_id = %s;
        """

        # 삭제할 클래스 ID 리스트
        class_ids_to_delete = combined_df['class_id'].unique()

        # 삽입 쿼리 작성
        insert_query = """
        INSERT INTO public.sentiment_table (class_id, title, sentiment_value) 
        VALUES (%s, %s, %s);
        """

        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cur:
                # 기존 데이터 삭제
                for class_id in class_ids_to_delete:
                    print(f"Deleting rows for class_id: {class_id}")
                    cur.execute(delete_query, (class_id,))

                # 새로운 데이터 삽입
                for _, row in combined_df.iterrows():
                    print(f"Inserting row: {row['class_id']}, {row['title']}, {row['sentiment_value']}")
                    cur.execute(insert_query, (row['class_id'], row['title'], row['sentiment_value']))

                conn.commit()
        print("기존 데이터를 삭제하고 새로운 데이터를 PostgreSQL에 성공적으로 삽입했습니다.")

    except Exception as e:
        print(f"insert_data_into_postgresql 에러: {e}")
        raise


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id='sentiment_s3_to_postgresql',
    default_args=default_args,
    description='Load sentiment data from S3 to PostgreSQL using multiple tasks',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    task_fetch_data = PythonOperator(
        task_id='fetch_data_from_s3',
        python_callable=fetch_data_from_s3,
        provide_context=True
    )

    task_merge_data = PythonOperator(
        task_id='merge_and_transform_data',
        python_callable=merge_and_transform_data,
        provide_context=True
    )

    task_insert_data = PythonOperator(
        task_id='insert_data_into_postgresql',
        python_callable=insert_data_into_postgresql,
        provide_context=True
    )

    task_fetch_data >> task_merge_data >> task_insert_data
