from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
import logging

def upload_to_s3(data, s3_key):
    """
    Upload all of the files to S3
    """
    logging.info(f"Starting upload to S3. File: {data}, S3 Key: {s3_key}")

    s3_hook = S3Hook(aws_conn_id='aws_s3')  # conn id

    try:
        # AWS 연결 정보에서 추가 속성(extra) 가져오기
        connection = BaseHook.get_connection('aws_s3')
        extra = connection.extra_dejson
        bucket_name = extra.get('bucket_name')  # Extra 필드에서 bucket_name 가져오기

        if not bucket_name:
            raise AirflowException("No bucket_name found in connection's extra field.")

        # 파일 업로드
        s3_hook.load_file(
            filename=data,  # dag에서 전달받음
            key=s3_key,
            bucket_name=bucket_name,
            replace=True  # 기존 파일 덮어쓰기
        )
        logging.info(f"File {data} successfully uploaded to {s3_key}.")
    except Exception as e:
        logging.error(f"Error uploading file {data} to S3: {str(e)}")
        raise AirflowException(f"Error uploading file {data} to S3: {str(e)}")
