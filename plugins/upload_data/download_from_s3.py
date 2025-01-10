from io import StringIO
import pandas as pd
import logging

def download_from_s3(bucket_name, s3_hook, files_to_download):
    # 파일을 읽어서 딕셔너리에 저장
    for file_name, variable_name in files_to_download.items():
        print(f"Streaming {file_name} from S3 bucket {bucket_name}...")

        try:
            # 기존 파일 다운로드
            existing_file = s3_hook.read_key(key=file_name, bucket_name=bucket_name)
            df = pd.read_csv(StringIO(existing_file))
            file_path = f"/tmp/{variable_name}.csv"
            df.to_csv(file_path, index=False)
            logging.info(f"Loaded and saved {file_name} to {file_path}.")

        except Exception as e:
            logging.warning(f"Error loading and saving {file_name}: {e}")

    logging.info(f"Successfully Loaded and saved all files.")