from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import pendulum
from datetime import timedelta

def insert_to_db(**context):
    # execution_date를 한국 시간으로 변환
    execution_date_kst = pendulum.instance(context['logical_date']).in_timezone('Asia/Seoul')
    print("logical_date_kst: ", execution_date_kst)

    # execution_date_kst에서 한 시간 빼기
    execution_date_kst_minus_1h = execution_date_kst.subtract(hours=1)

    # "시" 단위로 자르기
    execution_date_kst_hour = execution_date_kst_minus_1h.strftime('%Y-%m-%d %H:00:00')

    # 1시간 후 시간을 계산
    execution_date_kst_hour_plus_1h = (execution_date_kst_minus_1h + timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00')

    # XCom에서 데이터 가져오기
    output_file = context['ti'].xcom_pull(key='binance_hourly', task_ids='fetch_binance_hourly')
    
    # CSV 파일을 DataFrame으로 읽기
    df = pd.read_csv(output_file)

    # PostgreSQL 연결 설정 (Airflow Hook을 사용)
    hook = PostgresHook(postgres_conn_id="prod_db")  # Airflow Connections에서 설정한 conn_id 사용

    # 데이터 삽입 또는 업데이트 쿼리 작성
    upsert_query = """
        INSERT INTO public.forecast_table (date_time, real_price, predicted_price)
        VALUES (%s, %s, NULL)
        ON CONFLICT (date_time)
        DO UPDATE SET real_price = EXCLUDED.real_price;
    """
    
    # 데이터 준비
    data_to_upsert = [
        (row['Open Time'], row['Close']) for _, row in df.iterrows()
    ]
    connection = hook.get_conn()
    cursor = connection.cursor()

    try:
        cursor.executemany(upsert_query, data_to_upsert)
        connection.commit()
        print(f"{len(data_to_upsert)} rows successfully inserted or updated in public.forecast_table.")
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        cursor.close()
        connection.close()