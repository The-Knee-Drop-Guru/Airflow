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

    # DELETE 쿼리 실행 (중복 방지를 위해 기존 데이터 삭제)
    delete_query = """
        DELETE FROM public.forecast_table
        WHERE date_time >= %s
        AND date_time < %s;
    """
    hook.run(delete_query, parameters=(execution_date_kst_hour, execution_date_kst_hour_plus_1h))
    print(f"Deleted rows from public.forecast_table where date_time >= {execution_date_kst_hour} and date_time < {execution_date_kst_hour_plus_1h}")

    # 데이터 삽입 쿼리 작성
    insert_query = """
        INSERT INTO public.forecast_table (date_time, real_price, predicted_price)
        VALUES (%s, %s, NULL);
    """
    
    # 데이터 삽입 (executemany 방식 사용)
    data_to_insert = [
        (row['Open Time'], row['Close']) for _, row in df.iterrows()
    ]
    connection = hook.get_conn()
    cursor = connection.cursor()

    try:
        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
        print(f"{len(data_to_insert)} rows successfully inserted into public.forecast_table.")
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        cursor.close()
        connection.close()