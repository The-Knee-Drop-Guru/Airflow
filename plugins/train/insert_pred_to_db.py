from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import pendulum
from datetime import timedelta

def insert_pred_to_db(**context):

    # 어제 날짜 범위 계산 (하루 단위)
    execution_date_kst = pendulum.instance(context['logical_date']).in_timezone('Asia/Seoul')

    # 어제의 시작 날짜: 어제의 00:00:00
    execution_date_kst_start = (execution_date_kst - timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')

    # 어제의 종료 날짜: 오늘의 00:00:00
    execution_date_kst_end = execution_date_kst.strftime('%Y-%m-%d 00:00:00')

    print(f"Start date: {execution_date_kst_start}, End date: {execution_date_kst_end}")

    # XCom에서 데이터 가져오기
    output_file = context['ti'].xcom_pull(key='model_pred', task_ids='fetch_model_pred')

    # CSV 파일을 DataFrame으로 읽기
    df = pd.read_csv(output_file)

    # PostgreSQL 연결 설정 (Airflow Hook을 사용)
    hook = PostgresHook(postgres_conn_id="prod_db")  # Airflow Connections에서 설정한 conn_id 사용

    # DELETE 쿼리 실행 (어제 데이터 삭제)
    delete_query = """
        DELETE FROM public.forecast_table
        WHERE date_time >= %s
        AND date_time < %s;
    """
    hook.run(delete_query, parameters=(execution_date_kst_start, execution_date_kst_end))
    print(f"Deleted rows where date_time >= {execution_date_kst_start} and date_time < {execution_date_kst_end}")


    # 데이터 삽입 쿼리 작성
    insert_query = """
        INSERT INTO public.forecast_table (date_time, real_price, predicted_price)
        VALUES (%s, %s, %s); 
    """
    # %s 자리표시자

    # 데이터 삽입 (executemany 방식 사용)
    data_to_insert = [
    (row['date_time'], row['real_price'], row['predicted_price']) for _, row in df.iterrows()
    ]

    connection = hook.get_conn()
    cursor = connection.cursor()

    try:
        cursor.executemany(insert_query, data_to_insert) #execute
        connection.commit()
        print(f"{len(data_to_insert)} rows successfully inserted into public.forecast_table.")
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        cursor.close()
        connection.close()