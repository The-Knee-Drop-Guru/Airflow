from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

def insert_to_db_daily(ti):
    # XCom에서 데이터 가져오기
    pred = ti.xcom_pull(key='pred', task_ids='train_n_predict')

    # Airflow의 ti['logical_date']가 제공하는 logical_date를 가정
    # 예를 들어 2025-01-08 10:00:00+00:00 (UTC 시간)
    logical_date = pendulum.instance(ti.execution_date)

    # 해당 날짜의 23시 59분 (UTC 기준) 계산
    end_of_day_utc = logical_date.start_of('day').add(hours=23, minutes=59)

    # KST로 변환
    end_of_day_kst = end_of_day_utc.in_timezone('Asia/Seoul')
    end_of_day_kst_naive = end_of_day_kst.replace(tzinfo=None)

    print("Logical Date (UTC):", logical_date)
    print("End of Day (UTC):", end_of_day_utc)
    print("End of Day (KST):", end_of_day_kst)
    print("End of Day (KST Naive):", end_of_day_kst_naive)


    # PostgreSQL 연결 설정 (Airflow Hook을 사용)
    hook = PostgresHook(postgres_conn_id="prod_db")  # Airflow Connections에서 설정한 conn_id 사용

    # DELETE 쿼리 실행 (중복 방지를 위해 기존 데이터 삭제)
    delete_query = """
        DELETE FROM public.forecast_table
        WHERE date_time = %s;
    """
    hook.run(delete_query, parameters=(end_of_day_kst_naive,))
    print(f"Deleted a row from public.forecast_table where date_time = {end_of_day_kst_naive}")

    # 데이터 삽입 쿼리 작성
    insert_query = """
        INSERT INTO public.forecast_table (date_time, real_price, predicted_price)
        VALUES (%s, NULL, %s);
    """ 
    
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    try:
        cursor.execute(insert_query, (end_of_day_kst_naive, pred))
        connection.commit()
        print(f"Successfully inserted {pred} : {end_of_day_kst_naive} in public.forecast_table.")
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        cursor.close()
        connection.close()