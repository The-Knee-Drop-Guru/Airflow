import requests
import pandas as pd
from airflow.models import Variable

def fetch_news_daily(ti):
    NEWS_OUTPUT_FILE = "/tmp/NewsAPI_data.csv"  # 로컬 저장 경로
    API_KEY = Variable.get("newsapi_api_key")  # Airflow 변수에서 API 키 가져오기
    BASE_URL = "https://newsapi.org/v2/everything"

    print("뉴스 데이터 수집을 시작합니다.")  # 시작 로그 추가

    # API 키 확인 (Task 중단 조건 1)
    if not API_KEY:
        raise ValueError("API 키가 설정되지 않았습니다. 작업을 중단합니다.")  # Task 실패 처리

    try:
        parameters = {
            "q": "BTC",              # 검색어 (비트코인 관련 뉴스)
            "language": "en",        # 언어 설정: 영어 (en)
            "sortBy": "publishedAt", # 최신 뉴스 기준 정렬
            "pageSize": 100,         # 가져올 뉴스 개수
            "apiKey": API_KEY        # API 키
        }

        response = requests.get(BASE_URL, params=parameters)

        # API 호출 실패 처리 (Task 중단 조건 2)
        if response.status_code != 200:
            raise ValueError(f"API 호출 실패: {response.status_code}, {response.text}")

        print("뉴스 API 호출 성공")  # API 호출 성공 로그
        data = response.json()
        articles = data.get("articles", [])

        # 데이터 유효성 검사 (Task 중단 조건 3)
        if not articles:
            raise ValueError("검색된 뉴스 데이터가 없습니다. 작업을 중단합니다.")

        print(f"{len(articles)}개의 뉴스 기사를 수집했습니다.")  # 수집된 기사 수 로그
        news_df = pd.DataFrame(articles)

        # 필요한 컬럼 확인 및 필터링 (Task 중단 조건 4)
        available_columns = ["title", "description", "url", "publishedAt"]
        filtered_columns = [col for col in available_columns if col in news_df.columns]
        if not filtered_columns:
            raise ValueError("필요한 컬럼이 없습니다. 작업을 중단합니다.")

        news_df = news_df[filtered_columns]
        news_df.to_csv(NEWS_OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"뉴스 데이터를 로컬 파일로 저장: {NEWS_OUTPUT_FILE}")  # 파일 저장 성공 로그

        # 파일 경로를 XCom에 저장
        ti.xcom_push(key="news_daily", value=NEWS_OUTPUT_FILE)

    except Exception as e:
        raise RuntimeError(f"뉴스 데이터 수집 중 오류 발생: {e}")  # Task 실패 처리
