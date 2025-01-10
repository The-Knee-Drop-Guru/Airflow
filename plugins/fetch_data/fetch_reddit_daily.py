import praw
import csv
from airflow.models import Variable

def fetch_reddit_daily(ti):
    REDDIT_OUTPUT_FILE = "/tmp/RedditAPI_data.csv"  # 로컬 저장 경로
    print("Reddit 데이터 수집을 시작합니다.")  # 시작 로그

    try:
        # Airflow Variable에서 Reddit API 크레덴셜 가져오기
        client_id = Variable.get("reddit_client_id")
        client_secret = Variable.get("reddit_client_secret")
        user_agent = Variable.get("reddit_user_agent")

        # Task 중단 조건 1: Reddit API 크레덴셜 확인
        if not client_id or not client_secret or not user_agent:
            raise ValueError("Reddit API 크레덴셜이 설정되지 않았습니다. 작업을 중단합니다.")

        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )

        fields = ["title", "description", "url", "comment"]
        with open(REDDIT_OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()

            # Subreddit 초기화
            subreddit = reddit.subreddit("cryptocurrency")
            print("Subreddit 데이터 초기화 성공")  # 초기화 성공 로그

            # Subreddit 데이터 수집
            post_count = 0
            for post in subreddit.new(limit=100):
                post.comments.replace_more(limit=0)
                comments = [comment.body.replace("\n", " ") for comment in post.comments.list()[:5]]
                comments_str = " | ".join(comments) if comments else "댓글 없음"

                writer.writerow({
                    "title": post.title,
                    "description": post.selftext.replace("\n", " ") if post.selftext else "본문 없음",
                    "url": post.url,
                    "comment": comments_str
                })
                post_count += 1

            # Task 중단 조건 2: 수집된 데이터 없음
            if post_count == 0:
                raise ValueError("수집된 Reddit 데이터가 없습니다. 작업을 중단합니다.")

        print(f"Reddit 데이터를 로컬 파일로 저장: {REDDIT_OUTPUT_FILE}")
        ti.xcom_push(key="reddit_daily", value=REDDIT_OUTPUT_FILE)

    except Exception as e:
        # Task 중단 조건 3: 예외 발생 처리
        error_message = f"Reddit 데이터 수집 중 오류 발생: {e}"
        print(error_message)  # 실패 로그
        ti.xcom_push(key="reddit_daily_error", value=error_message)
        raise RuntimeError(error_message)
