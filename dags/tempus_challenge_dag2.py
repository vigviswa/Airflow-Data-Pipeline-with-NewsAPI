"""Simple DAG that uses a few python operators."""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from datetime import datetime, timedelta
import requests
from io import StringIO
import boto3

API_KEY = "70e33a83d22c4baeaa7dcbb044d32695"
ACCESS_KEY = "AKIARMJGF6RK2JFGHXQU"
SECRET_ACCESS_KEY = "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"
BUCKET_NAME = "tempuschallengeaditya"


def fetch_data(resource: str, from_date="2020-09-21"):

    base_url = "http://newsapi.org/v2"
    endpoint_url = f"{base_url}/{resource}?apiKey={API_KEY}"
    if resource == "top-headlines":
        endpoint_url = f"{endpoint_url}&q=any&from={from_date}&sortBy=publishedAt"

    return requests.get(endpoint_url).json()


class ExtractData:
    def __call__(self, **context) -> str:
        """Just Trying something"""
        articles_df = pd.DataFrame(fetch_data("top-headlines")["articles"])
        sources_df = pd.DataFrame(fetch_data("sources")["sources"])
        print(articles_df.head(5))
        print("------")
        print(sources_df.head(5))
        articles_df["source"] = articles_df["source"].apply(lambda x: x["id"])
        print("------")
        print(articles_df.head(5))
        merged_df = articles_df.merge(sources_df, left_on="source", right_on="id")
        headlines_df = merged_df[["source", "title", "description_x"]]
        return headlines_df


class TransformData:
    def __call__(self, **context) -> str:
        """Just Trying something"""

        headlines_df = context["task_instance"].xcom_pull(task_ids="extract_data")
        csv_buffer = StringIO()
        headlines_df.to_csv(csv_buffer, index=False)
        return csv_buffer


class LoadData:
    def __call__(self, **context) -> str:
        """Just Trying something"""
        file_name = f"{datetime.now().date()}_top_headlines.csv"
        csv_buffer = context["task_instance"].xcom_pull(task_ids="transform_data")
        s3_resource = boto3.resource(
            "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY,
        )

        s3_resource.Object(BUCKET_NAME, file_name).put(Body=csv_buffer.getvalue())
        print(f"FileName Uploaded is {file_name}")
        return "SUCCESS!!"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 4, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

# DAG Object
dag = DAG(
    "test_module",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),
    catchup=False,
)

extract_data = PythonOperator(
    task_id="extract_data",
    provide_context=True,
    python_callable=ExtractData(),
    dag=dag,
)

transform_data = PythonOperator(
    task_id="transform_data",
    provide_context=True,
    python_callable=TransformData(),
    dag=dag,
)

load_data = PythonOperator(
    task_id="load_data", provide_context=True, python_callable=LoadData(), dag=dag,
)

extract_data >> transform_data >> load_data
