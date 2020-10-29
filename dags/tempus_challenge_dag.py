"""Tempus Challenge DAG that fetches all the Top Headlines for all news sources in the NEWS API"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from datetime import datetime, timedelta
import requests
from io import StringIO
import boto3

"""Enter the API Key and AWS Credentials"""

API_KEY = ""
ACCESS_KEY = ""
SECRET_ACCESS_KEY = ""
BUCKET_NAME = ""

"""
    Being a Free API, I am limiting the number of sources to 4, 
    As the API allows only 50 calls in 12 hours
"""
NO_SOURCES = 4

"""For the Free API, It can only fetch requests for the past month"""

CURRENT_DATE = datetime.now().date()
FETCH_FROM_DATE = CURRENT_DATE - timedelta(days=30)


def fetch_data(resource: str, source: str, from_date=FETCH_FROM_DATE):
    """
        :param: resource[str]: Value 'sources' fetches all sources
                               Value 'everything' fetches all top-headlines

                source[str]: Indicates the source to fetch top-headlines for.

        This program fetches the data from the API for a given keyword.


    """

    base_url = "http://newsapi.org/v2"
    endpoint_url = f"{base_url}/{resource}?apiKey={API_KEY}"
    if resource == "everything":
        endpoint_url = (
            f"{endpoint_url}&q=any&from={from_date}&sources={source}&sortBy=publishedAt"
        )

    return requests.get(endpoint_url).json()


class ExtractData:
    def __call__(self, **context) -> str:
        """

            This function extracts the top-headlines for all the sources of the NEWS API.

            It returns a dataframe consisting of all data of all sources, each identified by
             a unique keyword key.

        """
        sources_df = pd.DataFrame(fetch_data("sources", "all")["sources"])
        allsources = sources_df["id"].tolist()
        headlines_df = pd.DataFrame()
        for source in allsources[:NO_SOURCES]:
            keyword_df = pd.DataFrame(fetch_data("everything", source)["articles"])

            if keyword_df.empty or len(keyword_df) == 0:
                """Raising an error if API Key is broken"""

                raise ValueError("The API KEY seems Broken")

            try:
                keyword_df["source"] = keyword_df["source"].apply(lambda x: x["id"])
            except ValueError:
                print("You have tried accessing an invalid source")

            keyword_df.insert(0, "key", source)
            headlines_df = headlines_df.append(
                keyword_df[["source", "title", "description", "key"]]
            )

        return headlines_df, allsources


class TransformData:
    def __call__(self, **context) -> str:
        """

            This function transforms the top-headlines dataframe from the previous task
            and produces a CSV buffer for it to be Loaded in the next step.

            It returns the CSV Buffer, the original dataframe and the list of keywords.

         """

        headlines_df, sources = context["task_instance"].xcom_pull(
            task_ids="extract_data"
        )
        csv_buffer = StringIO()
        return csv_buffer, sources, headlines_df


class LoadData:
    def __call__(self, **context) -> str:
        """

            This function extracts the data for each keyword from the csv buffer
            and writes it to the S3 bucket

        """

        csv_buffer, sources, headlines_df = context["task_instance"].xcom_pull(
            task_ids="transform_data"
        )

        for source in sources:

            keyword_df = headlines_df[headlines_df["key"] == source]
            csv_buffer = StringIO()
            keyword_df.to_csv(csv_buffer, index=False)
            file_name = (
                f"/{str(source)}/{datetime.now().date()}_{source}_top_headlines.csv"
            )
            try:
                s3_resource = boto3.resource(
                    "s3",
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_ACCESS_KEY,
                )

                s3_resource.Object(BUCKET_NAME, file_name).put(
                    Body=csv_buffer.getvalue()
                )
                print(f"FileName Uploaded is {file_name}")

            except RuntimeError:
                print(
                    "The AWS IAM credentials or permissions do not allow writing into S3"
                )

        return "SUCCESS!!"


"""

    Defining the DAG Processes.

"""

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
    "tempus_challenge_dag",
    default_args=default_args,
    schedule_interval="@daily",
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
