"""Simple DAG that uses a few python operators."""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import requests
import json
import csv
import os
import boto3
import pandas as pd
from io import StringIO

from datetime import datetime, timedelta


class HelloWorld:

    def __call__(self, **context) -> str:
        """Print and return `Hello, <name>!`."""
        hello = 'Hello, {}!'.format(context['params']['name'])
        print(hello)
        return hello


class ExtractAndFetch:

    def __call__(self, **context) -> str:
        """Just Trying something"""

        success = "Your API data has been successfully parsed into Sources and Headlines"
        return success


class Transform:

    def __call__(self, **context) -> str:
        """Just Trying something"""
        d = datetime.now().time()
        response_news_article = requests.get(
            "http://newsapi.org/v2/top-headlines?q=any&from=2020-09-21&sortBy=publishedAt&apiKey=70e33a83d22c4baeaa7dcbb044d32695")
        response_sources = requests.get('http://newsapi.org/v2/sources?apiKey=70e33a83d22c4baeaa7dcbb044d32695')

        response_json_string = json.dumps(response_news_article.json())

        # # A json object is equivalent to a dictionary in Python
        # # retrieve json objects to a python dict
        response_dict = json.loads(response_json_string)
        df2 = pd.DataFrame(response_dict['articles'])
        df2['source'] = df2['source'].apply(lambda x: x['id'])

        response_json_string = json.dumps(response_sources.json())

        response_dict = json.loads(response_json_string)
        sources_list = response_dict['sources']
        df = pd.DataFrame(sources_list)

        merged = df2.merge(df, left_on='source', right_on='id')
        actual = merged[['source', 'title', 'description_x']]
        csv_buffer = StringIO()

        actual.to_csv(csv_buffer, index=False)

        s3_resource = boto3.resource('s3', aws_access_key_id='AKIARMJGF6RK2JFGHXQU',
                                     aws_secret_access_key='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')
        filename = '{}_top_headlines.csv'.format(d)
        s3_resource.Object('tempuschallengeaditya', filename).put(Body=csv_buffer.getvalue())

        success = "Your API data has been successfully parsed into Sources and Headlines"
        return success


class PutInS3:
    def __call__(self, **context) -> str:
        """Just Trying something"""
        s3_resource = boto3.resource('s3', aws_access_key_id='AKIARMJGF6RK2JFGHXQU',
                                     aws_secret_access_key='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')

        filename = '{}/date60_top_headlines.csv'.format(os.getcwd())
        s3_resource.Bucket('tempuschallengeaditya').upload_file(
            Filename=filename, Key='V5')

        success = "Your API data has been successfully parsed into Sources and Headlines"
        return success


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 4, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG Object
dag = DAG(
    'tempus_challenge_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),  # DAG will run once every 1 minute
    catchup=False,
)

hello_world_task = PythonOperator(
    task_id='hello_world',
    provide_context=True,  # necessary to provide params to python_callable
    python_callable=HelloWorld(),
    params={'name': 'Aditya Shivshankar'},
    dag=dag
)
#
#
# fetch_apidata_task = PythonOperator(
#     task_id='fetch_apidata',
#     provide_context=True,   # necessary to provide params to python_callable
#     python_callable=ExtractAndFetch(),
#     params={'name': 'Aditya'},
#     dag=dag
# )
#
#

#
# putins3_task = PythonOperator(
#     task_id='putins3',
#     provide_context=True,   # necessary to provide params to python_callable
#     python_callable=PutInS3(),
#     params={'name': 'Aditya'},
#     dag=dag
# )

poc_task = PythonOperator(
    task_id='poc_task',
    provide_context=True,   # necessary to provide params to python_callable
    python_callable=Transform(),
    params={'name': 'Aditya'},
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# A visual representation of the following should be viewable at:
# http://localhost:8080/admin/airflow/graph?dag_id=sample_dag
# >> and << operators sets upstream and downstream relationships
# print_date_task is downstream from print_context_task.
# In other words, print_date_task will run after print_context_task

# print_date_task is upstream from end
# In other words, print_date_task will run before end
#
# hello_world_task >> fetch_apidata_task
#
# fetch_apidata_task >> flatten_apidata_task
#
# flatten_apidata_task >> putins3_task

hello_world_task >> poc_task

poc_task >> end

# flatten_apidata_task >> end

