from datetime import datetime

import numpy as np
import pandas as pd

from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def extract():
    path = Variable.get('airflow_introduction_path')
    data = pd.read_csv(path + '/tiktok_google_play_reviews.csv')
    data.to_csv('/home/ethereal/airflow/reviews.csv')


def transform():
    df = pd.read_csv('/home/ethereal/airflow/reviews.csv')
    df = df.replace(np.nan, '-')
    df = df.sort_values(by='at')
    df['content'] = df['content'].replace(r'[^A-Za-z0-9\.!()-;:\'\"\,?@ ]+', '', regex=True)
    df.to_csv('/home/ethereal/airflow/update_reviews.csv')


def load():
    data = pd.read_csv('/home/ethereal/airflow/update_reviews.csv')
    data = data.to_dict(orient='records')

    host = Variable.get('mongo_hostname')
    port = int(Variable.get('mongo_port'))

    client = MongoClient(host, port)
    db = client['airflow']
    collection = db['reviews']

    collection.drop()
    collection.insert_many(data)
    client.close()


with DAG('airfow_etl_task', schedule_interval='@once', start_date=datetime(2022, 8, 22, 0, 0)) as dag:
    extract_task = PythonOperator(task_id='extract', python_callable=extract)
    transform_task = PythonOperator(task_id='transform', python_callable=transform)
    load_task = PythonOperator(task_id='load', python_callable=load)
    extract_task >> transform_task >> load_task
