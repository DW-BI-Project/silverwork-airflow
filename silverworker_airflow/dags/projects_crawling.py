from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

import logging

import bs4
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta


dag = DAG(
    dag_id='projects_crawling',
    start_date=datetime(2023, 6, 24),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 * * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,)


def ProjectListCrawl(**context):
    import silverwork.project_list_crawl as project_list_crawl

    credentials_dict = Variable.get(
        'silverwokr_google_sheet_access_token', deserialize_json=True)
    processor = project_list_crawl.ProjectDataProcessor(credentials_dict)
    processor.process_data()
    logging.info("Project list crawling ended")


def GsheetToBigquery(**context):
    import silverwork.projects_gsheet_to_bigquery as projects_gsheet_to_bigquery

    credentials_dict = Variable.get(
        'silverwokr_google_sheet_access_token', deserialize_json=True)
    projects_gsheet_to_bigquery.load(credentials_dict)
    logging.info("Data moving gsheet to bigquery ended")


ProjectListCrawl = PythonOperator(
    task_id='ProjectList',
    python_callable=ProjectListCrawl,
    params={
    },
    dag=dag)

GsheetToBigquery = PythonOperator(
    task_id='GsheetToBigquery',
    python_callable=GsheetToBigquery,
    params={
    },
    dag=dag
)

ProjectListCrawl >> GsheetToBigquery