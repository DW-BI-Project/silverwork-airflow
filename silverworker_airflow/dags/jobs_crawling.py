from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime
import pendulum



def JobListCrawl():
    import silverwork.job_list_crawl as job_list_crawl

    credentials_dict = Variable.get(
        'silverwokr_google_sheet_access_token', deserialize_json=True)
    encoding_key = Variable.get('encoding_key')
    crawler = job_list_crawl.JobListCrawler(encoding_key, credentials_dict)
    crawler.crawl()


def JobDetailCrawl():
    import silverwork.job_detail_crawl as job_detail_crawl

    credentials_dict = Variable.get(
        'silverwokr_google_sheet_access_token', deserialize_json=True)
    encoding_key = Variable.get('encoding_key')
    crawler = job_detail_crawl.JobDetailCrawler(encoding_key, credentials_dict)
    crawler.crawl()


def Joining():
    import silverwork.join_list_and_detail as join_list_and_detail

    credentials_dict = Variable.get(
        'silverwokr_google_sheet_access_token', deserialize_json=True)
    manager = join_list_and_detail.GoogleSpreadsheetManager(credentials_dict)
    data = manager.get_data_from_spreadsheet()
    manager.update_spreadsheet(data)


def GsheetToBigquery():
    import silverwork.jobs_gsheet_to_bigquery as gsheet_to_bigquery

    credentials_dict = Variable.get(
        'silverwokr_google_sheet_access_token', deserialize_json=True)
    gsheet_to_bigquery.load(credentials_dict)


dag = DAG(
    dag_id='jobs_crawling',
    schedule='0 9 * * *',  # 매일 자정 실행
    start_date=pendulum.datetime(2023, 6, 28, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
)


JobListCrawl = PythonOperator(
    task_id='job_list',
    python_callable=JobListCrawl,
    params={
    },
    dag=dag
)

JobDetailCrawl = PythonOperator(
    task_id='job_detail',
    python_callable=JobDetailCrawl,
    params={
    },
    dag=dag
)

Joining = PythonOperator(
    task_id='Joining',
    python_callable=Joining,
    params={
    },
    dag=dag
)

GsheetToBigquery = PythonOperator(
    task_id='GtoB',
    python_callable=GsheetToBigquery,
    params={
    },
    dag=dag
)

JobListCrawl >> JobDetailCrawl >> Joining >> GsheetToBigquery
