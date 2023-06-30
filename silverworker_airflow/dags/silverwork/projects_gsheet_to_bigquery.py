import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import io

from google.cloud import storage, bigquery
from google.oauth2 import service_account
import csv

import silverwork.google_cloud_manager as GCM


class GoogleSpreadsheetManager:
    def __init__(self, credentials_dict):
        self.sheet_id = '1fLLCZaNY1Tu4J6mA4wwA59ON-KWYVmUE0u_cnCS4o7w'
        self.credentials_dict = credentials_dict
        self.client = GCM.GoogleCloudManager(
            self.credentials_dict).get_gspread_client(self.sheet_id)
        self.spreadsheet = self.client.open('spreadsheet-copy-testing')
        self.worksheet = self.spreadsheet.worksheet('projects')

    def get_data_from_spreadsheet(self):
        values = self.worksheet.get_all_values()
        csv_data = io.StringIO()
        writer = csv.writer(csv_data)
        return writer.writerows(values)


class GoogleCloudStorageManager:
    def __init__(self, credentials_dict):
        self.project_id = "speedy-octane-390814"
        self.bucket_name = "silverwork-bucket"
        self.credentials_dict = credentials_dict
        credentials = service_account.Credentials.from_service_account_info(
            self.credentials_dict)
        self.storage_client = GCM.GoogleCloudManager(
            self.credentials_dict).get_gcstorage_client(self.project_id)

    def export_sheet_to_csv(self, gcs_file_name):
        sheet_id = GoogleSpreadsheetManager(self.credentials_dict).sheet_id
        client = GCM.GoogleCloudManager(
            self.credentials_dict).get_gspread_client(sheet_id)
        spreadsheet = client.open('spreadsheet-copy-testing')
        worksheet = spreadsheet.worksheet('projects')
        values = worksheet.get_all_values()

        # Convert the 2D list to a pandas DataFrame
        df = pd.DataFrame(values)

        # Convert the DataFrame to a CSV-formatted string
        csv_data = df.to_csv(index=False, header=False)

        # Create a GCS client
        storage_client = self.storage_client

        # Get the GCS bucket
        bucket = storage_client.bucket(self.bucket_name)

        # Create a blob (file) within the bucket
        blob = bucket.blob(gcs_file_name)

        # Upload the CSV data to the blob
        blob.upload_from_string(csv_data, content_type='text/csv')

        print(
            f"File uploaded to Google Cloud Storage: gs://{self.bucket_name}/{gcs_file_name}")


class BigQueryManager:
    def __init__(self, credentials_dict):
        self.project_id = "speedy-octane-390814"
        self.dataset_id = 'RAW_DATA'
        self.table_id = 'PROJECTS_TEST'
        self.credentials_dict = credentials_dict
        self.bigquery_client = GCM.GoogleCloudManager(
            self.credentials_dict).get_bigquery_client(self.project_id)

    def execute_query(self, client, query):
        # Execute the query
        query_job = client.query(query)
        # Wait for the query job to complete
        query_job.result()

        # Check if the job has errors
        if query_job.errors:
            raise Exception(f"Job completed with errors: {query_job.errors}")

        print("Query executed successfully.")


def load(credentials_dict):
    # gspread to Google Cloud Storage bucket
    credentials_dict = credentials_dict

    gcs_manager = GoogleCloudStorageManager(credentials_dict)
    storage_client = gcs_manager.storage_client
    gcs_manager.export_sheet_to_csv("porjects_test.csv")

    bigquery_manager = BigQueryManager(credentials_dict)

    bigquery_dataset = bigquery_manager.dataset_id
    bigquery_table = bigquery_manager.table_id

    create_query = f"""
    CREATE OR REPLACE TABLE `{bigquery_dataset}.{bigquery_table}` (
        projType STRING NOT NULL,
        projNo STRING NOT NULL,
        projPlanChangeNo INT64,
        projYear STRING NOT NULL,
        contProjYn STRING NOT NULL,
        contProjStartYear STRING,
        projTypeCd STRING NOT NULL,
        projTypeNm STRING NOT NULL,
        nonBudgYn STRING,
        specProjCd STRING,
        projNm STRING,
        admProvNm STRING,
        admDistCd STRING,
        admDistNm STRING NOT NULL,
        institutionId STRING,
        projStartDd STRING NOT NULL,
        projEndDd STRING NOT NULL,
        planStatusCd STRING,
        targetEmployment INT64 NOT NULL,
        firstlAttachment STRING,
        recentApprovalAttachment STRING,
        delYn STRING NOT NULL,
        sysCreatedAt TIMESTAMP NOT NULL
    )
    """
    bigquery_client = bigquery_manager.bigquery_client
    bigquery_manager.execute_query(bigquery_client, create_query)

    # 구글 클라우드 버킷과 파일 정보
    bucket_name = gcs_manager.bucket_name  # 구글 클라우드 버킷 이름
    file_name = "porjects_test.csv"  # 구글 클라우드 버킷에서 읽을 파일 이름
    dataset_id = bigquery_manager.dataset_id  # 구글 빅쿼리 데이터셋 ID
    table_id = bigquery_manager.table_id  # 구글 빅쿼리 테이블 ID

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(file_name)

    url = f'gs://{bucket_name}/{file_name}'

    df = pd.read_csv(url, storage_options={
                     "token": credentials_dict})

    df = df.astype(str)
    # 스키마를 정의합니다.
    schema = [
        bigquery.SchemaField('projType', 'STRING'),
        bigquery.SchemaField('projNo', 'STRING'),
        bigquery.SchemaField('projPlanChangeNo', 'INT64', mode='NULLABLE'),
        bigquery.SchemaField('projYear', 'STRING'),
        bigquery.SchemaField('contProjYn', 'STRING'),
        bigquery.SchemaField('contProjStartYear', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('projTypeCd', 'STRING'),
        bigquery.SchemaField('projTypeNm', 'STRING'),
        bigquery.SchemaField('nonBudgYn', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('specProjCd', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('projNm', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('admProvNm', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('admDistCd', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('admDistNm', 'STRING'),
        bigquery.SchemaField('institutionId', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('projStartDd', 'STRING'),
        bigquery.SchemaField('projEndDd', 'STRING'),
        bigquery.SchemaField('planStatusCd', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('targetEmployment', 'INT64'),
        bigquery.SchemaField('firstlAttachment', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('recentApprovalAttachment',
                             'STRING', mode='NULLABLE'),
        bigquery.SchemaField('delYn', 'STRING'),
        bigquery.SchemaField('sysCreatedAt', 'TIMESTAMP', mode='NULLABLE'),
    ]

    df['projPlanChangeNo'] = pd.to_numeric(df["projPlanChangeNo"])
    df['targetEmployment'] = pd.to_numeric(df["targetEmployment"])

    df['sysCreatedAt'] = pd.Timestamp.now()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=schema, write_disposition="WRITE_TRUNCATE")
    job = bigquery_client.load_table_from_dataframe(
        df, table_ref, job_config=job_config)

    job.result()

    print(f'Bulk update to BigQuery completed: {table_ref.path}')
