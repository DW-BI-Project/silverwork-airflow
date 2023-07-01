import gspread
from airflow.models import Variable
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage, bigquery
from google.oauth2 import service_account


class GoogleCloudManager:
    def __init__(self, creadential_dict):
        self.creadential_dict = creadential_dict

    def get_gspread_client(self, sheet_id):
        scope = Variable.get("google_scope", deserialize_json=True)
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            self.creadential_dict, scope)
        scope = scope
        sheet_id = sheet_id
        gspread_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}'
        return gspread.authorize(credentials)

    def get_gcstorage_client(self, project_id):
        credentials = service_account.Credentials.from_service_account_info(
            self.creadential_dict)
        return storage.Client(
            credentials=credentials, project=project_id)

    def get_bigquery_client(self, project_id):
        credentials = service_account.Credentials.from_service_account_info(
            self.creadential_dict)
        return bigquery.Client(
            credentials=credentials, project=project_id)
