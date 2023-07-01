import pandas as pd
from airflow.models import Variable
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import silverwork.google_cloud_manager as GCM


class GoogleSpreadsheetManager:
    def __init__(self, credentials_dict):
        self.scope = Variable.get("google_scope", deserialize_json=True)
        self.credentials_dict = credentials_dict
        self.sheet_id = Variable.get('sheet_id')
        self.client = GCM.GoogleCloudManager(
            self.credentials_dict).get_gspread_client(self.sheet_id)
        self.spreadsheet = self.client.open(Variable.get('spreadsheet_name'))
        self.worksheet = self.spreadsheet.worksheet(
            Variable.get('jobs_worksheet_name'))

    def get_data_from_spreadsheet(self):
        job_list_data = self.spreadsheet.worksheet(
            'job_list_crawl').get_all_values()
        job_detail_data = self.spreadsheet.worksheet(
            'job_detail_crawl').get_all_values()

        df1 = pd.DataFrame(job_list_data[1:], columns=job_list_data[0])
        df2 = pd.DataFrame(job_detail_data[1:], columns=job_detail_data[0])

        df1 = df1[['acptMthd', 'deadline', 'emplymShp', 'emplymShpNm', 'frDd', 'oranNm',
                   'recrtTitle', 'stmNm', 'toDd', 'workPlc', 'jobId', 'jobcls', 'jobclsNm']]

        df = pd.merge(df1, df2, on='jobId', how='inner')

        df.rename(columns={'frDd': 'startDd', 'ageLim': 'ageYn',
                           'createDy': 'createDt', 'updDy': 'updDt', 'toDd': 'endDd'}, inplace=True)

        column_list = ['acptMthd', 'deadline', 'emplymShp', 'emplymShpNm', 'startDd', 'jobId', 'jobcls', 'jobclsNm', 'oranNm', 'organYn', 'recrtTitle', 'stmId', 'stmNm',
                       'endDd', 'workPlc', 'acptMthdCd', 'age', 'ageYn', 'clerk', 'clerkContt', 'clltPrnnum', 'createDt', 'detCnts', 'etcItm', 'homepage', 'plDetAddr', 'plbizNm', 'updDt']

        df = df[column_list]
        df = df.fillna('')

        return df

    def drop_duplicated_data(self, df):
        df = df.drop_duplicates(['jobId'], keep='first')

        return df

    def update_spreadsheet(self, data):
        existing_data = self.worksheet.get_all_values()
        current_row = len(existing_data)

        if current_row == 0:
            header = data.columns.tolist()
            values = [header] + data.values.tolist()
        else:
            print(f"업데이트 전 데이터 수: {current_row}")
            df_gs = pd.DataFrame(existing_data[1:], columns=existing_data[0])
            df_gs = df_gs.append(data)
            df_gs = self.drop_duplicated_data(df_gs)
            header = df_gs.columns.tolist()
            values = [header] + df_gs.values.tolist()
            print(f"업데이트 후 데이터 수: {len(values)}")

        self.worksheet.clear()
        self.worksheet.update(values)
        print("Completed to update jobs to google spreadsheet!")
