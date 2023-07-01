from airflow.models import Variable
import requests
import bs4
import time
from datetime import datetime, timedelta
import pandas as pd
import silverwork.google_cloud_manager as GCM


class JobListCrawler:
    def __init__(self, encoding_key, credetial_dict):
        self.encoding_key = encoding_key
        self.credential_dict = credetial_dict
        self.datas = []
        self.emplymShpNm_colums = {
            'CM0101': '정규직',
            'CM0102': '계약직',
            'CM0103': '시간제일자리',
            'CM0104': '일당직',
            'CM0105': '기타'
        }

    def crawl_job_list(self, pageNo, numOfRows):
        url = f'http://apis.data.go.kr/B552474/SenuriService/getJobList?serviceKey={self.encoding_key}&pageNo={pageNo}&numOfRows={numOfRows}'

        while True:
            try:
                res = requests.get(url)
                xml_obj = bs4.BeautifulSoup(res.text, 'lxml-xml')
                rows = xml_obj.findAll('item')

                if isinstance(rows, type(None)) is True:
                    print('api 요청시 SERVICE ERROR 발생... 다시 요청중...')
                    time.sleep(5)
                    continue
                else:
                    print("요청 성공")
                    break
            except Exception as e:
                print(e)
                continue
            break

        yesterday = datetime.today() - timedelta(1)
        testday = yesterday.strftime('%Y%m%d')

        for row in rows:
            if row.frDd.text == testday:
                acptMthd = self.get_value(row.acptMthd)
                deadline = self.get_value(row.deadline)
                emplymShp = self.get_value(row.emplymShp)
                emplymShpNm = self.get_value(row.emplymShpNm)
                frDd = self.get_value(row.frDd)
                jobId = self.get_value(row.jobId)
                jobcls = self.get_value(row.jobcls)
                jobclsNm = self.get_value(row.jobclsNm)
                oranNm = self.get_value(row.oranNm)
                organYn = self.get_value(row.organYn)
                recrtTitle = self.get_value(row.recrtTitle)
                stmId = self.get_value(row.stmId)
                stmNm = self.get_value(row.stmNm)
                toDd = self.get_value(row.toDd)
                workPlc = self.get_value(row.workPlc)
                workPlcNm = self.get_value(row.workPlcNm)

                data = [acptMthd, deadline, emplymShp, emplymShpNm, frDd, jobId, jobcls,
                        jobclsNm, oranNm, organYn, recrtTitle, stmId, stmNm, toDd, workPlc, workPlcNm]
                self.datas.append(data)
            else:
                continue

    def change_emplymShp(self, emplymShpNm):
        emplymShpNm = self.emplymShpNm_colums[emplymShpNm]
        return emplymShpNm

    def change_date_format(self, Dd):
        Dd = str(Dd)
        return f'{Dd[:4]}-{Dd[4:6]}-{Dd[6:]}'

    def drop_duplicated_data(self, df):
        df = df.drop_duplicates(['jobId'], keep='first')

        return df

    def transform_process(self, df):
        df['emplymShpNm'] = df['emplymShpNm'].apply(
            lambda x: self.change_emplymShp(x))
        df['frDd'] = df['frDd'].apply(
            lambda x: self.change_date_format(x))
        df['toDd'] = df['toDd'].apply(
            lambda x: self.change_date_format(x))
        df['workPlc'] = df['workPlc'].astype(str)
        df = self.drop_duplicated_data(df)

        return df

    def to_gspread(self, df):

        df = self.transform_process(df)
        # Google 스프레드시트에 접근하기 위한 인증 설정

        manager = GCM.GoogleCloudManager(self.credential_dict)
        sheet_id = Variable.get('sheet_id')
        client = manager.get_gspread_client(sheet_id)

        # Google 스프레드시트 문서 열기
        spreadsheet = client.open(Variable.get('spreadsheet_name'))

        # DataFrame을 Google 스프레드시트로 보내기
        worksheet = spreadsheet.worksheet('job_list_crawl')

        # DataFrame을 2차원 리스트로 변환
        data = df.values.tolist()

        # DataFrame의 열 이름을 2차원 리스트로 변환
        header = df.columns.tolist()

        # 데이터와 열 이름을 함께 보내기 위해 2차원 리스트 연결
        values = [header] + data

        # 데이터 업데이트
        worksheet.clear()  # 기존 데이터 삭제
        worksheet.update(values)  # 새로운 데이터 업데이트

        updated_data = worksheet.get_all_values()

        print(pd.DataFrame(updated_data))

        print("Completed to update job list to google spreadsheet")

    @staticmethod
    def get_value(attribute):
        if attribute is None:
            return ''
        else:
            return attribute.string.strip()

    def crawl(self):

        pageNo = 1
        numOfRows = 300

        self.crawl_job_list(pageNo, numOfRows)

        df = pd.DataFrame(self.datas, columns=['acptMthd', 'deadline', 'emplymShp', 'emplymShpNm', 'frDd', 'jobId',
                                               'jobcls', 'jobclsNm', 'oranNm', 'organYn', 'recrtTitle', 'stmId', 'stmNm',
                                               'toDd', 'workPlc', 'workPlcNm'])

        self.to_gspread(df)
