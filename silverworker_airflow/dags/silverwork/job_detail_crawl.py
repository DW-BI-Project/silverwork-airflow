from airflow.models import Variable
import concurrent.futures
import threading
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import silverwork.google_cloud_manager as GCM


lock = threading.Lock()


class JobDetailCrawler:
    def __init__(self, encoding_key, credentials_dict):
        self.idx = 0
        self.num = 0
        self.datas = []
        self.columns_form = ['acptMthdCd', 'age', 'ageLim', 'clerk', 'clerkContt', 'clltPrnnum', 'createDy',
                             'detCnts', 'etcItm', 'frAcptDd', 'homepage', 'jobId', 'lnkStmId', 'organYn',
                             'plDetAddr', 'plbizNm', 'repr', 'stmId', 'toAcptDd', 'updDy', 'wantedAuthNo',
                             'wantedTitle']
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        self.key = encoding_key
        self.sheet_id = Variable.get('sheet_id')
        self.credentials_dict = credentials_dict
        self.client = GCM.GoogleCloudManager(
            self.credentials_dict).get_gspread_client(self.sheet_id)
        self.spreadsheet = self.client.open(Variable.get('spreadsheet_name'))

    def process_api_request(self, url):
        worker_id = threading.get_ident()
        print(f"{worker_id} doing job.........")
        print(f'{self.idx}/{self.total_count}')
        self.idx += 1

        while True:
            try:
                response = requests.get(url)
                house = BeautifulSoup(response.text, 'lxml-xml')
                te = house.find('item')

                if isinstance(te, type(None)):
                    print('api 요청시 SERVICE ERROR 발생... 다시 요청중...')
                    time.sleep(5)
                    continue
                else:
                    break
            except Exception as e:
                print(e)
                continue
            break

        if isinstance(te.acptMthdCd, type(None)):
            acptMthdCd = ''
        else:
            acptMthdCd = te.acptMthdCd.string.strip()
        if isinstance(te.age, type(None)):
            age = ''
        else:
            age = te.age.string.strip()
        if isinstance(te.ageLim, type(None)):
            ageLim = 'N'
        else:
            ageLim = 'Y'
        if isinstance(te.clerk, type(None)):
            clerk = ''
        else:
            clerk = te.clerk.string.strip()
        if isinstance(te.clerkContt, type(None)):
            clerkContt = ''
        else:
            clerkContt = te.clerkContt.string.strip()
        if isinstance(te.clltPrnnum, type(None)):
            clltPrnnum = ''
        else:
            clltPrnnum = te.clltPrnnum.string.strip()
        if isinstance(te.createDy, type(None)):
            createDy = ''
        else:
            createDy = te.createDy.string.strip()
        if isinstance(te.detCnts, type(None)):
            detCnts = ''
        else:
            detCnts = te.detCnts.string.strip()
        if isinstance(te.etcItm, type(None)):
            etcItm = ''
        else:
            etcItm = te.etcItm.string.strip()
        if isinstance(te.frAcptDd, type(None)):
            frAcptDd = ''
        else:
            frAcptDd = te.frAcptDd.string.strip()
        if isinstance(te.homepage, type(None)):
            homepage = ''
        else:
            homepage = te.homepage.string.strip()
        if isinstance(te.jobId, type(None)):
            jobId = ''
        else:
            jobId = te.jobId.string.strip()
        if isinstance(te.lnkStmId, type(None)):
            lnkStmId = ''
        else:
            lnkStmId = te.lnkStmId.string.strip()
        if isinstance(te.organYn, type(None)):
            organYn = ''
        else:
            organYn = te.organYn.string.strip()
        if isinstance(te.plDetAddr, type(None)):
            plDetAddr = ''
        else:
            plDetAddr = te.plDetAddr.string.strip()
        if isinstance(te.plbizNm, type(None)):
            plbizNm = ''
        else:
            plbizNm = te.plbizNm.string.strip()
        if isinstance(te.repr, type(None)):
            repr = ''
        else:
            repr = te.repr.string.strip()
        if isinstance(te.stmId, type(None)):
            stmId = ''
        else:
            stmId = te.stmId.string.strip()
        if isinstance(te.toAcptDd, type(None)):
            toAcptDd = ''
        else:
            toAcptDd = te.toAcptDd.string.strip()
        if isinstance(te.updDy, type(None)):
            updDy = ''
        else:
            updDy = te.updDy.string.strip()
        if isinstance(te.wantedAuthNo, type(None)):
            wantedAuthNo = ''
        else:
            wantedAuthNo = te.wantedAuthNo.string.strip()
        if isinstance(te.wantedTitle, type(None)):
            wantedTitle = ''
        else:
            wantedTitle = te.wantedTitle.string.strip()

        data = [acptMthdCd, age, ageLim, clerk, clerkContt, clltPrnnum, createDy, detCnts, etcItm, frAcptDd,
                homepage, jobId, lnkStmId, organYn, plDetAddr, plbizNm, repr, stmId, toAcptDd, updDy,
                wantedAuthNo, wantedTitle]

        with lock:
            self.datas.append(data)

        print(str(worker_id) + ":::" + response.text[0:100])

    def transform_process(self, df):
        df['age'] = df['age'].astype(str)
        df['clltPrnnum'] = df['clltPrnnum'].astype(str)

    def crawl(self):
        job_list_data = self.spreadsheet.worksheet(
            'job_list_crawl').get_all_values()
        csv = pd.DataFrame(job_list_data[1:], columns=job_list_data[0])
        jobId = csv['jobId']
        jobId_list = jobId.values.tolist()
        self.api_urls = [
            f'http://apis.data.go.kr/B552474/SenuriService/getJobInfo?ServiceKey={self.key}&id={jobId}'
            for jobId in jobId_list
        ]
        self.total_count = len(self.api_urls)
        self.api_urls = self.api_urls[::-1]
        print(self.api_urls[:10])
        futures = [self.executor.submit(
            self.process_api_request, url) for url in self.api_urls]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                print(e)
        with lock:
            if self.datas:
                df = pd.DataFrame(self.datas, columns=self.columns_form)
                self.transform_process(df)
                worksheet = self.spreadsheet.worksheet('job_detail_crawl')
                data = df.values.tolist()
                header = df.columns.tolist()
                values = [header] + data
                worksheet.clear()
                worksheet.update(values)

                updated_data = worksheet.get_all_values()
                print(pd.DataFrame(updated_data))
                print("Completed to update job detail to google spreadsheet")

            else:
                print("결과가 없습니다.")
