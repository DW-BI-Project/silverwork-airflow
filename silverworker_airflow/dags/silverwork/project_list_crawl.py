import pandas as pd
import numpy as np
import requests
import json
import silverwork.api_url_crawling as ApiUrlCrawling
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import bs4
import xml.etree.ElementTree as ET
import time


class ProjectDataProcessor:
    def __init__(self, credentials_dict):
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]
        self.credentials_dict = credentials_dict
        self.sheet_id = '1fLLCZaNY1Tu4J6mA4wwA59ON-KWYVmUE0u_cnCS4o7w'
        self.gspread_url = 'https://docs.google.com/spreadsheets/d/{sheet_id}'
        self.columns = {
            '사업유형': 'projType',
            '사업번호': 'projNo',
            '사업계획변경순번': 'projPlanChangeNo',
            '사업년도': 'projYear',
            '계속사업여부': 'contProjYn',
            '계속사업시작년도': 'contProjStartYear',
            '사업유형코드': 'projTypeCd',
            '사업유형이름': 'projTypeNm',
            '비예산여부': 'nonBudgYn',
            '특수사업명코드': 'specProjCd',
            '사업명': 'projNm',
            '관할시도명': 'admProvNm',
            '시군구코드': 'admDistCd',
            '관할시군구': 'admDistNm',
            '기관아이디': 'institutionId',
            '사업기간시작일': 'projStartDd',
            '사업기간종료일': 'projEndDd',
            '사업계획서상태코드': 'planStatusCd',
            '목표일자리수': 'targetEmployment',
            '최초등록첨부파일': 'firstlAttachment',
            '최근승인첨부파일': 'recentApprovalAttachment',
            '삭제여부': 'delYn',
        }
        self.get_planStatusCd_info = {
            '102PC': '승인완료',
            '102AA': '임시',
            '102RJ': '반려',
            '102DR': '삭제요청',
            '102CR': '변경심사요청',
            '102CA': '조건부승인',
            '102AR': '심사요청'
        }
        self.adm_info = self.get_adm_info()
        self.common_code_info = self.csv_to_dataframe(
            sheet_name="한국노인인력개발원_취업연계_공통상세코드")
        self.proj_type_code_info = self.csv_to_dataframe(
            sheet_name="한국노인인력개발원_사업유형코드")

    def get_adm_info(self):
        df = pd.DataFrame()
        for sig_cd in range(1, 6):
            url = 'http://api.vworld.kr/req/data'
            params = {
                'request': 'GetFeature',
                'key': '9FBC48F7-7677-368C-B8FB-E846BF6AECE3',
                'size': 1000,
                'data': 'LT_C_ADSIGG_INFO',
                'attrfilter': f'sig_cd:like:{sig_cd}',
                'columns': 'sig_cd,full_nm,sig_kor_nm',
                'geometry': 'false'
            }

            response = requests.get(url, params=params)
            content = response.content
            # byte decode
            content = content.decode('utf-8-sig')
            # dictionary로 변경
            content = json.loads(content)
            features = content.get('response', {}).get('result', {}).get(
                'featureCollection', {}).get('features', [])

            result = []
            for feature in features:
                properties = feature.get('properties', {})
                result.append({
                    'adm_dist_cd': '{0:0<10}'.format(properties.get('sig_cd')),
                    'adm_prov_nm': properties.get('full_nm', '').split(' ')[0],
                    'adm_dist_nm': properties.get('sig_kor_nm')
                })
            # dictionary를 dataframe으로 변경
            adm_info = pd.DataFrame.from_dict(result)
            df = pd.concat([df, adm_info], axis=0)

        df.reset_index(drop=True, inplace=True)
        return df

    def find_adm_info(self, df, adm_dist_nm):
        if adm_dist_nm is not None:
            # 해당 문자열로 시작하는 경우 필터링
            result = df[df['adm_dist_nm'].str.startswith(
                adm_dist_nm, na=False)]
            if not result.empty:
                result = list(result.itertuples(index=False, name=None))
                if len(result) == 1:
                    return result[0]
                elif len(result) > 1 and adm_dist_nm.endswith('시'):
                    adm_dist_cd = result[0][0]
                    adm_dist_cd = adm_dist_cd[:4] + '0' + adm_dist_cd[5:]
                    adm_prov_nm = result[0][1].split(' ')[0]
                    return adm_dist_cd, adm_prov_nm, adm_dist_nm
        return None, None, adm_dist_nm

    def csv_to_dataframe(self, sheet_name):
        try:
            # Google 스프레드시트에 접근하기 위한 인증 설정
            scope = self.scope
            credentials_dict = self.credentials_dict
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                credentials_dict, scope)
            sheet_id = self.sheet_id
            gspread_url = self.gspread_url
            client = gspread.authorize(credentials)

            # Google 스프레드시트 문서 열기
            spreadsheet = client.open('spreadsheet-copy-testing')

            data = spreadsheet.worksheet(sheet_name).get_all_values()
            return pd.DataFrame(data[1:], columns=data[0])

        except Exception as e:
            print(repr(e))
            return pd.DataFrame()

    def to_gspread(self, scope, credentials_dict, sheet_id, gspread_url, sheet_name, df):
        try:
            # Google 스프레드시트에 접근하기 위한 인증 설정
            scope = scope
            credentials_dict = credentials_dict
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                credentials_dict, scope)
            sheet_id = sheet_id
            gspread_url = gspread_url
            client = gspread.authorize(credentials)

            # Google 스프레드시트 문서 열기
            spreadsheet = client.open('spreadsheet-copy-testing')

            worksheet = spreadsheet.worksheet(sheet_name)

            # DataFrame을 2차원 리스트로 변환
            data = df.values.tolist()

            # DataFrame의 열 이름을 2차원 리스트로 변환
            header = df.columns.tolist()

            # 데이터와 열 이름을 함께 보내기 위해 2차원 리스트 연결
            values = [header] + data

            # 데이터 업데이트
            worksheet.clear()  # 기존 데이터 삭제
            worksheet.update(values)  # 새로운 데이터 업데이트

        except Exception as e:
            print(repr(e))

    def find_common_code_info(self, df, code_id):
        if code_id is not None:
            # 코드가 숫자여서 dataframe에서 float타입으로 되어있어 string으로 바꾸기전 int로 형변환
            if isinstance(code_id, float):
                code_id = int(code_id)
            code_id = str(code_id)
            result = df[df['코드_ID'].str.startswith(code_id, na=False)]
            if not result.empty:
                result = result['코드명'].tolist()
                if len(result) == 1:
                    return result[0]
        return code_id

    def find_proj_type_code_info(self, df, code_id, code_nm):
        if code_id is not None:
            code_id = str(code_id)
            result = df[df['종류'].str.startswith(code_id, na=False)]
            if not result.empty:
                result = result['클렌징 데이터'].tolist()
                if len(result) == 1:
                    if result[0] is not None:
                        return code_id, result[0].replace(' ', '')
        if code_nm is not None:
            return code_id, code_nm.replace(' ', '')
        return code_id, code_nm

    def get_project_type(self, df, proj_type_nm):
        if proj_type_nm is not None:
            # 사업유형이름(사업유형코드) -> 사업유형코드, 사업유형이름
            # 문자열 뒷부분부터 해당 문자를 검색
            idx_first = proj_type_nm.rfind('(')
            idx_last = proj_type_nm.rfind(')')
            if idx_first >= 0 and idx_last >= 0:
                return self.find_proj_type_code_info(df, proj_type_nm[idx_first+1:idx_last], proj_type_nm[:idx_first])
        return self.find_proj_type_code_info(df, None, proj_type_nm)

    def change_planStatusCd(self, planStatusCd):
        if planStatusCd[0] == '1':
            planStatusCd = self.get_planStatusCd_info[planStatusCd]
        return planStatusCd

    def date_format(self, x):
        if len(x) == 8:
            return f'{x[:4]}-{x[4:6]}-{x[6:]}'
        else:
            return x

    def get_content(self, path, page='1', perPage='10'):
        serviceKey = 'P%2FX6ClUCvMUQWPExEwMla81XkeORZDQJ4l1O6BHKt5R3c51%2BSTGqvZqe9luBBQNwhJiYx%2Fun2AoluaptvRv1gw%3D%3D'
        url = 'https://api.odcloud.kr/api' + path + \
            f'?page={page}&perPage={perPage}&serviceKey={serviceKey}'

        while True:
            try:
                response = requests.get(url)

                if response.status_code != 200:
                    print('api 요청시 SERVICE ERROR 발생... 다시 요청중...')
                    time.sleep(5)
                    continue
                else:
                    break
            except Exception as e:
                print(e)
                continue
            break
        content = response.content
        # byte decode
        content = content.decode('utf-8-sig')
        # dictionary로 변경
        content = json.loads(content)

        return content

    def fetch_data(self, paths):
        df = pd.DataFrame()

        for year, path in paths.items():
            # totalCount 가져오기
            content = self.get_content(path=path, page='1', perPage='1')
            total_count = content['totalCount'] if 'totalCount' in content else 0
            print('{}년도 총 데이터 수: {}'.format(year, total_count))

            print(type(total_count))

            # 데이터 가져오기
            page = 0
            PERPAGEMAX = 500

            while total_count > 0:
                page += 1
                per_page = PERPAGEMAX if total_count > PERPAGEMAX else total_count
                total_count -= per_page
                # data 가져오기
                content = self.get_content(path=path, page=str(
                    page), perPage=str(per_page))
                if 'data' in content:
                    if per_page != content['currentCount']:
                        total_count += content['currentCount']
                    data = pd.DataFrame.from_dict(content['data'])

                    # 예산구분, 비예산여부 통일
                    if '예산구분' in data:
                        data['비예산여부'] = data['예산구분'].apply(
                            lambda x: 'Y' if x is None else 'N')
                    if '계속사업시작연도' in data:
                        data.rename(
                            columns={'계속사업시작연도': '계속사업시작년도'}, inplace=True)
                    if '기관ID' in data:
                        data.rename(columns={'기관ID': '기관아이디'}, inplace=True)
                    if '특수사업코드' in data:
                        data.rename(
                            columns={'특수사업코드': '특수사업명코드'}, inplace=True)
                    df = pd.concat([df, data], ignore_index=True)
                print(f'total count: {total_count}')

            print('데이터 호출 완료')

        return df

    def to_gspread(self, sheet_name, df):
        # 데이터프레임을 Google 스프레드시트에 저장하는 메서드
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            self.credentials_dict, scope)
        sheet_id = self.sheet_id
        gspread_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}'
        gc = gspread.authorize(credentials)

        sheet = gc.open('spreadsheet-copy-testing').worksheet(sheet_name)

        existing_data = sheet.get_all_values()
        current_row = len(existing_data)

        if current_row == 0:
            header = df.columns.tolist()
            values = [header] + df.values.tolist()
            sheet.clear()
            sheet.update(values)
        else:
            start_row = 0
            start_row = current_row + 1
            data_to_append = df.values.tolist()
            sheet.insert_rows(data_to_append, start_row)

        print("Completed to update jobs to google spreadsheet!")

    def process_data(self):
        scraper = ApiUrlCrawling.WebScraper()
        scraper.initialize_driver()
        paths = scraper.scrape_data(
            'https://www.data.go.kr/data/15050148/fileData.do')
        df = self.fetch_data(paths)

        # np.nan -> None으로 변경
        df = df.replace({np.nan: None})

        # 계속사업시작년도, 기관아이디 타입 변경 (object -> float)
        df = df.astype({'계속사업시작년도': 'float', '기관아이디': 'float'},
                       errors='ignore')
        print("# 계속사업시작년도, 기관아이디 타입 변경 (object -> float) 변경 완료")

        # 날짜 포맷 변경
        df['사업기간시작일'] = df['사업기간시작일'].apply(lambda x: self.date_format(x))
        df['사업기간종료일'] = df['사업기간종료일'].apply(lambda x: self.date_format(x))
        print("날짜 포맷 변경 완료")

        # '사업유형코드', '사업유형이름' 분리
        df[['사업유형코드', '사업유형이름']] = df.apply(
            lambda x:  self.get_project_type(
                self.proj_type_code_info, x['사업유형코드']), axis=1, result_type='expand')
        print("사업유형코드, 사업유형이름 분리 완료")

        # '시군구코드', '관할시도명', '관할시군구'
        df[['시군구코드', '관할시도명', '관할시군구']] = df.apply(lambda x: self.find_adm_info(
            self.adm_info, x['관할시군구']), axis=1, result_type='expand')

        # 사업계획서상태코드 변경
        df['사업계획서상태코드'] = df['사업계획서상태코드'].apply(
            lambda x: self.change_planStatusCd(x))
        print('사업계획서상태코드 완료')

        # 코드ID -> 코드명으로 변경
        df['특수사업명코드'] = df['특수사업명코드'].apply(
            lambda x: self.find_common_code_info(self.common_code_info, x))
        print('코드ID -> 코드명으로 변경 완료')

        try:
            df = df[self.columns.keys()].rename(columns=self.columns)
            print("column명을 수정하였습니다")
        except:
            print("column명이 올바르게 되어있습니다")
        finally:
            # dictionary value값을 열로 가지는 열만 가져옴
            df = df[self.columns.values()]

            df = df.fillna('')

            print(df)
            self.to_gspread(sheet_name="projects", df=df)
