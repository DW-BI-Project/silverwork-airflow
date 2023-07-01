import time
import requests


class WebScraper:
    def __init__(self, url):
        self.url = url

    def scrap_data(self):
        # path와 summary 텍스트 추출
        urls = []
        summaries = []

        while True:
            try:
                response = requests.get(self.url)
                data = response.json()

                if response.status_code != 200:
                    print('api 요청시 SERVICE ERROR 발생... 다시 요청중...')
                    time.sleep(5)
                    continue
                else:
                    break
            except Exception as e:
                print(e)
                continue

        for item in data["paths"]:
            urls.append(item)
            if "summary" in data["paths"][item]["get"]:
                summaries.append(data["paths"][item]["get"]["summary"])

        # path 텍스트 출력
        print("=== path 텍스트 ===")
        for url in urls:
            print(url)

        # summary 텍스트 출력
        for summary in summaries:
            print(summary)

        paths = {}

        for url, summary in zip(urls, summaries):
            year = int(summary[-8:-4])
            paths[year] = url

        print(paths)
        max_year = sorted(paths)[-1:]

        path = {}
        path[2023] = paths[max_year]

        return path
