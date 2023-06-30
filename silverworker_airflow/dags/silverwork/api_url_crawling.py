from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


class WebScraper:
    def __init__(self):
        self.driver = None

    def initialize_driver(self):
        # Chrome WebDriver 생성
        chrome_options = Options()
        # 브라우저를 표시하지 않고 실행 (headless 모드)
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--headless')
        remote_webdriver = 'remote_chromedriver'
        self.driver = webdriver.Remote(
            f'{remote_webdriver}:4444/wd/hub', options=chrome_options)

    def scrape_data(self, url):
        # 웹 페이지 접속
        self.driver.get(url)

        # 탭 클릭 (id가 'data-type-openapi'인 li 태그)
        tab_li = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, 'data-type-openapi')))
        tab_li.click()

        paths = {}

        # 데이터 가져오기 (span 태그의 class가 'opblock-summary-path'와 'opblock-summary-description'인 태그들)
        span_tags = WebDriverWait(self.driver, 10).until(EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'span.opblock-summary-path')))
        div_tags = WebDriverWait(self.driver, 10).until(EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'div.opblock-summary-description')))

        for span_tag, div_tag in zip(span_tags, div_tags):
            opblock_summary_description = div_tag.text.strip()
            opblock_summary_path = span_tag.text.strip()
            print(opblock_summary_description)
            print(opblock_summary_path)

            year = int(opblock_summary_description[-8:-4])
            paths[year] = opblock_summary_path

        # WebDriver 종료
        self.driver.quit()

        print(paths)
        max_year = sorted(paths)[-1]

        path = {}
        path[max_year] = paths[max_year]

        return path
