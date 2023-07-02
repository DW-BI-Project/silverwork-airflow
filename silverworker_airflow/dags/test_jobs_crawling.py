import unittest
from unittest import mock
from airflow.models import DagBag
from datetime import datetime
from dags.jobs_crawling import JobListCrawl, JobDetailCrawl, Joining, GsheetToBigquery


class TestJobsCrawling(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag()

    def test_dag_loaded(self):
        dag_id = 'jobs_crawling'
        self.assertTrue(self.dagbag.get_dag(dag_id) is not None,
                        f"DAG '{dag_id}' not found in DagBag")

    def test_job_list_crawl_task(self):
        dag_id = 'jobs_crawling'
        task_id = 'job_list'

        dag = self.dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)

        # 테스트용 날짜 및 시간 설정
        execution_date = datetime(2023, 6, 28)
        context = {'execution_date': execution_date}

        # Mock을 사용하여 의존성 모듈의 메서드를 가로채기
        with mock.patch('silverwork.job_list_crawl.JobListCrawler') as mock_crawler:
            # 실행
            task.execute(context=context)

            # 의존성 모듈의 메서드가 예상대로 호출되었는지 확인
            mock_crawler.assert_called_once()

    def test_job_detail_crawl_task(self):
        dag_id = 'jobs_crawling'
        task_id = 'job_detail'

        dag = self.dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)

        # 테스트용 날짜 및 시간 설정
        execution_date = datetime(2023, 6, 28)
        context = {'execution_date': execution_date}

        # Mock을 사용하여 의존성 모듈의 메서드를 가로채기
        with mock.patch('silverwork.job_detail_crawl.JobDetailCrawler') as mock_crawler:
            # 실행
            task.execute(context=context)

            # 의존성 모듈의 메서드가 예상대로 호출되었는지 확인
            mock_crawler.assert_called_once()

    def test_joining_task(self):
        dag_id = 'jobs_crawling'
        task_id = 'Joining'

        dag = self.dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)

        # 테스트용 날짜 및 시간 설정
        execution_date = datetime(2023, 6, 28)
        context = {'execution_date': execution_date}

        # Mock을 사용하여 의존성 모듈의 메서드를 가로채기
        with mock.patch('silverwork.join_list_and_detail.GoogleSpreadsheetManager') as mock_manager:
            # 실행
            task.execute(context=context)

            # 의존성 모듈의 메서드가 예상대로 호출되었는지 확인
            mock_manager.assert_called_once()

    def test_gsheet_to_bigquery_task(self):
        dag_id = 'jobs_crawling'
        task_id = 'GtoB'

        dag = self.dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)

        # 테스트용 날짜 및 시간 설정
        execution_date = datetime(2023, 6, 28)
        context = {'execution_date': execution_date}

        # Mock을 사용하여 의존성 모듈의 메서드를 가로채기
        with mock.patch('silverwork.jobs_gsheet_to_bigquery.load') as mock_load:
            # 실행
            task.execute(context=context)

            # 의존성 모듈의 메서드가 예상대로 호출되었는지 확인
            mock_load.assert_called_once()


if __name__ == '__main__':
    unittest.main()
