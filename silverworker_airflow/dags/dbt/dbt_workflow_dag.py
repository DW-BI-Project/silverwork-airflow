from airflow import DAG

from datetime import datetime
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from silverworker_airflow.utils import alert

configmaps = [
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='dbt')),
]


def dbt_task(task: str) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=f"dbt-{task}",
        name=f"dbt-{task}",
        image=Variable.get('dbt_image_url'),
        namespace='airflow',
        env_from=configmaps,
        image_pull_policy='Always',
        is_delete_operator_pod=False,
        in_cluster=True,
        get_logs=True,
        cmds=["dbt", task],
    )


with DAG('dbt_workflow',
         start_date=datetime(2023, 1, 1),
         schedule_interval='@daily',
         tags=["DBT", "Data"],
         default_args={
             'on_success_callback': alert.slack_success_alert,
             'on_failure_callback': alert.slack_failure_alert,
         },
         catchup=False,
         ) as dag:
    dbt_seed_k8s = dbt_task(task="seed")
    dbt_snapshot_k8s = dbt_task(task="snapshot")
    dbt_run_k8s = dbt_task(task="run")
    dbt_test_k8s = dbt_task(task="test")

    dbt_seed_k8s >> dbt_snapshot_k8s >> dbt_run_k8s >> dbt_test_k8s
