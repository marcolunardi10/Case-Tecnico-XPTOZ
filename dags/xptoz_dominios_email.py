import os
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt/xptoz")
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"


def dbt_task(task_id, comando, **kwargs):
    return BashOperator(task_id=task_id, bash_command=f"{DBT_CMD} {comando}", **kwargs)


@dag(
    dag_id="xptoz_dominios_email",
    description="Carga raw + transformações dbt (staging, intermediate, marts) dos domínios de e-mail",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "engenharia-de-dados",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    params={
        "carregar_novos_dados": Param(
            False,
            type="boolean",
            description="Gera e insere novos registros fictícios na users_info antes das transformações.",
        ),
        "qtd_registros": Param(1000, type="integer", minimum=1, maximum=100000),
        "dominios_variados": Param(
            False,
            type="boolean",
            description="Gera e-mails com domínios realistas em vez de example.com/.net/.org.",
        ),
    },
    tags=["xptoz", "dbt"],
)
def xptoz_dominios_email():

    @task
    def carregar_dados(params=None):
        if not params["carregar_novos_dados"]:
            raise AirflowSkipException("Parâmetro carregar_novos_dados=False: usando os dados já existentes.")

        import sys

        sys.path.insert(0, "/opt/airflow/scripts")
        import generate_data

        generate_data.main(params["qtd_registros"], params["dominios_variados"])

    testar_raw = dbt_task(
        "testar_raw",
        "test --select source:raw --indirect-selection cautious",
        trigger_rule="none_failed",
    )
    staging = dbt_task("dbt_staging", "build --select staging")
    intermediate = dbt_task("dbt_intermediate", "build --select intermediate")
    marts = dbt_task("dbt_marts", "build --select marts")

    carregar_dados() >> testar_raw >> staging >> intermediate >> marts


xptoz_dominios_email()
