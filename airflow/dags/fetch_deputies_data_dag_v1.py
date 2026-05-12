from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import datetime, os, dotenv
import command_fetch_data_base as c

dotenv.load_dotenv()

with DAG(
    dag_id="fetch_deputies_data",
    start_date=datetime.datetime(2026, 5, 11),
    schedule=None,
    catchup=False,
    default_args={
    "owner": "Gabriel",
    "depends_on_past": False
    }
) as dag:
    a = EmptyOperator(task_id="start")
    b = BashOperator(task_id="submit_to_spark", bash_command=f"{c.COMMAND} --entity deputies") 
    c = TriggerDagRunOperator(task_id="end", trigger_dag_id="fetch_expenses_data", trigger_run_id="trigger_from_dep_{{ ds }}",  wait_for_completion=True, poke_interval=10)

    a >> b >> c