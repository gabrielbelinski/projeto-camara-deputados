from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import datetime, os, dotenv
import command_fetch_data_base as c

dotenv.load_dotenv()

with DAG(
    dag_id="fetch_expenses_data",
    start_date=datetime.datetime(2026, 5, 15),
    schedule=None,
    catchup=False,
    default_args={
    "owner": "Gabriel",
    "depends_on_past": False
    }
) as dag:
    a = EmptyOperator(task_id="start")
    b = BashOperator(task_id="submit_to_spark", bash_command=f"{c.COMMAND} /opt/spark/spark-jobs/fetch_data.py --entity expenses") 
    c = TriggerDagRunOperator(task_id="end",trigger_dag_id="silver_layer_processing", trigger_run_id="trigger_from_exp_{{ ds }}",  wait_for_completion=True, poke_interval=10)

    a >> b >> c