from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
import datetime, os, dotenv
import command_fetch_data_base as c

dotenv.load_dotenv()

with DAG(
    dag_id="gold_layer_processing",
    start_date=datetime.datetime(2026, 5, 11),
    schedule=None,
    catchup=False,
    default_args={
    "owner": "Gabriel",
    "depends_on_past": False
    }
) as dag:
    a = EmptyOperator(task_id="start")
    b = BashOperator(task_id="submit_to_spark", bash_command=f"{c.COMMAND} /opt/spark/spark-jobs/gold_refactor.py") 
    c = EmptyOperator(task_id="end")

    a >> b >> c