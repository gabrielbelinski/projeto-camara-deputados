from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskMarker
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import datetime, os, dotenv

dotenv.load_dotenv()

with DAG(
    dag_id="fetch_legislature_data",
    start_date=datetime.datetime(2026, 5, 11),
    schedule="0 0 1 */3 *",
    catchup=False,
    default_args={
    "owner": "Gabriel",
    "depends_on_past": False
    }
) as dag:
    a = EmptyOperator(task_id="start")
    b = BashOperator(task_id="submit_to_spark", bash_command=f"""
                    docker exec spark-master spark-submit \
                    --master spark://spark-master:7077 \
                    --conf spark.hadoop.fs.s3a.endpoint={os.getenv('MINIO_ADDRESS')} \
                    --conf spark.hadoop.fs.s3a.access.key={os.getenv('MINIO_ROOT_USER')} \
                    --conf spark.hadoop.fs.s3a.secret.key={os.getenv('MINIO_ROOT_PASSWORD')} \
                    --conf spark.hadoop.fs.s3a.path.style.access=true \
                    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
                    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
                    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
                        /opt/spark/spark-jobs/fetch_legislature_data.py""") 
    c = TriggerDagRunOperator(task_id="end",trigger_dag_id="fetch_parties_data", trigger_run_id="trigger_from_leg_{{ ds }}",  wait_for_completion=True, poke_interval=10)

    a >> b >> c