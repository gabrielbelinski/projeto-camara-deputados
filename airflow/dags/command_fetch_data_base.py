import os, dotenv
dotenv.load_dotenv()

COMMAND = f"""
                    docker exec spark-master spark-submit \
                    --master spark://spark-master:7077 \
                    --name "Extract Chamber Data" \
                    --conf spark.hadoop.fs.s3a.endpoint={os.getenv('MINIO_ADDRESS')} \
                    --conf spark.hadoop.fs.s3a.access.key={os.getenv('MINIO_ROOT_USER')} \
                    --conf spark.hadoop.fs.s3a.secret.key={os.getenv('MINIO_ROOT_PASSWORD')} \
                    --conf spark.hadoop.fs.s3a.path.style.access=true \
                    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
                    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
                    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
                    /opt/spark/spark-jobs/fetch_data.py"""