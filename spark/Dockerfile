FROM apache/spark:4.1.1-scala2.13-java21-python3-r-ubuntu
USER root

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars

RUN curl -L -o /opt/spark/jars/hadoop-aws-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar

RUN curl -L -o /opt/spark/jars/bundle-2.44.0.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.44.0/bundle-2.44.0.jar

RUN curl -L -o /opt/spark/jars/analyticsaccelerator-s3-1.3.1.jar \
    https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar

RUN curl -L -o /opt/spark/jars/delta-spark_2.13-4.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.2.0/delta-spark_2.13-4.2.0.jar && \
    curl -L -o /opt/spark/jars/delta-storage-4.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/4.2.0/delta-storage-4.2.0.jar

RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    delta-spark==4.2.0 \
    python-dotenv \
    requests \
    beautifulsoup4

RUN chmod 644 /opt/spark/jars/*.jar

USER spark

WORKDIR /opt/spark