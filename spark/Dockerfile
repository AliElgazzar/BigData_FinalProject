FROM apache/spark-py:v3.4.0

USER root

WORKDIR /app

RUN mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars /tmp/spark-local \
    && chmod -R 777 /tmp/.ivy2 /tmp/spark-local

COPY spark_streaming_app.py .

ENV HOME=/tmp
ENV IVY_HOME=/tmp/.ivy2
ENV SPARK_SUBMIT_OPTS="-Divy.home=/tmp/.ivy2 -Divy.cache.dir=/tmp/.ivy2/cache"

CMD ["/opt/spark/bin/spark-submit", "--conf", "spark.jars.ivy=/tmp/.ivy2", "--conf", "spark.local.dir=/tmp/spark-local", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0", "/app/spark_streaming_app.py"]
