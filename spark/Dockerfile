FROM apache/spark:3.5.3-python3

USER root

# Connettori Kafka e Iceberg scaricati al build: a runtime non serve Internet
ARG MVN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar /opt/spark/jars/

COPY spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY common/ /opt/app/
RUN python3 /opt/app/download_models.py /models sentiment embedding && chmod -R a+rX /models

COPY spark/app/ /opt/app/
RUN mkdir -p /warehouse /checkpoints /home/spark && chown -R spark:spark /warehouse /checkpoints /home/spark

USER spark
ENV HOME=/home/spark PYTHONPATH=/opt/app
WORKDIR /opt/app

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--driver-memory", "2g", \
     "--py-files", "/opt/app/onnx_models.py", "/opt/app/streaming_job.py"]
