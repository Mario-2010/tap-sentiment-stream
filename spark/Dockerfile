FROM apache/spark:4.0.1-python3

USER root

# Connettori Kafka e Iceberg scaricati al build: a runtime non serve Internet
ARG MVN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.1/iceberg-spark-runtime-4.0_2.13-1.10.1.jar /opt/spark/jars/

COPY spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY common/ /opt/app/
RUN python3 /opt/app/download_models.py /models sentiment embedding && chmod -R a+rX /models

COPY spark/app/ /opt/app/
RUN mkdir -p /warehouse /checkpoints /home/spark && chown -R spark:spark /warehouse /checkpoints /home/spark

USER spark
ENV HOME=/home/spark PYTHONPATH=/opt/app
WORKDIR /opt/app

# Memoria del driver e numero di core da variabili d'ambiente (SPARK_DRIVER_MEMORY, SPARK_CORES)
CMD ["/bin/sh", "-c", "exec /opt/spark/bin/spark-submit --master \"local[${SPARK_CORES:-2}]\" --driver-memory \"${SPARK_DRIVER_MEMORY:-1g}\" --py-files /opt/app/onnx_models.py /opt/app/streaming_job.py"]
