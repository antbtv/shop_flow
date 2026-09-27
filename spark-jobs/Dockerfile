# Spark image for the streaming job (ADR-0008). Jars are pinned and checksummed at build time,
# so the job never resolves --packages from Maven at startup.
FROM spark:4.0.4-scala2.13-java17-python3-ubuntu

ARG MAVEN=https://repo1.maven.org/maven2

# Kafka source for Spark 4.0.4 and its runtime dependencies (versions from the Spark 4.0.4 POM).
ADD --chown=spark:spark --checksum=sha256:b2f7b3a4fbf292b5bf10bd4dd6af301eded9c9985b16239b407401d66dbc8a25 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.4/spark-sql-kafka-0-10_2.13-4.0.4.jar /opt/spark/jars/
ADD --chown=spark:spark --checksum=sha256:ae9817053af94a21992e948269eba1a50d511333cb51462865983abfbeea7f60 \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.4/spark-token-provider-kafka-0-10_2.13-4.0.4.jar /opt/spark/jars/
ADD --chown=spark:spark --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/spark/jars/
ADD --chown=spark:spark --checksum=sha256:6d3bd18df8410f3e31b031aca582cc109342358a62a2759ebd0c4cdf30d06f8b \
    ${MAVEN}/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/spark/jars/
# ClickHouse DataSource V2 connector: a fat jar that bundles ClickHouse client-v2 0.9.5.
ADD --chown=spark:spark --checksum=sha256:f1af7fcaee42970f0234cc3567e636ac9490422bacfcbdb8c2110f2eaeafceb0 \
    ${MAVEN}/com/clickhouse/spark/clickhouse-spark-runtime-4.0_2.13/0.10.1/clickhouse-spark-runtime-4.0_2.13-0.10.1.jar /opt/spark/jars/

COPY --chown=spark:spark spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER root
# The checkpoint directory exists in the image, so a fresh named volume inherits the spark owner.
RUN mkdir -p /checkpoints && chown spark:spark /checkpoints
USER spark

ENV TZ=UTC
WORKDIR /opt/shopflow
COPY --chown=spark:spark . /opt/shopflow/
