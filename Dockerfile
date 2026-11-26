FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /opt/jdbc \
    && curl -fL \
       https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar \
       -o /opt/jdbc/mysql-connector-j-8.0.32.jar

ENV MYSQL_JDBC_JAR=/opt/jdbc/mysql-connector-j-8.0.32.jar

# Kafka connector for Spark Structured Streaming (versions match pyspark 4.3.0, Scala 2.13)
ARG MAVEN=https://repo1.maven.org/maven2
RUN mkdir -p /opt/kafka-jars \
    && cd /opt/kafka-jars \
    && curl -fLO $MAVEN/org/apache/spark/spark-sql-kafka-0-10_2.13/4.3.0/spark-sql-kafka-0-10_2.13-4.3.0.jar \
    && curl -fLO $MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.3.0/spark-token-provider-kafka-0-10_2.13-4.3.0.jar \
    && curl -fLO $MAVEN/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar \
    && curl -fLO $MAVEN/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar

ENV KAFKA_JARS=/opt/kafka-jars/spark-sql-kafka-0-10_2.13-4.3.0.jar,/opt/kafka-jars/spark-token-provider-kafka-0-10_2.13-4.3.0.jar,/opt/kafka-jars/kafka-clients-3.9.2.jar,/opt/kafka-jars/commons-pool2-2.13.1.jar

COPY . .
