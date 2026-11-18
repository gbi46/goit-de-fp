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

COPY . .
