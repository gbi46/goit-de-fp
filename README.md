# Spark + MySQL JDBC

A small project for building a Docker image with PySpark, Java, and the MySQL JDBC driver.

## Current status

- `Dockerfile` installs Java 17, the dependencies listed in `requirements.txt`, and MySQL Connector/J 8.0.32.
- The JDBC driver is stored at `/opt/jdbc/mysql-connector-j-8.0.32.jar`.
- `MYSQL_JDBC_JAR` points to the driver file.
- The script does not currently read data from MySQL or test the connection.
- The Docker image does not run the script automatically.

## Prerequisites

Docker is required. Internet access is needed while building the image because the Dockerfile downloads dependencies and the JDBC driver.

## Build the image

Run this from the project root:

```sh
./run.sh
```

The script builds the `spark-mysql-driver:latest` image. To build it manually:

```sh
docker build -t spark-mysql-driver:latest .
```

To confirm that the image was created:

```sh
docker image inspect spark-mysql-driver:latest
```

## MySQL configuration

`.env.example` provides a template for the connection settings. Copy it to `.env` and set `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, and `MYSQL_PASSWORD`.
