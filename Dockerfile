FROM python:3.8-alpine
WORKDIR /app
RUN wget https://downloads.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
RUN tar -xzf spark-3.3.0-bin-hadoop3.tgz
RUN apk update && apk add bash
RUN apk add openjdk11
RUN pwd
RUN ls
COPY . .
RUN ./spark-3.3.0-bin-hadoop3/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector:10.0.0 read_stream_v3.py
