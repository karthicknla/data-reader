FROM python:3.8-alpine

WORKDIR /app

# Install required packages
RUN apk update && apk add bash openjdk11 wget

# Download and extract Spark
RUN wget https://downloads.apache.org/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz && \
    tar -xzf spark-3.2.4-bin-hadoop3.2.tgz && \
    rm spark-3.2.4-bin-hadoop3.2.tgz

# Copy the application code
COPY . .

# Expose Spark UI port
EXPOSE 4040

# Set environment variables
ENV SPARK_HOME=/app/spark-3.2.4-bin-hadoop3.2

# Submit the Spark application
CMD ["/app/spark-3.2.4-bin-hadoop3.2/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.mongodb.spark:mongo-spark-connector:10.0.0", \
     "/app/spark-app/read_stream_v3.py"]