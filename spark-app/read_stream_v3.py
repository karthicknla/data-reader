
## pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector:10.0.0

# Its a Spark Consumer that consumes data from Kafka topic and write it to MongoDb Collection
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,TimestampType, DoubleType, StringType, StructField

# Creating SparkSession and Streaming Context
spark = SparkSession.\
        builder.\
        appName("streamingExampleWrite").\
        config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.0').\
        config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0').\
        getOrCreate()

#Reads the data from the kafka topic test
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:29092") \
  .option("subscribe", "test") \
  .option("startingOffsets", "earliest") \
  .option("maxOffsetsPerTrigger", 1000) \
  .load()
  

# schemaTest = StructType([ \
#   StructField("_id",StringType(),True), \
#   StructField("header",StringType(), True), \
#   StructField("data",StringType(), True), \
#   StructField("identity",StringType(), True), \
#   StructField("language",StringType(), True)])

# schemaKafka = StructType([ \
#    StructField("data",StringType(),True)])


#Casts the value column to string.
testDF=df.selectExpr("CAST(value AS STRING)")
	
# testDF=testDF.select(from_json(col('data'),schemaTest).alias("json_data")).selectExpr('json_data.*')	

	
#Sets the format to "mongodb".
#Sets the query name to "ToMDB".
#Sets the checkpoint location to "/tmp/pyspark3/".
#Sets the connection URI to "mongodb://localhost:27017/".
#Sets the database to "myDb".
#Sets the collection to "test".
#Sets the trigger to "continuous".
#Sets the output mode to "append".
#Starts the stream


dsw = (
  testDF.writeStream \
    .format("mongodb") \
    .queryName("ToMDB") \
	.option("checkpointLocation", "/tmp/pyspark3/") \
    .option('spark.mongodb.connection.uri', 'mongodb://mongodb:27017/') \
    .option('spark.mongodb.database', 'myDb') \
    .option('spark.mongodb.collection', 'test') \
    .trigger(continuous="10 seconds") \
    .outputMode("append") \
    .start().awaitTermination());
'''
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .start()
'''	
