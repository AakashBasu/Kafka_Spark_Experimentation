# from typing import cast

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
import logging

spark = SparkSession \
    .builder \
    .appName("Spark_Streaming_Transformation") \
    .getOrCreate()

# spark.setLogLevel("WARN")

# logger = logging.getLogger('py4j')
# logger.level = "WARN"

data = spark.readStream.format("kafka") \
    .option("startingOffsets", "latest") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test1") \
    .load()

ID = data.select('value')\
        .withColumn('value', data.value.cast("string")) \
        .withColumn("ID", split(col("value"), ",").getItem(0)) \
        .withColumn("First_Name", split(col("value"), ",").getItem(1)) \
        .withColumn("Last_Name", split(col("value"), ",").getItem(2)) \
        .withColumn("Salary", split(col("value"), ",").getItem(3)) \
        .drop('value')

# print(ID)


ID.createOrReplaceTempView("transformed_Stream_DF")
case_Change = spark.sql("select ID, upper(First_Name) as UpperCase_FirstName, upper(Last_Name) as UpperCase_LastName from transformed_Stream_DF")
aggregate_func = spark.sql("select AVG(Salary) as Average_Salary,SUM(Salary) as Total_Salary from transformed_Stream_DF")

# -----------For Console Print-----------
query = case_Change \
    .writeStream.format("console") \
    .start()

query2 = aggregate_func\
    .writeStream\
    .outputMode("complete") \
    .format("console")\
    .start()
# -----------Console Print ends-----------


# -----------For Kafka Feed-----------
# ds1 = case_Change \
#   .writeStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("topic", "test2") \
#   .option("checkpointLocation", "/home/aakashbasu/Downloads/Kafka_Testing/") \
#   .start().awaitTermination()

# ds2 = aggregate_func \
#   .writeStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("topic", "test2") \
#   .option("checkpointLocation", "/home/aakashbasu/Downloads/Kafka_Testing/") \
#   .outputMode("complete") \
#   .start().awaitTermination()
# -----------Kafka Feed ends-----------

query.awaitTermination()
query2.awaitTermination()
#query = line.writeStream.format("console").start


# /home/kafka/Downloads/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 /home/aakashbasu/PycharmProjects/AllMyRnD/Kafka_Spark/Kafka_Spark_Transformation.py