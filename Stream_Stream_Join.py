from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import split, col

class test:


    spark = SparkSession.builder \
        .appName("DirectKafka_Spark_Stream_Stream_Join") \
        .getOrCreate()

    table1_stream = (spark.readStream.format("kafka").option("startingOffsets", "latest").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test1").load())

    table2_stream = (spark.readStream.format("kafka").option("startingOffsets", "latest").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test2").load())


    query1 = table1_stream.select('value')\
        .withColumn('value', table1_stream.value.cast("string")) \
        .withColumn("ID", split(col("value"), ",").getItem(0)) \
        .withColumn("First_Name", split(col("value"), ",").getItem(1)) \
        .withColumn("Last_Name", split(col("value"), ",").getItem(2)) \
        .drop('value')

    query2 = table2_stream.select('value') \
        .withColumn('value', table2_stream.value.cast("string")) \
        .withColumn("ID", split(col("value"), ",").getItem(0)) \
        .withColumn("Department", split(col("value"), ",").getItem(1)) \
        .withColumn("Date_joined", split(col("value"), ",").getItem(2)) \
        .drop('value')

    joined_Stream = query1.join(query2, "Id", how='inner')

    # a = query1.writeStream.format("console").start()
    # b = query2.writeStream.format("console").start()
    c = joined_Stream.writeStream.format("console").start()  # .trigger(10)

    # a.awaitTermination()
    # b.awaitTermination()
    c.awaitTermination()

# /home/kafka/Downloads/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 /home/aakashbasu/PycharmProjects/AllMyRnD/Kafka_Spark/Stream_Stream_Join.py

