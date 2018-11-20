from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

class test:


    spark = SparkSession.builder \
        .appName("Stream_Col_Oper_Spark") \
        .getOrCreate()

    data = spark.readStream.format("kafka") \
        .option("startingOffsets", "latest") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test1") \
        .load()

    ID = data.select('value') \
        .withColumn('value', data.value.cast("string")) \
        .withColumn("Col1", split(col("value"), ",").getItem(0)) \
        .withColumn("Col2", split(col("value"), ",").getItem(1)) \
        .drop('value')

    ID.createOrReplaceTempView("transformed_Stream_DF")

    df = spark.sql("select avg(col1) as aver from transformed_Stream_DF")

    df.createOrReplaceTempView("abcd")

    wordCounts = spark.sql("Select col1, col2, col2/(select aver from abcd) col3 from transformed_Stream_DF")

    # wordCounts.createOrReplaceTempView("final")
    #
    # final_DF = spark.sql("select * from final")

    # -----------------------#

    query1 = df \
        .writeStream \
        .format("console") \
        .outputMode("complete") \
        .trigger(processingTime='3 seconds') \
        .start()

    # -----------------------#

    query2 = wordCounts \
        .writeStream \
        .format("console") \
        .trigger(processingTime='3 seconds') \
        .start()

    query1.awaitTermination()
    query2.awaitTermination()

    # query3 = final_DF \
    #     .writeStream \
    #     .format("console") \
    #     .trigger(processingTime='3 seconds') \
    #     .start()
    #
    # query3.awaitTermination()

    # /home/kafka/Downloads/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0,com.databricks:spark-csv_2.10:1.0.3 /home/aakashbasu/PycharmProjects/AllMyRnD/Kafka_Spark/Stream_Col_Oper_Spark.py
