from typing import cast

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

data = spark \
    .readStream \
    .format("socket") \
    .option("header","true") \
    .option("host", "localhost") \
    .option("port", 9998) \
    .load("csv")

# .option("includeTimestamp", "true") \

#---------------Experiment--------------#

# data2 = spark \
#     .readStream \
#     .format("socket") \
#     .option("header","true") \
#     .option("host", "localhost") \
#     .option("port", 9999) \
#     .option("includeTimestamp", "true") \
#     .load("csv")
#
# id2 = data2.select(split(data2.value, ",").getItem(0).alias("col1"), split(data2.value, ",").getItem(1).alias("col2"), split(data2.value, ",").getItem(2).alias("timeStamp"))
#
# id2.createOrReplaceTempView("ds2")

#-----------------Ends------------------#

id = data.select(split(data.value, ",").getItem(0).alias("col1"), split(data.value, ",").getItem(1).alias("col2"))  # id = data.select(split(data.value, ",").getItem(0).alias("col1"), split(data.value, ",").getItem(1).alias("col2"))  #

id.createOrReplaceTempView("ds")
avg = id.groupBy("col2").avg("col1")
print(avg)

#df = spark.sql("select avg(col1) as aver from ds")

#df.createOrReplaceTempView("abcd")

wordCounts = spark.sql("Select col1, col2, col2/(select aver from abcd) col3 from ds")  # (select aver from abcd)

# wordCounts.createOrReplaceTempView("final")
#
# final_DF = spark.sql("select * from final")

#-----------------------#



query2 = df \
    .writeStream \
    .format("console") \
    .outputMode("complete") \
    .trigger(processingTime='5 seconds') \
    .start()


#------------Saving the aggregation to Kafka----------------#

# ds1 = df \
#   .writeStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("topic", "test2") \
#   .option("checkpointLocation", "/home/aakashbasu/Downloads/Kafka_Testing/") \
#   .start().awaitTermination()

#-----------------------#

query = wordCounts \
    .writeStream \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()


# query = wordCounts \
#     .coalesce(1) \
#     .writeStream \
#     .format("csv") \
#     .option("path", "/home/aakashbasu/Downloads/Kafka_Testing/Temp_AvgStore/RESULT/") \
#     .option("checkpointLocation", "/home/aakashbasu/Downloads/Kafka_Testing/Temp_AvgStore/") \
#     .trigger(processingTime='10 seconds') \
#     .start()

spark.streams.awaitAnyTermination()

# query3 = final_DF \
#     .writeStream \
#     .format("console") \
#     .trigger(processingTime='3 seconds') \
#     .start()
#
# query3.awaitTermination()

#query = line.writeStream.format("console").start