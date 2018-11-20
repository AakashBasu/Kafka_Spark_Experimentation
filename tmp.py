from pyspark.sql import SparkSession
from pyspark.sql.functions import split

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


id_DF = data.select(split(data.value, ",").getItem(0).alias("col1"), split(data.value, ",").getItem(1).alias("col2"))

id_DF.createOrReplaceTempView("ds")

df = spark.sql("select avg(col1) as aver from ds")
query2 = df \
    .writeStream \
    .format("memory") \
    .queryName("ABCD") \
    .outputMode("complete") \
    .trigger(processingTime='5 seconds') \
    .start()

wordCounts = spark.sql("Select col1, col2, col2/(select aver from ABCD) col3 from ds")

query = wordCounts \
    .writeStream \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()

spark.streams.awaitAnyTermination()
