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

id = data.select(
   explode(
       split(data.value, ",")
   ).alias("customerid")#,("acountno"),("money1"),("money2")
)
print(id)


id.createOrReplaceTempView("ds")
wordCounts = spark.sql("select AVG(customerid) as avg,SUM(customerid) as SUM from ds")

query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime='4 seconds') \
    .start()

query.awaitTermination()
#query = line.writeStream.format("console").start