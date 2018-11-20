from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

# spark = SparkSession \
#     .builder \
#     .appName("Spark_Old_DStream") \
#     .getOrCreate()

sc = SparkContext(appName="PythonStreamingKafka")

schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("First_Name", StringType(), True),
    StructField("Last_Name", StringType(), True)
])


ssc = StreamingContext(sc, 10)
topic = "test1"
brokers = "localhost:9092"
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})



# lines = kvs.map(lambda x: x[1])
# counts = lines.flatMap(lambda line: line.split(",")) \
#     .map(lambda word: (word, 1)) \
#     .reduceByKey(lambda a, b: a+b)
# counts.pprint()
ssc.start()
ssc.awaitTermination()




# /home/kafka/Downloads/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 /home/aakashbasu/PycharmProjects/AllMyRnD/Kafka_Spark/Kafka_Spark_Old_Streaming.py
