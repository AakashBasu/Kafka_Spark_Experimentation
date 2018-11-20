from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 20)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a+b)
    # counts.pprint()

    words = lines.flatMap(lambda line: line.split(" "))
    print(type(words))
    # df1 = words.toDF()
    # df1.show()
    words.pprint()

    ssc.start()
    ssc.awaitTermination()

## My RUNNING test
# /home/kafka/Downloads/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 Kafka_Spark_Test.py localhost:9092 test1
