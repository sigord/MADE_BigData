"""
HW 8 part 1
"""
import argparse
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import split
from pyspark.sql.functions import expr
from pyspark.sql.functions import approx_count_distinct
from pyspark.sql.functions import count

parser = argparse.ArgumentParser()
parser.add_argument("--kafka-brokers", required=True)
parser.add_argument("--topic-name", required=True)
parser.add_argument("--starting-offsets", default='latest')
group = parser.add_mutually_exclusive_group()
group.add_argument("--processing-time", default='0 seconds')
group.add_argument("--once", action='store_true')
args = parser.parse_args()
if args.once:
    args.processing_time = None
else:
    args.once = None
# spaek config
spark_conf = (
    SparkConf()
    .set("spark.driver.memory", "512m")
    .set("spark.executor.instances", "2")
    .set("spark.executor.cores", "1")
    .setAppName("domain_stat_test")
    .setMaster("yarn")
)
sc = SparkContext(conf=spark_conf)
spark = SparkSession(sc)
# read df
input_df = (
    spark
    .readStream
    .format('kafka')
    .option("kafka.bootstrap.servers", args.kafka_brokers) # arg
    .option('startingOffsets', args.starting_offsets) # arg
    .option("subscribe", args.topic_name) # arg
    .load()
)
# solution
columns_names = ['unixtime', 'UID', 'URL', 'Title', 'User-Agent']
split_col = split(input_df['value'], '\t')
i = columns_names.index('UID')
input_df = input_df.withColumn('UID', split_col.getItem(i))
i = columns_names.index('URL')
input_df = input_df.withColumn('URL', split_col.getItem(i))
#for i, column_n in enumerate(columns_names):
#    input_df = input_df.withColumn(column_n, split_col.getItem(i))
result = (
    input_df
    .select('UID', expr("""parse_url(URL, 'HOST')""").alias("domain"))
    .groupBy('domain')
    .agg(
        count("domain").alias("view"),
        approx_count_distinct('UID').alias("unique")
    )
    .sort("view", ascending=False)
    .limit(10)
)
# output
query = (
    result
    .writeStream
    .outputMode("complete") # ?complete
    .format("console")
    .trigger(once=args.once, processingTime=args.processing_time) # arg
    .option("truncate", "false")
    .start()
)
# exit
query.awaitTermination()
