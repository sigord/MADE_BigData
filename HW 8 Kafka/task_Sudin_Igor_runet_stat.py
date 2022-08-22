"""
HW 8 part 2
"""
import argparse
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.functions import approx_count_distinct
from pyspark.sql.functions import count
from pyspark.sql.functions import when
from pyspark.sql.functions import window
from pyspark.sql.types import DoubleType
from pyspark.sql.types import TimestampType

# parser setup
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
# spark config
#spark_conf = (
#    SparkConf()
#    .set("spark.driver.memory", "512m")
#    .set("spark.executor.instances", "2")
#    .set("spark.executor.cores", "1")
#    .setAppName("hw8part2")
#    .setMaster("yarn")
#)
#sc = SparkContext(conf=spark_conf)
#spark = SparkSession(sc)
spark = SparkSession\
    .builder \
    .appName("word_count_test") \
    .getOrCreate()
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
columns_names = ['TS', 'UID', 'URL', 'Title', 'User-Agent']
split_col = split(input_df['value'], '\t')
i = columns_names.index('UID')
input_df = input_df.withColumn('UID', split_col.getItem(i))
i = columns_names.index('URL')
input_df = input_df.withColumn('URL', split_col.getItem(i))
i = columns_names.index('TS')
input_df = input_df.withColumn('TS', split_col.getItem(i))
result = (
    input_df
    .select('TS', 'UID', expr("""parse_url(URL, 'HOST')""").alias("domain"))
    .withColumn("zone", when(col("domain").endswith(".ru"), "ru").otherwise("not ru"))
    .withColumn('TS', col('TS').cast(DoubleType()).cast(TimestampType()))
    .groupBy(
        window(col('TS'), "2 seconds", "1 second"),
        col("zone")
    )
    .agg(
        count("UID").alias("view"),
        approx_count_distinct('UID').alias("unique")
    )
    .sort(col("window"), col("view").desc())
    .limit(20)
)
# output
query = (
    result
    .writeStream
    .outputMode("complete")
    .format("console")
    .trigger(once=args.once, processingTime=args.processing_time) # arg
    .option("truncate", "false")
    .start()
)
# exit
query.awaitTermination()
