"""
HW 6
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
# set spark
spark_conf = (
    SparkConf()
    .set("spark.driver.memory", "512m")
    .set("spark.executor.instances", "2")
    .set("spark.executor.cores", "1")
    .setAppName("hw6_sudin")
    .setMaster("yarn")
)
sc = SparkContext(conf=spark_conf)
spark = SparkSession(sc)
# const
PATH_FULL = "/data/twitter/twitter.txt"
START = 12
END = 34
# read data
df = spark.read.format("csv").options(header=False, inferSchema=True, sep="\t").load(PATH_FULL)
df = df.select(col("_c0").alias("users"), col("_c1").alias("folls"))
tmp_df = (
    df
    .filter(col("folls") == START)
    .select("users")
    .withColumnRenamed("users", "right_folls")
)
join_condition = (col("folls") == col("right_folls"))
if tmp_df.filter(col("users") == END).count() != 0:
    print(1)
else:
    for path_len in range(2, 10**3):
        tmp_df = (
            df
            .join(tmp_df, join_condition, "inner")
            .select("users")
            .withColumnRenamed("users", "right_folls")
        )
        if tmp_df.filter(col("users") == END).count() != 0:
            print(path_len)
            break
