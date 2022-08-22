"""
HW 9 part 3
"""
import sys
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import split
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import trim
from pyspark.sql.functions import explode
from pyspark.sql.types import IntegerType
# setup parser
keyspace_name = sys.argv[1]
# spark settings
spark = SparkSession\
    .builder \
    .appName("hw9_part3") \
    .getOrCreate()
# read and prepare data
df_movies = spark.read.csv(path="/data/movielens/movies.csv", header=True)
df_movies = (
    df_movies
    .withColumn('title', trim(col('title')))
    .withColumn('genres', trim(col('genres')))
    .withColumn('year', regexp_extract(col('title'), r"(\(\d\d\d\d\))", 1))
    .withColumn('year', regexp_extract(col('year'), r"(\d+)", 1))
    .filter(col('year') != '')
    .filter(col('genres') != '(no genres listed)')
    .withColumn('genres', split(col('genres'), '\|'))
    .withColumn('movieid', col('movieId').cast(IntegerType()))
    .withColumn('year', col('year').cast(IntegerType()))
    .withColumn('genre', explode(col('genres')))
    .select('movieid', 'title', 'year', 'genre')
)
# write data into cassandra
df_movies \
    .write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="movies_by_genre", keyspace=keyspace_name).mode("append").save()
