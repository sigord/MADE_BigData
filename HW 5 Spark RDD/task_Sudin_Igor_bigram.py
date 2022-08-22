"""
Task 1
"""
import re
from pyspark import SparkConf, SparkContext
spark_conf = (
    SparkConf()
    .set("spark.ui.port", 22172)
    .set("spark.driver.memory", "512m")
    .set("spark.executor.instances", "2")
    .set("spark.executor.cores", "1")
    .setAppName("hw5_task1_sudin")
    .setMaster("yarn")
)
sc = SparkContext(conf=spark_conf)
word_pair = re.compile(r"(narodnaya).?\s(\w+)")
wiki_rdd = sc.textFile("hdfs:///data/wiki/en_articles_part")
word_pairs_rdd = (
    wiki_rdd
    .map(lambda x: x.split('\t', 1))
    .map(lambda pair: pair[1].lower())
    .map(word_pair.findall)
    .filter(lambda x: len(x) > 0)
    .flatMap(list)
    .map(lambda x: x[0] + "_" + x[1])
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
    .sortByKey()
    .map(lambda x: x[0] + '\t' + str(x[1]))
)
print(*word_pairs_rdd.collect(), sep="\n")
