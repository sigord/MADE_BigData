"""
Task 2
"""
import re
import math
from pyspark import SparkConf, SparkContext
spark_conf = (
    SparkConf()
    .set("spark.driver.memory", "512m")
    .set("spark.executor.instances", "2")
    .set("spark.executor.cores", "1")
    .setAppName("hw5_task2_sudin")
    .setMaster("yarn")
)
sc = SparkContext(conf=spark_conf)
words = re.compile(r"\w+")
wiki_rdd = sc.textFile("hdfs:///data/wiki/en_articles_part")
stop_words = sc.textFile("hdfs:///data/stop_words/stop_words_en-xpo6.txt")
sw_broadcast = sc.broadcast(stop_words.collect())
clear_wiki = (
    wiki_rdd
    .map(lambda x: x.split('\t', 1))
    .map(lambda pair: pair[1].lower())
    .map(words.findall)
    .map(lambda x: [word for word in x if word not in sw_broadcast.value])
)
clear_wiki.cache()
wiki_bigrams = (
    clear_wiki
    .map(lambda x: [(x[i], x[i + 1]) for i in range(len(x) - 1)])
    .flatMap(lambda content: content)
)
wiki_bigrams.cache()
num_of_bigrams = wiki_bigrams.count()
word_count = (
    clear_wiki
    .flatMap(lambda content: content)
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
    .map(lambda x: (x[0], x[1] / num_of_bigrams))
)
word_count.cache()
bigrams_count = (
    wiki_bigrams
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
    .filter(lambda x: x[1] >= 500)
    .map(lambda x: (x[0][0], x[0][1], x[1] / num_of_bigrams))
)
result_rdd = (
    bigrams_count
    .map(lambda x: (x[0], (x[1], x[2])))
    .join(word_count)
    .map(lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1])))
    .join(word_count)
    .map(lambda x: (x[1][0][0], x[0], x[1][0][1], x[1][0][2], x[1][1]))
    .map(lambda x: (x[0] + "_" + x[1], round(math.log(x[2] / (x[3]*x[4])) / -math.log(x[2]), 3)))
    .sortBy(lambda x: x[1], ascending=False)
    .map(lambda x: x[0] + "\t" + str(x[1]))
)
print(*result_rdd.take(39), sep="\n")
