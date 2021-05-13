from pyspark.sql.functions import *
contentRDD =spark.read.text('/tesco_uk/Lev1/cmp/sample/dummyfile.txt')
# count a word in whole text file
x = contentRDD.rdd.map(lambda x: x[0].split(' ').count('human')).sum()
for t in x.collect():
    print t
# count a word in each line
k = contentRDD.rdd.Map(lambda x: x[0].split(' ').count('human'))

#without RDD
#check the word in each line
wordcntudf = udf(lambda s, word: s.split(' ').count(word))
contentRDD.withColumn('wordcount', wordcntudf(col('value'), lit('is'))).show()
# Check in whole file
contentRDD.withColumn('wordcount', wordcntudf(col('value'), lit('is'))).select(sum(col('wordcount'))).show()

##############
contentRDD.rdd.map(lambda x : [((x, i), 1) for i in x[0].split(' ')]) \
.flatMap(lambda x : x) \
.reduceByKey(lambda x, y : x + y) \
.sortByKey(False) \
.map(lambda x : (x[0][1], x[1])) \
.collect()

#############Count the word in whole file ################
import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # create Spark context with necessary configuration
    sc = SparkContext("local", "PySpark Word Count Exmaple")

    # read data from text file and split each line into words
    words = sc.textFile("D:/workspace/spark/input.txt").flatMap(lambda line: line.split(" "))

    # count the occurrence of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    # save the counts to output
    wordCounts.saveAsTextFile("D:/workspace/spark/output/")

# count the word line by line
contentRDD.rdd.map(lambda x: list((i, x.split(' ').count(i)) for i in set(x.split(' ')))).collect()