"""
San Francisco's open data portal provides a Web site with data on the city's film sites.
The data can obtained as a CSV.


This exercise is about developing a RDD-based Spark program in Java/Python that finds those films
with 20 or more locations and print them on the screen. Furthermore, the program must print the number
of films and the average of locations per flim. A typical output will be as follows:

(58, Murder in the First)
(30, Chance Season 2)
(29, The Dead Pool)
(28, Etruscan Smile)
(28, Blue Jasmine)
Total number of films: 309
The average of film locations per film: 5.333333



The deliverables of this exercise will be three files, two containing the source codes and another one
with the output file.
"""

import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit filmlocation <file>", file=sys.stderr)
        exit(-1)

    spark_conf = SparkConf().setAppName("filmlocation").setMaster("local[4]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    #se ha supuesto que el numero de veces que sale el titulo en el CSV es el numero
    # de localizaciones diferentes que ha participado cada pelicula

    #no se ha comprobado si se repiten las localizaciones
    output1 = spark_context \
        .textFile(sys.argv[1]) \
        .map(lambda line: line.split(',')) \
        .map(lambda word: (word[0],1)) \
        .reduceByKey(lambda a,b: a+b) \
        .filter(lambda x: x[1] > 20) \

    output2 = output1 \
        .sortBy(lambda pair: pair[1], ascending=False)\
        .take(5)

    for (word, count) in output2:
        print("%s: %i" % (word, count))


    output3 = spark_context \
        .textFile(sys.argv[1]) \
        .map(lambda line: line.split(',')) \
        .map(lambda word: (word[0],1)) \
        .reduceByKey(lambda a, b: a) \
        .count()

    print("Total number of films: " + str(output3))

    output4 = spark_context \
        .textFile(sys.argv[1]) \
        .map(lambda line: line.split(',')) \
        .map(lambda word: (word[0],1)) \
        .reduceByKey(lambda a,b: a+b) \
        .map(lambda x: x[1]).sum()

    print("The average of film location per film: " + str(output4/output3))

