"""
Taking as data source the file named "tweets.tsv" (attached below), which contains tweets written during a game
of the USA universitary basketball league, write two programas called "tweetsRDD.py/java" and "tweetsDF.py/java"
to carry out the following tasks:

    Search for the 10 most repeated words in the matter field of the tweets and prints on the screen the words and
    their frequency, ordered by frequency. Common words such as "RT", "htttp", etc, should be filtered.
    Find the user how has written the mosts tweets and print the user name and the numer of tweets
    Find the shortest tweet emitted by any user. The program will print in the screen the user name, the length of
    the tweet, and the time and date of the tweet.


The deliverables of this exercise will be the two programs and a capture of their outputs.
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

    stop_words = ("the","RT","in","to","at","with","a","-","for","of"\
                  ,"on","and","will","tomorrow","is","has")

    output = spark_context \
        .textFile(sys.argv[1]) \
        .flatMap(lambda line: line.split("\n")) \
        .map(lambda line: line.split('\t')) \
        .map(lambda line: line[2]) \
        .flatMap(lambda line: line.split(" ")) \
        .filter(lambda word: word not in stop_words) \
        .filter(lambda x: ~x.startswith('http')) \
        .map(lambda word: (word,1)) \
        .reduceByKey(lambda a,b: a+b)

    output2 = output \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(10)

    for (word, count) in output2:
        print("%s: %i" % (word, count))



    print("\n","El usuario que más tweets ha publicado:")

    name=(""," ")

    output3 = spark_context \
        .textFile(sys.argv[1]) \
        .flatMap(lambda line: line.split("\n")) \
        .map(lambda line: line.split('\t')) \
        .map(lambda line: line[1]) \
        .map(lambda word: (word,1)) \
        .reduceByKey(lambda a,b: a+b)

    output4 = output3 \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(1)

    for (word, count) in output4:
        print("%s: %i" % (word, count))




    output5 = spark_context \
        .textFile(sys.argv[1]) \
        .flatMap(lambda line: line.split("\n")) \
        .map(lambda line: line.split('\t')) \
        .map(lambda line: line[2]) \
        .map(lambda line: len(line)).min()

    output6 = spark_context \
        .textFile(sys.argv[1]) \
        .flatMap(lambda line: line.split("\n")) \
        .map(lambda line: line.split('\t')) \
        .filter(lambda line: len(line[2]) == output5) \
        .map(lambda line: (line[1],len(line[2]),line[3])) \

    output6.saveAsTextFile("dataanalysis/prueba.txt")


