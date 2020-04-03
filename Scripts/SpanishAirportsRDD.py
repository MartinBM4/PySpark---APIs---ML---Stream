"""
The Web site OurAirports provides open data about international airports.
The data file can be downloaded from this link: airports.csv. This is a CSV file containing,
among other information, the following fields:

- type: the type of the airport ("heliport", "small_airport", "large_airport", "medium_airport", etc)

- iso_country: the code of Spain is "ES".


Taking as example the WordCount program, write two versions of a RDD-based Spark program called
"SpanishAirports" (SpanishAirports.java and SpanishAirports.py) that read the airports.csv file
and counts the number of spanish airports for each of the four types, writing the results in a
text file.


The program outputs should look similar to this:


"small_airport": 291
"heliport": 60
"medium_airport": 41
"closed": 13
"large_airport": 10
"seaplane_base": 1



The deliverables of this exercise will be three files, two containing the source codes and another
one with the output file.
"""



import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit spanishairports <file>", file=sys.stderr)
        exit(-1)

    spark_conf = SparkConf().setAppName("SpanishAirports").setMaster("local[4]") #local 4 = number of Cores will work (4 cores)
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)



    output = spark_context \
        .textFile(sys.argv[1]) \
        .map(lambda line: line.split(',')) \
        .filter(lambda line: line[8] == '"ES"') \
        .map(lambda line: (line[2], 1)) \
        .reduceByKey(lambda a, b: a + b)


    output.saveAsTextFile("data/type_airports_count.txt")

    for (word, count) in output.collect():
        print("%s: %i" % (word, count))



    spark_context.stop()








"""     *__this way won't work__*

        de esta forma no he conseguido que funcione:

def main() -> None:
            +
    (configuracion spark)
            +
    output = spark_context \
        .textFile("/home/master/Descargas/airports.csv") \          <---
        .map(lambda line: line.split(",")) \
        .filter(lambda line: line[8] == '"ES"') \
        .map(lambda word: (word[2],1)) \
        .reduceByKey(lambda a,b: a + b)
        
    if __name__ == '__main__':
    main()

"""

