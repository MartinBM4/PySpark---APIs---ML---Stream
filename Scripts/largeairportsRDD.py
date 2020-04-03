"""
The Web site OurAirports provides open data about international airports. Among the provided data files we can find:

    airports.csv: general information on all airports
    countries.csv: A list of the world's countries. You need this spreadsheet to interpret the .country codes in
    the airports file

This task is intended to find the 10 countries having more airports of the "large" category, displaying on the
screen the country name and the number of airports, ordered in descending order by the number of airports.
The output should look like this:

+--------------+-----+
|          name|count|
+--------------+-----+
| United States|  170|
|         China|   33|
|United Kingdom|   29|
|        Russia|   19|
|         Italy|   18|
|       Germany|   17|
|        Turkey|   14|
|        Canada|   12|
|         Japan|   12|
|         India|   11|
+--------------+-----+

Develop two programs to carry out this task, one called "largeAirportsRDD.py/java" and another
called "largeAirportsDT.py/java".

The deliverable will be composed of the two programs and a screen capture of their outputs.
"""


import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usage: spark-submit largeairportsRDD <file>", file=sys.stderr)
        exit(-1)

    spark_conf = SparkConf().setAppName("filmlocation").setMaster("local[4]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)


    output1 = spark_context \
        .textFile("/home/master/Descargas/countries.csv") \
        .map(lambda line: line.split(',')) \
        .map(lambda line: (line[1],line[2]))

    """"
    He intendado hacer este ejercicio cargando los CSV's como argumento y justamente 
    detras del join me devolvía un array vacío. Después de muchos intentos de cambios absurdos
    en el codigo y con otras preguntas a los compañeros, he conseguido resolverlo.
    
    También me he dado cuenta, que si se realiza "filter(lambda line: line[2].lower().find("large_airport") == True)" antes 
    de ".map(lambda line: (line[8]))" 
    devuelve un array vacio y no se consigue el resultado.
    
    Tampoco me funcionaba el código con el comando startWith dentro del filter.
    """


    output = spark_context \
        .textFile("/home/master/Descargas/airportstask4.csv") \
        .map(lambda line: line.split(',')) \
        .map(lambda line: (line[8],line[2])) \
        .filter(lambda line: line[1].lower().find("large_airport") == True) \
        .join(output1) \
        .map(lambda line: (line[1][1])) \
        .map(lambda line: (line,1)) \
        .reduceByKey(lambda a,b: a+b) \
        .sortBy(lambda pair: pair[1], ascending=False).take(10)

    for (word, count) in output:
        print("%s: %i" % (word, count))









    spark_context.stop()