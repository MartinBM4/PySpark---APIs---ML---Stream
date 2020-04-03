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


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# este ejercicio era para probar que pasa si modificamos el csv quitando un campo, cambiando un numero por HOLA, etc
from pyspark.sql.functions import explode, split


def main() -> None:
    spark_session = SparkSession \
        .builder \
        .master("local[4]") \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    df_airport = spark_session \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("/home/master/Descargas/airportstask4.csv") \
        .persist()


    df_country = spark_session \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("/home/master/Descargas/countries.csv") \
        .persist()


    df_airport.printSchema()
    df_airport.show()
    print()
    df_country.printSchema()
    df_country.show()
    print()



    output2 = df_country \
        .select("code","name")

    output2.printSchema()
    output2.show()


    output1 = df_airport \
        .select("type", "iso_country") \
        .filter(F.col('type').isin("large_airport")) \
        .select("iso_country")

    output1.printSchema()
    output1.show()


    varjoin = output1.join(output2, output1.iso_country == output2.code) \
        .select("name") \
        .groupBy("name") \
        .count() \
        .sort("count", ascending=False).show(10)





if __name__ == '__main__':
    main()