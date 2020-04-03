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

    data_frame = spark_session \
        .read \
        .options(header='false', inferschema='true') \
        .option("delimiter", "\t") \
        .csv("/home/master/Descargas/tweets.tsv") \
        .persist()

    data_frame.printSchema()
    data_frame.show()

    stop_words = ["the","RT","in","to","at","with","a","-","for","of"\
                  ,"on","and","will","tomorrow","is","has","updated","set"]


    print("10 Trending topic: ")
    df1 = data_frame.withColumn("value", explode(split("_c2", " "))) \
        .groupBy("value") \
        .count() \
        .sort("count", ascending=False) \
        .filter(~F.col('value').isin(stop_words)) \
        .show(10)



    print("Most number tweeted: ")
    df2 = data_frame.groupBy("_c1")\
        .count()\
        .sort("count", ascending=False)\
        .show(1)


    print("Shortest tweet: ") #em legnth ponemos value o _c2
    df3 = data_frame \
        .withColumn("length",F.length('_c2')) \
        .sort("length", ascending=True) \
        .select("_c1","length","_c3") \
        .show(1)



if __name__ == '__main__':
    main()