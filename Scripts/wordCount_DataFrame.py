from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# este ejercicio era para probar que pasa si modificamos el csv quitando un campo, cambiando un numero por HOLA, etc
from pyspark.sql.functions import explode, split


def main() -> None:

    # Handler to get access to the Spark Dataframe API
    spark_session = SparkSession \
        .builder \
        .getOrCreate()

    # Logger configuration
    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    data_frame = spark_session\
        .read\
        .text("/home/master/Descargas/quijote.txt")
    # si ponemos "\t" el limitador seria un tabulador

    stop_words = ["que", "de", "y", "la", "el", "en", "los", "del", "a", "con", "le", "mi", "si"]

    df = data_frame.withColumn("value", explode(split("value", " "))) \
        .groupBy("value") \
        .count() \
        .sort("count", ascending=False) \
        .filter(~F.col('value').isin(stop_words)) \
        .show()









if __name__ == '__main__':
    main()