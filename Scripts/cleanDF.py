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




if __name__ == '__main__':
    main()