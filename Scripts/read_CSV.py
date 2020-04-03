

from pyspark.sql import SparkSession

# este ejercicio era para probar que pasa si modificamos el csv quitando un campo, cambiando un numero por HOLA, etc

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
        .options(header='true', inferschema='true') \
        .option("delimiter", ",")\
        .json("/home/master/Descargas/restaurants.json")
    # si ponemos "\t" el limitador seria un tabulador

    data_frame.printSchema()
    data_frame.show()


if __name__ == '__main__':
    main()
