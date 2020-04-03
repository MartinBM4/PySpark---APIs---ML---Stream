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
        .options(header='false', inferschema='true') \
        .option("delimiter", ",")\
        .csv("/home/master/Descargas/numbers.txt")
    # si ponemos "\t" el limitador seria un tabulador

    data_frame.printSchema()
    data_frame.show()


    from pyspark.sql import functions as F

    data = data_frame.select(F.sum("_c0")).collect()
    data2 = data_frame.select("_c0").groupBy().sum().collect()

    print("La suma es: " + str(data[0][0]))
    print("La suma es: " + str(data2[0][0]))


if __name__ == '__main__':
    main()
