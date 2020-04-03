from pyspark.sql import SparkSession


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
        .csv("/home/master/Descargas/Film_Locations_in_San_Francisco.csv")

    data_frame.printSchema()
    data_frame.show()


if __name__ == '__main__':
    main()
