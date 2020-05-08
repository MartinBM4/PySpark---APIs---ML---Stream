import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructField, DoubleType, StringType, TimestampType, StructType


def main(directory) -> None:
    """ Program that reads temperatures in streaming from a directory, computing the average temperature.

    It is assumed that an external entity is writing files in that directory, and every file contains a
    temperature value, the name of the sensor and a timestamp

    :param directory: streaming directory
    """
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("StreamingAverageTemperatureV2") \
        .getOrCreate()

    fields = [StructField("sensor", StringType(), True),
              StructField("value", DoubleType(), True),
              StructField("timestamp", TimestampType(), True)]

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("csv") \
        .schema(StructType(fields)) \
        .load(directory) 

    lines.printSchema()

    # Compute the average temperature
    values = lines\
        .groupBy(lines.sensor)\
        .agg(functions.mean("value").alias("mean"))

    values.printSchema()

    # Start running the query that prints the output in the screen
    query = values \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit StreamingAverageTemperatureV2 <directory>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
