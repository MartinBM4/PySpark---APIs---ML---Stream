import sys

from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit wordcount <file>", file=sys.stderr)
        exit(-1)

    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    stop_words = ("que", "de", "y", "la", "el", "en", "los", "del", "a", "con", "le", "mi", "si")

    output = spark_context \
        .textFile(sys.argv[1]) \
        .flatMap(lambda line: line.split(' ')) \
        .filter(lambda word: word not in stop_words) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False)\
        .take(20)

    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark_context.stop()


