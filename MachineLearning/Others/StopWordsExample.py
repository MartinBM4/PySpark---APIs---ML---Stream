from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sparkSession = SparkSession\
        .builder\
        .getOrCreate()

    sentenceData = sparkSession.createDataFrame([
        (0, ["I", "saw", "the", "red", "balloon"]),
        (1, ["Mary", "had", "a", "little", "lamb"])
    ], ["id", "raw"])

    StopWordsRemover.loadDefaultStopWords("english")

    remover = StopWordsRemover(inputCol="raw", outputCol="filtered")
    remover.transform(sentenceData).show(truncate=False)

    sparkSession.stop()
