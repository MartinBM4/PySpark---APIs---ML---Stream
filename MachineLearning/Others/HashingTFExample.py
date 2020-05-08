from pyspark.ml.feature import Tokenizer, HashingTF
from pyspark.sql import SparkSession

"""

Term frequency-inverse document frequency (TF-IDF) is a feature vectorization 
method widely used in text mining to reflect the importance of a term to a 
document in the corpus. 
"""
if __name__ == "__main__":
    sparkSession = SparkSession\
        .builder\
        .getOrCreate()

    # $example on$
    sentenceDataFrame = sparkSession.createDataFrame([
        (0, "Hi I heard about Spark", 1.0),
        (1, "I wish Java could use case classes", 1.0),
        (2, "Logistic regression models are neat", 0.0)
    ], ["id", "sentence", "label"])

    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

    words_data = tokenizer.transform(sentenceDataFrame)
    words_data.printSchema()
    words_data.show()

    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
    featurized_data = hashingTF.transform(words_data)
    featurized_data.printSchema()
    featurized_data.show(truncate=False)

    sparkSession.stop()
