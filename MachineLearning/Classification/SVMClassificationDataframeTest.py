from pyspark.ml.classification import LinearSVCModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark_session = SparkSession \
        .builder \
        .appName("Spark ML SVM") \
        .master("local[4]") \
        .getOrCreate()

    model = LinearSVCModel.load("SVMModel")
    print("Model loaded")

    test = spark_session.createDataFrame([
        (0, Vectors.dense([1.0, 1.2])),
        (1, Vectors.dense([5.3, 2.4])),
        (0, Vectors.dense([1.2, 1.3])),
        (1, Vectors.dense([5.1, 2.3]))],
        ["label", "features"]) \
        .cache()

    for row in test.collect():
        print(row)

    test.printSchema()
    test.show()

    prediction = model.transform(test)

    prediction.printSchema()
    prediction.show()

    # Model evaluation
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(prediction)
    print("Test Error = %g " % (1.0 - accuracy))

    spark_session.stop()
