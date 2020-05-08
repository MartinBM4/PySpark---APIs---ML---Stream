from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession

if __name__ == "__main__":


    spark_session = SparkSession\
        .builder\
        .appName("Spark ML decision tree example")\
        .master("local[4]")\
        .getOrCreate()

    data_frame = spark_session\
        .read\
        .format("libsvm")\
        .load("data/classificationDataLibsvm.txt")

    data_frame.show()

    (training_data, test_data) = data_frame.randomSplit([0.7, 0.3])

    print("training data: " + str(training_data.count()))
    training_data.printSchema()
    training_data.show()

    print("test data: " + str(test_data.count()))
    test_data.printSchema()
    test_data.show()

    decision_tree = DecisionTreeClassifier(labelCol="label", featuresCol="features")

    model = decision_tree.fit(training_data)

    prediction = model.transform(test_data)
    prediction.printSchema()
    prediction.show()

    prediction\
        .select("prediction", "label", "features")\
        .show(5)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(prediction)
    print("Test Error = %g " % (1.0 - accuracy))

    spark_session.stop()

