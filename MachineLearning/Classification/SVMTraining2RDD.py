import sys

from pyspark import SparkConf, SparkContext
from pyspark.mllib.classification import SVMModel, SVMWithSGD
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils

""" Input data format: libsvm"""


def score_function(data, model):
    print(str(data.features))
    score = float(model.predict(data.features))
    print("Score: " + str(score) + ". Label: " + str(data.label))
    return score, data.label


if __name__ == "__main__":

    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    spark_context.setLogLevel("OFF")

    parsed_data = MLUtils\
        .loadLibSVMFile(spark_context, "data/classificationdata.txt")\
        .cache()

    print("Parsed data size: " + str(parsed_data.count()))

    # Split initial RDD into two... [60% training data, 40% testing data]
    training, test = parsed_data\
        .randomSplit([0.6, 0.4], seed=3)

    print("Training points size: " + str(training.count()))
    print("Test points size    : " + str(test.count()))

    # Build the model
    model = SVMWithSGD.train(training, iterations= 100)

    score_and_labels = test.map(lambda point: score_function(point, model))

    #for score, label in score_and_labels.collect():
    #    print("Score: %d, label: %f" % (score, label))

    # Get evaluation metrics
    metrics = BinaryClassificationMetrics(score_and_labels)
    auROC = metrics.areaUnderROC
    print("Area under ROC: %f" % auROC)

    # Save and load model
    model.save(spark_context, "SVMModel3")
    sameModel = SVMModel.load(spark_context, "SVMModel2")

    spark_context.stop()