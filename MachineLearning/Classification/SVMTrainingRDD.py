import sys

from pyspark import SparkConf, SparkContext
from pyspark.mllib.classification import SVMModel, SVMWithSGD
from pyspark.mllib.regression import LabeledPoint

""" Input data format:

    0 2.228387042742021 2.228387042742023 0 0 0 0 0 0   
    1 0 0 0 2.619965104088255 0 2.004684436494304 2.000347299268466 0 2.228387042742
    ...
"""

def parse_point(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])


if __name__ == "__main__":
    sparkConf = SparkConf()
    sparkContext = SparkContext(conf=sparkConf)

    sparkContext.setLogLevel("OFF")

    # Load and parse the data
    data = sparkContext \
        .textFile("data/classificationData.txt")

    parsed_data = data \
        .map(parse_point) \
        .cache()

    # Build the model
    model = SVMWithSGD.train(parsed_data, iterations= 100)

    # Save and load model
    model.save(sparkContext, "SVMModel")
    sameModel = SVMModel.load(sparkContext, "SVMModel")