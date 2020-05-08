import sys

from numpy import array
from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import KMeansModel

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Model directory missing", file=sys.stderr)
        exit(-1)

    sparkConf = SparkConf()
    sparkContext = SparkContext(conf=sparkConf)

    sparkContext.setLogLevel("OFF")

    model = KMeansModel.load(sparkContext, path=sys.argv[1])

    print("Model loaded")

    point = array([1.1, 3.2])
    print("Point 1: " + str(point))
    print("Predict: " + str(model.predict(point)))

    point = array([5.1, 1.4])
    print("Point 2: " + str(point))
    print("Predict: " + str(model.predict(point)))

    point = array([5.2, 2.0])
    print("Point 3: " + str(point))
    print("Predict: " + str(model.predict(point)))

    point = array([1.0, 4.0])
    print("Point 4: " + str(point))
    print("Predict: " + str(model.predict(point)))


    sparkContext.stop()
