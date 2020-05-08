from pyspark.ml.clustering import KMeansModel
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

if __name__ == "__main__":

    sparkSession = SparkSession\
        .builder\
        .appName("Spark ML KMeans")\
        .getOrCreate()

    model = KMeansModel.load("KMEANSMODELML")
    print("Model loaded")

    # Prepare test data
    test = sparkSession.createDataFrame([
        (1, Vectors.dense([1.1, 3.2])),
        (2, Vectors.dense([5.1, 1.4])),
        (3, Vectors.dense([5.2, 2.0])),
        (4, Vectors.dense([1.0, 4.0]))],
        ["id", "features"])\
        .cache()

    for row in test.collect():
        print(row)

    prediction = model.transform(test)
    prediction.printSchema()
    prediction.show()

    selected = prediction.select("id", "prediction")
    selected.printSchema()
    print(selected)

    for row in selected.collect():
        print(str(row))
        print("(%d) -->  prediction=%f" % (row[0], row[1]))

    sparkSession.stop()
