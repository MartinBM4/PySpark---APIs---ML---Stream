import time

from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession

""" Input data format: libsvm

    0 1:1   2:3
    0 1:1.1 2:2.5
    1 1:5   2:1
    1 1:6   2:1.2
    0 1:2   2:2.5
    ...
"""
if __name__ == "__main__":
    sparkConf = SparkSession\
        .builder\
        .appName("Spark ML KMeans")\
        .getOrCreate()

    # Loads data
    dataset = sparkConf\
        .read\
        .format("libsvm")\
        .load("data/HIGGS")

    dataset.show()

    start_computing_time = time.time()

    # Trains a k-means model
    kmeans = KMeans()\
        .setK(4)\
        .setSeed(1)

    kmeans_model = kmeans.fit(dataset)

    total_computing_time = time.time() - start_computing_time

    print("Sum: ", sum)
    print("Computing time: ", str(total_computing_time))

    # Shows the result
    centers = kmeans_model.clusterCenters()
    print("Cluster centers: ")

    for center in centers:
        print(center)

    kmeans_model.save("KMEANSMODELMLv2")

    print(str(kmeans_model.hasSummary))

    sparkConf.stop()


