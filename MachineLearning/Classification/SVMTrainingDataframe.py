from pyspark.ml.classification import LinearSVC
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark_session = SparkSession\
        .builder\
        .appName("Spark SVM")\
        .getOrCreate()

    # Loads data
    dataset = spark_session\
        .read\
        .format("libsvm")\
        .load("data/classificationDataLibsvm.txt")

    dataset.printSchema()
    dataset.show()

    linear_SVM = LinearSVC(maxIter=10, regParam=0.1)

    svm_model = linear_SVM.fit(dataset)

    print("Coefficients: " + str(svm_model.coefficients))
    print("Intercept: " + str(svm_model.intercept))

    svm_model.save("SVMModel")

    spark_session.stop()


