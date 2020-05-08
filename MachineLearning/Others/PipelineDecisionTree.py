from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sparkSession = SparkSession\
        .builder\
        .getOrCreate()

    # Load the data stored in LIBSVM format as a DataFrame.
    data_frame = sparkSession\
        .read\
        .format("libsvm")\
        .load("data/simple_libsvm_data.txt")

    data_frame.printSchema()
    data_frame.show()

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel")\
        .fit(data_frame)

    # Automatically identify categorical features, and index them.
    # We specify maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer = \
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4)\
            .fit(data_frame)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data_frame.randomSplit([0.7, 0.3])

    # Train a DecisionTree model.
    decision_tree = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

    # Chain indexers and tree in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, decision_tree])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    model.save("PIPELINE")

    loaded_mode = PipelineModel.load("PIPELINE")

    # Make predictions.
    predictions = loaded_mode.transform(testData)

    # Select example rows to display.
    predictions\
        .select("prediction", "indexedLabel", "features")\
        .show(5)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g " % (1.0 - accuracy))

    treeModel = model.stages[2]

    # summary only
    print(treeModel)

    sparkSession.stop()
