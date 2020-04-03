from pyspark.sql import SparkSession, functions

"""
Name,Age,Weight,HasACar,BirthDate
Luis,23,84.5,TRUE,2019-02-29
Lola,42,70.2,false,2000-10-01
Paco,66,90.1,False,1905-12-03
Manolo,68,75.3,true,2000-01-04
"""


def main() -> None:
    spark_session = SparkSession \
        .builder \
        .master("local[4]") \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    data_frame = spark_session \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("data/simple.csv") \
        .persist()

    data_frame.printSchema()
    data_frame.show()

    # Print the data types of the data frame
    print("data types: " + str(data_frame.dtypes))

    # Describe the dataframe
    data_frame \
        .describe() \
        .show()

    # Explain the dataframe
    data_frame \
        .explain()

    # Select everything
    data_frame.select("Name").show()

    # Select columns name and age, but adding 1 to age
    data_frame.select("Name", data_frame["Age"] + 1) \
        .show()

    # Select the rows having a name length > 4
    data_frame.select(functions.length(data_frame["Name"]) > 4).show()

    # Select names staring with L
    data_frame.select("name", data_frame["name"].startswith("L")) \
        .show()

    # Add a new column "Senior" containing true if the person age is > 45
    data_frame.withColumn("Senior", data_frame["Age"] > 45) \
        .show()

    # Rename column HasACar as Owner
    data_frame.withColumnRenamed("HasACar", "Owner") \
        .show()

    # Remove column DateBirth
    data_frame.drop("BirthDate") \
        .show()

    # Sort by age
    data_frame.sort(data_frame.Age.desc()).show()
    data_frame.sort("Age", ascending=False).show()

    data_frame.orderBy(["Age", "Weight"], ascending=[0, 1]).show()

    # Get a RDD
    rdd_from_dataframe = data_frame \
        .rdd \
        .persist()

    for i in rdd_from_dataframe.collect():
        print(i)

    # Sum all the weights (RDD)
    sum_of_weights = rdd_from_dataframe \
        .map(lambda row: row[2]) \
        .reduce(lambda x, y: x + y)  # sum()
    print("Sum of weights (RDDs): " + str(sum_of_weights))

    # Sum all the weights (dataframe)
    weights = data_frame \
        .select("Weight") \
        .groupBy() \
        .sum() \
        .collect()

    print(weights)
    print("Sum of weights (dataframe): " + str(weights[0][0]))

    data_frame.select(functions.sum(data_frame["Weight"])).show()
    data_frame.agg({"Weight": "sum"}).show()

    # Get the mean age (RDD)
    total_age = rdd_from_dataframe \
        .map(lambda row: row[1]) \
        .reduce(lambda x, y: x + y)

    mean_age = total_age / rdd_from_dataframe.count()

    print("Mean age (RDDs): " + str(mean_age))

    # Get the mean age (dataframe)
    data_frame.select(functions.avg(data_frame["Weight"])) \
        .withColumnRenamed("avg(Weight)", "Average") \
        .show()

    data_frame.agg({"Weight": "avg"}).show()
    """
    # Write to a json file
    data_frame\
        .write\
        .save("output.json", format="json")

    # Write to a CSV file
    data_frame\
        .write\
        .format("csv")\
        .save("output.csv")
    """

if __name__ == '__main__':
    main()
