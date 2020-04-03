"""
Typically, data to be analyzed in real world applications is not fully clean. Frequently, there are missing
fields, invalid values, etc.


A civil engineer is working in the design of a bridge, trying to find different alternatives, each of them
having a total bridge weight and the degree of deformation in certain parts
(e.g., see http://ebesjmetal.sourceforge.net/problems.html). After using an optimization software, she/he has
obtained a .txt file (attached to this task) with a number of rows indicating different trade-off designs.


Unfortunately, some lines/fields have invalid values (blank lines, missing values, characters instead of
numbers, etc), and there are also repeteated lines.


This task consists in developing a Jupyter notebook with PySpark to read the file, remove all the invalid lines
and remove those that are appears more than one time, and plot the clean data.


The deliverable will be the Jupyter notebook.
"""



import sys

import matplotlib.pyplot as plt
import numpy as np
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit filmlocation <file>", file=sys.stderr)
        exit(-1)

    spark_conf = SparkConf().setAppName("filmlocation").setMaster("local[4]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)


    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False


    """
    Cometía el error de filtrar primero por [1] y despues por [0], y daba error 
    porque hay algunas lineas que vienen enteras vacias, por lo que el índice [1]
    se salía del rango.
    También, hay que hacer todos los filtros necesarios a la columna [0] y despues 
    a la columna [1], si no el collect no funciona.
    """


    output = spark_context \
        .textFile(sys.argv[1]) \
        .map(lambda line: line.split("\n")) \
        .map(lambda line: line[0].split(",")) \
        .filter(lambda line: line is not None) \
        .filter(lambda line: line != "") \
        .filter(lambda line: is_number(line[0])) \
        .filter(lambda line: is_number(line[1]))


    t = output.map(lambda line: float(line[0])).collect()
    s = output.map(lambda line: float(line[1])).collect()


    plt.plot(t, s, 'ro')
    plt.show()