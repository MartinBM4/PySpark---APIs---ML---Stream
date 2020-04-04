"""
The World Bank offers a large amount of open data. In this exercise we will focus on the percentage of population
having access to electricity in all the countries of the world from 1960 to 2014.  The data can be obtained from this
link: https://data.worldbank.org/indicator/EG.ELC.ACCS.ZS. After selecting the CSV format you will get a zip file
containing, among others, these two files:

    API_EG.ELC.ACCS.ZS_DS2_en_csv_v2.csv

    Metadata_Country_API_EG.ELC.ACCS.ZS_DS2_en_csv_v2.csv

After taking a look at the content of these files, write a program named electricityAnalysis.py/java which, given a
range of years (e.g., 1970 and 1980), prints on the screen the average number of countries of each of the regions
(South Asia, North America, etc.) having at least 99% of electricity for all the years between the indicated range.
The output must be ordered in descendent order by the percentage value, and it should look like this example
(note: these data are invented):



Years: 1970 - 1980


    Region                   Percentage

-------------------        --------------

North America                  100%

Europe & Central Asia           99%

...


You have to take into accout that the two files have headers and some lines might have missing fields.


Developing a notebook solution to the exercise is an optional task that will be considered as a plus over the maximum grade.


The deliverables of this exercise will be the program file and the screen capture of its output.
"""


from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import *
import pyspark.sql.functions as F
from functools import reduce


if __name__ == "__main__":

    spark_session = SparkSession \
        .builder \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    """
    Si no definimos una estructura para las columnas del data frame, despues hay que cambiarlas 
    una a una. Porque en algunas columnas asigna string cuando deberia de ser double.
    """

    struct_df = [StructField(name="Country", dataType=StringType(), nullable=False),
                   StructField(name="Code", dataType=StringType(), nullable=False),
                   StructField(name="IndicatorName", dataType=StringType(), nullable=False),
                   StructField(name="IndicatorCode", dataType=StringType(), nullable=False)]

    for years in range(1960, 2019):
        struct_df.append(StructField(name="_" + str(years), dataType=DoubleType(), nullable=False))

    schemadf = StructType(struct_df)


    """
    Cargamos los CSV teniendo en cuenta el schema predefinido anteriormente
    """
    elec_accessdf = spark_session \
        .read \
        .csv(path="/home/master/data/API_EG.ELC.ACCS.ZS_DS2_en_csv_v2_889816.csv",
             sep=",",
             header="false",
             schema=schemadf)

    countrydf = spark_session \
        .read \
        .csv(path="/home/master/data/Metadata_Country_API_EG.ELC.ACCS.ZS_DS2_en_csv_v2_889816.csv",
             sep=",",
             header="true")

    """
    Unimos los dos dataframe por la clave en comun (Code - Country Code)
    """
    elec_countrydf = elec_accessdf.join(countrydf, elec_accessdf["Code"] == countrydf["Country Code"])

    """
    Ponemos los años que queremos que nos muestre los porcentajes, en este caso de 1980 a 2009
    """
    years2analise = range(1980, 2010)           # 2010 - 1 = 2009


    """
    Necesitamos crear el filtro para cribar aquellos años no que tengan un porcentaje mayor que 99%
    """
    filter99 = "_" + str(years2analise[-1] + 1) + " >= 99"


    """
    Calculamos aquellos que superen el 99%
    """
    filtered99_df = elec_countrydf \
        .filter(filter99) \
        .groupBy("Region") \
        .count() \
        .withColumnRenamed("count", "Country_filtered") \
        .sort("Country_filtered", ascending=False)      #se necesita cambiar el nombre de la columna "count" porque
                                                        # posteriormente necesitamos usarla y si se llama "count" para
                                                        # acceder a ella hay qe poner dataframeNAME.count y esto da error
                                                        # de referencias.
    """
    Calculamos el total para sacar el porcentaje
    """
    total_df = elec_countrydf \
        .groupBy("Region") \
        .count() \
        .withColumnRenamed("count", "Country_total") \
        .sort("Country_total", ascending=False)         #se necesita cambiar el nombre de la columna "count" porque
                                                        # posteriormente necesitamos usarla y si se llama "count" para
                                                        # acceder a ella hay qe poner dataframeNAME.count y esto da error
                                                        # de referencias.
    """
    Unimos los dataframes anteriormente calculados (total_df y filtered99_df) para poder calcular el porcentaje,
    ya que necesitaremos los que superen 99% y el total
    """
    totalandfiltered = total_df.join(filtered99_df, "Region")

    """
    Calculamos el porcentaje
    """
    output = totalandfiltered \
        .select("Region", (100 * filtered99_df.Country_filtered / total_df.Country_total).alias("Percentage")) \
        .sort("Percentage", ascending=False) \
        .select("Region", F.format_string("%2.0f%%", functions.col("Percentage")).alias("Percentage"))

    """
    Y lo mostramos por pantalla
    """
    output.show(truncate=False)



    print("The result is considered between: " +
          str(years2analise[0]) + " - " +
          str(years2analise[len(years2analise)-1]))
    print()

    spark_session.stop()


    #df___.show(truncate=False) -> para mostrar cualqiera de los dataframe que hemos ido creando