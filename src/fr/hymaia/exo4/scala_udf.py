from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
from time import time


spark = SparkSession.builder.appName("exo4_scla_udf").config('spark.jars', 'src/resources/exo4/udf.jar').master(
        "local[*]").getOrCreate()
sc = spark.sparkContext

def addCategoryName(col, sc):
    # on récupère le SparkContext

    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

def main():


    df_sell_udf_scala = spark.read.option("header", True).csv(
        "src"
        "/resources/exo4/sell.csv")


    df_sell_new_column = df_sell_udf_scala.withColumn("category_name", addCategoryName(col("category"), sc))

    '''start_time = time()
    df_sell_new_column.write.csv("result_scala_udf.csv", header=True)
    end_time = time()
    print("Temp execution scala_udf write", end_time-start_time)'''

    start_time = time()
    df_sell_new_column.show()
    end_time = time()
    print("Temp execution scala_udf show", end_time - start_time)


if __name__ == "__main__":
    main()
