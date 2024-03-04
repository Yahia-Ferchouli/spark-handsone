from pyspark.sql.functions import col
from pyspark.sql import SparkSession

def count_population(df):
    df_filtered = df.filter(col("departement").isNotNull())

    df_output = df_filtered.groupBy("departement") \
                           .count() \
                           .withColumnRenamed("count", "nb_people")

    return df_output.sort(col("nb_people").desc(), "departement")

def main():
    spark = SparkSession.builder.appName("exo2_second_job").master(
        "local[*]").getOrCreate()

    df = spark.read.parquet('/Users/yahiaferchouli/Documents/4IABD2/core-spark/spark-handson/data/exo2/clean')

    df_output = count_population(df)

    df_output.write.mode("overwrite").csv("/Users/yahiaferchouli/Documents/4IABD2/core-spark/spark-handson/data/exo2/aggregate")


if __name__ == "__main__":
    main()
