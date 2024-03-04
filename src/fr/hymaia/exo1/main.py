import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def wordcount(df, col_name):
    #expression régulière dans split pour gérer les mots entourés de ponctuations
    # \W+ signifie tout caractère qui n'est pas un mot (incluant la ponctuation et les espaces)
    exploded_df = df.withColumn('word', f.explode(f.split(f.col(col_name), r'\W+')))

    # Conversion des mots en minuscules pour une insensibilité à la casse
    lowercase_df = exploded_df.withColumn('word', f.lower(f.col('word')))

    filtered_df = lowercase_df.filter(f.col('word') != '')

    return filtered_df.groupBy('word').count()

def main():

    # set master à local[*] celui qui va gérer les ressources dans l'application(ressource manager)
    # local[*] pour tester qu'on peut bien paraliser contrairement à local

    spark = SparkSession.builder.appName("wordcount").master(
        "local[*]").getOrCreate()

    df = spark.read.option("header", True).csv(
        "/Users/yahiaferchouli/Documents/4IABD2/core-spark/spark-handson/src/resources/exo1/data.csv")

    df_count = wordcount(df, "text")

    df_count.write.partitionBy("count").mode("overwrite").parquet(
        "/Users/yahiaferchouli/Documents/4IABD2/core-spark/spark-handson/data/exo1/output")


if __name__ == "__main__":
    main()


