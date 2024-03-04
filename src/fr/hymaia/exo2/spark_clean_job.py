from pyspark.sql.functions import when, col
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def create_departement(df):
    return df.withColumn("departement",
                         when(col("zip").isNull(),
                              None)  # Ajout de cette ligne pour gérer les valeurs NULL
                         .when(col("zip").rlike("^[0-9]{5}$"),
                               # Vérifier si le code postal a 5 chiffres
                               when((col("zip").substr(1, 2) == "20") &
                                    (col("zip").substr(3, 5).cast(
                                        IntegerType()) <= 190), "2A")
                               .when((col("zip").substr(1, 2) == "20") &
                                     (col("zip").substr(3, 5).cast(
                                         IntegerType()) > 190), "2B")
                               .otherwise(col("zip").substr(1, 2)))
                         .otherwise(None))  # Code postal non valide

def main():
    spark = SparkSession.builder.appName("exo2").master(
        "local[*]").getOrCreate()

    df_client = spark.read.option("header", True).csv(
        "/Users/yahiaferchouli/Documents/4IABD2/core-spark/spark-handson/src"
        "/resources/exo2/clients_bdd.csv")
    df_city = spark.read.option("header", True).csv(
        "/Users/yahiaferchouli/Documents/4IABD2/core-spark/spark-handson/src"
        "/resources/exo2/city_zipcode.csv")

    df_client = df_client.filter("age >= 18")

    df_joined = df_client.join(df_city, df_client["zip"] == df_city["zip"],
                               "left").select(df_client.name, df_client.age,
                                              df_client.zip, df_city.city)

    df_joined = create_departement(df_joined)


    df_joined.write.mode("overwrite").parquet(
        "/Users/yahiaferchouli/Documents/4IABD2/core-spark/spark-handson"
        "/data/exo2/clean")


if __name__ == "__main__":
    main()
