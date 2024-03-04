from pyspark.sql.functions import when, col
from pyspark.sql import SparkSession

def addCategoryName(df_sell):
    # Add 'category_name' column based on condition
    df_sell = df_sell.withColumn("category_name",
                                 when(col("category") < 6, "food").otherwise("furniture"))
    return df_sell

def main():
    spark = SparkSession.builder.appName("exo4_no_udf").master("local[*]").getOrCreate()

    df_sell = spark.read.option("header", True).csv("src/resources/exo4/sell.csv")
    df_sell = addCategoryName(df_sell)
    df_sell.show()

if __name__ == "__main__":
    main()
