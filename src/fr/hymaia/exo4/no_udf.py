from pyspark.sql.functions import when, col
from pyspark.sql import SparkSession, functions as f, Window
from time import time

def addCategoryName(df_sell):
    # Add 'category_name' column based on condition
    df_sell = df_sell.withColumn("category_name",
                                 when(col("category") < 6, "food").otherwise("furniture"))
    return df_sell


def calculate_total_price_per_category_per_day(df):
    window_spec = Window.partitionBy("category", "date")
    df = df.withColumn("total_price_per_category_per_day",
                       f.sum("price").over(window_spec))
    df = df.dropDuplicates(['date', "category_name", "total_price_per_category_per_day"])
    return df


def calculate_total_price_per_category_per_day_last_30_days(df):
    df = df.dropDuplicates(['date',"category_name"])
    window_spec = Window.partitionBy("category_name").orderBy("date").rowsBetween(-29, 0)
    df = df.withColumn("total_price_per_category_per_day_last_30_days", f.sum(
        "price").over(window_spec))
    return df.select("id", "date", "category", "price", "category_name", "total_price_per_category_per_day_last_30_days")

def main():
    spark = SparkSession.builder.appName("exo4_no_udf").master("local[*]").getOrCreate()

    df_sell = spark.read.option("header", True).csv("src/resources/exo4/sell.csv")
    df_sell = addCategoryName(df_sell)

    calculate_total_price_per_category_per_day(df_sell)

    '''start_time = time()
    df_sell.write.csv("result_no_udf.csv", header=True)
    end_time = time()
    print("Temp execution no_udf write", end_time - start_time)'''

    start_time = time()
    df_sell.show()
    end_time = time()
    print("Temp execution no_udf show", end_time - start_time)

if __name__ == "__main__":
    main()
