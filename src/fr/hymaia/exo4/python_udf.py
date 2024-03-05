from pyspark.sql.functions import udf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, IntegerType
from time import time

def addCategoryName(category):
    category = int(category)  # Cast to integer
    if category < 6:
        return "food"
    else:
        return "furniture"



def main():

    spark = SparkSession.builder.appName("exo4_python_udf").master(
        "local[*]").getOrCreate()
    df_sell = spark.read.option("header", True).csv(
        "src/resources/exo4/sell.csv")

    # Register UDF
    category_name_udf = udf(addCategoryName, StringType())

    # Apply UDF
    df = df_sell.withColumn("category_name", category_name_udf(df_sell["category"]))

    '''start_time = time()
    df.write.csv("result_python_udf.csv", header=True)
    end_time = time()
    print("Temp execution python_udf write", end_time - start_time)'''

    start_time = time()
    df.show()
    end_time = time()
    print("Temp execution python_udf show", end_time - start_time)

if __name__ == "__main__":
    main()
