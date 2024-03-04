import unittest
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from src.fr.hymaia.exo4.python_udf import addCategoryName
from spark_test_case import spark



class TestPythonUDF(unittest.TestCase):

    def test_addcategoryname(self):

        input_data = [Row(id=0, date="2019-02-17", category=6, price=40.0),
                      Row(id=1, date="2015-10-01", category=4, price=69.0)]
        input_df = spark.createDataFrame(input_data)

        # Register UDF
        category_name_udf = udf(addCategoryName, StringType())

        # Apply the UDF
        result_df = input_df.withColumn("category_name", category_name_udf(input_df["category"]))

        expected_data = [Row(id=0, date="2019-02-17", category=6, price=40.0, category_name="furniture"),
                         Row(id=1, date="2015-10-01", category=4, price=69.0, category_name="food")]
        expected_df = spark.createDataFrame(expected_data)

        self.assertEqual(result_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()
