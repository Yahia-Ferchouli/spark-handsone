import unittest
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from src.fr.hymaia.exo4.no_udf import addCategoryName
from spark_test_case import spark



class TestNoUDF(unittest.TestCase):

    def test_addcategoryname(self):
        input_data = [Row(id=0, date="2019-02-17", category=6, price=40.0),
                      Row(id=1, date="2015-10-01", category=4, price=69.0)]
        input_df = spark.createDataFrame(input_data)

        # Apply the function to the entire DataFrame
        result_df = addCategoryName(input_df)

        expected_data = [Row(id=0, date="2019-02-17", category=6, price=40.0,
                             category_name="furniture"),
                         Row(id=1, date="2015-10-01", category=4, price=69.0,
                             category_name="food")]
        expected_df = spark.createDataFrame(expected_data)

        self.assertEqual(result_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()
