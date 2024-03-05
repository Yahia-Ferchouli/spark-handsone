import unittest
from spark_test_case import spark
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.fr.hymaia.exo2.spark_aggregate_job import count_population
from pyspark.sql.functions import col

class SparkJobTests(unittest.TestCase):

    schema_input = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("zip", StringType(), True),
        StructField("city", StringType(), True),
        StructField("departement", StringType(), True)
    ])

    schema_expected = StructType([
        StructField("departement", StringType(), True),
        StructField("nb_people", IntegerType(), True),

    ])

    def create_expected_df(self, data):
        return spark.createDataFrame(data, self.schema_expected).sort(
            col("nb_people").desc(), "departement")

    def test_count_population(self):
        data = [
            Row(name="Cussac", age=27, zip="75020", city="Paris",
                departement="75"),
            Row(name="Titi", age=35, zip="20200", city="Bastia",
                departement="2B"),
            Row(name="Tata", age=40, zip="75008", city="Paris",
                departement="75"),
            Row(name="Toto", age=45, zip="33120", city="Arcachon",
                departement="33"),
            Row(name="Dudu", age=55, zip="75016", city="Paris",
                departement="75"),
            Row(name="Mimi", age=25, zip="13002", city="Marseille",
                departement="13"),
            Row(name="Fifi", age=37, zip="33000", city="Bordeaux",
                departement="33"),
            Row(name="Loulou", age=29, zip="20200", city="Bastia",
                departement="2B")
        ]

        df = spark.createDataFrame(data, self.schema_input)

        df_result = count_population(df)


        expected_data = [
            Row(departement="13", nb_people=1),
            Row(departement="2B", nb_people=2),
            Row(departement="33", nb_people=2),
            Row(departement="75", nb_people=3)
        ]

        df_expected = self.create_expected_df(expected_data)

        self.assertEqual(df_result.schema(), df_expected.schema())

        self.assertEqual(df_result.collect(), df_expected.collect())


    def test_count_population_with_nulls(self):
        data = [
            Row(name="Cussac", age=27, zip="75020", city="Paris",
                departement="75"),
            Row(name="Titi", age=35, zip="20200", city="Bastia",
                departement=None),
            Row(name="Tata", age=40, zip="75008", city="Paris",
                departement="75"),
        ]
        df = spark.createDataFrame(data, self.schema_input)
        df_result = count_population(df).sort("departement")

        expected_data = [
            Row(departement="75", nb_people=2)
        ]
        df_expected = self.create_expected_df(expected_data)

        self.assertEqual(df_result.schema(), df_expected.schema())

        self.assertEqual(df_result.collect(), df_expected.collect())

    def test_count_population_empty_df(self):
        data = []
        df = spark.createDataFrame(data, self.schema_input)
        df_result = count_population(df)

        self.assertTrue(df_result.collect() == [])

    def test_count_population_sorting(self):
        data = [
            Row(name="Toto", age=45, zip="33120", city="Arcachon",
                departement="33"),
            Row(name="Titi", age=35, zip="20200", city="Bastia",
                departement="2B"),
            Row(name="Tata", age=40, zip="75008", city="Paris",
                departement="75"),
            Row(name="Toto", age=18, zip="75006", city="Paris",
                departement="75")
        ]
        df = spark.createDataFrame(data, self.schema_input)
        df_result = count_population(df)

        expected_data = [
            Row(departement="33", nb_people=1),
            Row(departement="75", nb_people=2),
            Row(departement="2B", nb_people=1)
        ]

        df_expected = self.create_expected_df(expected_data)

        self.assertEqual(df_result.schema(), df_expected.schema())

        self.assertEqual(df_result.collect(), df_expected.collect())

if __name__ == '__main__':
    unittest.main()
