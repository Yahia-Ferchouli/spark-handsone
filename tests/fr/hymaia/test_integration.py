import unittest
from pyspark.sql import Row
from src.fr.hymaia.exo1.main import wordcount
from src.fr.hymaia.exo2.spark_clean_job import create_departement
from src.fr.hymaia.exo2.spark_aggregate_job import count_population
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
from spark_test_case import spark


class IntegrationTest(unittest.TestCase):
    schema_input_count_population = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("zip", StringType(), True),
        StructField("city", StringType(), True),
        StructField("departement", StringType(), True)
    ])

    schema_expected_count_population = StructType([
        StructField("departement", StringType(), True),
        StructField("nb_people", IntegerType(), True),

    ])

    def test_integration(self):
        # Préparation des données d'entrée pour chaque fonction
        wordcount_data = [
            Row(text="chat chat chien"),
            Row(text="chien chien chien"),
            Row(text="chat chien cafard")
        ]

        departement_data = [
            Row(zip="75001"),
            Row(zip="20190"),
            Row(zip="12345")
        ]

        population_data = [
            Row(name="Cussac", age=27, zip="75020", city="Paris", departement="75"),
            Row(name="Titi", age=35, zip="20200", city="Bastia", departement="2B"),
            Row(name="Tata", age=40, zip="75008", city="Paris", departement="75"),
            Row(name="Toto", age=45, zip="33120", city="Arcachon", departement="33"),
            Row(name="Dudu", age=55, zip="75016", city="Paris", departement="75"),
            Row(name="Mimi", age=25, zip="13002", city="Marseille", departement="13"),
            Row(name="Fifi", age=37, zip="33000", city="Bordeaux", departement="33"),
            Row(name="Loulou", age=29, zip="20200", city="Bastia", departement="2B")
        ]

        wordcount_df = spark.createDataFrame(wordcount_data)
        departement_df = spark.createDataFrame(departement_data)
        population_df = spark.createDataFrame(population_data)


        wordcount_result = wordcount(wordcount_df, "text")

        expected_wordcount = [
            Row(word="chat", count=3),
            Row(word="chien", count=5),
            Row(word="cafard", count=1)
        ]
        expected_wordcount_df = spark.createDataFrame(expected_wordcount)
        self.assertCountEqual(wordcount_result.collect(), expected_wordcount_df.collect())

        # Application de la fonction 'create_departement' sur la colonne 'zip'
        departement_result = create_departement(departement_df)

        # Vérification des résultats de 'create_departement'
        expected_departement = [
            Row(zip="75001", departement="75"),
            Row(zip="20190", departement="2A"),
            Row(zip="12345", departement="12")
        ]
        expected_departement_df = spark.createDataFrame(expected_departement)
        self.assertEqual(departement_result.collect(), expected_departement_df.collect())

        # Application de la fonction 'count_population' sur le résultat de 'create_departement'
        population_count_result = count_population(population_df)

        # Vérification des résultats de 'count_population'
        expected_population_count = [
            Row(departement="75", nb_people=3),
            Row(departement="33", nb_people=2),
            Row(departement="13", nb_people=1),
            Row(departement="2B", nb_people=2)

        ]
        expected_population_count_df = spark.createDataFrame(expected_population_count, self.schema_expected_count_population).sort(
            col("nb_people").desc(), "departement").collect()
        self.assertEqual(population_count_result.collect(), expected_population_count_df)

if __name__ == "__main__":
    unittest.main()
