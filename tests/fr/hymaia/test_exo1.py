import unittest
from pyspark.sql import Row
from src.fr.hymaia.exo1.main import wordcount
from spark_test_case import spark

class TestMain(unittest.TestCase):
    def test_wordcount(self):
        sample_data = [
            Row(text="chat chien cafard"),
            Row(text="chat cafard")
        ]

        input = spark.createDataFrame(sample_data)

        expected = spark.createDataFrame(
            [
            Row(word="chat", count=2),
            Row(word="chien", count=1),
            Row(word="cafard", count=2),
            ]
        )

        actual = wordcount(input, "text")

        self.assertCountEqual(actual.collect(), expected.collect())

    # Test avec des entrées vides et NULL pour voir comment elles sont gérées
    def test_wordcount_with_empty_and_null_values(self):

        sample_data = [
            Row(text=""),
            Row(text=None),
            Row(text="   "),
            Row(text=" chat  chien ")
        ]

        input_df = spark.createDataFrame(sample_data)

        expected_data = [
            Row(word="chat", count=1),
            Row(word="chien", count=1)
        ]
        expected_df = spark.createDataFrame(expected_data)

        actual_df = wordcount(input_df, "text")

        self.assertCountEqual(actual_df.collect(), expected_df.collect())

    # Vérification de l'insensibilité à la casse de la fonction 'wordcount'
    def test_wordcount_case_sensitivity(self):
        sample_data = [Row(text="Chat chat CHAT")]
        input_df = spark.createDataFrame(sample_data)

        expected_data = [Row(word="chat", count=3)]
        expected_df = spark.createDataFrame(expected_data)

        actual_df = wordcount(input_df, "text")

        self.assertCountEqual(actual_df.collect(), expected_df.collect())

    # Test de la gestion des espaces superflus
    def test_wordcount_handling_spaces(self):
        # Test de la gestion des espaces superflus
        sample_data = [Row(text="  espace   en trop   entre   mots  ")]
        input_df = spark.createDataFrame(sample_data)

        expected_data = [
            Row(word="espace", count=1),
            Row(word="en", count=1),
            Row(word="trop", count=1),
            Row(word="entre", count=1),
            Row(word="mots", count=1)
        ]
        expected_df = spark.createDataFrame(expected_data)

        actual_df = wordcount(input_df, "text")
        self.assertCountEqual(actual_df.collect(), expected_df.collect())

    # Test pour voir comment la ponctuation est gérée
    def test_wordcount_with_punctuation(self):
        # Test pour voir comment la ponctuation est gérée
        sample_data = [Row(text="salut, monde! salut; monde?")]
        input_df = spark.createDataFrame(sample_data)

        expected_data = [
            Row(word="salut", count=2),
            Row(word="monde", count=2)
        ]
        expected_df = spark.createDataFrame(expected_data)

        actual_df = wordcount(input_df, "text")
        self.assertCountEqual(actual_df.collect(), expected_df.collect())

    def test_wordcount_with_accent(self):
        # Test pour voir comment la ponctuation est gérée
        sample_data = [Row(text="etage étage")]
        input_df = spark.createDataFrame(sample_data)

        expected_data = [
            Row(word="etage", count=2),
        ]
        expected_df = spark.createDataFrame(expected_data)

        actual_df = wordcount(input_df, "text")
        self.assertCountEqual(actual_df.collect(), expected_df.collect())

    # Test du comptage des mots sur plusieurs lignes
    def test_wordcount_multiple_lines(self):
        sample_data = [
            Row(text="premier etage"),
            Row(text="deuxieme etage troisieme etage")
        ]
        input_df = spark.createDataFrame(sample_data)

        # j'ai pas géré les accents :) je devrais utiliser une fonction udf pour normaliser le texte et et retirer les accents ...
        expected_data = [
            Row(word="etage", count=3),
            Row(word="premier", count=1),
            Row(word="deuxieme", count=1),
            Row(word="troisieme", count=1)
        ]
        expected_df = spark.createDataFrame(expected_data)

        actual_df = wordcount(input_df, "text")

        self.assertCountEqual(actual_df.collect(), expected_df.collect())




