import unittest
from pyspark.sql import Row
from src.fr.hymaia.exo2.spark_clean_job import create_departement  # Import de notre fonction personnalisée
from pyspark.sql.types import StructType, StructField, StringType
from spark_test_case import spark

# Définition de la classe de test pour notre fonction 'create_departement'
class CreateDepartementTestCase(unittest.TestCase):

    # Test pour vérifier le comportement de la fonction avec un code postal conduisant au département '2A'
    def test_departement_2A(self):
        # Préparation des données de test et exécution de la fonction
        sample_data = [Row(zip="20190")]
        df = spark.createDataFrame(sample_data)

        result = create_departement(df)
        expected_data = [Row(ziip="20190", departement="2A")]
        expected_df = spark.createDataFrame(expected_data)

        # Vérification que le résultat correspond aux attentes
        self.assertEqual(result.collect(), expected_df.collect())

    # Test similaire au précédent mais pour le département '2B'
    def test_departement_2B(self):
        sample_data = [Row(zip="20200")]
        df = spark.createDataFrame(sample_data)

        result = create_departement(df)
        expected_data = [Row(zip="20200", departement="2B")]
        expected_df = spark.createDataFrame(expected_data)

        self.assertEqual(result.collect(), expected_df.collect())

    # Test pour des codes postaux standards, vérifiant que la fonction extrait correctement les deux premiers chiffres
    def test_other_departement(self):
        sample_data = [Row(zip="05001")]
        df = spark.createDataFrame(sample_data)

        result = create_departement(df)
        expected_data = [Row(zip="05001", departement="05")]
        expected_df = spark.createDataFrame(expected_data)

        self.assertEqual(result.collect(), expected_df.collect())

    # Test pour les cas limites des codes postaux à la frontière entre '2A' et '2B'
    def test_edge_case_zip_codes(self):
        sample_data = [Row(zip="20190"), Row(zip="20200")]
        df = spark.createDataFrame(sample_data)

        result = create_departement(df)
        expected_data = [Row(zip="20190", departement="2A"),
                         Row(zip="20200", departement="2B")]
        expected_df = spark.createDataFrame(expected_data)

        self.assertEqual(result.collect(), expected_df.collect())

    # Test pour s'assurer que la fonction gère correctement les valeurs NULL dans le code postal
    def test_zip_code_with_null_values(self):
        schema = StructType([
            StructField("zip", StringType(), True),
            StructField("departement", StringType(), True)
        ])

        sample_data = [Row(zip=None)]
        df = spark.createDataFrame(sample_data, schema[:1])

        result = create_departement(df)
        expected_data = [Row(zip=None, departement=None)]
        expected_df = spark.createDataFrame(expected_data, schema)

        self.assertEqual(result.collect(), expected_df.collect())

    # Test pour vérifier le comportement de la fonction avec des codes postaux non standards
    def test_non_standard_zip_code(self):
        sample_data = [
            Row(zip="123"),  # Code postal non valide
            Row(zip="123456"),  # Code postal non valide
        ]
        df = spark.createDataFrame(sample_data)

        result = create_departement(df)
        expected_data = [
            Row(zip="123", departement=None),
            Row(zip="123456", departement=None),
        ]
        schema = StructType([StructField("zip", StringType(), True),
                             StructField("departement", StringType(), True)])
        expected_df = spark.createDataFrame(expected_data, schema=schema)

        self.assertEqual(result.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()
