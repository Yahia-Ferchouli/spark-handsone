import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import main

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "PARAM_1", "PARAM_2", "PARAM_3"])
    job.init(args['JOB_NAME'], args)

    param1 = args["PARAM_1"]
    param2 = args["PARAM_2"]
    param3 = args["PARAM_3"]

    main(param1, param2, param3, spark)

    job.commit()