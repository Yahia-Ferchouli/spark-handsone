resource "aws_glue_job" "spark-glue-job" {
  name     = "spark-glue-job"
  role_arn = aws_iam_role.glue.arn

  command {
    script_location = "s3://${aws_s3_bucket.bucket.bucket}/spark-jobs/exo2_glue_job.py"
  }

  glue_version = "4.0"
  number_of_workers = 2
  worker_type = "Standard"

  default_arguments = {
    "--additional-python-modules"       = "s3://${aws_s3_bucket.bucket.bucket}/wheel/spark_handson-0.1.0-py3-none-any.whl"
    "--python-modules-installer-option" = "--upgrade"
    "--job-language"                    = "python"
    "--PARAM_1"                         = "s3://${aws_s3_bucket.bucket.bucket}/dataset/exo2/clients_bdd.csv"
    "--PARAM_2"                         = "s3://${aws_s3_bucket.bucket.bucket}/dataset/exo2/city_zipcode.csv"
    "--PARAM_3"                         = "s3://${aws_s3_bucket.bucket.bucket}/output/exo2/clean"
  }
  tags = local.tags
}