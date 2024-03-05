resource "aws_s3_bucket" "bucket" {
  bucket = "spark-handsone"
  tags = local.tags
}