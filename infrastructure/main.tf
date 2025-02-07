resource "aws_s3_bucket" "raw_bucket" {
  bucket = "${var.project_name}-raw-bucket"
#  acl    = "private"
}

resource "aws_s3_bucket" "processed_bucket" {
  bucket = "${var.project_name}-processed-bucket"
  acl    = "private"
}
