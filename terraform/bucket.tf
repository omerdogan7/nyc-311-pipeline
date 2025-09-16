resource "aws_s3_bucket" "nyc_311_bronze" {
  bucket = "nyc-311-bronze"  # Unique bucket name

  tags = {
    Name        = "NYC 311 Bronze"
    Environment = "dev"
    Project     = "NYC311 Ingestion"
  }
}
