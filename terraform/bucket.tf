# Bronze bucket - original ingested data
resource "aws_s3_bucket" "nyc_311_bronze" {
  bucket = "nyc-311-bronze"

  tags = {
    Name        = "NYC 311 Bronze"
    Environment = "dev"
    Project     = "NYC311 Ingestion"
  }
}

# Raw bucket - external volume source
resource "aws_s3_bucket" "nyc_311_raw" {
  bucket = "nyc-311-raw"

  tags = {
    Name        = "NYC 311 Raw"
    Environment = "dev"
    Project     = "NYC311 Pipeline"
  }
}

# Dev bucket - managed tables
resource "aws_s3_bucket" "nyc_311_data_dev" {
  bucket = "nyc-311-data-dev"

  tags = {
    Name        = "NYC 311 Data Dev"
    Environment = "dev"
    Project     = "NYC311 Pipeline"
  }
}

# Prod bucket - managed tables
resource "aws_s3_bucket" "nyc_311_data_prod" {
  bucket = "nyc-311-data-prod"

  tags = {
    Name        = "NYC 311 Data Prod"
    Environment = "prod"
    Project     = "NYC311 Pipeline"
  }
}