import dlt
from pyspark.sql.functions import (
    current_timestamp, col, count, when, min, max, countDistinct,
    year, month, dayofmonth, to_date
)

catalog = spark.conf.get("pipeline.catalog", "nyc_311_dev")
schema = "bronze"
# Parquet için özel schema location
schema_location = "s3://nyc-311-bronze/_schemas/nyc_311_parquet_only"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Önce eski schema'yı temizle (bir kerelik çalıştır)
# dbutils.fs.rm(schema_location, True)

@dlt.table(
    name="nyc_311_raw",
    comment="Raw NYC 311 data from S3 Parquet files only",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def bronze_nyc_311_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("pathGlobFilter", "*.parquet")  # Sadece .parquet uzantılı dosyalar
        .option("recursiveFileLookup", "false")  # Alt klasörlere bakma
        .load("s3://nyc-311-bronze/parquet/")  # Sadece parquet klasörü
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_file_modification_time", col("_metadata.file_modification_time"))
        .withColumn("year", year(col("created_date")))
        .withColumn("month", month(col("created_date")))
        .withColumn("day", dayofmonth(col("created_date")))
    )

@dlt.table(
    name="daily_metrics",
    comment="Daily metrics - updated with each run"
)
def daily_metrics():
    return (
        dlt.read("nyc_311_raw")
        .groupBy(
            to_date("_ingestion_timestamp").alias("date")
        )
        .agg(
            count("*").alias("total_records"),
            countDistinct("_source_file").alias("total_files"),
            countDistinct("unique_key").alias("unique_complaints"),
            countDistinct("complaint_type").alias("complaint_types"),  # Daha anlamlı
            count(when(col("_rescued_data").isNotNull(), 1)).alias("data_quality_issues"),  # Veri kalitesi
            max("_ingestion_timestamp").alias("last_updated")
        )
    )