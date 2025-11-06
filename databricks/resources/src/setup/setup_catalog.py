# Databricks notebook source
# COMMAND ----------
"""
NYC 311 Pipeline Setup - Unity Catalog with External Volume
"""

from pyspark.sql import SparkSession

# COMMAND ----------

catalog = "nyc_311_dev"
raw_files_bucket = "s3://nyc-311-raw"     # ← Existing raw parquet files (read-only)
data_bucket = "s3://nyc-311-data-dev"     # ← DLT managed tables (write)

print(f"📋 Setup Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Raw Files Bucket: {raw_files_bucket}")
print(f"  Data Tables Bucket: {data_bucket}")

# COMMAND ----------

# 1. Create Catalog with MANAGED LOCATION
print(f"\n1️⃣ Creating catalog: {catalog}")

# First drop if exists to recreate with correct location
spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")

spark.sql(f"""
    CREATE CATALOG {catalog}
    MANAGED LOCATION '{data_bucket}/'
    COMMENT 'NYC 311 Data Lakehouse - Managed by Unity Catalog'
""")

spark.sql(f"USE CATALOG {catalog}")
print("   ✅ Done")

# COMMAND ----------

# 2. Create Schemas
print(f"\n2️⃣ Creating schemas...")

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS bronze
    COMMENT 'Bronze layer - raw data tables'
""")

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS silver
    COMMENT 'Silver layer - cleaned data'
""")

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS gold
    COMMENT 'Gold layer - aggregated data'
""")
print("   ✅ Done")

# COMMAND ----------

# 3. Create External Volume
print(f"\n3️⃣ Creating external volume...")

spark.sql("""
    CREATE EXTERNAL VOLUME IF NOT EXISTS bronze.raw_files
    LOCATION 's3://nyc-311-raw/'
    COMMENT 'External volume for NYC 311 raw parquet files'
""")

print("   ✅ Done")

# COMMAND ----------

# 4. Create Checkpoint Volumes
print(f"\n4️⃣ Creating checkpoint volumes...")

spark.sql(f"CREATE VOLUME IF NOT EXISTS bronze.checkpoints")
spark.sql(f"CREATE VOLUME IF NOT EXISTS silver.checkpoints")
spark.sql(f"CREATE VOLUME IF NOT EXISTS gold.checkpoints")
print("   ✅ Done")

# COMMAND ----------

# 5. Verify Setup
print(f"\n5️⃣ Verifying setup...")

print("\n📋 Catalog Details:")
spark.sql(f"DESCRIBE CATALOG EXTENDED {catalog}").show(truncate=False)

print("\n📋 Schemas:")
spark.sql(f"SHOW SCHEMAS IN {catalog}").show()

print("\n📋 Volumes in Bronze:")
spark.sql("SHOW VOLUMES IN bronze").show()

print("\n📋 External Volume Details:")
spark.sql("DESCRIBE VOLUME bronze.raw_files").show(truncate=False)

print(f"""
{'='*60}
✅ SETUP COMPLETE - DLT READY!
{'='*60}

Catalog: {catalog}
Catalog Managed Location: {data_bucket}/

📦 EXTERNAL VOLUME (Read-Only):
/Volumes/{catalog}/bronze/raw_files/
  → {raw_files_bucket}/
     ├── year=2010/           ← Existing parquet files
     │   ├── file1.parquet
     │   └── ...
     ├── year=2011/
     └── year=2025/

📊 UNITY CATALOG MANAGED STORAGE (DLT Writes):
{data_bucket}/
├── {catalog}.db/
│   ├── bronze.db/       ← Bronze tables
│   │   └── nyc_311_raw/
│   ├── silver.db/       ← Silver tables
│   │   └── nyc_311_cleaned/
│   └── gold.db/         ← Gold tables
│       └── complaint_summary/

📁 Checkpoint Volumes (Managed):
  • bronze.checkpoints
  • silver.checkpoints
  • gold.checkpoints

🔄 DLT Data Flow:
  READ  → /Volumes/{catalog}/bronze/raw_files/year=*/
  WRITE → {catalog}.bronze.nyc_311_raw (Delta table)
  WRITE → {catalog}.silver.nyc_311_cleaned (Delta table)
  WRITE → {catalog}.gold.complaint_summary (Delta table)

📝 Next Step:
   Use this path in DLT pipeline:
   source_path = "/Volumes/{catalog}/bronze/raw_files/"
{'='*60}
""")

# COMMAND ----------