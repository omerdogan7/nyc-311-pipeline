# Databricks notebook source
"""
Minimal Infrastructure Setup for NYC 311 Data Pipeline
Single-person project optimized version with External Volumes
"""

# COMMAND ----------

import logging
from pyspark.sql import SparkSession

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Parameters from Job

# COMMAND ----------

# Get parameters from notebook widgets (set by job)
dbutils.widgets.text("catalog", "nyc_311_dev", "Catalog Name")
dbutils.widgets.text("bronze_bucket", "s3://nyc-311-bronze", "Bronze Bucket")
dbutils.widgets.text("data_bucket", "s3://nyc-311-data-dev", "Data Bucket")
dbutils.widgets.dropdown("environment", "dev", ["dev", "prod"], "Environment")

# Read parameters
catalog_name = dbutils.widgets.get("catalog")
bronze_bucket = dbutils.widgets.get("bronze_bucket")
data_bucket = dbutils.widgets.get("data_bucket")
environment = dbutils.widgets.get("environment")

print(f"📋 Configuration:")
print(f"  Catalog: {catalog_name}")
print(f"  Bronze Bucket: {bronze_bucket}")
print(f"  Data Bucket: {data_bucket}")
print(f"  Environment: {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Functions

# COMMAND ----------

def setup_infrastructure(catalog_name, bronze_bucket, data_bucket, environment="dev"):
    """
    Minimal infrastructure setup - creates catalog, schemas, and external volumes
    
    Args:
        catalog_name: Unity Catalog name (e.g., 'nyc_311_dev')
        bronze_bucket: S3 path for raw data (e.g., 's3://nyc-311-bronze')
        data_bucket: S3 path for silver/gold (e.g., 's3://nyc-311-data-dev')
        environment: 'dev' or 'prod'
    """
    
    # Initialize Spark Session
    spark = SparkSession.builder.getOrCreate()
    
    logger.info(f"🚀 Starting infrastructure setup for: {catalog_name}")
    logger.info(f"Environment: {environment}")
    logger.info(f"Bronze Bucket: {bronze_bucket}")
    logger.info(f"Data Bucket: {data_bucket}")
    
    # 1. Create Catalog
    logger.info(f"Creating catalog: {catalog_name}")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"USE CATALOG {catalog_name}")
    logger.info(f"✅ Catalog created")
    
    # 2. Create Schemas
    schemas = ["bronze", "silver", "gold", "_bundle_storage"]
    logger.info(f"Creating schemas: {', '.join(schemas)}")
    
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        logger.info(f"  ✅ {schema}")
    
    # 3. Create External Volumes for data storage
    logger.info("Creating external volumes...")
    
    # Bronze volume - Raw data bucket (already has year=XXXX/ partitions)
    bronze_path = f"{bronze_bucket}/"
    spark.sql(f"""
        CREATE EXTERNAL VOLUME IF NOT EXISTS bronze.external_data
        LOCATION '{bronze_path}'
    """)
    logger.info(f"  ✅ bronze.external_data → {bronze_path}")
    
    # Silver volume - Processed data bucket
    silver_path = f"{data_bucket}/silver_external/"
    spark.sql(f"""
        CREATE EXTERNAL VOLUME IF NOT EXISTS silver.external_data
        LOCATION '{silver_path}'
    """)
    logger.info(f"  ✅ silver.external_data → {silver_path}")
    
    # Gold volume - Analytics data bucket
    gold_path = f"{data_bucket}/gold_external/"
    spark.sql(f"""
        CREATE EXTERNAL VOLUME IF NOT EXISTS gold.external_data
        LOCATION '{gold_path}'
    """)
    logger.info(f"  ✅ gold.external_data → {gold_path}")
    
    # 4. Create Checkpoint Volumes (MANAGED)
    logger.info("Creating checkpoint volumes...")
    spark.sql("CREATE VOLUME IF NOT EXISTS bronze.checkpoints")
    spark.sql("CREATE VOLUME IF NOT EXISTS silver.checkpoints")
    spark.sql("CREATE VOLUME IF NOT EXISTS gold.checkpoints")
    
    # 5. Create Bundle Storage Volume for DLT pipeline metadata (MANAGED)
    logger.info("Creating bundle storage volume...")
    spark.sql("CREATE VOLUME IF NOT EXISTS _bundle_storage.pipelines")
    logger.info(f"  ✅ _bundle_storage.pipelines (managed - for DLT metadata)")
    
    # 6. Print Summary
    print_summary(catalog_name, bronze_bucket, data_bucket, environment)
    
    logger.info("🎉 Infrastructure setup complete!")
    return True

# COMMAND ----------

def print_summary(catalog_name, bronze_bucket, data_bucket, environment):
    """Print setup summary"""
    summary = f"""
{'='*70}
📁 INFRASTRUCTURE SETUP COMPLETE
{'='*70}

Environment: {environment}
Catalog: {catalog_name}

S3 BUCKETS:
  • Bronze (Raw): {bronze_bucket}
  • Data (Silver/Gold): {data_bucket}

STRUCTURE:
{catalog_name}/
  ├── bronze/              (raw data schema)
  ├── silver/              (cleaned data schema)
  ├── gold/                (aggregated data schema)
  └── _bundle_storage/     (pipeline metadata)

EXTERNAL VOLUMES (S3 Backed):
  ├── bronze.external_data → {bronze_bucket}/
  ├── silver.external_data → {data_bucket}/silver_external/
  └── gold.external_data → {data_bucket}/gold_external/

MANAGED VOLUMES:
  ├── bronze.checkpoints
  ├── silver.checkpoints
  ├── gold.checkpoints
  └── _bundle_storage.pipelines

VOLUME PATHS:
  • /Volumes/{catalog_name}/bronze/external_data
  • /Volumes/{catalog_name}/silver/external_data
  • /Volumes/{catalog_name}/gold/external_data
  • /Volumes/{catalog_name}/bronze/checkpoints
  • /Volumes/{catalog_name}/silver/checkpoints
  • /Volumes/{catalog_name}/gold/checkpoints
  • /Volumes/{catalog_name}/_bundle_storage/pipelines

{'='*70}
"""
    print(summary)

# COMMAND ----------

def verify_volumes(catalog_name):
    """
    Verify that all volumes were created successfully
    
    Args:
        catalog_name: Unity Catalog name to verify
    """
    spark = SparkSession.builder.getOrCreate()
    
    logger.info(f"🔍 Verifying volumes in catalog: {catalog_name}")
    
    volumes_to_check = [
        "bronze.external_data",
        "silver.external_data", 
        "gold.external_data",
        "bronze.checkpoints",
        "silver.checkpoints",
        "gold.checkpoints",
        "_bundle_storage.pipelines"
    ]
    
    spark.sql(f"USE CATALOG {catalog_name}")
    
    all_ok = True
    for volume in volumes_to_check:
        try:
            result = spark.sql(f"DESCRIBE VOLUME {volume}").collect()
            logger.info(f"  ✅ {volume} - OK")
        except Exception as e:
            logger.error(f"  ❌ {volume} - FAILED: {e}")
            all_ok = False
    
    return all_ok

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Setup

# COMMAND ----------

try:
    # Run setup
    success = setup_infrastructure(
        catalog_name=catalog_name,
        bronze_bucket=bronze_bucket,
        data_bucket=data_bucket,
        environment=environment
    )
    
    if success:
        # Verify volumes
        verify_success = verify_volumes(catalog_name)
        
        if verify_success:
            print("\n✅ All volumes created and verified successfully!")
            dbutils.notebook.exit("SUCCESS")
        else:
            print("\n⚠️  Some volumes failed verification")
            dbutils.notebook.exit("PARTIAL_SUCCESS")
    else:
        print("\n❌ Setup failed")
        dbutils.notebook.exit("FAILED")
        
except Exception as e:
    logger.error(f"❌ Setup failed with error: {e}", exc_info=True)
    dbutils.notebook.exit(f"ERROR: {str(e)}")

# COMMAND ----------