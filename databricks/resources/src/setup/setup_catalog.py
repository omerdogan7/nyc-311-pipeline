"""
Minimal Infrastructure Setup for NYC 311 Data Pipeline
Single-person project optimized version with External Volumes
"""

# %%
import logging
from databricks.connect import DatabricksSession

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# %%
def setup_infrastructure(catalog_name, bronze_bucket, data_bucket, environment="dev"):
    """
    Minimal infrastructure setup - creates catalog, schemas, and external volumes
    
    Args:
        catalog_name: Unity Catalog name (e.g., 'nyc_311_dev')
        bronze_bucket: S3 path for raw data (e.g., 's3://nyc-311-bronze')
        data_bucket: S3 path for silver/gold (e.g., 's3://nyc-311-data-dev')
        environment: 'dev' or 'prod'
    """
    
    # Initialize Databricks Session
    spark = DatabricksSession.builder.getOrCreate()
    
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
        CREATE EXTERNAL VOLUME IF NOT EXISTS bronze.yearly_data
        LOCATION '{bronze_path}'
    """)
    logger.info(f"  ✅ bronze.yearly_data → {bronze_path}")
    
    # Silver volume - Processed data bucket
    silver_path = f"{data_bucket}/silver_external/"
    spark.sql(f"""
        CREATE EXTERNAL VOLUME IF NOT EXISTS silver.external_location
        LOCATION '{silver_path}'
    """)
    logger.info(f"  ✅ silver.external_location → {silver_path}")
    
    # Gold volume - Analytics data bucket
    gold_path = f"{data_bucket}/gold_external/"
    spark.sql(f"""
        CREATE EXTERNAL VOLUME IF NOT EXISTS gold.external_location
        LOCATION '{gold_path}'
    """)
    logger.info(f"  ✅ gold.external_location → {gold_path}")
    
    # 4. Create Bundle Storage Volume for DLT pipeline metadata (MANAGED)
    logger.info("Creating bundle storage volume...")
    spark.sql("""
        CREATE VOLUME IF NOT EXISTS _bundle_storage.pipelines
    """)
    logger.info(f"  ✅ _bundle_storage.pipelines (managed - for DLT metadata)")
    
    # 5. Print Summary
    print_summary(catalog_name, bronze_bucket, data_bucket, environment)
    
    logger.info("🎉 Infrastructure setup complete!")

# %%
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
  ├── bronze.yearly_data → {bronze_bucket}/
  ├── silver.external_location → {data_bucket}/silver_external/
  └── gold.external_location → {data_bucket}/gold_external/

MANAGED VOLUMES:
  └── _bundle_storage.pipelines (for DLT pipeline metadata)

S3 BUCKET STRUCTURE:
{bronze_bucket}/
  ├── year=2010/          (raw parquet files)
  ├── year=2011/
  ├── year=2012/
  └── ...

{data_bucket}/
  ├── silver_external/    (cleaned/processed data)
  └── gold_external/      (aggregated analytics data)

NOTE: 
- External volumes are accessible from outside Databricks via S3
- Raw data (bronze) isolated in separate bucket
- Additional checkpoint volumes will be created automatically by streaming jobs
- Make sure IAM permissions are configured for both S3 buckets

{'='*70}
"""
    print(summary)

# %%
def verify_volumes(catalog_name):
    """
    Verify that all volumes were created successfully
    
    Args:
        catalog_name: Unity Catalog name to verify
    """
    spark = DatabricksSession.builder.getOrCreate()
    
    logger.info(f"🔍 Verifying volumes in catalog: {catalog_name}")
    
    volumes_to_check = [
        "bronze.yearly_data",
        "silver.external_location", 
        "gold.external_location",
        "_bundle_storage.pipelines"
    ]
    
    spark.sql(f"USE CATALOG {catalog_name}")
    
    for volume in volumes_to_check:
        try:
            result = spark.sql(f"DESCRIBE VOLUME {volume}").collect()
            logger.info(f"  ✅ {volume} - OK")
        except Exception as e:
            logger.error(f"  ❌ {volume} - FAILED: {e}")

# %%
def cleanup_infrastructure(catalog_name):
    """
    Optional: Clean up all infrastructure (use with caution!)
    
    Args:
        catalog_name: Unity Catalog name to drop
    """
    spark = DatabricksSession.builder.getOrCreate()
    
    logger.warning(f"⚠️  CLEANING UP: {catalog_name}")
    response = input(f"Are you sure you want to drop catalog '{catalog_name}'? (yes/no): ")
    
    if response.lower() == 'yes':
        try:
            spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")
            logger.info(f"✅ Catalog {catalog_name} dropped successfully")
        except Exception as e:
            logger.error(f"❌ Failed to drop catalog: {e}")
    else:
        logger.info("Cleanup cancelled")

# %% [markdown]
# ## Main Execution
# Configuration and setup

# %%
# Configuration - edit these values for your project
CONFIG = {
    "catalog_name": "nyc_311_dev",
    "bronze_bucket": "s3://nyc-311-bronze",        # Raw data bucket
    "data_bucket": "s3://nyc-311-data-dev",        # Silver/Gold bucket
    "environment": "dev"
}

# %%
# Run setup
setup_infrastructure(
    catalog_name=CONFIG["catalog_name"],
    bronze_bucket=CONFIG["bronze_bucket"],
    data_bucket=CONFIG["data_bucket"],
    environment=CONFIG["environment"]
)

# %%
# Verify volumes were created
verify_volumes(CONFIG["catalog_name"])

# %%
# Uncomment to cleanup (USE WITH CAUTION!)
# cleanup_infrastructure(CONFIG["catalog_name"])