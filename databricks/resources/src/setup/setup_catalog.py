# Databricks notebook source
# COMMAND ----------

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# COMMAND ----------

@dataclass
class InfrastructureConfig:
    catalog_name: str
    bronze_bucket: str
    data_bucket: str
    environment: str = "dev"
    
    def validate(self):
        """Validate configuration"""
        if not self.catalog_name:
            raise ValueError("Catalog name is required")
        if not self.bronze_bucket.startswith("s3://"):
            raise ValueError("Bronze bucket must be S3 path")
        if not self.data_bucket.startswith("s3://"):
            raise ValueError("Data bucket must be S3 path")
        logger.info("Configuration validated successfully")

# COMMAND ----------

class InfrastructureSetup:
    def __init__(self, config: InfrastructureConfig, spark):
        self.config = config
        self.spark = spark
        self.created_resources = []
        self.schemas = ["bronze", "silver", "gold", "_bundle_storage"]
        self.checkpoint_volumes = [
            ("bronze", "checkpoints"),
            ("bronze", "autoloader"),
            ("silver", "checkpoints"),
            ("silver", "watermarks"),
            ("gold", "checkpoints"),
            ("_bundle_storage", "pipelines")
        ]
        
    def setup(self):
        """Main setup orchestration"""
        try:
            logger.info(f"Starting infrastructure setup for environment: {self.config.environment}")
            self.config.validate()
            self.check_existing_infrastructure()
            self.create_catalog()
            self.create_schemas()
            self.create_checkpoint_volumes()
            self.create_external_volumes()
            self.set_permissions()
            self.verify_setup()
            self.print_summary()
            logger.info("Infrastructure setup completed successfully")
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            self.rollback()
            raise
    
    def check_existing_infrastructure(self):
        """Check what already exists"""
        logger.info("Checking existing infrastructure...")
        try:
            catalogs = self.spark.sql("SHOW CATALOGS").collect()
            catalog_exists = any(c.catalog == self.config.catalog_name for c in catalogs)
            if catalog_exists:
                logger.info(f"Catalog '{self.config.catalog_name}' already exists")
        except Exception as e:
            logger.warning(f"Could not check existing catalogs: {e}")
    
    def create_catalog(self):
        """Create catalog"""
        try:
            logger.info(f"Creating catalog: {self.config.catalog_name}")
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.config.catalog_name}")
            self.spark.sql(f"USE CATALOG {self.config.catalog_name}")
            self.created_resources.append(("CATALOG", self.config.catalog_name))
            logger.info(f"✅ Catalog '{self.config.catalog_name}' created successfully")
        except Exception as e:
            logger.error(f"Failed to create catalog: {e}")
            raise
    
    def create_schemas(self):
        """Create schemas"""
        for schema in self.schemas:
            try:
                logger.info(f"Creating schema: {schema}")
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                self.created_resources.append(("SCHEMA", f"{self.config.catalog_name}.{schema}"))
                logger.info(f"✅ Schema '{schema}' created successfully")
            except Exception as e:
                logger.error(f"Failed to create schema '{schema}': {e}")
                raise
    
    def create_checkpoint_volumes(self):
        """Create checkpoint volumes for incremental loading"""
        logger.info("Creating checkpoint volumes for incremental loading...")
        for schema, volume in self.checkpoint_volumes:
            try:
                self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {schema}.{volume}")
                self.created_resources.append(("VOLUME", f"{self.config.catalog_name}.{schema}.{volume}"))
                logger.info(f"✅ Volume '{schema}.{volume}' created successfully")
            except Exception as e:
                logger.warning(f"Warning for {schema}.{volume}: {e}")
    
    def create_external_volumes(self):
        """Create external volumes"""
        logger.info("Creating external volumes...")
        
        # Bronze volume
        try:
            logger.info(f"Creating bronze external volume pointing to: {self.config.bronze_bucket}/")
            self.spark.sql(f"""
            CREATE EXTERNAL VOLUME IF NOT EXISTS bronze.yearly_data
            LOCATION '{self.config.bronze_bucket}/'
            """)
            self.created_resources.append(("EXTERNAL VOLUME", f"{self.config.catalog_name}.bronze.yearly_data"))
            logger.info(f"✅ Bronze external volume created → {self.config.bronze_bucket}/")
        except Exception as e:
            logger.warning(f"Bronze volume warning: {e}")
        
        # Silver volume
        try:
            logger.info(f"Creating silver external volume pointing to: {self.config.data_bucket}/silver/")
            self.spark.sql(f"""
            CREATE EXTERNAL VOLUME IF NOT EXISTS silver.external_location  
            LOCATION '{self.config.data_bucket}/silver/'
            """)
            self.created_resources.append(("EXTERNAL VOLUME", f"{self.config.catalog_name}.silver.external_location"))
            logger.info(f"✅ Silver external volume created → {self.config.data_bucket}/silver/")
        except Exception as e:
            logger.warning(f"Silver volume warning: {e}")
        
        # Gold volume
        try:
            logger.info(f"Creating gold external volume pointing to: {self.config.data_bucket}/gold/")
            self.spark.sql(f"""
            CREATE EXTERNAL VOLUME IF NOT EXISTS gold.external_location
            LOCATION '{self.config.data_bucket}/gold/'
            """)
            self.created_resources.append(("EXTERNAL VOLUME", f"{self.config.catalog_name}.gold.external_location"))
            logger.info(f"✅ Gold external volume created → {self.config.data_bucket}/gold/")
        except Exception as e:
            logger.warning(f"Gold volume warning: {e}")
    
    def set_permissions(self):
        """Set basic permissions for the project"""
        logger.info("Setting basic permissions...")
        
        try:
            # Grant basic catalog access to all workspace users
            self.spark.sql(f"""
                GRANT USE CATALOG ON CATALOG {self.config.catalog_name} 
                TO `account users`
            """)
            
            # Grant schema access based on environment
            for schema in self.schemas:
                if self.config.environment == "dev":
                    # Development: more open access
                    self.spark.sql(f"""
                        GRANT USE SCHEMA, SELECT ON SCHEMA {self.config.catalog_name}.{schema} 
                        TO `account users`
                    """)
                else:
                    # Production: restricted access
                    if schema in ["silver", "gold"]:
                        self.spark.sql(f"""
                            GRANT USE SCHEMA, SELECT ON SCHEMA {self.config.catalog_name}.{schema} 
                            TO `account users`
                        """)
            
            logger.info("✅ Basic permissions set successfully")
        except Exception as e:
            logger.warning(f"Could not set permissions (might need admin rights): {e}")
            logger.info("Continuing without permissions - you may need to set them manually")
    
    def verify_setup(self):
        """Verify all components were created"""
        logger.info("Verifying infrastructure setup...")
    
        try:
            # Basit bir test - catalog'u kullanmayı dene
            self.spark.sql(f"USE CATALOG {self.config.catalog_name}")
        
            # Her schema için basit bir test
            for schema in self.schemas:
                try:
                    self.spark.sql(f"USE SCHEMA {schema}")
                    logger.info(f"✅ Schema '{schema}' is accessible")
                except Exception as e:
                    logger.warning(f"⚠️ Schema '{schema}' may have issues: {e}")
        
            logger.info("✅ Basic infrastructure verification passed")
        
        except Exception as e:
            logger.error(f"Verification encountered issues: {e}")
            logger.info("Infrastructure created but verification had warnings")
    
    def rollback(self):
        """Rollback created resources in case of failure"""
        logger.info("Starting rollback of created resources...")
        for resource_type, resource_name in reversed(self.created_resources):
            try:
                if resource_type == "VOLUME" or resource_type == "EXTERNAL VOLUME":
                    logger.info(f"Dropping {resource_type}: {resource_name}")
                    self.spark.sql(f"DROP VOLUME IF EXISTS {resource_name}")
                elif resource_type == "SCHEMA":
                    logger.info(f"Dropping {resource_type}: {resource_name} CASCADE")
                    self.spark.sql(f"DROP SCHEMA IF EXISTS {resource_name} CASCADE")
                elif resource_type == "CATALOG":
                    logger.info(f"Note: Catalog {resource_name} not dropped - manual cleanup required")
            except Exception as e:
                logger.error(f"Failed to rollback {resource_type} {resource_name}: {e}")
    
    def print_summary(self):
        """Print infrastructure setup summary"""
        summary = f"""
📁 Infrastructure Setup Complete:

ENVIRONMENT: {self.config.environment}
CATALOG: {self.config.catalog_name}

CATALOGS & SCHEMAS:
{self.config.catalog_name}/
├── bronze/              (schema only)
├── silver/              (schema only)  
├── gold/                (schema only)
└── _bundle_storage/     (schema only)

EXTERNAL VOLUMES (Data):
├── bronze.yearly_data → {self.config.bronze_bucket}/
├── silver.external_location → {self.config.data_bucket}/silver/
└── gold.external_location → {self.config.data_bucket}/gold/

CHECKPOINT VOLUMES (For Incremental Loading):
├── bronze.checkpoints/     # Track processed files
├── bronze.autoloader/      # Auto Loader checkpoint
├── silver.checkpoints/     # Silver incremental state
├── silver.watermarks/      # Streaming watermarks
└── gold.checkpoints/       # Gold incremental state

NO TABLES CREATED - You will create them during pipeline development.
        """
        print(summary)
        logger.info("Infrastructure summary displayed")

# COMMAND ----------

# Main execution
if __name__ == "__main__":
    # Check if running locally or in Databricks
    IS_LOCAL = os.getenv("DATABRICKS_RUNTIME_VERSION") is None
    
    # Get configuration
    if IS_LOCAL:
        logger.info("🔧 Running locally - setting default values...")
        catalog_name = "nyc_311_dev"
        bronze_bucket = "s3://nyc-311-bronze"
        data_bucket = "s3://nyc-311-data-dev"
        environment = "dev"
    else:
        try:
            # Widget parameters for Databricks runs
            dbutils.widgets.text("catalog", "nyc_311", "Catalog Name")
            dbutils.widgets.text("bronze_bucket", "s3://nyc-311-bronze", "Bronze Bucket")
            dbutils.widgets.text("data_bucket", "s3://nyc-311-data-dev", "Data Bucket")
            dbutils.widgets.text("environment", "dev", "Environment")
            
            catalog_name = dbutils.widgets.get("catalog")
            bronze_bucket = dbutils.widgets.get("bronze_bucket")
            data_bucket = dbutils.widgets.get("data_bucket")
            environment = dbutils.widgets.get("environment")
        except:
            # Fallback to spark configuration
            catalog_name = spark.conf.get("spark.databricks.bundle.var.catalog", "nyc_311")
            bronze_bucket = spark.conf.get("spark.databricks.bundle.var.bronze_bucket", "s3://nyc-311-bronze")
            data_bucket = spark.conf.get("spark.databricks.bundle.var.data_bucket", "s3://nyc-311-data-dev")
            environment = spark.conf.get("spark.databricks.bundle.var.environment", "dev")
    
    # Create configuration
    config = InfrastructureConfig(
        catalog_name=catalog_name,
        bronze_bucket=bronze_bucket,
        data_bucket=data_bucket,
        environment=environment
    )
    
    logger.info(f"Configuration loaded - Catalog: {config.catalog_name}, Environment: {config.environment}")
    
    # Run setup
    setup = InfrastructureSetup(config, spark)
    setup.setup()

# COMMAND ----------

# Optional: Show created resources
if 'setup' in locals():
    logger.info("\n📋 Created Resources Summary:")
    for resource_type, resource_name in setup.created_resources:
        logger.info(f"  - {resource_type}: {resource_name}")

# COMMAND ----------

# ADDITIONAL BEST PRACTICES FOR PRODUCTION-READY INFRASTRUCTURE:
# 
# 1. **Configuration Management**: Consider using environment-specific config files (YAML/JSON) instead of 
#    hardcoded values. This allows for easier maintenance and deployment across environments.
#
# 2. **Secret Management**: For production, use Databricks Secrets or cloud provider's secret management 
#    services (AWS Secrets Manager, Azure Key Vault) instead of exposing S3 paths directly.
#
# 3. **Monitoring and Alerting**: Implement monitoring for infrastructure health, volume usage, and access 
#    patterns. Set up alerts for failed operations or unusual activities.
#
# 4. **Data Retention Policies**: Define and implement data retention policies for each layer (bronze: 90 days, 
#    silver: 1 year, gold: indefinite) to optimize storage costs.
#
# 5. **Backup and Disaster Recovery**: Implement regular backups of metadata and critical data. Consider 
#    cross-region replication for disaster recovery scenarios.
#
# 6. **Cost Optimization**: Use lifecycle policies on S3 buckets to move older data to cheaper storage classes. 
#    Monitor and optimize compute resources based on actual usage patterns.
#
# 7. **Documentation and Change Management**: Maintain detailed documentation of infrastructure changes and 
#    use version control for all configuration code. Implement a proper CI/CD pipeline for infrastructure changes.
#
# 8. **Data Quality Checks**: Add automated data quality checks at each layer transition. Implement circuit 
#    breakers to prevent bad data from propagating through the pipeline.