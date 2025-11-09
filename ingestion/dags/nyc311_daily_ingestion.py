from airflow.decorators import dag, task
from datetime import datetime, timedelta
from include.nyc311_ingestion import NYC311DataIngestion
from airflow.operators.empty import EmptyOperator
import logging

# DAG default configuration value
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}

@dag(
    dag_id="nyc311_daily_ingestion",
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": True,
        "email_on_retry": False,
    },
    description="Daily NYC 311 data ingestion (T-2 delay pattern) - Parquet format",
    schedule="0 9 * * *",  # 9 AM UTC (4 AM EST, 5 AM EDT)
    start_date=datetime(2025, 11, 8),  # ✅ Fresh start 
    catchup=False,  # ✅ manual backfill 
    max_active_runs=1,
    max_active_tasks=1,
    tags=["nyc311", "daily", "production", "parquet", "t-2"],
)
def nyc311_daily_ingestion():

    start_task = EmptyOperator(task_id="start_daily_ingestion")

    @task(
        task_id="extract_and_load_daily",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )
    def extract_and_load_daily(**context):
        """
        Daily incremental data extraction with T-2 delay pattern
        
        T-2 Pattern: Fetch data from 2 days ago to ensure completeness
        Example: DAG runs on Nov 9 → Fetches Nov 7 data
        
        Why T-2?
        - NYC 311 API has 24-48h data latency
        - Ensures 100% data completeness
        - No need for backfill/updates
        """
        # DAG execution date (when DAG runs)
        execution_date = context['ds']  # e.g., '2025-11-09'
        execution_date_obj = datetime.strptime(execution_date, '%Y-%m-%d')
        
        # ✅ T-2 Pattern: Target date is 2 days before execution date
        target_date = execution_date_obj - timedelta(days=2)
        
        logging.info(f"📅 DAG Execution Date: {execution_date}")
        logging.info(f"🎯 Target Data Date (T-2): {target_date.date()}")
        
        # Safety check: Don't fetch data that's too recent (enforce T-2 policy)
        min_allowed_date = datetime.now().date() - timedelta(days=2)
        if target_date.date() > min_allowed_date:
            logging.warning(f"⚠️ Target date {target_date.date()} is too recent (within T-2 window)")
            return {
                "status": "skipped",
                "execution_date": execution_date,
                "target_date": str(target_date.date()),
                "record_count": 0,
                "reason": "too_recent_t2_policy"
            }
        
        # Safety check: Don't process future dates
        if target_date.date() > datetime.now().date():
            logging.info(f"⏭️ Skipping future date: {target_date.date()}")
            return {
                "status": "skipped",
                "execution_date": execution_date,
                "target_date": str(target_date.date()),
                "record_count": 0,
                "reason": "future_date"
            }
        
        ingestion = NYC311DataIngestion()
        
        # ✅ S3 key uses TARGET DATE (not execution date)
        s3_key = f"year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}/nyc_311_{target_date.strftime('%Y_%m_%d')}.parquet"

        # Check if already exists (idempotency)
        if ingestion.check_file_exists(s3_key):
            logging.info(f"⏭️ Data already exists for target date {target_date.date()}, skipping...")
            
            # Get existing file metadata
            file_info = ingestion.get_s3_file_info(s3_key)
            existing_count = 0
            if file_info and 'metadata' in file_info:
                existing_count = int(file_info['metadata'].get('record_count', 0))
            
            return {
                "status": "skipped",
                "execution_date": execution_date,
                "target_date": str(target_date.date()),
                "s3_key": s3_key,
                "record_count": existing_count,
                "reason": "file_exists"
            }

        try:
            # ✅ Fetch data for TARGET DATE
            logging.info(f"🔄 Starting data ingestion for target date: {target_date.date()}")
            data = ingestion.fetch_data_for_date(target_date)
            
            # Handle no data scenario - Only fail if API returns nothing
            if not data or len(data) == 0:
                logging.error(f"🚨 API returned ZERO records for target date: {target_date.date()}")
                raise ValueError(f"No data returned from API for {target_date.date()}")
            
            # ✅ Upload data for TARGET DATE
            result = ingestion.upload_to_s3_parquet(target_date, data, monthly=False)
            
            # Add execution metadata
            result.update({
                'execution_date': execution_date,
                'target_date': str(target_date.date()),
                'processing_timestamp': datetime.now().isoformat(),
                'delay_days': 2  # T-2 pattern
            })
            
            logging.info(f"✅ Ingestion completed for target date {target_date.date()}: {len(data):,} records")
            return result
            
        except Exception as e:
            logging.error(f"❌ Ingestion failed for target date {target_date.date()}: {str(e)}", exc_info=True)
            raise

    @task(task_id="validate_daily_data")
    def validate_daily_data(**context):
        """
        Daily data quality validation with T-2 pattern awareness
        """
        ti = context['task_instance']
        result = ti.xcom_pull(task_ids='extract_and_load_daily')
        
        execution_date = result.get('execution_date')
        target_date = result.get('target_date')
        status = result.get('status')
        record_count = result.get('record_count', 0)
        
        validation_result = {
            "execution_date": execution_date,
            "target_date": target_date,
            "status": status,
            "record_count": record_count,
            "validation_status": "passed",
            "format": result.get('format', 'parquet')
        }
        
        # Validation logic
        if status == 'success':
            logging.info(f"✅ Validation passed for {target_date}: {record_count:,} records")
                
        elif status == 'skipped':
            logging.info(f"⏭️ Validation skipped for {target_date} (reason: {result.get('reason')})")
            
        elif status == 'no_data':
            validation_result["validation_status"] = "warning"
            logging.warning(f"📭 No data available for {target_date}")
            
        else:
            validation_result["validation_status"] = "failed"
            logging.error(f"❌ Validation failed for {target_date}")
        
        return validation_result

    @task(task_id="send_daily_summary")
    def send_daily_summary(**context):
        """
        Send daily processing summary with T-2 pattern metrics
        """
        ti = context['task_instance']
        extract_result = ti.xcom_pull(task_ids='extract_and_load_daily')
        validation_result = ti.xcom_pull(task_ids='validate_daily_data')
        
        # Create comprehensive summary
        summary = {
            "dag_run_date": context['ds'],
            "execution_timestamp": context['ts'],
            "target_date": extract_result.get('target_date'),
            "delay_days": extract_result.get('delay_days', 2),
            "extract_status": extract_result.get('status'),
            "record_count": extract_result.get('record_count', 0),
            "file_size_mb": extract_result.get('file_size_mb', 0),
            "validation_status": validation_result.get('validation_status'),
            "s3_key": extract_result.get('s3_key'),
            "format": extract_result.get('format', 'parquet'),
            "compression": extract_result.get('compression', 'snappy')
        }
        
        # Log summary
        logging.info(f"📊 Daily Summary:")
        logging.info(f"   DAG Run: {summary['dag_run_date']}")
        logging.info(f"   Target Date: {summary['target_date']} (T-2)")
        logging.info(f"   Status: {summary['extract_status']}")
        logging.info(f"   Records: {summary['record_count']:,}")
        logging.info(f"   Size: {summary['file_size_mb']} MB")
        logging.info(f"   Validation: {summary['validation_status']}")
        
        # Optional: Send to monitoring system (Slack, email, etc.)
        # Example: send_slack_notification(summary)
        
        return summary

    end_task = EmptyOperator(task_id="end_daily_ingestion")

    # Task dependencies
    extract_task = extract_and_load_daily()
    validate_task = validate_daily_data()
    summary_task = send_daily_summary()
    
    start_task >> extract_task >> validate_task >> summary_task >> end_task

# Instantiate DAG
daily_ingestion_dag = nyc311_daily_ingestion()