from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from include.nyc311_ingestion import NYC311DataIngestion
import logging

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,  # Increased retries for stability
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": True,  # Enable for important backfill
    "email_on_retry": False,
}

@dag(
    dag_id="nyc311_monthly_backfill",
    default_args=default_args,
    description="Historical NYC 311 monthly data backfill (2020-2024)",
    schedule=None,  # Manual trigger only
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2024, 12, 31),  # Explicit end date
    catchup=False,  # Manual control preferred for backfill
    max_active_runs=1,
    max_active_tasks=3,  # Slightly increased
    tags=["nyc311", "backfill", "monthly", "historical"],
)
def nyc311_monthly_backfill():

    start_task = EmptyOperator(task_id="start_monthly_backfill")

    @task(
        task_id="extract_and_load_monthly",
        retries=3,
        retry_delay=timedelta(minutes=20),
        execution_timeout=timedelta(hours=2),  # Timeout for large months
    )
    def extract_and_load_monthly(**context):
        """Monthly data extraction with enhanced error handling"""
        execution_date = context['ds']
        execution_date_obj = datetime.strptime(execution_date, '%Y-%m-%d')
        
        ingestion = NYC311DataIngestion()
        year = execution_date_obj.year
        month = execution_date_obj.month

        # Enhanced S3 key
        s3_key = f"year={year}/month={month:02d}/nyc_311_{year}_{month:02d}.json.gz"

        # Skip if exists
        if ingestion.check_file_exists(s3_key):
            logging.info(f"⏭️ Monthly data exists for {year}-{month:02d}, skipping...")
            return {
                "status": "skipped",
                "year": year,
                "month": month,
                "s3_key": s3_key,
                "record_count": 0,
                "reason": "file_exists"
            }

        try:
            # Fetch data
            logging.info(f"🔄 Starting monthly ingestion for {year}-{month:02d}")
            data = ingestion.fetch_data_for_month(year, month)
            
            # Upload with monthly flag
            result = ingestion.upload_to_s3(execution_date_obj, data, monthly=True)
            result.update({
                'year': year,
                'month': month,
                'processing_date': datetime.now().isoformat()
            })
            
            logging.info(f"✅ Monthly ingestion completed for {year}-{month:02d}: {len(data)} records")
            return result
            
        except Exception as e:
            logging.error(f"❌ Monthly ingestion failed for {year}-{month:02d}: {str(e)}")
            raise

    @task(task_id="validate_monthly_data")
    def validate_monthly_data(**context):
        """Enhanced monthly data validation"""
        ti = context['task_instance']
        result = ti.xcom_pull(task_ids='extract_and_load_monthly')
        
        year = result.get('year')
        month = result.get('month')
        status = result.get('status')
        record_count = result.get('record_count', 0)
        
        # Validation thresholds for monthly data
        min_expected = 50_000   # Minimum monthly records
        max_expected = 300_000  # Maximum monthly records
        
        validation_result = {
            "year": year,
            "month": month,
            "status": status,
            "record_count": record_count,
            "validation_status": "passed"
        }
        
        if status == 'success' and record_count > 0:
            if record_count < min_expected:
                validation_result["validation_status"] = "warning"
                logging.warning(f"⚠️ Low record count for {year}-{month:02d}: {record_count}")
            elif record_count > max_expected:
                validation_result["validation_status"] = "warning"  
                logging.warning(f"⚠️ High record count for {year}-{month:02d}: {record_count}")
            else:
                logging.info(f"✅ Monthly validation passed for {year}-{month:02d}: {record_count} records")
        elif status == 'skipped':
            logging.info(f"⏭️ Validation skipped for {year}-{month:02d} (file exists)")
        else:
            validation_result["validation_status"] = "failed"
            logging.error(f"❌ Monthly validation failed for {year}-{month:02d}")
        
        return validation_result

    end_task = EmptyOperator(task_id="end_monthly_backfill")

    # Task flow
    extract_task = extract_and_load_monthly()
    validate_task = validate_monthly_data()
    
    start_task >> extract_task >> validate_task >> end_task

monthly_backfill_dag = nyc311_monthly_backfill()