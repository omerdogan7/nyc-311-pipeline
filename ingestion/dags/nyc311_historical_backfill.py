from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from include.nyc311_ingestion import NYC311DataIngestion
import logging

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": True,
    "email_on_retry": False,
}

@dag(
    dag_id="nyc311_monthly_backfill_idempotent",
    default_args=default_args,
    description="Idempotent NYC 311 monthly backfill (2010-2025 Sept)",
    schedule='@monthly',
    start_date=datetime(2010, 1, 1),
    end_date=datetime(2025, 10, 31),
    catchup=True,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["nyc311", "backfill", "monthly", "idempotent"],
)
def nyc311_monthly_backfill_idempotent():

    start_task = EmptyOperator(task_id="start_monthly_backfill")

    @task(
        task_id="check_and_extract_monthly",
        retries=3,
        retry_delay=timedelta(minutes=20),
        execution_timeout=timedelta(hours=2),
    )
    def check_and_extract_monthly(**context):
        """Idempotent monthly extraction - checks S3 first"""
        execution_date = context['ds']
        execution_date_obj = datetime.strptime(execution_date, '%Y-%m-%d')
        
        ingestion = NYC311DataIngestion()
        year = execution_date_obj.year
        month = execution_date_obj.month

        # S3 key for monthly Parquet file
        s3_key = f"year={year}/month={month:02d}/nyc_311_{year}_{month:02d}.parquet"
        
        context_str = f"{year}-{month:02d}"

        # IDEMPOTENCY CHECK: Skip if file already exists
        if ingestion.check_file_exists(s3_key):
            file_info = ingestion.get_s3_file_info(s3_key)
            record_count = int(file_info.get('metadata', {}).get('record_count', 0)) if file_info else 0
            
            logging.info(f"✅ [{context_str}] File exists, skipping (records: {record_count:,})")
            return {
                "status": "skipped",
                "reason": "file_exists",
                "year": year,
                "month": month,
                "s3_key": s3_key,
                "record_count": record_count,
                "existing_file": True
            }

        # File doesn't exist, fetch data
        try:
            logging.info(f"🔄 [{context_str}] Starting extraction (file not found)")
            data = ingestion.fetch_data_for_month(year, month)
            
            if not data:
                logging.warning(f"⚠️ [{context_str}] No data returned from API")
                return {
                    "status": "no_data",
                    "year": year,
                    "month": month,
                    "s3_key": s3_key,
                    "record_count": 0,
                    "existing_file": False
                }
            
            # Upload to S3 as Parquet
            result = ingestion.upload_to_s3_parquet(execution_date_obj, data, monthly=True)
            result.update({
                'year': year,
                'month': month,
                'processing_timestamp': datetime.now().isoformat(),
                'existing_file': False
            })
            
            logging.info(f"✅ [{context_str}] Extraction completed: {len(data):,} records")
            return result
            
        except Exception as e:
            logging.error(f"❌ [{context_str}] Extraction failed: {str(e)}")
            raise

    @task(task_id="validate_monthly_data")
    def validate_monthly_data(**context):
        """Validate monthly data with idempotent checks"""
        ti = context['task_instance']
        result = ti.xcom_pull(task_ids='check_and_extract_monthly')
        
        year = result.get('year')
        month = result.get('month')
        status = result.get('status')
        record_count = result.get('record_count', 0)
        existing_file = result.get('existing_file', False)
        
        context_str = f"{year}-{month:02d}"
        
        # Validation thresholds
        min_expected = 50_000
        max_expected = 350_000
        
        validation_result = {
            "year": year,
            "month": month,
            "status": status,
            "record_count": record_count,
            "existing_file": existing_file,
            "validation_status": "passed",
            "validation_timestamp": datetime.now().isoformat()
        }
        
        # Skip validation for skipped files (already validated previously)
        if status == 'skipped' and existing_file:
            logging.info(f"⏭️ [{context_str}] Validation skipped (file exists, assumed valid)")
            validation_result["validation_status"] = "skipped"
            return validation_result
        
        # No data case
        if status == 'no_data' or record_count == 0:
            validation_result["validation_status"] = "warning"
            logging.warning(f"⚠️ [{context_str}] No data found")
            return validation_result
        
        # Normal validation
        if status == 'success' and record_count > 0:
            if record_count < min_expected:
                validation_result["validation_status"] = "warning"
                logging.warning(f"⚠️ [{context_str}] Low record count: {record_count:,} (expected >{min_expected:,})")
            elif record_count > max_expected:
                validation_result["validation_status"] = "warning"
                logging.warning(f"⚠️ [{context_str}] High record count: {record_count:,} (expected <{max_expected:,})")
            else:
                logging.info(f"✅ [{context_str}] Validation passed: {record_count:,} records")
        else:
            validation_result["validation_status"] = "failed"
            logging.error(f"❌ [{context_str}] Validation failed")
        
        return validation_result

    @task(task_id="log_summary")
    def log_summary(**context):
        """Log processing summary for monitoring"""
        ti = context['task_instance']
        extract_result = ti.xcom_pull(task_ids='check_and_extract_monthly')
        validation_result = ti.xcom_pull(task_ids='validate_monthly_data')
        
        year = extract_result.get('year')
        month = extract_result.get('month')
        status = extract_result.get('status')
        record_count = extract_result.get('record_count', 0)
        file_size_mb = extract_result.get('file_size_mb', 0)
        existing_file = extract_result.get('existing_file', False)
        validation_status = validation_result.get('validation_status')
        
        summary = {
            "dag_run_id": context['run_id'],
            "execution_date": context['ds'],
            "year": year,
            "month": month,
            "status": status,
            "record_count": record_count,
            "file_size_mb": file_size_mb,
            "validation_status": validation_status,
            "existing_file": existing_file,
            "idempotent": True,
            "processing_timestamp": datetime.now().isoformat()
        }
        
        # Status logging
        emoji = "⏭️" if existing_file else ("✅" if status == "success" else "⚠️")
        action = "Skipped (exists)" if existing_file else ("Processed" if status == "success" else "No data")
        
        logging.info(f"{emoji} [{year}-{month:02d}] {action} | Records: {record_count:,} | Size: {file_size_mb:.2f}MB | Validation: {validation_status}")
        
        return summary

    end_task = EmptyOperator(task_id="end_monthly_backfill")

    # Task flow
    extract_task = check_and_extract_monthly()
    validate_task = validate_monthly_data()
    summary_task = log_summary()
    
    start_task >> extract_task >> validate_task >> summary_task >> end_task

monthly_backfill_dag = nyc311_monthly_backfill_idempotent()