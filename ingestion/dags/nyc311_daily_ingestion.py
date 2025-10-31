from airflow.decorators import dag, task
from datetime import datetime, timedelta
from include.nyc311_ingestion import NYC311DataIngestion
from airflow.operators.empty import EmptyOperator
import logging

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
    description="Daily NYC 311 data ingestion (current year) - Parquet format",
    schedule="0 9 * * *",  # 9 AM UTC (4 AM EST, 5 AM EDT)
    start_date=datetime(2025, 10, 1),
    catchup=True,  # Can catch up recent days
    max_active_runs=1,
    max_active_tasks=1,
    tags=["nyc311", "daily", "production", "parquet"],
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
        """Daily incremental data extraction - Parquet format"""
        execution_date = context['ds']
        execution_date_obj = datetime.strptime(execution_date, '%Y-%m-%d')
        
        # Skip future dates and weekends for efficiency (optional)
        today = datetime.now().date()
        if execution_date_obj.date() > today:
            logging.info(f"⏭️ Skipping future date: {execution_date}")
            return {
                "status": "skipped",
                "date": execution_date,
                "record_count": 0,
                "reason": "future_date"
            }
        
        ingestion = NYC311DataIngestion()
        # Parquet dosya yolu
        s3_key = f"year={execution_date_obj.year}/month={execution_date_obj.month:02d}/day={execution_date_obj.day:02d}/nyc_311_{execution_date_obj.strftime('%Y_%m_%d')}.parquet"

        # Check if already exists
        if ingestion.check_file_exists(s3_key):
            logging.info(f"⏭️ Daily data exists for {execution_date}, skipping...")
            return {
                "status": "skipped", 
                "date": execution_date,
                "s3_key": s3_key,
                "record_count": 0,
                "reason": "file_exists"
            }

        try:
            # Fetch daily data
            logging.info(f"🔄 Starting daily ingestion for {execution_date}")
            data = ingestion.fetch_data_for_date(execution_date_obj)
            
            # Upload daily data as Parquet
            result = ingestion.upload_to_s3_parquet(execution_date_obj, data, monthly=False)
            result.update({
                'date': execution_date,
                'processing_timestamp': datetime.now().isoformat()
            })
            
            logging.info(f"✅ Daily ingestion completed for {execution_date}: {len(data)} records (Parquet)")
            return result
            
        except Exception as e:
            logging.error(f"❌ Daily ingestion failed for {execution_date}: {str(e)}")
            raise

    @task(task_id="validate_daily_data")
    def validate_daily_data(**context):
        """Daily data quality validation - Simple check"""
        ti = context['task_instance']
        result = ti.xcom_pull(task_ids='extract_and_load_daily')
        
        date = result.get('date')
        status = result.get('status')
        record_count = result.get('record_count', 0)
        
        validation_result = {
            "date": date,
            "status": status,
            "record_count": record_count,
            "validation_status": "passed",
            "format": result.get('format', 'parquet')
        }
        
        if status == 'success':
            logging.info(f"✅ Daily validation passed for {date}: {record_count} records")
        elif status == 'skipped':
            logging.info(f"⏭️ Daily validation skipped for {date}")
        elif status == 'no_data':
            logging.info(f"📭 No data available for {date}")
        else:
            validation_result["validation_status"] = "failed"
            logging.error(f"❌ Daily validation failed for {date}")
        
        return validation_result

    @task(task_id="send_daily_summary")
    def send_daily_summary(**context):
        """Optional: Send daily processing summary"""
        ti = context['task_instance']
        extract_result = ti.xcom_pull(task_ids='extract_and_load_daily')
        validation_result = ti.xcom_pull(task_ids='validate_daily_data')
        
        # Create summary
        summary = {
            "dag_run_date": context['ds'],
            "execution_date": context['ts'],
            "extract_status": extract_result.get('status'),
            "record_count": extract_result.get('record_count', 0),
            "file_size_mb": extract_result.get('file_size_mb', 0),
            "validation_status": validation_result.get('validation_status'),
            "s3_key": extract_result.get('s3_key'),
            "format": extract_result.get('format', 'parquet'),
            "compression": extract_result.get('compression', 'snappy')
        }
        
        logging.info(f"📊 Daily Summary for {context['ds']}: {summary}")
        return summary

    end_task = EmptyOperator(task_id="end_daily_ingestion")

    # Task dependencies
    extract_task = extract_and_load_daily()
    validate_task = validate_daily_data()
    summary_task = send_daily_summary()
    
    start_task >> extract_task >> validate_task >> summary_task >> end_task

daily_ingestion_dag = nyc311_daily_ingestion()