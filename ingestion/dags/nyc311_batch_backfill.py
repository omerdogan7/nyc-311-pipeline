from airflow.decorators import dag, task
from datetime import datetime, timedelta
from include.nyc311_ingestion import NYC311DataIngestion
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email  # Email için import ekle
import logging

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email": ["omrdgn2212@gmail.com"],  # Email adresi ekle
    "email_on_failure": True,
    "email_on_retry": False,
}

@dag(
    dag_id="nyc311_daily_ingestion",
    default_args=default_args,
    description="Daily NYC 311 data ingestion (current year)",
    schedule="0 9 * * *",  # 9 AM UTC (4 AM EST, 5 AM EDT)
    start_date=datetime(2025, 9, 1),
    catchup=True,  # Can catch up recent days
    max_active_runs=1,
    max_active_tasks=1,
    tags=["nyc311", "daily", "production"],
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
        """Daily incremental data extraction"""
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
        s3_key = f"year={execution_date_obj.year}/month={execution_date_obj.month:02d}/day={execution_date_obj.day:02d}/nyc_311_{execution_date_obj.strftime('%Y_%m_%d')}.json.gz"

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
            
            # Upload daily data
            result = ingestion.upload_to_s3(execution_date_obj, data, monthly=False)
            result.update({
                'date': execution_date,
                'processing_timestamp': datetime.now().isoformat()
            })
            
            logging.info(f"✅ Daily ingestion completed for {execution_date}: {len(data)} records")
            return result
            
        except Exception as e:
            logging.error(f"❌ Daily ingestion failed for {execution_date}: {str(e)}")
            raise

    @task(task_id="validate_daily_data")
    def validate_daily_data(**context):
        """Daily data quality validation with day-specific rules"""
        ti = context['task_instance']
        result = ti.xcom_pull(task_ids='extract_and_load_daily')
        
        date = result.get('date')
        status = result.get('status')
        record_count = result.get('record_count', 0)
        
        # Get day of week for validation (NYC 311 patterns vary by day)
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        day_of_week = date_obj.weekday()  # 0=Monday, 6=Sunday
        
        # Day-specific thresholds
        if day_of_week < 5:  # Weekdays
            min_expected, max_expected = 1_000, 15_000
        else:  # Weekends  
            min_expected, max_expected = 500, 8_000
            
        validation_result = {
            "date": date,
            "day_of_week": day_of_week,
            "status": status,
            "record_count": record_count,
            "validation_status": "passed"
        }
        
        if status == 'success' and record_count > 0:
            if record_count < min_expected:
                validation_result["validation_status"] = "warning"
                logging.warning(f"⚠️ Low daily record count for {date}: {record_count}")
            elif record_count > max_expected:
                validation_result["validation_status"] = "warning"
                logging.warning(f"⚠️ High daily record count for {date}: {record_count}")
            else:
                logging.info(f"✅ Daily validation passed for {date}: {record_count} records")
        elif status == 'skipped':
            logging.info(f"⏭️ Daily validation skipped for {date}")
        else:
            validation_result["validation_status"] = "failed"
            logging.error(f"❌ Daily validation failed for {date}")
        
        return validation_result

    @task(task_id="send_daily_summary")
    def send_daily_summary(**context):
        """Send daily processing summary email"""
        ti = context['task_instance']
        extract_result = ti.xcom_pull(task_ids='extract_and_load_daily')
        validation_result = ti.xcom_pull(task_ids='validate_daily_data')
        
        date = extract_result.get('date')
        status = extract_result.get('status')
        record_count = extract_result.get('record_count', 0)
        file_size_mb = extract_result.get('file_size_mb', 0)
        validation_status = validation_result.get('validation_status')
        s3_key = extract_result.get('s3_key', 'N/A')
        
        # Email subject
        subject = f"NYC 311 Daily Report - {date}"
        if status == 'failed' or validation_status == 'failed':
            subject = f"❌ {subject} - Failed"
        elif validation_status == 'warning':
            subject = f"⚠️ {subject} - Warning"
        elif status == 'skipped':
            subject = f"⏭️ {subject} - Skipped"
        else:
            subject = f"✅ {subject} - Success"
        
        # Basit HTML email içeriği
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h3 style="color: #2c3e50;">NYC 311 Daily Ingestion Report</h3>
            
            <div style="background-color: #ecf0f1; padding: 15px; border-radius: 5px; margin: 10px 0;">
                <p><strong>📅 Date:</strong> {date}</p>
                <p><strong>📊 Status:</strong> {status.upper()}</p>
                <p><strong>📈 Records:</strong> {record_count:,}</p>
                <p><strong>💾 File Size:</strong> {file_size_mb:.2f} MB</p>
                <p><strong>✔️ Validation:</strong> {validation_status.upper()}</p>
            </div>
            
            <p style="color: #7f8c8d; font-size: 12px;">
                S3 Location: {s3_key}<br>
                Processed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </p>
        </body>
        </html>
        """
        
        # Email gönder
        try:
            send_email(
                to=["omrdgn2212@gmail.com"],
                subject=subject,
                html_content=html_content,
                conn_id='smtp_default'
            )
            logging.info(f"📧 Daily summary email sent for {date}")
        except Exception as e:
            logging.error(f"Failed to send daily email: {e}")
        
        # Summary döndür
        summary = {
            "dag_run_date": context['ds'],
            "execution_date": context['ts'],
            "extract_status": status,
            "record_count": record_count,
            "file_size_mb": file_size_mb,
            "validation_status": validation_status,
            "s3_key": s3_key
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