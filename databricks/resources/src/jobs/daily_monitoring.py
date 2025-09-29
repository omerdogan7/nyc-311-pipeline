# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Data Monitoring & Alerting Job

# COMMAND ----------

from datetime import datetime, timedelta
import json

# COMMAND ----------

# Parameters - catalog bundle'dan gelecek
dbutils.widgets.text("date", "")
dbutils.widgets.text("catalog", "")  # Bundle'dan set edilecek
dbutils.widgets.text("deviation_threshold", "50")
dbutils.widgets.text("lookback_days", "30")

process_date = dbutils.widgets.get("date") or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
catalog = dbutils.widgets.get("catalog")  # nyc_311_dev veya nyc_311_prod olacak
deviation_threshold = float(dbutils.widgets.get("deviation_threshold"))
lookback_days = int(dbutils.widgets.get("lookback_days"))
process_date = dbutils.widgets.get("date") or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
catalog = dbutils.widgets.get("catalog")
deviation_threshold = float(dbutils.widgets.get("deviation_threshold"))
lookback_days = int(dbutils.widgets.get("lookback_days"))

# COMMAND ----------

def calculate_historical_baseline(date_str, lookback_days):
    """Calculate historical average and patterns"""
    
    # Get day of week for pattern analysis
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    day_of_week = date_obj.strftime("%A")
    
    baseline_query = f"""
    WITH daily_counts AS (
        SELECT 
            date(_ingestion_timestamp) as ingestion_date,
            dayofweek(date(_ingestion_timestamp)) as day_of_week,
            COUNT(*) as daily_count
        FROM {catalog}.bronze.nyc_311_raw
        WHERE date(_ingestion_timestamp) >= date_sub('{date_str}', {lookback_days})
          AND date(_ingestion_timestamp) < '{date_str}'
        GROUP BY date(_ingestion_timestamp)
    )
    SELECT 
        AVG(daily_count) as avg_daily_records,
        STDDEV(daily_count) as stddev_daily_records,
        MIN(daily_count) as min_daily_records,
        MAX(daily_count) as max_daily_records,
        PERCENTILE(daily_count, 0.25) as percentile_25,
        PERCENTILE(daily_count, 0.75) as percentile_75,
        AVG(CASE WHEN day_of_week = dayofweek('{date_str}') THEN daily_count END) as same_day_avg
    FROM daily_counts
    """
    
    return spark.sql(baseline_query).collect()[0]

# COMMAND ----------

def check_daily_data(date_str):
    """Monitor data quality and completeness for a specific date"""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    day_of_week = date_obj.strftime("%A")
    
    # Expected S3 path pattern
    s3_pattern = f"year={date_obj.year}/month={date_obj.month:02d}/day={date_obj.day:02d}"
    
    print(f"Checking data for: {date_str} ({day_of_week})")
    
    metrics = {'date': date_str, 'day_of_week': day_of_week}
    
    try:
        # Get historical baseline
        baseline = calculate_historical_baseline(date_str, lookback_days)
        metrics['historical_avg'] = int(baseline['avg_daily_records'] or 0)
        metrics['historical_stddev'] = int(baseline['stddev_daily_records'] or 0)
        metrics['same_day_avg'] = int(baseline['same_day_avg'] or metrics['historical_avg'])
        
        # 1. Check record count for the date
        record_count_query = f"""
        SELECT COUNT(*) as total_records
        FROM {catalog}.bronze.nyc_311_raw
        WHERE date(_ingestion_timestamp) = '{date_str}'
           OR _source_file LIKE '%{s3_pattern}%'
        """
        metrics['total_records'] = spark.sql(record_count_query).collect()[0][0]
        
        # 2. Check unique files processed
        file_count_query = f"""
        SELECT COUNT(DISTINCT _source_file) as file_count
        FROM {catalog}.bronze.nyc_311_raw
        WHERE _source_file LIKE '%{s3_pattern}%'
        """
        metrics['files_processed'] = spark.sql(file_count_query).collect()[0][0]
        
        # 3. Check for data quality issues
        quality_query = f"""
        SELECT 
            COUNT(*) FILTER (WHERE _rescued_data IS NOT NULL) as rescued_records,
            COUNT(*) FILTER (WHERE _ingestion_timestamp IS NULL) as missing_timestamp,
            MIN(_ingestion_timestamp) as first_ingestion,
            MAX(_ingestion_timestamp) as last_ingestion
        FROM {catalog}.bronze.nyc_311_raw
        WHERE date(_ingestion_timestamp) = '{date_str}'
           OR _source_file LIKE '%{s3_pattern}%'
        """
        quality_results = spark.sql(quality_query).collect()[0]
        metrics['rescued_records'] = quality_results[0]
        metrics['missing_timestamp'] = quality_results[1]
        metrics['first_ingestion'] = str(quality_results[2]) if quality_results[2] else None
        metrics['last_ingestion'] = str(quality_results[3]) if quality_results[3] else None
        
        # 4. Calculate deviations
        if metrics['same_day_avg'] > 0:
            metrics['deviation_from_same_day'] = ((metrics['total_records'] - metrics['same_day_avg']) / metrics['same_day_avg']) * 100
        else:
            metrics['deviation_from_same_day'] = 0
            
        if metrics['historical_avg'] > 0:
            metrics['deviation_from_avg'] = ((metrics['total_records'] - metrics['historical_avg']) / metrics['historical_avg']) * 100
        else:
            metrics['deviation_from_avg'] = 0
        
        # 5. Determine status with dynamic thresholds
        if metrics['total_records'] == 0:
            metrics['status'] = 'NO_DATA'
            metrics['alert'] = True
            metrics['message'] = f"No data found for {date_str}"
            
        elif metrics['files_processed'] == 0:
            metrics['status'] = 'NO_FILES'
            metrics['alert'] = True
            metrics['message'] = f"No files processed for {date_str}"
            
        elif abs(metrics['deviation_from_same_day']) > deviation_threshold:
            metrics['status'] = 'ANOMALY_DETECTED'
            metrics['alert'] = True
            metrics['message'] = f"{day_of_week} data ({metrics['total_records']:,}) deviates {metrics['deviation_from_same_day']:.1f}% from typical {day_of_week} ({metrics['same_day_avg']:,})"
            
        elif metrics['historical_stddev'] > 0 and abs(metrics['total_records'] - metrics['historical_avg']) > (2 * metrics['historical_stddev']):
            metrics['status'] = 'STATISTICAL_ANOMALY'
            metrics['alert'] = True
            metrics['message'] = f"Data volume ({metrics['total_records']:,}) is outside 2 standard deviations from average ({metrics['historical_avg']:,} ± {metrics['historical_stddev']:,})"
            
        elif metrics['rescued_records'] > metrics['total_records'] * 0.05:
            metrics['status'] = 'QUALITY_ISSUE'
            metrics['alert'] = True
            metrics['message'] = f"High number of rescued records: {metrics['rescued_records']:,} ({(metrics['rescued_records']/metrics['total_records']*100):.1f}%)"
            
        else:
            metrics['status'] = 'OK'
            metrics['alert'] = False
            metrics['message'] = f"Data looks normal: {metrics['total_records']:,} records (typical for {day_of_week}: {metrics['same_day_avg']:,})"
            
        # 6. Add trend information
        trend_query = f"""
        SELECT 
            date(_ingestion_timestamp) as date,
            COUNT(*) as count
        FROM {catalog}.bronze.nyc_311_raw
        WHERE date(_ingestion_timestamp) >= date_sub('{date_str}', 7)
          AND date(_ingestion_timestamp) <= '{date_str}'
        GROUP BY date(_ingestion_timestamp)
        ORDER BY date
        """
        metrics['recent_trend'] = [row.asDict() for row in spark.sql(trend_query).collect()]
            
        return metrics
        
    except Exception as e:
        return {
            'status': 'ERROR',
            'alert': True,
            'message': str(e),
            'total_records': 0,
            'date': date_str,
            'day_of_week': day_of_week
        }

# COMMAND ----------

# Run monitoring
monitoring_results = check_daily_data(process_date)
print(json.dumps(monitoring_results, indent=2))

# COMMAND ----------

# Visualize recent trend
if 'recent_trend' in monitoring_results:
    trend_df = spark.createDataFrame(monitoring_results['recent_trend'])
    display(trend_df)

# COMMAND ----------

# Log monitoring results
monitoring_log_query = f"""
INSERT INTO {catalog}.bronze.monitoring_log
VALUES (
    '{process_date}',
    current_timestamp(),
    '{monitoring_results['status']}',
    {monitoring_results.get('total_records', 0)},
    {monitoring_results.get('files_processed', 0)},
    {monitoring_results.get('rescued_records', 0)},
    {monitoring_results.get('historical_avg', 0)},
    {monitoring_results.get('deviation_from_avg', 0)},
    {monitoring_results.get('alert', False)},
    '{monitoring_results.get('message', '')}'
)
"""

try:
    spark.sql(monitoring_log_query)
except:
    # Create table if not exists
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.bronze.monitoring_log (
        monitoring_date DATE,
        check_timestamp TIMESTAMP,
        status STRING,
        record_count BIGINT,
        file_count INT,
        rescued_records BIGINT,
        historical_avg BIGINT,
        deviation_percentage DOUBLE,
        alert_triggered BOOLEAN,
        message STRING
    )
    """)
    spark.sql(monitoring_log_query)

# COMMAND ----------

# Create comparison dashboard
comparison_query = f"""
WITH daily_stats AS (
    SELECT 
        date(_ingestion_timestamp) as date,
        COUNT(*) as daily_count,
        dayname(date(_ingestion_timestamp)) as day_name
    FROM {catalog}.bronze.nyc_311_raw
    WHERE date(_ingestion_timestamp) >= date_sub(current_date(), 30)
    GROUP BY date(_ingestion_timestamp)
),
averages AS (
    SELECT 
        day_name,
        AVG(daily_count) as avg_count,
        COUNT(*) as sample_size
    FROM daily_stats
    GROUP BY day_name
)
SELECT 
    d.date,
    d.day_name,
    d.daily_count,
    a.avg_count as typical_for_day,
    ROUND((d.daily_count - a.avg_count) / a.avg_count * 100, 1) as deviation_pct
FROM daily_stats d
JOIN averages a ON d.day_name = a.day_name
ORDER BY d.date DESC
"""

print("📊 Last 30 Days Pattern Analysis:")
display(spark.sql(comparison_query))

# COMMAND ----------

# Exit with appropriate status
if monitoring_results['alert']:
    dbutils.notebook.exit(f"ALERT: {monitoring_results['message']}")
else:
    dbutils.notebook.exit(f"SUCCESS: {monitoring_results['message']}")