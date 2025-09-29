#!/usr/bin/env python3
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

try:
    # Tek bir aylık dosya test et
    test_monthly = "s3://nyc-311-bronze/year=2020/month=01/nyc_311_2020_01.json.gz"
    df1 = spark.read.json(test_monthly)
    print(f"Aylık dosya OK: {df1.count()} kayıt")
    
    # Tek bir günlük dosya test et  
    test_daily = "s3://nyc-311-bronze/year=2025/month=09/day=01/nyc_311_2025_09_01.json.gz"
    df2 = spark.read.json(test_daily)
    print(f"Günlük dosya OK: {df2.count()} kayıt")
    
    # Schema'ları karşılaştır
    print("\nSchema kontrolü:")
    print(df1.columns == df2.columns)
    
except Exception as e:
    print(f"HATA: {e}")
    import traceback
    traceback.print_exc()

spark.stop()