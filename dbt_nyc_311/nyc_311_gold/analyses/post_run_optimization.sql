-- analyses/post_run_optimization.sql
-- Run after dbt run to optimize all tables

/*
Usage:
  dbt run-operation optimize_all_gold_tables
  
Or add as post-hook in dbt_project.yml:
  on-run-end:
    - "{{ optimize_all_gold_tables() }}"
*/

-- This file documents the optimization strategy

-- FACT TABLE (fact_311)
-- Optimize weekly, ZORDER by most filtered columns
-- OPTIMIZE gold.fact_311 ZORDER BY (agency_key, location_key, created_date_key);
-- VACUUM gold.fact_311 RETAIN 168 HOURS;

-- DAILY AGGREGATIONS (agg_daily_summary)
-- Optimize monthly, ZORDER by date
-- OPTIMIZE gold.agg_daily_summary ZORDER BY (date_key);

-- AGENCY METRICS (agg_agency_metrics)
-- Optimize monthly, ZORDER by composite key
-- OPTIMIZE gold.agg_agency_metrics ZORDER BY (agency_key, date_key);

-- LOCATION METRICS (agg_location_metrics)
-- Optimize monthly, ZORDER by geographic hierarchy
-- OPTIMIZE gold.agg_location_metrics ZORDER BY (location_key, date_key);

-- SCHEDULE:
-- Daily: ANALYZE TABLE (statistics)
-- Weekly: OPTIMIZE with ZORDER
-- Monthly: VACUUM old files
