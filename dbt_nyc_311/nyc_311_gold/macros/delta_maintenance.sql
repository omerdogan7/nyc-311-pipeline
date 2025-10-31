-- macros/delta_maintenance.sql
-- Delta Lake table maintenance utilities for Databricks

{% macro optimize_table(table_name, zorder_columns=none) %}
  {#- Run OPTIMIZE command for Delta tables #}
  {% set optimize_query %}
    optimize {{ table_name }}
    {% if zorder_columns %}
      zorder by ({{ zorder_columns | join(', ') }})
    {% endif %}
  {% endset %}
  
  {% do log("Optimizing table: " ~ table_name, info=true) %}
  {% do run_query(optimize_query) %}
  {% do log("Optimization complete for: " ~ table_name, info=true) %}
{% endmacro %}


{% macro vacuum_table(table_name, retention_hours=168) %}
  {#- Run VACUUM command to clean up old files #}
  {#- Default retention: 168 hours = 7 days #}
  {% set vacuum_query %}
    vacuum {{ table_name }} retain {{ retention_hours }} hours
  {% endset %}
  
  {% do log("Vacuuming table: " ~ table_name ~ " (retention: " ~ retention_hours ~ " hours)", info=true) %}
  {% do run_query(vacuum_query) %}
  {% do log("Vacuum complete for: " ~ table_name, info=true) %}
{% endmacro %}


{% macro analyze_table(table_name) %}
  {#- Compute statistics for query optimization #}
  {% set analyze_query %}
    analyze table {{ table_name }} compute statistics for all columns
  {% endset %}
  
  {% do log("Analyzing table: " ~ table_name, info=true) %}
  {% do run_query(analyze_query) %}
  {% do log("Analysis complete for: " ~ table_name, info=true) %}
{% endmacro %}


{% macro optimize_all_gold_tables() %}
  {#- Optimize all gold layer tables #}
  {% set tables_to_optimize = [
    ('fact_311', ['agency_key', 'location_key', 'created_date_key']),
    ('agg_daily_summary', ['date_key']),
    ('agg_agency_metrics', ['agency_key', 'date_key']),
    ('agg_location_metrics', ['location_key', 'date_key'])
  ] %}
  
  {% for table_name, zorder_cols in tables_to_optimize %}
    {{ optimize_table(ref(table_name), zorder_cols) }}
  {% endfor %}
{% endmacro %}


{% macro maintenance_report() %}
  {#- Generate maintenance report for all tables #}
  {% set report_query %}
    select 
      table_name,
      num_files,
      total_size_bytes / 1024 / 1024 / 1024 as size_gb,
      data_change_rate,
      last_modified
    from 
      (describe extended {{ target.schema }}.fact_311) -- Change to any table to get schema info
    where 
      col_name in ('num_files', 'total_size_bytes', 'data_change_rate', 'last_modified')
  {% endset %}
  
  {% do log("=== Delta Lake Maintenance Report ===", info=true) %}
  {{ return(run_query(report_query)) }}
{% endmacro %}
