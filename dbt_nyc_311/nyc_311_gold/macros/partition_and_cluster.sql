-- macros/partition_and_cluster.sql
-- Custom macro for Databricks Delta Lake partitioning and clustering

{% macro partition_by_clause(config) %}
  {%- if config.get('partition_by') -%}
    {%- set partition_config = config.get('partition_by') -%}
    {%- if partition_config is mapping -%}
      {#- Structured partition config #}
      {%- set field = partition_config.get('field') -%}
      {%- set granularity = partition_config.get('granularity', 'day') -%}
      
      {%- if granularity == 'year' -%}
        partitioned by (year({{ field }}))
      {%- elif granularity == 'month' -%}
        partitioned by (year({{ field }}), month({{ field }}))
      {%- elif granularity == 'day' -%}
        partitioned by (date({{ field }}))
      {%- else -%}
        partitioned by ({{ field }})
      {%- endif -%}
    {%- else -%}
      {#- Simple field name #}
      partitioned by ({{ partition_config }})
    {%- endif -%}
  {%- endif -%}
{% endmacro %}


{% macro cluster_by_clause(config) %}
  {%- if config.get('cluster_by') -%}
    {%- set cluster_fields = config.get('cluster_by') -%}
    {%- if cluster_fields is string -%}
      cluster by ({{ cluster_fields }})
    {%- elif cluster_fields is sequence -%}
      cluster by ({{ cluster_fields | join(', ') }})
    {%- endif -%}
  {%- endif -%}
{% endmacro %}


{% macro optimize_table_properties() %}
  -- Delta Lake optimization properties
  tblproperties (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.tuneFileSizesForRewrites' = 'true'
  )
{% endmacro %}
