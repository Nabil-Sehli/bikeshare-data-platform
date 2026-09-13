{#
    Use the configured schema name as-is (staging, intermediate, marts) instead of
    dbt's default "<target_schema>_<custom_schema>". The schemas are created and
    permissioned by Terraform, so names must match exactly.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
