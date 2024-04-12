{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}
        mazuria_dbt_prod
    {%- elif target.name == 'uat' -%}
        mazuria_dbt_uat
    {%- elif target.user == 'oleksandrmazur' -%} prod_ci_job
        mazuria_dbt_{{target.user}}
    {%- elif target.name == 'prod_ci_job' -%} prod_ci_job
        mazuria_dbt_prod_ci
    {%- elif target.name == 'uat_ci_job' -%} prod_ci_job
        mazuria_dbt_uat_ci
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
        

{%- endmacro %}

