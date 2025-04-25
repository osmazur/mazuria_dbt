{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}
        dbt_prod_{{ custom_schema_name | trim }}
    {%- elif target.name == 'uat' -%}
        dbt_uat_{{ custom_schema_name | trim }}
    {%- elif target.user == 'oleksandrmazur' -%}
        embucket
    {%- elif target.name == 'prod_ci_job' -%}
        dbt_prod_ci
    {%- elif target.name == 'uat_ci_job' -%}
        dbt_uat_ci
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
        

{%- endmacro %}

