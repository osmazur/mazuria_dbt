{%- macro grant_usage_to_schemas() -%}

    {%- set users_uat = 'user_a, user_b' -%}
    {%- set users_prod = 'user_c, user_d' -%}

     {%- if  target.name == "dbt_prod" or target.name == 'ci_job_prod' -%}
        {% for schema in schemas%} grant usage on schema {{ schema }} to {{ users_prod }}; {% endfor%}
        {% for schema in schemas %}grant select on all tables in schema {{ schema }} to {{ users_prod }};{% endfor%}
        {% for schema in schemas %}alter default privileges in schema {{ schema }}  grant select on tables to {{ users_prod }};{% endfor %}
    {%- else -%}
        {% for schema in schemas%} grant usage on schema {{ schema }} to {{ users_uat }}; {% endfor%}
        {% for schema in schemas %}grant select on all tables in schema {{ schema }} to {{ users_uat }};{% endfor%}
        {% for schema in schemas %}alter default privileges in schema {{ schema }}  grant select on tables to {{ users_uat }};{% endfor %}
    {%- endif -%}
    
{%- endmacro -%} 