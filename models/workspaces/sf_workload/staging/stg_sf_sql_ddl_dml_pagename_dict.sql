with source as (

    select
        ddl_dml_name,
        ddl_dml_page_name,
        ddl_dml_url,
        ddl_dml_category_array,
        ddl_dml_category,
        ddl_dml_description
        --object_type
    from {{ref('sf_sql_ddl_dml_pagename_dict')}}

)

select
    0 as id,
    ddl_dml_name as function_name,
    ddl_dml_page_name as page_name,
    ddl_dml_url as url,
    ddl_dml_category_array as category_array,
    ddl_dml_category as category,
    ddl_dml_description as description,
    ddl_dml_category as category_pn,
    ddl_dml_name as function_name_unified
    --object_type
from source
where ddl_dml_name != 'select'