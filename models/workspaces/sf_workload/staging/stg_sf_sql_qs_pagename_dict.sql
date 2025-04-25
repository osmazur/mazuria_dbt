with source as (

    select
        qs_name,
        qs_page_name,
        qs_url,
        qs_category_array,
        qs_category,
        qs_description
    from {{ref('sf_sql_query_syntax_pagename_dict_seed')}}

)

select
    0 as id,
    qs_name as function_name,
    qs_page_name as page_name,
    qs_url as url,
    qs_category_array as category_array,
    qs_category as category,
    qs_description as description,
    qs_category as category_pn,
    qs_name as function_name_unified
from source
