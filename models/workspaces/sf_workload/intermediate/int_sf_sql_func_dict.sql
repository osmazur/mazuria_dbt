
with stg_sf_sql_func_dict as (

    select * from {{ref('stg_sf_sql_func_dict')}}

),

pagename as (

    select * from {{ref('stg_sf_sql_func_pagename_dict')}}
),

query_syntax_dict as (

    select * from {{ref('stg_sf_sql_qs_pagename_dict')}}
    
),

ddl_dml_dict as (

    select * from {{ref('stg_sf_sql_ddl_dml_pagename_dict')}}

),

functions_dict as (

select 
    stg_sf_sql_func_dict.*, 
    pagename.category_name as category_pn,
    case 
        when function_name = '::' then 'cast' 
        when function_name = '||' then 'concat'
        when function_name = ':' then 'get_path'
        else function_name 
    end as function_name_unified
from stg_sf_sql_func_dict
left join pagename on pagename.page_name = stg_sf_sql_func_dict.page_name
where function_name not like ('%deprecated%') and function_name not like ('%finetune%')
)

select
    id,
    function_name,
    page_name,
    url,
    --category_array,
    category,
    description,
    category_pn,
    function_name_unified,
    'function' as object_type
from functions_dict
union all 
select
    id,
    function_name,
    page_name,
    url,
    --category_array,
    category,
    description,
    category_pn,
    function_name_unified,
    'query_syntax' as object_type
from query_syntax_dict
union all 
select
    id,
    function_name,
    page_name,
    url,
    --category_array,
    category,
    description,
    category_pn,
    function_name_unified,
    'ddl_dml' as object_type
from ddl_dml_dict