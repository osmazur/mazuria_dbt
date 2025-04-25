with source as (

    select
        page_name,
        category_name
    from {{ref('sf_sql_pagename_dict_seed')}}

)

select * from source
