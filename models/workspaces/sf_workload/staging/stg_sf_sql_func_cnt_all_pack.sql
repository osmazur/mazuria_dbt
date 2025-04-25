

with source as (

    select
        project,
        folder,
        file,
        function,
        count,
        added_at
    from {{ source('raw_sf', 'sf_sql_func_cnt_all_pack') }}

)

select
    *
from source
