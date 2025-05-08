

with source as (

    select
        project,
        folder,
        file,
        lower(function) as function,
        count,
        added_at
    from {{ source('raw_sf', 'sf_sql_ddl_dml_cnt_all_pack') }}

)

select
    *
from source
