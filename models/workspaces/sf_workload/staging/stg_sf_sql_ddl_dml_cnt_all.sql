

with source as (

    select
        project,
        folder,
        file,
        lower(function) as function,
        count,
        added_at
    from {{ source('raw_sf', 'sf_sql_ddl_dml_cnt_all') }}

)

select * from source

-- select
--     function, sum(count)
-- from source
-- group by 1
-- order by 2 desc