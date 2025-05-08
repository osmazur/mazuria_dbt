with 

stg_sf_sql_func_cnt_all as (

    select
        project,
        folder,
        file,
        function,
        count,
        added_at
    from {{ref('stg_sf_sql_func_cnt_all')}}

),

stg_sf_sql_qs_cnt_all as (

    select
    project,
        folder,
        file,
        function,
        count,
        added_at
    from {{ref('stg_sf_sql_qs_cnt_all')}}

),

stg_sf_sql_func_cnt_all_pack as (

    select
    project,
        folder,
        file,
        function,
        count,
        added_at
    from {{ref('stg_sf_sql_func_cnt_all_pack')}}


),

stg_sf_sql_qs_cnt_all_pack as (

    select
    project,
        folder,
        file,
        function,
        count,
        added_at
    from {{ref('stg_sf_sql_qs_cnt_all_pack')}}

),

stg_sf_sql_ddl_dml_cnt_all as (

    select
    project,
        folder,
        file,
        function,
        count,
        added_at
    from {{ref('stg_sf_sql_ddl_dml_cnt_all')}}

),

stg_sf_sql_ddl_dml_cnt_all_pack as (

    select
    project,
        folder,
        file,
        function,
        count,
        added_at
    from {{ref('stg_sf_sql_ddl_dml_cnt_all_pack')}}

),


unionall as (

    select *, 'function' as object_type from stg_sf_sql_func_cnt_all
    union all
    select *, 'function' as object_type from stg_sf_sql_func_cnt_all_pack
    union all 
    select *, 'query_syntax' as object_type from stg_sf_sql_qs_cnt_all
    union all
    select *, 'query_syntax' as object_type from stg_sf_sql_qs_cnt_all_pack
    union all
    select *, 'ddl_dml' as object_type from stg_sf_sql_ddl_dml_cnt_all
    union all
    select *, 'ddl_dml' as object_type from stg_sf_sql_ddl_dml_cnt_all_pack
    
    
),

renamed as (

    select
        project as project,
        folder as folder,
        file as file_nm,
        lower(function) as function_name,
        coalesce(count,0) as function_cnt,
        case 
            when project like ('%-pack-%') then 'package_compl'
            when project in ('pr-compl', 'pr-compl', 'pr1', 'pr2') then 'real_world'
            when project like ('%demo') then 'demo'
            when project like ('%notcompl') then 'package_all_not_compl'
            when project like ('gitlab-run') then 'gitlab-run'
            when project like ('%-run') then 'package-run'
            else project
        end as project_type,
        object_type,
        case when project like ('%compl') then true else false end as is_compiled,
        added_at

    from unionall

)

select * from renamed
--where object_type = 'ddl_dml'
--select project from renamed group by 1