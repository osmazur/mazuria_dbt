with stg_sf_sql_func_cnt_all as (

	select 
		*
	from {{ref('int_sf_sql_func_cnt_all')}}

),


stg_sf_sql_func_dict as (

	select 
		*
	from {{ref('int_sf_sql_func_dict')}}

),
finale as (
    select 
        fcnt.project,
        fcnt.folder,
        fcnt.project_type,
        fcnt.file_nm,
        fdict.page_name,
        fdict.object_type,
        case when fdict.category_pn is null then 'CATEGORY_NOT_MAPPED' else fdict.category_pn end as category,
        case 
            when fdict.function_name = '::' then 'cast' 
            when fdict.function_name = '||' then 'concat'
            when fdict.function_name = ':' then 'get_path'
        else fdict.function_name end as function_name,
        fcnt.function_name as fname_from_file,
        --fdict.category_array,
        --fdict.category_2,
        --fdict.category_3,
        --case when fdict.function_name is null then false else true end as is_sf_function,
        case when  fcnt.project like '%demo%' then true else false end as is_demo_project,
        fcnt.is_compiled,
        fcnt.function_cnt
    from stg_sf_sql_func_dict as fdict
    left join stg_sf_sql_func_cnt_all as fcnt  on fdict.function_name = fcnt.function_name
    
),

int_sf_func as (

	select 
		project,
        project_type,
        object_type,
        folder,
        file_nm,
        page_name,
        category,
        lower(function_name) as function_name,
        lower(fname_from_file) as fname_from_file,
        --category_array,
        is_demo_project,
        is_compiled,
        function_cnt
	from finale

)

select * from int_sf_func
