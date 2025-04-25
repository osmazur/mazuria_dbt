
{{ config(
  enabled=false
) }}

with prep as (
        select
            project,
            function_name,
            file_nm,
            category,
            case when function_cnt is null then 0 else function_cnt end as function_cnt
        from embucket.fct_sf_sql_func
    ),
    project as (
        select
            project,
            sum(function_cnt) as total_func_per_project,
            count(distinct file_nm) as total_files_per_project
        from embucket.fct_sf_sql_func 
        group by 1
    ),

    lastp as (

        select
            project,
            function_name,
            category
        from embucket.fct_sf_sql_func
        group by 1,2,3


    ),
    finale as (
        select
            prep.project,
            function_name,
            category,
            round(
                cast(sum(function_cnt) as numeric)
                / project.total_func_per_project
                * 100,
                2
            ) as "usage_func_%",
            round(
                cast(count(distinct file_nm) as numeric)
                / project.total_files_per_project
                * 100,
                2
            ) as "usage_files_%",
            rank() over (partition by prep.project order by sum(function_cnt) desc)
            -- else null end 
            as rk_total_func_count,

            rank() over (
                partition by prep.project order by count(distinct file_nm) desc
            )
            as rk_total_file_count,
            sum(function_cnt) as total_func_per_func,
            count(distinct file_nm) as total_files_per_func,
            round(
                cast(sum(function_cnt) as numeric) / count(distinct file_nm), 1
            ) func_per_file,
            total_func_per_project,
            total_files_per_project
        from prep
        left join project on project.project = prep.project
        group by
            prep.project,
            function_name,
            category,
            project.total_files_per_project,
            project.total_func_per_project
        order by total_func_per_project desc
    ), pr as (
select
   row_number() over (partition by project order by (rk_total_file_count + rk_total_func_count) asc) as score,
    (rk_total_file_count + rk_total_func_count) as rank_score,
    *
from finale
order by 1 asc

    )

    select
    --lastp.project,
    row_number() over(order by (gl_compl.score + pr3_compl.score) asc) rn_compl,
    row_number() over(order by (gl.score + pr3.score +pr1.score + pr2.score) asc) rn_not_compl,
    row_number() over(order by (mm_demo.score + sm_demo.score) asc) rn_demo,
    lastp.function_name,
    lastp.category,
    gl_compl.score + pr3_compl.score as coml_total_score,
    gl.score + pr3.score + pr1.score + pr2.score as not_coml_total_score,
    mm_demo.score + sm_demo.score as demo_total_score,
    gl_compl.score as gl_compl_score,
    gl.score as gl_score,
    pr3_compl.score as pr3_compl_score,
    pr3.score as pr3_score,
    pr2.score as pr2_score,
    pr1.score as pr1_score,
    mm_demo.score as mm_demo_score,
    sm_demo.score as sm_demo_score
    from lastp
    left join pr as pr3_compl on pr3_compl.function_name = lastp.function_name and pr3_compl.project = 'pr3-compl'
    left join pr as pr3 on pr3.function_name = lastp.function_name  and pr3.project = 'pr3'
    left join pr as gl_compl on gl_compl.function_name = lastp.function_name and gl_compl.project = 'gl-compl'
    left join pr as gl on gl.function_name = lastp.function_name  and gl.project = 'gl'
    left join pr as pr2 on pr2.function_name = lastp.function_name  and pr2.project = 'pr2'
    left join pr as pr1 on pr1.function_name = lastp.function_name  and pr1.project = 'pr1'
    left join pr as mm_demo on mm_demo.function_name = lastp.function_name  and mm_demo.project = 'mm-demo'
    left join pr as sm_demo on sm_demo.function_name = lastp.function_name  and sm_demo.project = 'sm-demo'
    group by 4,5,8,9,10,11,12,13,14,15,16
    order by  1 asc