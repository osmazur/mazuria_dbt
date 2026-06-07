with groups as (

    select * from {{ source('notion', 'group') }}

),

students as (

    select
        _dlt_parent_id,
        id as student_id
    from {{ source('notion', 'group__properties__student__relation') }}

),

courses as (

    select
        _dlt_parent_id,
        id as course_id
    from {{ source('notion', 'group__properties__course__relation') }}

),

dict_groups as (

    select
        _dlt_parent_id,
        id as dict_group_id
    from {{ source('notion', 'group__properties__group__relation') }}

),

teachers as (

    select
        _dlt_parent_id,
        id as teacher_id
    from {{ source('notion', 'group__properties__teacher__relation') }}

),

notes as (

    select
        _dlt_parent_id,
        string_agg(plain_text, '' order by _dlt_list_idx) as note
    from {{ source('notion', 'group__properties__note__title') }}
    group by _dlt_parent_id

),

payments as (

    select
        _dlt_parent_id,
        string_agg(plain_text, '' order by _dlt_list_idx) as payment_info
    from {{ source('notion', 'group__properties__payment__rich_text') }}
    group by _dlt_parent_id

),

min_attendance_dates as (

    select
        ar._dlt_parent_id,
        min(a.properties__day__date__start::date) as first_attendance_date
    from {{ source('notion', 'group__properties__attendance__relation') }} ar
    inner join {{ source('notion', 'attendance') }} a
        on a.id = ar.id
    group by ar._dlt_parent_id

),

final as (

    select
        g.id,
        g.created_time,
        g.last_edited_time,
        g.archived,
        g.in_trash,
        g.properties__started_at__date__start              as started_at,
        g.properties__ended_at__date__start                as ended_at,
        g.properties__total_classes__rollup__number         as total_classes,
        g.properties__present_count__rollup__number         as present_count,
        g.properties__absent_count__rollup__number          as absent_count,
        g.properties__attendance_percent__formula__number   as attendance_percent,
        g.properties__is_active__formula__string            as is_active,
        g.properties__phone__formula__string                as phone,
        g.properties__discount_pr__number                   as discount_percent,
        s.student_id,
        c.course_id,
        dg.dict_group_id,
        t.teacher_id,
        n.note,
        p.payment_info,
        mad.first_attendance_date

    from groups g
    left join students s on s._dlt_parent_id = g._dlt_id
    left join courses c on c._dlt_parent_id = g._dlt_id
    left join dict_groups dg on dg._dlt_parent_id = g._dlt_id
    left join teachers t on t._dlt_parent_id = g._dlt_id
    left join notes n on n._dlt_parent_id = g._dlt_id
    left join payments p on p._dlt_parent_id = g._dlt_id
    left join min_attendance_dates mad on mad._dlt_parent_id = g._dlt_id

)

select * from final
