with attendances as (

    select * from {{ source('notion', 'attendance') }}

),

student_names as (

    select
        _dlt_parent_id,
        string_agg(plain_text, '' order by _dlt_list_idx) as student_name
    from {{ source('notion', 'attendance__properties__student_name__rich_text') }}
    group by _dlt_parent_id

),

student_phones as (

    select
        _dlt_parent_id,
        string_agg(plain_text, '' order by _dlt_list_idx) as student_phone
    from {{ source('notion', 'attendance__properties__student_phone__rich_text') }}
    group by _dlt_parent_id

),

teacher_names as (

    select
        _dlt_parent_id,
        string_agg(plain_text, '' order by _dlt_list_idx) as teacher_name
    from {{ source('notion', 'attendance__properties__teacher__rich_text') }}
    group by _dlt_parent_id

),

course_names as (

    select
        _dlt_parent_id,
        string_agg(plain_text, '' order by _dlt_list_idx) as course_name
    from {{ source('notion', 'attendance__properties__course__rich_text') }}
    group by _dlt_parent_id

),

group_names as (

    select
        _dlt_parent_id,
        string_agg(plain_text, '' order by _dlt_list_idx) as group_name
    from {{ source('notion', 'attendance__properties__group__rich_text') }}
    group by _dlt_parent_id

),

payments as (

    select
        _dlt_parent_id,
        string_agg(plain_text, '' order by _dlt_list_idx) as payment_info
    from {{ source('notion', 'attendance__properties__payment__rich_text') }}
    group by _dlt_parent_id

),

final as (

    select
        a.id,
        a.created_time,
        a.last_edited_time,
        a.archived,
        a.in_trash,
        a.properties__id__unique_id__number             as unique_id,
        a.properties__day__date__start                  as day,
        a.properties__status__status__name              as status,
        a.properties__is_present__formula__number       as is_present,
        a.properties__is_absent__formula__number        as is_absent,
        sn.student_name,
        sp.student_phone,
        tn.teacher_name,
        cn.course_name,
        gn.group_name,
        p.payment_info

    from attendances a
    left join student_names sn on sn._dlt_parent_id = a._dlt_id
    left join student_phones sp on sp._dlt_parent_id = a._dlt_id
    left join teacher_names tn on tn._dlt_parent_id = a._dlt_id
    left join course_names cn on cn._dlt_parent_id = a._dlt_id
    left join group_names gn on gn._dlt_parent_id = a._dlt_id
    left join payments p on p._dlt_parent_id = a._dlt_id

)

select * from final
