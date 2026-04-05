with teachers as (

    select * from {{ source('notion', 'teacher') }}

),

names as (

    select
        _dlt_parent_id,
        plain_text as name
    from {{ source('notion', 'teacher__properties__name__title') }}

),

birthdays as (

    select
        _dlt_parent_id,
        plain_text as birthday
    from {{ source('notion', 'teacher__properties__birthday__rich_text') }}

),

employment_types as (

    select
        _dlt_parent_id,
        string_agg(name, ', ' order by _dlt_list_idx) as employment_type
    from {{ source('notion', 'teacher__properties__type__multi_select') }}
    group by _dlt_parent_id

),

final as (

    select
        t.id,
        t.created_time,
        t.last_edited_time,
        t.archived,
        t.in_trash,
        n.name,
        b.birthday,
        et.employment_type

    from teachers t
    left join names n on n._dlt_parent_id = t._dlt_id
    left join birthdays b on b._dlt_parent_id = t._dlt_id
    left join employment_types et on et._dlt_parent_id = t._dlt_id

)

select * from final
