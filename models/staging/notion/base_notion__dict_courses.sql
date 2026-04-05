with dict_courses as (

    select * from {{ source('notion', 'dict_course') }}

),

names as (

    select
        _dlt_parent_id,
        plain_text as name
    from {{ source('notion', 'dict_course__properties__name__title') }}

),

final as (

    select
        c.id,
        c.created_time,
        c.last_edited_time,
        c.archived,
        c.in_trash,
        c.properties__price__number                 as price,
        c.properties__hours__number                 as hours,
        c.properties__classes__number               as classes,
        c.properties__items__number                 as items,
        c.properties__created_time__created_time    as notion_created_time,
        n.name

    from dict_courses c
    left join names n on n._dlt_parent_id = c._dlt_id

)

select * from final
