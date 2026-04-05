with students as (

    select * from {{ source('notion', 'student') }}

),

names as (

    select
        _dlt_parent_id,
        plain_text as name
    from {{ source('notion', 'student__properties__name__title') }}

),

notes as (

    select
        _dlt_parent_id,
        string_agg(plain_text, '' order by _dlt_list_idx) as note
    from {{ source('notion', 'student__properties__note__rich_text') }}
    group by _dlt_parent_id

),

final as (

    select
        s.id,
        s.created_time,
        s.last_edited_time,
        s.archived,
        s.in_trash,
        s.properties__id__unique_id__number         as unique_id,
        s.properties__phone__phone_number            as phone,
        s.properties__rollup__rollup__number         as enrollments_count,
        s.properties__created_time__created_time     as notion_created_time,
        n.name,
        no.note

    from students s
    left join names n on n._dlt_parent_id = s._dlt_id
    left join notes no on no._dlt_parent_id = s._dlt_id

)

select * from final
