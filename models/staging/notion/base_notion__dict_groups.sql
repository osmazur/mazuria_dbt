with dict_groups as (

    select * from {{ source('notion', 'dict_group') }}

),

names as (

    select
        _dlt_parent_id,
        plain_text as name
    from {{ source('notion', 'dict_group__properties__name__title') }}

),

days as (

    select
        _dlt_parent_id,
        string_agg(plain_text, '' order by _dlt_list_idx) as days
    from {{ source('notion', 'dict_group__properties__days__rich_text') }}
    group by _dlt_parent_id

),

hours as (

    select
        _dlt_parent_id,
        string_agg(plain_text, '' order by _dlt_list_idx) as hours
    from {{ source('notion', 'dict_group__properties__hours__rich_text') }}
    group by _dlt_parent_id

),

final as (

    select
        g.id,
        g.created_time,
        g.last_edited_time,
        g.archived,
        g.in_trash,
        g.properties__formula__formula__string  as schedule,
        n.name,
        d.days,
        h.hours

    from dict_groups g
    left join names n on n._dlt_parent_id = g._dlt_id
    left join days d on d._dlt_parent_id = g._dlt_id
    left join hours h on h._dlt_parent_id = g._dlt_id

)

select * from final
