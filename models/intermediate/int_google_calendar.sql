with stg_gc__google_calendar as (

	select 
		*
	from {{ref('stg_gc__google_calendar')}}

),

prep as (
    select 
        event_id,
        event_title,
-- date and time
        event_start_date,
        event_start_time,
        event_end_date,
        event_end_time,
        event_end_time - event_start_time as event_time_spent,
       
-- other
        event_guests,
        -- event_teacher,
        REPLACE(split_part(event_teacher, '@', 1),'.',' ') AS event_teacher,
        split_part(event_lesson_type, '@', 1) AS event_lesson_type,
        -- event_lesson_type,
        is_finished,
        event_description,
        loaded_at
    from stg_gc__google_calendar
    where event_title != 'Неробочий час'
)

select *, EXTRACT(EPOCH FROM event_time_spent) / 3600 AS "event_time_spent_to_num" from prep