-- New and active clients broken down per course (day x course).
-- purchase_type distinguishes 'group' courses from 'subscription' courses
-- (course_name ilike 'абонемент%').

select
    date_day,
    course_id,
    course_name,
    purchase_type,
    new_customers,
    active_customers
from {{ ref('int_customers_by_course_daily') }}
