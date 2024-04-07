select * from {{ ref('model_a') }} as a
left join {{ ref('model_b') }} as b on b.date = a.date