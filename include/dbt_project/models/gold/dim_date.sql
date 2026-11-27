with days as (
    select
        dateadd(day, row_number() over (order by seq4()) - 1, '2016-01-01'::date) as date_day
    from table(generator(rowcount => 1096))
)

select
    date_day                          as date_key,
    year(date_day)                    as year,
    quarter(date_day)                 as quarter,
    month(date_day)                   as month,
    monthname(date_day)               as month_name,
    to_char(date_day, 'YYYY-MM')      as year_month,
    day(date_day)                     as day_of_month,
    dayofweekiso(date_day)            as day_of_week,
    dayname(date_day)                 as day_name,
    dayofweekiso(date_day) in (6, 7)  as is_weekend
from days