-- dbt model: calendar.sql
select *
from {{ ref('public.calendar') }};
