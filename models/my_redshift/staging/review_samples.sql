-- dbt model: review_samples.sql
select *
from {{ ref('public.review_samples') }};
