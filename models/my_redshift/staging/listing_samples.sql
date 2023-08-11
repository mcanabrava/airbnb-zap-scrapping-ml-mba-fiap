-- dbt model: listing_samples.sql
select *
from {{ ref('public.listing_samples') }};
