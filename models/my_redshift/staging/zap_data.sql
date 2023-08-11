-- dbt model: zap_data.sql
select *
from {{ ref('public.s3_to_rs_zap') }} as zap_data;
