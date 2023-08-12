-- dbt model: minimum_nights_tier.sql
WITH base_table as (
    SELECT distinct minimum_nights,
           count(*) as counter
    FROM public.listings
    GROUP BY minimum_nights
)
SELECT 
    CASE 
        WHEN minimum_nights BETWEEN 1 AND 5 THEN '1-5'
        WHEN minimum_nights BETWEEN 6 AND 29 THEN '6-29'
        WHEN minimum_nights BETWEEN 30 AND 60 THEN '30-60'
        WHEN minimum_nights > 60 THEN '60+'
    END as minimum_nights_tier,
    sum(counter) as num_properties
FROM base_table
GROUP BY minimum_nights_tier
ORDER BY num_properties DESC;
