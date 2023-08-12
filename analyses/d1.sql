-- dbt model: host_listing_tier.sql
WITH BASE_TABLE AS (
    SELECT 
        host_id,
        count(id) as num_listings,
        CASE 
            WHEN count(id) > 50 THEN '50+'
            WHEN count(id) >= 10 THEN '10-49'
            WHEN count(id) >= 4 THEN '4-9'
            WHEN count(id) = 3 THEN '3'
            WHEN count(id) = 2 THEN '2'
            WHEN count(id) = 1 THEN '1'
        END as num_listings_tier
    FROM public.listings
    GROUP BY host_id
)
SELECT 
    num_listings_tier,
    count(*) as num_hosts
FROM BASE_TABLE
GROUP BY num_listings_tier
ORDER BY num_hosts desc;
