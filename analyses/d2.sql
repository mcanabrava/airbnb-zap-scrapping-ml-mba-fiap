

SELECT 
    host_id,
    neighbourhood_cleansed,
    count(id) num_listings
FROM listings
GROUP BY host_id, neighbourhood_cleansed
HAVING num_listings > 10
ORDER BY host_id desc, num_listings desc;