SELECT
    neighbourhood_cleansed,
    median(price) median_price,
    count(*) as num_listings
FROM listings
GROUP BY 
    neighbourhood_cleansed
HAVING num_listings > 10
ORDER BY median_price desc;