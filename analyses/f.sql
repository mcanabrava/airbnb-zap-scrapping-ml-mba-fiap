SELECT
    neighbourhood_cleansed,
    property_type,
    avg(price) avg_price,
    count(*) as num_listings
FROM listings
WHERE neighbourhood_cleansed = 'Copacabana'
GROUP BY 
    neighbourhood_cleansed, property_type
ORDER BY avg_price desc;