SELECT
    neighbourhood_cleansed,
    property_type,
    --room_type,
    count(*) as num_listings
FROM listings
WHERE neighbourhood_cleansed = 'Copacabana'
GROUP BY 
    neighbourhood_cleansed, property_type--, room_type
ORDER BY num_listings desc;