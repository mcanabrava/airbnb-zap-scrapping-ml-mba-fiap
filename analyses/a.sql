SELECT
    distinct host_neighbourhood as neighborhood,
    count(*) as counter
FROM listings
GROUP BY neighborhood
ORDER BY counter desc;