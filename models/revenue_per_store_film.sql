-- Revenue per store and film
-- Aggregates rental payments grouped by store and film
SELECT
    s.store_id,
    ci.city,
    co.country,
    f.film_id,
    f.title AS film_title,
    COUNT(DISTINCT r.rental_id) AS total_rentals,
    COALESCE(SUM(p.amount), 0) AS total_revenue
FROM {{ source('dvdrental', 'store') }} s
INNER JOIN {{ source('dvdrental', 'inventory') }} i ON s.store_id = i.store_id
INNER JOIN {{ source('dvdrental', 'film') }} f ON i.film_id = f.film_id
INNER JOIN {{ source('dvdrental', 'rental') }} r ON i.inventory_id = r.inventory_id
LEFT JOIN {{ source('dvdrental', 'payment') }} p ON r.rental_id = p.rental_id
INNER JOIN {{ source('dvdrental', 'address') }} a ON s.address_id = a.address_id
INNER JOIN {{ source('dvdrental', 'city') }} ci ON a.city_id = ci.city_id
INNER JOIN {{ source('dvdrental', 'country') }} co ON ci.country_id = co.country_id
GROUP BY
    s.store_id,
    ci.city,
    co.country,
    f.film_id,
    f.title
ORDER BY
    s.store_id,
    total_revenue DESC
