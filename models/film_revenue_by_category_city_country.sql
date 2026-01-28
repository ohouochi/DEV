-- Film gross revenue per category, city, and country
SELECT
    cat.name AS category,
    ci.city,
    co.country,
    f.title AS film_title,
    COUNT(DISTINCT r.rental_id) AS total_rentals,
    COALESCE(SUM(p.amount), 0) AS gross_revenue
FROM {{ source('dvdrental', 'film') }} f
INNER JOIN {{ source('dvdrental', 'film_category') }} fc ON f.film_id = fc.film_id
INNER JOIN {{ source('dvdrental', 'category') }} cat ON fc.category_id = cat.category_id
INNER JOIN {{ source('dvdrental', 'inventory') }} i ON f.film_id = i.film_id
INNER JOIN {{ source('dvdrental', 'rental') }} r ON i.inventory_id = r.inventory_id
LEFT JOIN {{ source('dvdrental', 'payment') }} p ON r.rental_id = p.rental_id
INNER JOIN {{ source('dvdrental', 'store') }} s ON i.store_id = s.store_id
INNER JOIN {{ source('dvdrental', 'address') }} a ON s.address_id = a.address_id
INNER JOIN {{ source('dvdrental', 'city') }} ci ON a.city_id = ci.city_id
INNER JOIN {{ source('dvdrental', 'country') }} co ON ci.country_id = co.country_id
GROUP BY
    cat.name,
    ci.city,
    co.country,
    f.title
ORDER BY
    gross_revenue DESC,
    cat.name,
    ci.city,
    co.country