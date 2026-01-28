-- Model joining customers and orders from dvdrental database
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    r.rental_id,
    r.rental_date,
    SUM(p.amount) AS total_spent
FROM {{ source('dvdrental', 'customer') }} c
LEFT JOIN {{ source('dvdrental', 'rental') }} r ON c.customer_id = r.customer_id
LEFT JOIN {{ source('dvdrental', 'payment') }} p ON r.rental_id = p.rental_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    r.rental_id,
    r.rental_date
ORDER BY c.customer_id