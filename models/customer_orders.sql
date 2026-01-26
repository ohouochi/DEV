-- Model joining customers and orders
SELECT
    c.id AS customer_id,
    c.name,
    c.email,
    o.id AS order_id,
    o.order_date,
    o.total
FROM {{ source('sample_schema', 'customers') }} c
LEFT JOIN {{ source('sample_schema', 'orders') }} o ON c.id = o.customer_id
ORDER BY c.id, o.order_date