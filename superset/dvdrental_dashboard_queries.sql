-- DVD Rental Revenue Dashboard SQL Queries for Superset
-- These queries can be used in Superset to create charts and dashboards

-- Query 1: Revenue per Movie (Main Dataset)
-- Use this for multiple visualizations
SELECT
    f.film_id,
    f.title AS film_title,
    c.name AS category,
    f.rating,
    f.rental_rate,
    f.length AS film_length,
    f.release_year,
    COUNT(DISTINCT r.rental_id) AS total_rentals,
    COALESCE(SUM(p.amount), 0) AS total_revenue,
    COALESCE(AVG(p.amount), 0) AS avg_revenue_per_rental,
    COALESCE(MAX(p.amount), 0) AS max_payment,
    COALESCE(MIN(p.amount), 0) AS min_payment
FROM film f
LEFT JOIN film_category fc ON f.film_id = fc.film_id
LEFT JOIN category c ON fc.category_id = c.category_id
LEFT JOIN inventory i ON f.film_id = i.film_id
LEFT JOIN rental r ON i.inventory_id = r.inventory_id
LEFT JOIN payment p ON r.rental_id = p.rental_id
GROUP BY
    f.film_id,
    f.title,
    c.name,
    f.rating,
    f.rental_rate,
    f.length,
    f.release_year
ORDER BY total_revenue DESC;

-- Query 2: Top 10 Films by Revenue
-- Use for Bar Chart
SELECT
    f.title AS film_title,
    c.name AS category,
    COALESCE(SUM(p.amount), 0) AS total_revenue
FROM film f
LEFT JOIN film_category fc ON f.film_id = fc.film_id
LEFT JOIN category c ON fc.category_id = c.category_id
LEFT JOIN inventory i ON f.film_id = i.film_id
LEFT JOIN rental r ON i.inventory_id = r.inventory_id
LEFT JOIN payment p ON r.rental_id = p.rental_id
GROUP BY f.title, c.name
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 3: Revenue by Category
-- Use for Pie Chart
SELECT
    c.name AS category,
    COUNT(DISTINCT f.film_id) AS film_count,
    COUNT(DISTINCT r.rental_id) AS rental_count,
    COALESCE(SUM(p.amount), 0) AS total_revenue
FROM category c
LEFT JOIN film_category fc ON c.category_id = fc.category_id
LEFT JOIN film f ON fc.film_id = f.film_id
LEFT JOIN inventory i ON f.film_id = i.film_id
LEFT JOIN rental r ON i.inventory_id = r.inventory_id
LEFT JOIN payment p ON r.rental_id = p.rental_id
GROUP BY c.name
ORDER BY total_revenue DESC;

-- Query 4: Revenue by Rating
-- Use for Bar Chart
SELECT
    f.rating,
    COUNT(DISTINCT f.film_id) AS film_count,
    COUNT(DISTINCT r.rental_id) AS rental_count,
    COALESCE(SUM(p.amount), 0) AS total_revenue,
    COALESCE(AVG(p.amount), 0) AS avg_revenue_per_rental
FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
LEFT JOIN rental r ON i.inventory_id = r.inventory_id
LEFT JOIN payment p ON r.rental_id = p.rental_id
GROUP BY f.rating
ORDER BY total_revenue DESC;

-- Query 5: Revenue by Store
-- Use for comparison charts
SELECT
    s.store_id,
    ci.city,
    co.country,
    COUNT(DISTINCT f.film_id) AS unique_films,
    COUNT(DISTINCT r.rental_id) AS total_rentals,
    COALESCE(SUM(p.amount), 0) AS total_revenue
FROM store s
INNER JOIN address a ON s.address_id = a.address_id
INNER JOIN city ci ON a.city_id = ci.city_id
INNER JOIN country co ON ci.country_id = co.country_id
LEFT JOIN inventory i ON s.store_id = i.store_id
LEFT JOIN rental r ON i.inventory_id = r.inventory_id
LEFT JOIN payment p ON r.rental_id = p.rental_id
GROUP BY s.store_id, ci.city, co.country
ORDER BY total_revenue DESC;

-- Query 6: Monthly Revenue Trend
-- Use for Time Series Chart
SELECT
    DATE_TRUNC('month', p.payment_date) AS month,
    COUNT(DISTINCT r.rental_id) AS rental_count,
    COALESCE(SUM(p.amount), 0) AS total_revenue,
    COALESCE(AVG(p.amount), 0) AS avg_payment
FROM payment p
INNER JOIN rental r ON p.rental_id = r.rental_id
GROUP BY DATE_TRUNC('month', p.payment_date)
ORDER BY month;

-- Query 7: Top Customers by Revenue
-- Use for Table or Bar Chart
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    COUNT(DISTINCT r.rental_id) AS total_rentals,
    COALESCE(SUM(p.amount), 0) AS total_spent
FROM customer c
LEFT JOIN rental r ON c.customer_id = r.customer_id
LEFT JOIN payment p ON r.rental_id = p.rental_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
ORDER BY total_spent DESC
LIMIT 20;

-- Query 8: Summary Metrics
-- Use for Big Number charts
SELECT
    COUNT(DISTINCT f.film_id) AS total_films,
    COUNT(DISTINCT r.rental_id) AS total_rentals,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COALESCE(SUM(p.amount), 0) AS total_revenue,
    COALESCE(AVG(p.amount), 0) AS avg_payment
FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
LEFT JOIN rental r ON i.inventory_id = r.inventory_id
LEFT JOIN payment p ON r.rental_id = p.rental_id
LEFT JOIN customer c ON r.customer_id = c.customer_id;
