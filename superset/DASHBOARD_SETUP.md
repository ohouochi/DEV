# DVD Rental Dashboard Setup for Superset

This guide helps you set up the DVD Rental Revenue Dashboard in Apache Superset.

## Prerequisites

- Superset running (via Docker)
- Access to dvdrental PostgreSQL database (172.24.112.138:5432)

## Setup Instructions

### 1. Start Superset

```bash
cd /home/oshiz/superset
docker-compose up -d
```

Access Superset at: http://localhost:8088

### 2. Add Database Connection

1. Log into Superset (default: admin/admin)
2. Go to **Settings** → **Database Connections** → **+ Database**
3. Select **PostgreSQL**
4. Enter connection details:
   - **Host**: 172.24.112.138
   - **Port**: 5432
   - **Database**: dvdrental
   - **Username**: dbt_user
   - **Password**: [your password]
   - **Display Name**: DVD Rental Database

Or use this SQLAlchemy URI:
```
postgresql://dbt_user:[password]@172.24.112.138:5432/dvdrental
```

### 3. Create Datasets

Use the SQL queries in `dvdrental_dashboard_queries.sql` to create datasets:

1. Go to **Data** → **Datasets** → **+ Dataset**
2. Select **DVD Rental Database**
3. Choose **SQL** tab
4. Paste a query from the SQL file
5. Click **Create Dataset**

### 4. Create Charts

For each dataset, create visualizations:

#### Chart 1: Top 10 Films by Revenue (Bar Chart)
- **Dataset**: Query 2
- **Chart Type**: Bar Chart
- **X-Axis**: film_title
- **Metric**: SUM(total_revenue)

#### Chart 2: Revenue by Category (Pie Chart)
- **Dataset**: Query 3
- **Chart Type**: Pie Chart
- **Dimension**: category
- **Metric**: SUM(total_revenue)

#### Chart 3: Revenue by Rating (Bar Chart)
- **Dataset**: Query 4
- **Chart Type**: Bar Chart
- **X-Axis**: rating
- **Metric**: SUM(total_revenue)

#### Chart 4: Monthly Revenue Trend (Line Chart)
- **Dataset**: Query 6
- **Chart Type**: Line Chart
- **X-Axis**: month
- **Metric**: SUM(total_revenue)

#### Chart 5: Summary Metrics (Big Number)
- **Dataset**: Query 8
- **Chart Type**: Big Number
- **Metric**: total_revenue, total_rentals, total_customers

### 5. Create Dashboard

1. Go to **Dashboards** → **+ Dashboard**
2. Name it: "DVD Rental Revenue Dashboard"
3. Drag and drop the charts you created
4. Arrange them in a grid layout
5. Add filters if needed:
   - Category filter
   - Rating filter
   - Date range filter

### 6. Configure Filters

Add dashboard-level filters:
1. Click **Edit Dashboard**
2. Click **Add Filter** (funnel icon)
3. Add filters for:
   - Category (from Query 3)
   - Rating (from Query 4)

### 7. Save and Share

1. Click **Save** to save the dashboard
2. Share the dashboard URL with your team
3. Set up scheduled email reports if needed

## Available Queries

The `dvdrental_dashboard_queries.sql` file contains 8 pre-built queries:

1. **Revenue per Movie** - Main dataset with all metrics
2. **Top 10 Films** - Best performing films
3. **Revenue by Category** - Category breakdown
4. **Revenue by Rating** - Rating analysis
5. **Revenue by Store** - Store performance
6. **Monthly Revenue Trend** - Time series data
7. **Top Customers** - Customer analysis
8. **Summary Metrics** - Overall KPIs

## Dashboard Layout Suggestion

```
+-------------------------+-------------------------+
|   Total Revenue         |   Total Rentals         |
|   (Big Number)          |   (Big Number)          |
+-------------------------+-------------------------+
|   Top 10 Films by Revenue                         |
|   (Horizontal Bar Chart)                          |
+---------------------------------------------------+
|   Revenue by Category   |   Revenue by Rating    |
|   (Pie Chart)           |   (Bar Chart)          |
+-------------------------+-------------------------+
|   Monthly Revenue Trend                           |
|   (Line Chart)                                    |
+---------------------------------------------------+
```

## Troubleshooting

- **Connection Error**: Verify database credentials and network access
- **Query Timeout**: Increase timeout in superset_config.py
- **Missing Data**: Check that dvdrental database has data

## Additional Resources

- Superset Documentation: https://superset.apache.org/docs
- SQL Query Reference: dvdrental_dashboard_queries.sql
