# DVD Rental Revenue Dashboard

Interactive dashboard for analyzing revenue per movie from the dvdrental PostgreSQL database.

## Features

- 📊 Real-time revenue analytics
- 🎬 Revenue per movie breakdown
- 📈 Visual analytics with interactive charts:
  - Top 10 films by revenue
  - Revenue by category
  - Revenue by rating
  - Rental rate vs revenue correlation
- 🔍 Filters by category and rating
- 📥 Export data to CSV

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure database connection in `app.py` or use environment variables

## Running the Dashboard

```bash
streamlit run app.py
```

The dashboard will open in your browser at `http://localhost:8501`

## Database Connection

Update the database credentials in `app.py`:
- Host: 172.24.112.138
- Port: 5432
- Database: dvdrental
- User: dbt_user

Or set them as environment variables for security.

## Data Source

The dashboard connects directly to the dvdrental PostgreSQL database and queries:
- Film information
- Rental history
- Payment data
- Categories and ratings

## Technologies

- **Streamlit**: Interactive web application framework
- **Plotly**: Interactive visualizations
- **Pandas**: Data manipulation
- **PostgreSQL**: Database connection
