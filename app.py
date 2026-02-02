import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from datetime import datetime

# Database configuration
DB_CONFIG = {
    'host': '172.24.112.138',
    'port': 5432,
    'database': 'dvdrental',
    'user': 'dbt_user',
    'password': ''  # Set this in environment variable or Streamlit secrets
}

st.set_page_config(page_title="DVD Rental Analytics", page_icon="🎬", layout="wide")

@st.cache_data(ttl=600)
def load_data():
    """Load revenue per movie data from PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
        SELECT
            f.film_id,
            f.title AS film_title,
            c.name AS category,
            f.rating,
            f.rental_rate,
            COUNT(DISTINCT r.rental_id) AS total_rentals,
            COALESCE(SUM(p.amount), 0) AS total_revenue,
            COALESCE(AVG(p.amount), 0) AS avg_revenue_per_rental
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
            f.rental_rate
        ORDER BY total_revenue DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        return pd.DataFrame()

# Title and header
st.title("🎬 DVD Rental Revenue Dashboard")
st.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

# Load data
with st.spinner('Loading data from database...'):
    df = load_data()

if df.empty:
    st.warning("No data available. Please check database connection.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")
categories = ['All'] + sorted(df['category'].dropna().unique().tolist())
selected_category = st.sidebar.selectbox("Category", categories)

ratings = ['All'] + sorted(df['rating'].dropna().unique().tolist())
selected_rating = st.sidebar.selectbox("Rating", ratings)

# Apply filters
filtered_df = df.copy()
if selected_category != 'All':
    filtered_df = filtered_df[filtered_df['category'] == selected_category]
if selected_rating != 'All':
    filtered_df = filtered_df[filtered_df['rating'] == selected_rating]

# Key metrics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Revenue", f"${filtered_df['total_revenue'].sum():,.2f}")
with col2:
    st.metric("Total Rentals", f"{filtered_df['total_rentals'].sum():,.0f}")
with col3:
    st.metric("Average Revenue per Film", f"${filtered_df['total_revenue'].mean():,.2f}")
with col4:
    st.metric("Total Films", len(filtered_df))

st.markdown("---")

# Charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 10 Films by Revenue")
    top_10 = filtered_df.nlargest(10, 'total_revenue')
    fig1 = px.bar(top_10, x='total_revenue', y='film_title', 
                  orientation='h',
                  labels={'total_revenue': 'Total Revenue ($)', 'film_title': 'Film'},
                  color='total_revenue',
                  color_continuous_scale='Blues')
    fig1.update_layout(showlegend=False, height=400)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("Revenue by Category")
    category_revenue = filtered_df.groupby('category')['total_revenue'].sum().reset_index()
    category_revenue = category_revenue.sort_values('total_revenue', ascending=False)
    fig2 = px.pie(category_revenue, values='total_revenue', names='category',
                  hole=0.4,
                  labels={'total_revenue': 'Total Revenue ($)'})
    fig2.update_layout(height=400)
    st.plotly_chart(fig2, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader("Revenue by Rating")
    rating_revenue = filtered_df.groupby('rating')['total_revenue'].sum().reset_index()
    rating_revenue = rating_revenue.sort_values('total_revenue', ascending=False)
    fig3 = px.bar(rating_revenue, x='rating', y='total_revenue',
                  labels={'total_revenue': 'Total Revenue ($)', 'rating': 'Film Rating'},
                  color='total_revenue',
                  color_continuous_scale='Viridis')
    fig3.update_layout(showlegend=False, height=400)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("Rental Rate vs Revenue")
    fig4 = px.scatter(filtered_df, x='rental_rate', y='total_revenue',
                     size='total_rentals', hover_data=['film_title'],
                     labels={'rental_rate': 'Rental Rate ($)', 
                            'total_revenue': 'Total Revenue ($)',
                            'total_rentals': 'Total Rentals'},
                     color='category')
    fig4.update_layout(height=400)
    st.plotly_chart(fig4, use_container_width=True)

# Data table
st.markdown("---")
st.subheader("📊 Detailed Revenue Data")
st.dataframe(
    filtered_df[['film_title', 'category', 'rating', 'rental_rate', 
                 'total_rentals', 'total_revenue', 'avg_revenue_per_rental']]
    .sort_values('total_revenue', ascending=False)
    .reset_index(drop=True),
    use_container_width=True
)

# Download button
csv = filtered_df.to_csv(index=False)
st.download_button(
    label="📥 Download Data as CSV",
    data=csv,
    file_name=f"dvdrental_revenue_{datetime.now().strftime('%Y%m%d')}.csv",
    mime="text/csv"
)
