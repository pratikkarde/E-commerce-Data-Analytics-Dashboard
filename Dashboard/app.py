import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

# Page configuration
st.set_page_config(
    page_title="E-commerce Data Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.markdown('<h1 class="main-header">📊 E-commerce Data Analytics Dashboard</h1>', unsafe_allow_html=True)

@st.cache_data
def load_data():
    """Load data from SQLite database"""
    try:
        conn = sqlite3.connect('cleaned_data.sqlite')
        
        # Load all tables
        customers_df = pd.read_sql_query("SELECT * FROM customers", conn)
        orders_df = pd.read_sql_query("SELECT * FROM orders", conn)
        products_df = pd.read_sql_query("SELECT * FROM products", conn)
        reconciliation_df = pd.read_sql_query("SELECT * FROM reconciliation_data", conn)
        
        conn.close()
        
        return customers_df, orders_df, products_df, reconciliation_df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None, None, None, None

def calculate_kpis(customers_df, orders_df, products_df):
    """Calculate key performance indicators"""
    kpis = {}
    
    # Customer KPIs
    kpis['total_customers'] = len(customers_df)
    kpis['active_customers'] = len(customers_df[customers_df['status'] == 'active'])
    kpis['avg_customer_age'] = customers_df['age'].mean()
    kpis['total_revenue'] = orders_df['total_amount'].sum()
    
    # Order KPIs
    kpis['total_orders'] = len(orders_df)
    kpis['avg_order_value'] = orders_df['total_amount'].mean()
    kpis['completed_orders'] = len(orders_df[orders_df['status'] == 'delivered'])
    
    # Product KPIs
    kpis['total_products'] = len(products_df)
    kpis['active_products'] = len(products_df[products_df['is_active'] == True])
    kpis['avg_product_price'] = products_df['price'].mean()
    
    return kpis

def create_kpi_cards(kpis):
    """Create KPI cards"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div style="font-size: 2rem; font-weight: bold; color: #1f77b4;">{kpis['total_customers']:,}</div>
            <div style="font-size: 0.9rem; color: #666; text-transform: uppercase;">Total Customers</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div style="font-size: 2rem; font-weight: bold; color: #1f77b4;">${kpis['total_revenue']:,.0f}</div>
            <div style="font-size: 0.9rem; color: #666; text-transform: uppercase;">Total Revenue</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div style="font-size: 2rem; font-weight: bold; color: #1f77b4;">{kpis['total_orders']:,}</div>
            <div style="font-size: 0.9rem; color: #666; text-transform: uppercase;">Total Orders</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <div style="font-size: 2rem; font-weight: bold; color: #1f77b4;">${kpis['avg_order_value']:.2f}</div>
            <div style="font-size: 0.9rem; color: #666; text-transform: uppercase;">Avg Order Value</div>
        </div>
        """, unsafe_allow_html=True)

def create_customer_analysis(customers_df):
    """Create customer analysis visualizations"""
    st.subheader("👥 Customer Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Customer status distribution
        status_counts = customers_df['status'].value_counts()
        fig_status = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="Customer Status Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig_status.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        # Customer segment distribution
        segment_counts = customers_df['segment'].value_counts()
        fig_segment = px.bar(
            x=segment_counts.index,
            y=segment_counts.values,
            title="Customer Segment Distribution",
            color=segment_counts.values,
            color_continuous_scale='viridis'
        )
        fig_segment.update_layout(xaxis_title="Segment", yaxis_title="Count")
        st.plotly_chart(fig_segment, use_container_width=True)
    
    # Age distribution
    st.subheader("📊 Customer Demographics")
    col1, col2 = st.columns(2)
    
    with col1:
        fig_age = px.histogram(
            customers_df,
            x='age',
            nbins=20,
            title="Customer Age Distribution",
            color_discrete_sequence=['#1f77b4']
        )
        fig_age.update_layout(xaxis_title="Age", yaxis_title="Count")
        st.plotly_chart(fig_age, use_container_width=True)
    
    with col2:
        # Gender distribution
        gender_counts = customers_df['gender'].value_counts()
        fig_gender = px.pie(
            values=gender_counts.values,
            names=gender_counts.index,
            title="Customer Gender Distribution",
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_gender.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_gender, use_container_width=True)

def create_order_analysis(orders_df, customers_df, products_df):
    """Create order analysis visualizations"""
    st.subheader("📦 Order Analysis")
    
    # Merge data for analysis
    orders_with_customers = orders_df.merge(
        customers_df[['customer_id', 'full_name', 'segment', 'city', 'state']],
        on='customer_id',
        how='left'
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Order status distribution
        status_counts = orders_df['status'].value_counts()
        fig_status = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="Order Status Distribution",
            color_discrete_sequence=px.colors.qualitative.Set1
        )
        fig_status.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        # Payment method distribution
        payment_counts = orders_df['payment_method'].value_counts()
        fig_payment = px.bar(
            x=payment_counts.index,
            y=payment_counts.values,
            title="Payment Method Distribution",
            color=payment_counts.values,
            color_continuous_scale='plasma'
        )
        fig_payment.update_layout(xaxis_title="Payment Method", yaxis_title="Count")
        st.plotly_chart(fig_payment, use_container_width=True)
    
    # Revenue trends
    st.subheader("📈 Revenue Analysis")
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue by segment
        segment_revenue = orders_with_customers.groupby('segment')['total_amount'].sum().sort_values(ascending=False)
        fig_segment_rev = px.bar(
            x=segment_revenue.index,
            y=segment_revenue.values,
            title="Revenue by Customer Segment",
            color=segment_revenue.values,
            color_continuous_scale='viridis'
        )
        fig_segment_rev.update_layout(xaxis_title="Segment", yaxis_title="Revenue ($)")
        st.plotly_chart(fig_segment_rev, use_container_width=True)
    
    with col2:
        # Top spending customers
        top_customers = customers_df.nlargest(10, 'total_spent')[['full_name', 'total_spent', 'segment']]
        fig_top_customers = px.bar(
            top_customers,
            x='total_spent',
            y='full_name',
            orientation='h',
            title="Top 10 Customers by Total Spent",
            color='segment',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig_top_customers.update_layout(xaxis_title="Total Spent ($)", yaxis_title="Customer")
        st.plotly_chart(fig_top_customers, use_container_width=True)

def create_product_analysis(products_df, orders_df):
    """Create product analysis visualizations"""
    st.subheader("🛍️ Product Analysis")
    
    # Merge orders with products for analysis
    orders_with_products = orders_df.merge(
        products_df[['product_id', 'product_name', 'category', 'brand', 'price']],
        on='product_id',
        how='left'
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Product category distribution
        category_counts = products_df['category'].value_counts()
        fig_category = px.pie(
            values=category_counts.values,
            names=category_counts.index,
            title="Product Category Distribution",
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        fig_category.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_category, use_container_width=True)
    
    with col2:
        # Product price distribution
        fig_price = px.histogram(
            products_df,
            x='price',
            nbins=20,
            title="Product Price Distribution",
            color_discrete_sequence=['#ff7f0e']
        )
        fig_price.update_layout(xaxis_title="Price ($)", yaxis_title="Count")
        st.plotly_chart(fig_price, use_container_width=True)
    
    # Top selling products
    st.subheader("🌟 Top Performing Products")
    col1, col2 = st.columns(2)
    
    with col1:
        # Top selling products by quantity
        top_products_qty = orders_with_products.groupby('product_name')['quantity'].sum().nlargest(10)
        fig_top_qty = px.bar(
            x=top_products_qty.values,
            y=top_products_qty.index,
            orientation='h',
            title="Top 10 Products by Quantity Sold",
            color=top_products_qty.values,
            color_continuous_scale='viridis'
        )
        fig_top_qty.update_layout(xaxis_title="Quantity Sold", yaxis_title="Product")
        st.plotly_chart(fig_top_qty, use_container_width=True)
    
    with col2:
        # Top selling products by revenue
        top_products_rev = orders_with_products.groupby('product_name')['total_amount'].sum().nlargest(10)
        fig_top_rev = px.bar(
            x=top_products_rev.values,
            y=top_products_rev.index,
            orientation='h',
            title="Top 10 Products by Revenue",
            color=top_products_rev.values,
            color_continuous_scale='plasma'
        )
        fig_top_rev.update_layout(xaxis_title="Revenue ($)", yaxis_title="Product")
        st.plotly_chart(fig_top_rev, use_container_width=True)

def create_data_quality_insights(customers_df, orders_df, products_df):
    """Create data quality insights"""
    st.subheader("🔍 Data Quality Insights")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### Customers Data Quality")
        total_customers = len(customers_df)
        missing_email = customers_df['email'].isnull().sum()
        missing_phone = customers_df['phone'].isnull().sum()
        missing_age = customers_df['age'].isnull().sum()
        
        st.metric("Missing Email", f"{missing_email:,} ({missing_email/total_customers*100:.1f}%)")
        st.metric("Missing Phone", f"{missing_phone:,} ({missing_phone/total_customers*100:.1f}%)")
        st.metric("Missing Age", f"{missing_age:,} ({missing_age/total_customers*100:.1f}%)")
    
    with col2:
        st.markdown("### Orders Data Quality")
        total_orders = len(orders_df)
        missing_customer = orders_df['customer_id'].isnull().sum()
        missing_product = orders_df['product_id'].isnull().sum()
        missing_amount = orders_df['total_amount'].isnull().sum()
        
        st.metric("Missing Customer ID", f"{missing_customer:,} ({missing_customer/total_orders*100:.1f}%)")
        st.metric("Missing Product ID", f"{missing_product:,} ({missing_product/total_orders*100:.1f}%)")
        st.metric("Missing Amount", f"{missing_amount:,} ({missing_amount/total_orders*100:.1f}%)")
    
    with col3:
        st.markdown("### Products Data Quality")
        total_products = len(products_df)
        missing_price = products_df['price'].isnull().sum()
        missing_category = products_df['category'].isnull().sum()
        missing_stock = products_df['stock_quantity'].isnull().sum()
        
        st.metric("Missing Price", f"{missing_price:,} ({missing_price/total_products*100:.1f}%)")
        st.metric("Missing Category", f"{missing_category:,} ({missing_category/total_products*100:.1f}%)")
        st.metric("Missing Stock", f"{missing_stock:,} ({missing_stock/total_products*100:.1f}%)")

def create_interactive_filters(customers_df, orders_df, products_df):
    """Create interactive filters"""
    st.sidebar.header("🔎 Filters")
    
    # Customer filters
    st.sidebar.subheader("Customer Filters")
    
    # Status filter
    status_options = ['All'] + list(customers_df['status'].unique())
    selected_status = st.sidebar.selectbox("Customer Status", status_options)
    
    # Segment filter
    segment_options = ['All'] + list(customers_df['segment'].unique())
    selected_segment = st.sidebar.selectbox("Customer Segment", segment_options)
    
    # Gender filter
    gender_options = ['All'] + list(customers_df['gender'].unique())
    selected_gender = st.sidebar.selectbox("Gender", gender_options)
    
    # Age range filter
    min_age = int(customers_df['age'].min())
    max_age = int(customers_df['age'].max())
    age_range = st.sidebar.slider("Age Range", min_age, max_age, (min_age, max_age))
    
    # Apply filters
    filtered_customers = customers_df.copy()
    
    if selected_status != 'All':
        filtered_customers = filtered_customers[filtered_customers['status'] == selected_status]
    
    if selected_segment != 'All':
        filtered_customers = filtered_customers[filtered_customers['segment'] == selected_segment]
    
    if selected_gender != 'All':
        filtered_customers = filtered_customers[filtered_customers['gender'] == selected_gender]
    
    filtered_customers = filtered_customers[
        (filtered_customers['age'] >= age_range[0]) & 
        (filtered_customers['age'] <= age_range[1])
    ]
    
    # Product filters
    st.sidebar.subheader("Product Filters")
    
    # Category filter
    category_options = ['All'] + list(products_df['category'].unique())
    selected_category = st.sidebar.selectbox("Product Category", category_options)
    
    # Price range filter
    min_price = float(products_df['price'].min())
    max_price = float(products_df['price'].max())
    price_range = st.sidebar.slider("Price Range ($)", min_price, max_price, (min_price, max_price))
    
    # Apply product filters
    filtered_products = products_df.copy()
    
    if selected_category != 'All':
        filtered_products = filtered_products[filtered_products['category'] == selected_category]
    
    filtered_products = filtered_products[
        (filtered_products['price'] >= price_range[0]) & 
        (filtered_products['price'] <= price_range[1])
    ]
    
    return filtered_customers, filtered_products

def create_data_tables(customers_df, orders_df, products_df):
    """Create interactive data tables"""
    st.subheader("📋 Data Tables")
    
    tab1, tab2, tab3 = st.tabs(["Customers", "Orders", "Products"])
    
    with tab1:
        st.dataframe(
            customers_df,
            use_container_width=True,
            hide_index=True
        )
    
    with tab2:
        st.dataframe(
            orders_df,
            use_container_width=True,
            hide_index=True
        )
    
    with tab3:
        st.dataframe(
            products_df,
            use_container_width=True,
            hide_index=True
        )

def main():
    """Main dashboard function"""
    # Load data
    customers_df, orders_df, products_df, reconciliation_df = load_data()
    
    if customers_df is None:
        st.error("Failed to load data. Please ensure the SQLite database exists.")
        return
    
    # Calculate KPIs
    kpis = calculate_kpis(customers_df, orders_df, products_df)
    
    # Display KPIs
    create_kpi_cards(kpis)
    
    # Create filters
    filtered_customers, filtered_products = create_interactive_filters(customers_df, orders_df, products_df)
    
    # Display filtered data info
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Filtered Results:**")
    st.sidebar.markdown(f"Customers: {len(filtered_customers):,}")
    st.sidebar.markdown(f"Products: {len(filtered_products):,}")
    
    # Create analysis sections
    create_customer_analysis(filtered_customers)
    create_order_analysis(orders_df, filtered_customers, filtered_products)
    create_product_analysis(filtered_products, orders_df)
    create_data_quality_insights(customers_df, orders_df, products_df)
    
    # Create data tables
    create_data_tables(filtered_customers, orders_df, filtered_products)
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>Data Engineering Assignment - E-commerce Analytics Dashboard</p>
            <p>Built with Streamlit, Plotly, and SQLite</p>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()