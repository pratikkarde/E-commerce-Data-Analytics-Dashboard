# 📊 E-commerce Data Engineering Project

A comprehensive data engineering solution for processing and analyzing messy e-commerce datasets using Python, Pandas, SQLite, and Streamlit.

## 🎯 Project Overview

This project demonstrates a complete data engineering pipeline that handles real-world data quality challenges including:
- Inconsistent field names and data types
- Mixed date formats and null representations
- Redundant and conflicting columns
- Schema reconciliation across multiple datasets

## 📂 Datasets

The project processes 4 messy real-world e-commerce datasets:

1. **`customers_messy_data.json`** - Customer information with inconsistent field names
2. **`orders_unstructured_data.csv`** - Order transactions with mixed formats
3. **`products_inconsistent_data.json`** - Product catalog with schema inconsistencies
4. **`reconciliation_challenge_data.csv`** - Additional transaction data for schema reconciliation

## 🏗️ Architecture

### Phase 1: Data Discovery & Analysis
- **File**: `01_data_discovery_analysis.ipynb`
- **Purpose**: Comprehensive data exploration and quality assessment
- **Deliverables**: Data quality reports, schema mapping, cleaning strategy

### Phase 2: ETL Pipeline Development
- **File**: `etl_pipeline.py`
- **Purpose**: Data cleaning, normalization, and database creation
- **Deliverables**: Cleaned datasets, normalized SQLite database

### Phase 3: Streamlit Dashboard
- **File**: `app.py`
- **Purpose**: Interactive data visualization and analysis
- **Deliverables**: Web-based dashboard with KPIs and filters

### Phase 4: Schema Reconciliation (Bonus)
- **Purpose**: AI-assisted schema matching and data reconciliation
- **Deliverables**: Reconciliation logic and documentation

## 🚀 Quick Start

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Installation

1. **Clone or download the project files**
   ```bash
   # Ensure all dataset files are in the project directory:
   # - customers_messy_data.json
   # - orders_unstructured_data.csv
   # - products_inconsistent_data.json
   # - reconciliation_challenge_data.csv
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the ETL pipeline**
   ```bash
   python etl_pipeline.py
   ```

4. **Launch the Streamlit dashboard**
   ```bash
   streamlit run app.py
   ```

## 📋 Detailed Setup Instructions

### Step 1: Environment Setup
```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Data Processing
```bash
# Run ETL pipeline
python etl_pipeline.py
```

This will:
- Load and clean all datasets
- Handle data quality issues
- Create normalized SQLite database (`cleaned_data.sqlite`)
- Generate summary report (`etl_summary_report.json`)

### Step 3: Launch Dashboard
```bash
# Start Streamlit dashboard
streamlit run app.py
```

The dashboard will open in your browser at `http://localhost:8501`

## 🔍 Data Quality Issues Addressed

### 1. Inconsistent Field Names
- **Customers**: `customer_id` vs `cust_id`, `email` vs `email_address`
- **Orders**: `order_id` vs `ord_id`, `product_id` vs `item_id`
- **Products**: `product_name` vs `item_name`, `category` vs `product_category`

### 2. Mixed Data Types
- Numeric fields stored as strings (`total_spent`, `price`)
- Boolean fields as mixed types (`is_active`)
- Date fields in multiple formats

### 3. Null Representations
- Multiple null variants: `null`, `''`, `"N/A"`, `"NULL"`
- Inconsistent handling across datasets

### 4. Redundant/Conflicting Columns
- Duplicate information across field pairs
- Conflicting values between redundant fields

### 5. Inconsistent Enums
- Status values: `active`, `ACTIVE`, `inactive`, `INACTIVE`
- Gender values: `F`, `Female`, `Male`, `M`, `Other`

## 🗂️ Database Schema

### Customers Table
```sql
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    email VARCHAR(255),
    phone VARCHAR(20),
    full_name VARCHAR(255) NOT NULL,
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    registration_date DATE,
    status VARCHAR(20) DEFAULT 'inactive',
    total_orders INTEGER DEFAULT 0,
    total_spent DECIMAL(10,2) DEFAULT 0.00,
    loyalty_points INTEGER DEFAULT 0,
    preferred_payment VARCHAR(50),
    age INTEGER,
    birth_date DATE,
    gender VARCHAR(10),
    segment VARCHAR(20) DEFAULT 'regular'
);
```

### Products Table
```sql
CREATE TABLE products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    weight DECIMAL(8,2),
    dimensions VARCHAR(50),
    color VARCHAR(50),
    size VARCHAR(20),
    stock_quantity INTEGER DEFAULT 0,
    reorder_level INTEGER DEFAULT 10,
    supplier_id VARCHAR(20),
    created_date DATE,
    last_updated TIMESTAMP,
    is_active BOOLEAN DEFAULT 1,
    rating DECIMAL(3,1)
);
```

### Orders Table
```sql
CREATE TABLE orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id INTEGER,
    product_id VARCHAR(20),
    order_date DATE,
    order_datetime TIMESTAMP,
    quantity INTEGER DEFAULT 1,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    shipping_cost DECIMAL(8,2) DEFAULT 0.00,
    tax DECIMAL(8,2) DEFAULT 0.00,
    discount DECIMAL(8,2) DEFAULT 0.00,
    status VARCHAR(20) DEFAULT 'pending',
    payment_method VARCHAR(50),
    shipping_address VARCHAR(500),
    notes TEXT,
    tracking_number VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
    FOREIGN KEY (product_id) REFERENCES products (product_id)
);
```

## 📊 Dashboard Features

### Key Performance Indicators (KPIs)
- Total customers, revenue, orders, and average order value
- Real-time metrics with visual indicators

### Interactive Visualizations
- Customer demographics and segmentation
- Order status and payment method distribution
- Product category and price analysis
- Revenue trends and top performers

### Data Quality Insights
- Missing value analysis for each dataset
- Data completeness metrics
- Quality assessment reports

### Interactive Filters
- Customer status, segment, gender, and age range
- Product category and price range
- Real-time data filtering

### Data Tables
- Interactive tables with search and sort functionality
- Export capabilities
- Detailed record inspection

## 🛠️ Technical Implementation

### Data Cleaning Strategy
1. **Field Consolidation**: Merge redundant fields with priority logic
2. **Type Standardization**: Convert data types to appropriate formats
3. **Null Handling**: Replace null variants with actual NULL values
4. **Enum Normalization**: Standardize categorical values
5. **Duplicate Removal**: Remove duplicates based on business rules

### ETL Pipeline Features
- Comprehensive logging and error handling
- Data validation at each stage
- Performance optimization with indexes
- Summary reporting and statistics

### Dashboard Technologies
- **Streamlit**: Web application framework
- **Plotly**: Interactive visualizations
- **SQLite**: Lightweight database
- **Pandas**: Data manipulation and analysis

## 📈 Performance Considerations

### Database Optimization
- Primary and foreign key constraints
- Strategic indexes on frequently queried columns
- Efficient data types and storage

### Dashboard Performance
- Cached data loading for faster response
- Optimized queries and aggregations
- Responsive design for various screen sizes

## 🔧 Troubleshooting

### Common Issues

1. **Database Connection Error**
   ```
   Error: No such table
   Solution: Run the ETL pipeline first: python etl_pipeline.py
   ```

2. **Missing Dependencies**
   ```
   Error: ModuleNotFoundError
   Solution: Install requirements: pip install -r requirements.txt
   ```

3. **Dataset Loading Error**
   ```
   Error: File not found
   Solution: Ensure all dataset files are in the project directory
   ```

### Log Files
- **ETL Logs**: `etl_pipeline.log`
- **Summary Report**: `etl_summary_report.json`

## 📚 Learning Outcomes

This project demonstrates:

### Data Engineering Skills
- Real-world data quality assessment
- ETL pipeline development
- Database design and optimization
- Data validation and testing

### Technical Skills
- Python programming with pandas
- SQL database operations
- Web application development
- Data visualization

### Business Skills
- KPI definition and measurement
- Data-driven decision making
- Stakeholder communication
- Project documentation

## 🎯 Future Enhancements

### Potential Improvements
1. **Real-time Data Processing**: Implement streaming data pipelines
2. **Advanced Analytics**: Add machine learning models for customer segmentation
3. **API Integration**: Connect to external data sources
4. **Cloud Deployment**: Deploy to AWS, Azure, or Google Cloud
5. **Advanced Visualizations**: Add more interactive charts and dashboards

### Schema Reconciliation (Phase 4)
- AI-assisted field matching using Gemini AI
- Automated schema mapping
- Data lineage tracking
- Conflict resolution strategies

## 📄 License

This project is created for educational purposes as part of a data engineering assignment.

## 👥 Contributing

This is a demonstration project, but suggestions and improvements are welcome through issues and pull requests.

---

**Built with ❤️ for Data Engineering Excellence** 