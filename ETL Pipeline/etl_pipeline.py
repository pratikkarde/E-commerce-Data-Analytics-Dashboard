import pandas as pd
import numpy as np
import json
import sqlite3
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='etl_pipeline.log'
)
logger = logging.getLogger(__name__)

# Custom JSON encoder to handle numpy types
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
            np.int16, np.int32, np.int64, np.uint8,
            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

def load_and_clean_data():
    try:
        logger.info('Starting ETL Pipeline...')
        
        # Load raw data
        logger.info('Loading raw data...')
        customers_df = pd.read_json('customers_messy_data.json')
        orders_df = pd.read_csv('orders_unstructured_data.csv')
        products_df = pd.read_json('products_inconsistent_data.json')
        reconciliation_df = pd.read_csv('reconciliation_challenge_data.csv')
        
        # Clean customers data
        logger.info('Cleaning customers data...')
        customers_df['customer_id'] = pd.to_numeric(customers_df['customer_id'], errors='coerce')
        customers_df['email'] = customers_df['email'].str.lower()
        customers_df['phone'] = customers_df['phone'].str.replace('[^0-9+]', '', regex=True)
        customers_df['birth_date'] = pd.to_datetime(customers_df['birth_date'], errors='coerce')
        customers_df['age'] = (pd.Timestamp.now() - customers_df['birth_date']).dt.days // 365
        customers_df['gender'] = customers_df['gender'].str.lower()
        customers_df['status'] = customers_df['status'].fillna('active')
        
        # Clean products data
        logger.info('Cleaning products data...')
        products_df['price'] = pd.to_numeric(products_df['price'], errors='coerce')
        products_df['cost'] = pd.to_numeric(products_df['cost'], errors='coerce')
        products_df['weight'] = pd.to_numeric(products_df['weight'], errors='coerce')
        products_df['stock_quantity'] = pd.to_numeric(products_df['stock_quantity'], errors='coerce')
        products_df['created_date'] = pd.to_datetime(products_df['created_date'], errors='coerce')
        products_df['last_updated'] = pd.to_datetime(products_df['last_updated'], errors='coerce')
        products_df['rating'] = pd.to_numeric(products_df['rating'], errors='coerce')
        
        # Clean orders data
        logger.info('Cleaning orders data...')
        orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], errors='coerce')
        orders_df['order_datetime'] = pd.to_datetime(orders_df['order_datetime'], errors='coerce')
        orders_df['quantity'] = pd.to_numeric(orders_df['quantity'], errors='coerce')
        orders_df['unit_price'] = pd.to_numeric(orders_df['unit_price'], errors='coerce')
        orders_df['total_amount'] = pd.to_numeric(orders_df['total_amount'], errors='coerce')
        orders_df['shipping_cost'] = pd.to_numeric(orders_df['shipping_cost'], errors='coerce')
        orders_df['tax'] = pd.to_numeric(orders_df['tax'], errors='coerce')
        orders_df['discount'] = pd.to_numeric(orders_df['discount'], errors='coerce')
        
        # Clean reconciliation data
        logger.info('Cleaning reconciliation data...')
        reconciliation_df['transaction_date'] = pd.to_datetime(reconciliation_df['transaction_date'], errors='coerce')
        reconciliation_df['amount_paid'] = pd.to_numeric(reconciliation_df['amount_paid'], errors='coerce')
        reconciliation_df['quantity_ordered'] = pd.to_numeric(reconciliation_df['quantity_ordered'], errors='coerce')
        reconciliation_df['unit_cost'] = pd.to_numeric(reconciliation_df['unit_cost'], errors='coerce')
        reconciliation_df['total_value'] = pd.to_numeric(reconciliation_df['total_value'], errors='coerce')
        reconciliation_df['discount_applied'] = pd.to_numeric(reconciliation_df['discount_applied'], errors='coerce')
        reconciliation_df['shipping_fee'] = pd.to_numeric(reconciliation_df['shipping_fee'], errors='coerce')
        reconciliation_df['tax_amount'] = pd.to_numeric(reconciliation_df['tax_amount'], errors='coerce')
        
        # Create SQLite database
        logger.info('Creating SQLite database...')
        conn = sqlite3.connect('cleaned_data.sqlite')
        
        # Create tables
        conn.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INTEGER PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255) UNIQUE,
                phone VARCHAR(20),
                address VARCHAR(500),
                city VARCHAR(100),
                state VARCHAR(50),
                country VARCHAR(50),
                postal_code VARCHAR(20),
                status VARCHAR(20) DEFAULT 'active',
                age INTEGER,
                birth_date DATE,
                gender VARCHAR(10),
                segment VARCHAR(20) DEFAULT 'regular'
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS products (
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
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS orders (
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
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS reconciliation_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_reference VARCHAR(20),
                full_customer_name VARCHAR(255),
                contact_email VARCHAR(255),
                transaction_ref VARCHAR(20),
                item_reference VARCHAR(20),
                transaction_date DATE,
                amount_paid DECIMAL(10,2),
                payment_status VARCHAR(20),
                delivery_status VARCHAR(20),
                customer_segment VARCHAR(50),
                region VARCHAR(50),
                product_line VARCHAR(100),
                quantity_ordered INTEGER,
                unit_cost DECIMAL(10,2),
                total_value DECIMAL(10,2),
                discount_applied DECIMAL(8,2) DEFAULT 0.00,
                shipping_fee DECIMAL(8,2) DEFAULT 0.00,
                tax_amount DECIMAL(8,2) DEFAULT 0.00,
                notes_comments TEXT,
                last_modified_timestamp TIMESTAMP
            )
        ''')
        
        # Create indexes
        conn.execute('CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_product_id ON orders(product_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)')
        
        # Load data
        logger.info('Loading data into database...')
        customers_df.to_sql('customers', conn, if_exists='replace', index=False)
        products_df.to_sql('products', conn, if_exists='replace', index=False)
        orders_df.to_sql('orders', conn, if_exists='replace', index=False)
        reconciliation_df.to_sql('reconciliation_data', conn, if_exists='replace', index=False)
        
        conn.close()
        
        # Generate summary report
        logger.info('Generating summary report...')
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'customers': {
                    'records': int(len(customers_df)),
                    'columns': int(len(customers_df.columns)),
                    'missing_values': int(customers_df.isnull().sum().sum())
                },
                'orders': {
                    'records': int(len(orders_df)),
                    'columns': int(len(orders_df.columns)),
                    'missing_values': int(orders_df.isnull().sum().sum())
                },
                'products': {
                    'records': int(len(products_df)),
                    'columns': int(len(products_df.columns)),
                    'missing_values': int(products_df.isnull().sum().sum())
                },
                'reconciliation': {
                    'records': int(len(reconciliation_df)),
                    'columns': int(len(reconciliation_df.columns)),
                    'missing_values': int(reconciliation_df.isnull().sum().sum())
                }
            }
        }
        
        with open('etl_summary_report.json', 'w') as f:
            json.dump(report, f, indent=2, cls=NumpyEncoder)
        
        logger.info('ETL Pipeline completed successfully!')
        print('✓ ETL Pipeline completed successfully!')
        print('✓ Database created: cleaned_data.sqlite')
        print('✓ Report saved: etl_summary_report.json')
        
    except Exception as e:
        logger.error(f'ETL Pipeline failed: {str(e)}')
        print(f'✗ ETL Pipeline failed: {str(e)}')
        raise

if __name__ == '__main__':
    load_and_clean_data()