#!/usr/bin/env python3
"""
Data Engineering Assignment - Phase 2: ETL Pipeline Development

This script implements a comprehensive ETL pipeline that:
1. Loads and cleans all 4 datasets
2. Handles data quality issues
3. Creates normalized SQLite schema
4. Loads cleaned data with proper relationships
5. Implements logging and error handling

Author: Data Engineering Team
Date: 2024
"""

import pandas as pd
import numpy as np
import json
import sqlite3
import logging
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataCleaner:
    """Handles data cleaning operations for all datasets."""
    
    def __init__(self):
        self.cleaning_stats = {}
    
    def clean_customers_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize customers data."""
        logger.info("Starting customers data cleaning...")
        
        # Create a copy to avoid modifying original
        cleaned_df = df.copy()
        
        # 1. Merge redundant fields
        cleaned_df = self._merge_redundant_fields(cleaned_df, {
            'email': ['email', 'email_address'],
            'phone': ['phone', 'phone_number'],
            'customer_id': ['customer_id', 'cust_id'],
            'full_name': ['full_name', 'customer_name'],
            'zip_code': ['zip_code', 'postal_code'],
            'registration_date': ['registration_date', 'reg_date'],
            'status': ['status', 'customer_status']
        })
        
        # 2. Clean and standardize data types
        cleaned_df = self._standardize_customer_data_types(cleaned_df)
        
        # 3. Normalize categorical fields
        cleaned_df = self._normalize_customer_categoricals(cleaned_df)
        
        # 4. Handle missing values
        cleaned_df = self._handle_customer_missing_values(cleaned_df)
        
        # 5. Remove duplicates
        cleaned_df = self._remove_customer_duplicates(cleaned_df)
        
        logger.info(f"Customers cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize orders data."""
        logger.info("Starting orders data cleaning...")
        
        cleaned_df = df.copy()
        
        # 1. Merge redundant fields
        cleaned_df = self._merge_redundant_fields(cleaned_df, {
            'order_id': ['order_id', 'ord_id'],
            'customer_id': ['customer_id', 'cust_id'],
            'product_id': ['product_id', 'item_id'],
            'quantity': ['quantity', 'qty'],
            'unit_price': ['unit_price', 'price'],
            'total_amount': ['total_amount', 'order_total'],
            'status': ['status', 'order_status']
        })
        
        # 2. Clean and standardize data types
        cleaned_df = self._standardize_orders_data_types(cleaned_df)
        
        # 3. Normalize categorical fields
        cleaned_df = self._normalize_orders_categoricals(cleaned_df)
        
        # 4. Handle missing values
        cleaned_df = self._handle_orders_missing_values(cleaned_df)
        
        # 5. Remove duplicates
        cleaned_df = self._remove_orders_duplicates(cleaned_df)
        
        logger.info(f"Orders cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def clean_products_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize products data."""
        logger.info("Starting products data cleaning...")
        
        cleaned_df = df.copy()
        
        # 1. Merge redundant fields
        cleaned_df = self._merge_redundant_fields(cleaned_df, {
            'product_id': ['product_id', 'item_id'],
            'product_name': ['product_name', 'item_name'],
            'category': ['category', 'product_category'],
            'brand': ['brand', 'manufacturer'],
            'price': ['price', 'list_price'],
            'stock_quantity': ['stock_quantity', 'stock_level']
        })
        
        # 2. Clean and standardize data types
        cleaned_df = self._standardize_products_data_types(cleaned_df)
        
        # 3. Normalize categorical fields
        cleaned_df = self._normalize_products_categoricals(cleaned_df)
        
        # 4. Handle missing values
        cleaned_df = self._handle_products_missing_values(cleaned_df)
        
        # 5. Remove duplicates
        cleaned_df = self._remove_products_duplicates(cleaned_df)
        
        logger.info(f"Products cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def clean_reconciliation_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize reconciliation data."""
        logger.info("Starting reconciliation data cleaning...")
        
        cleaned_df = df.copy()
        
        # 1. Clean and standardize data types
        cleaned_df = self._standardize_reconciliation_data_types(cleaned_df)
        
        # 2. Normalize categorical fields
        cleaned_df = self._normalize_reconciliation_categoricals(cleaned_df)
        
        # 3. Handle missing values
        cleaned_df = self._handle_reconciliation_missing_values(cleaned_df)
        
        # 4. Remove duplicates
        cleaned_df = self._remove_reconciliation_duplicates(cleaned_df)
        
        logger.info(f"Reconciliation cleaning completed. Final shape: {cleaned_df.shape}")
        return cleaned_df
    
    def _merge_redundant_fields(self, df: pd.DataFrame, field_mappings: Dict[str, List[str]]) -> pd.DataFrame:
        """Merge redundant fields with priority logic."""
        for target_field, source_fields in field_mappings.items():
            if target_field not in df.columns:
                # Find the first available source field
                for source_field in source_fields:
                    if source_field in df.columns:
                        df[target_field] = df[source_field]
                        break
            else:
                # Merge with priority logic
                for source_field in source_fields:
                    if source_field in df.columns and source_field != target_field:
                        # Fill missing values in target with non-null values from source
                        mask = df[target_field].isna() & df[source_field].notna()
                        df.loc[mask, target_field] = df.loc[mask, source_field]
        
        return df
    
    def _standardize_customer_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data types for customers data."""
        # Convert total_spent to float
        if 'total_spent' in df.columns:
            df['total_spent'] = pd.to_numeric(df['total_spent'], errors='coerce')
        
        # Convert numeric fields
        numeric_fields = ['total_orders', 'loyalty_points', 'age']
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        # Convert date fields
        date_fields = ['registration_date', 'birth_date']
        for field in date_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce')
        
        return df
    
    def _standardize_orders_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data types for orders data."""
        # Convert numeric fields
        numeric_fields = ['quantity', 'unit_price', 'total_amount', 'shipping_cost', 'tax', 'discount']
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        # Convert date fields
        date_fields = ['order_date', 'order_datetime']
        for field in date_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce')
        
        return df
    
    def _standardize_products_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data types for products data."""
        # Convert numeric fields
        numeric_fields = ['price', 'cost', 'weight', 'stock_quantity', 'reorder_level', 'rating']
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        # Convert boolean fields
        if 'is_active' in df.columns:
            df['is_active'] = df['is_active'].map({
                True: True, False: False, 1: True, 0: False,
                'yes': True, 'no': False, 'true': True, 'false': False,
                '1': True, '0': False
            }).fillna(False)
        
        # Convert date fields
        date_fields = ['created_date', 'last_updated']
        for field in date_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce')
        
        return df
    
    def _standardize_reconciliation_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data types for reconciliation data."""
        # Convert numeric fields
        numeric_fields = ['amount_paid', 'quantity_ordered', 'unit_cost', 'total_value', 
                         'discount_applied', 'shipping_fee', 'tax_amount']
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        # Convert date fields
        date_fields = ['transaction_date', 'last_modified_timestamp']
        for field in date_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce')
        
        return df
    
    def _normalize_customer_categoricals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize categorical fields for customers."""
        # Normalize status
        if 'status' in df.columns:
            df['status'] = df['status'].str.lower().map({
                'active': 'active', 'inactive': 'inactive', 'suspended': 'suspended',
                'act': 'active', 'inact': 'inactive', 'sus': 'suspended'
            }).fillna('inactive')
        
        # Normalize gender
        if 'gender' in df.columns:
            df['gender'] = df['gender'].str.lower().map({
                'male': 'male', 'female': 'female', 'other': 'other',
                'm': 'male', 'f': 'female', 'o': 'other'
            }).fillna('other')
        
        # Normalize segment
        if 'segment' in df.columns:
            df['segment'] = df['segment'].str.lower().map({
                'regular': 'regular', 'vip': 'vip', 'new': 'new',
                'reg': 'regular', 'premium': 'vip'
            }).fillna('regular')
        
        return df
    
    def _normalize_orders_categoricals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize categorical fields for orders."""
        # Normalize status
        if 'status' in df.columns:
            df['status'] = df['status'].str.lower().map({
                'pending': 'pending', 'processing': 'processing', 'shipped': 'shipped',
                'delivered': 'delivered', 'cancelled': 'cancelled', 'returned': 'returned',
                'pend': 'pending', 'proc': 'processing', 'ship': 'shipped',
                'deliv': 'delivered', 'cancel': 'cancelled', 'ret': 'returned'
            }).fillna('pending')
        
        # Normalize payment method
        if 'payment_method' in df.columns:
            df['payment_method'] = df['payment_method'].str.lower().map({
                'credit_card': 'credit_card', 'debit_card': 'debit_card', 'paypal': 'paypal',
                'bank_transfer': 'bank_transfer', 'cash': 'cash',
                'credit': 'credit_card', 'debit': 'debit_card', 'transfer': 'bank_transfer'
            }).fillna('credit_card')
        
        return df
    
    def _normalize_products_categoricals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize categorical fields for products."""
        # Normalize category
        if 'category' in df.columns:
            df['category'] = df['category'].str.lower().map({
                'electronics': 'electronics', 'clothing': 'clothing', 'books': 'books',
                'sports': 'sports', 'toys': 'toys', 'home': 'home',
                'elec': 'electronics', 'cloth': 'clothing', 'book': 'books',
                'sport': 'sports', 'toy': 'toys'
            }).fillna('other')
        
        # Normalize brand
        if 'brand' in df.columns:
            df['brand'] = df['brand'].str.lower().str.strip()
        
        return df
    
    def _normalize_reconciliation_categoricals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize categorical fields for reconciliation data."""
        # Normalize payment status
        if 'payment_status' in df.columns:
            df['payment_status'] = df['payment_status'].str.lower().map({
                'completed': 'completed', 'pending': 'pending', 'failed': 'failed',
                'complete': 'completed', 'pend': 'pending', 'fail': 'failed'
            }).fillna('pending')
        
        # Normalize delivery status
        if 'delivery_status' in df.columns:
            df['delivery_status'] = df['delivery_status'].str.lower().map({
                'pending': 'pending', 'in_transit': 'in_transit', 'delivered': 'delivered',
                'pend': 'pending', 'transit': 'in_transit', 'deliv': 'delivered'
            }).fillna('pending')
        
        return df
    
    def _handle_customer_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in customers data."""
        # Replace null variants with actual None
        null_variants = ['null', 'NULL', 'N/A', 'NA', '', 'nan', 'NaN']
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].replace(null_variants, None)
        
        # Impute age with median if reasonable
        if 'age' in df.columns:
            median_age = df['age'].median()
            if pd.notna(median_age) and 18 <= median_age <= 80:
                df['age'] = df['age'].fillna(median_age)
        
        return df
    
    def _handle_orders_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in orders data."""
        # Replace null variants
        null_variants = ['null', 'NULL', 'N/A', 'NA', '', 'nan', 'NaN']
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].replace(null_variants, None)
        
        # Set default values for missing quantities
        if 'quantity' in df.columns:
            df['quantity'] = df['quantity'].fillna(1)
        
        return df
    
    def _handle_products_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in products data."""
        # Replace null variants
        null_variants = ['null', 'NULL', 'N/A', 'NA', '', 'nan', 'NaN']
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].replace(null_variants, None)
        
        # Set default values
        if 'stock_quantity' in df.columns:
            df['stock_quantity'] = df['stock_quantity'].fillna(0)
        
        if 'is_active' in df.columns:
            df['is_active'] = df['is_active'].fillna(True)
        
        return df
    
    def _handle_reconciliation_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in reconciliation data."""
        # Replace null variants
        null_variants = ['null', 'NULL', 'N/A', 'NA', '', 'nan', 'NaN']
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].replace(null_variants, None)
        
        return df
    
    def _remove_customer_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate customers based on business rules."""
        # Remove exact duplicates
        initial_count = len(df)
        df = df.drop_duplicates()
        
        # Remove duplicates based on email (if available)
        if 'email' in df.columns:
            df = df.drop_duplicates(subset=['email'], keep='first')
        
        # Remove duplicates based on customer_id
        if 'customer_id' in df.columns:
            df = df.drop_duplicates(subset=['customer_id'], keep='first')
        
        final_count = len(df)
        logger.info(f"Removed {initial_count - final_count} duplicate customer records")
        
        return df
    
    def _remove_orders_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate orders based on business rules."""
        # Remove exact duplicates
        initial_count = len(df)
        df = df.drop_duplicates()
        
        # Remove duplicates based on order_id
        if 'order_id' in df.columns:
            df = df.drop_duplicates(subset=['order_id'], keep='first')
        
        final_count = len(df)
        logger.info(f"Removed {initial_count - final_count} duplicate order records")
        
        return df
    
    def _remove_products_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate products based on business rules."""
        # Remove exact duplicates
        initial_count = len(df)
        df = df.drop_duplicates()
        
        # Remove duplicates based on product_id
        if 'product_id' in df.columns:
            df = df.drop_duplicates(subset=['product_id'], keep='first')
        
        final_count = len(df)
        logger.info(f"Removed {initial_count - final_count} duplicate product records")
        
        return df
    
    def _remove_reconciliation_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate reconciliation records."""
        # Remove exact duplicates
        initial_count = len(df)
        df = df.drop_duplicates()
        
        final_count = len(df)
        logger.info(f"Removed {initial_count - final_count} duplicate reconciliation records")
        
        return df

class DatabaseManager:
    """Handles SQLite database operations."""
    
    def __init__(self, db_path: str = 'cleaned_data.sqlite'):
        self.db_path = db_path
        self.conn = None
    
    def create_database(self):
        """Create the SQLite database with normalized schema."""
        logger.info(f"Creating database: {self.db_path}")
        
        self.conn = sqlite3.connect(self.db_path)
        
        # Create tables with proper schema
        self._create_customers_table()
        self._create_products_table()
        self._create_orders_table()
        self._create_reconciliation_table()
        
        # Create indexes for performance
        self._create_indexes()
        
        logger.info("Database schema created successfully")
    
    def _create_customers_table(self):
        """Create customers table."""
        query = """
        CREATE TABLE IF NOT EXISTS customers (
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
            segment VARCHAR(20) DEFAULT 'regular',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.conn.execute(query)
    
    def _create_products_table(self):
        """Create products table."""
        query = """
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
            rating DECIMAL(3,1),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.conn.execute(query)
    
    def _create_orders_table(self):
        """Create orders table."""
        query = """
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
            FOREIGN KEY (product_id) REFERENCES products (product_id)
        )
        """
        self.conn.execute(query)
    
    def _create_reconciliation_table(self):
        """Create reconciliation table."""
        query = """
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
            last_modified_timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.conn.execute(query)
    
    def _create_indexes(self):
        """Create indexes for better performance."""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email)",
            "CREATE INDEX IF NOT EXISTS idx_customers_status ON customers(status)",
            "CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_orders_product_id ON orders(product_id)",
            "CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date)",
            "CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)",
            "CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand)",
            "CREATE INDEX IF NOT EXISTS idx_reconciliation_email ON reconciliation_data(contact_email)",
            "CREATE INDEX IF NOT EXISTS idx_reconciliation_date ON reconciliation_data(transaction_date)"
        ]
        
        for index_query in indexes:
            self.conn.execute(index_query)
    
    def load_data(self, table_name: str, df: pd.DataFrame):
        """Load data into specified table."""
        logger.info(f"Loading {len(df)} records into {table_name} table")
        
        try:
            df.to_sql(table_name, self.conn, if_exists='replace', index=False)
            logger.info(f"Successfully loaded {len(df)} records into {table_name}")
        except Exception as e:
            logger.error(f"Error loading data into {table_name}: {str(e)}")
            raise
    
    def close_connection(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

class ETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self):
        self.cleaner = DataCleaner()
        self.db_manager = DatabaseManager()
        self.cleaned_data = {}
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline."""
        logger.info("Starting ETL Pipeline")
        
        try:
            # Step 1: Load raw data
            raw_data = self._load_raw_data()
            
            # Step 2: Clean data
            cleaned_data = self._clean_data(raw_data)
            
            # Step 3: Create database
            self.db_manager.create_database()
            
            # Step 4: Load data into database
            self._load_data_to_database(cleaned_data)
            
            # Step 5: Generate summary report
            self._generate_summary_report(cleaned_data)
            
            logger.info("ETL Pipeline completed successfully!")
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {str(e)}")
            raise
        finally:
            self.db_manager.close_connection()
    
    def _load_raw_data(self) -> Dict[str, pd.DataFrame]:
        """Load all raw datasets."""
        logger.info("Loading raw datasets...")
        
        raw_data = {}
        
        # Load customers data
        try:
            with open('customers_messy_data.json', 'r') as f:
                customers_data = json.load(f)
            raw_data['customers'] = pd.DataFrame(customers_data)
            logger.info(f"Loaded customers data: {raw_data['customers'].shape}")
        except Exception as e:
            logger.error(f"Error loading customers data: {str(e)}")
            raise
        
        # Load orders data
        try:
            raw_data['orders'] = pd.read_csv('orders_unstructured_data.csv')
            logger.info(f"Loaded orders data: {raw_data['orders'].shape}")
        except Exception as e:
            logger.error(f"Error loading orders data: {str(e)}")
            raise
        
        # Load products data
        try:
            with open('products_inconsistent_data.json', 'r') as f:
                products_data = json.load(f)
            raw_data['products'] = pd.DataFrame(products_data)
            logger.info(f"Loaded products data: {raw_data['products'].shape}")
        except Exception as e:
            logger.error(f"Error loading products data: {str(e)}")
            raise
        
        # Load reconciliation data
        try:
            raw_data['reconciliation'] = pd.read_csv('reconciliation_challenge_data.csv')
            logger.info(f"Loaded reconciliation data: {raw_data['reconciliation'].shape}")
        except Exception as e:
            logger.error(f"Error loading reconciliation data: {str(e)}")
            raise
        
        return raw_data
    
    def _clean_data(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Clean all datasets."""
        logger.info("Cleaning datasets...")
        
        cleaned_data = {}
        
        # Clean each dataset
        cleaned_data['customers'] = self.cleaner.clean_customers_data(raw_data['customers'])
        cleaned_data['orders'] = self.cleaner.clean_orders_data(raw_data['orders'])
        cleaned_data['products'] = self.cleaner.clean_products_data(raw_data['products'])
        cleaned_data['reconciliation'] = self.cleaner.clean_reconciliation_data(raw_data['reconciliation'])
        
        return cleaned_data
    
    def _load_data_to_database(self, cleaned_data: Dict[str, pd.DataFrame]):
        """Load cleaned data into database."""
        logger.info("Loading data into database...")
        
        # Load in order to respect foreign key constraints
        self.db_manager.load_data('customers', cleaned_data['customers'])
        self.db_manager.load_data('products', cleaned_data['products'])
        self.db_manager.load_data('orders', cleaned_data['orders'])
        self.db_manager.load_data('reconciliation_data', cleaned_data['reconciliation'])
    
    def _generate_summary_report(self, cleaned_data: Dict[str, pd.DataFrame]):
        """Generate summary report of the ETL process."""
        logger.info("Generating summary report...")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'datasets': {}
        }
        
        for dataset_name, df in cleaned_data.items():
            report['datasets'][dataset_name] = {
                'records': len(df),
                'columns': len(df.columns),
                'missing_values': df.isnull().sum().sum(),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2
            }
        
        # Save report
        with open('etl_summary_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info("Summary report saved to etl_summary_report.json")

def main():
    """Main execution function."""
    try:
        pipeline = ETLPipeline()
        pipeline.run_pipeline()
        print("✅ ETL Pipeline completed successfully!")
        print("📊 Check 'etl_pipeline.log' for detailed logs")
        print("📋 Check 'etl_summary_report.json' for summary report")
        print("🗄️ Database created: 'cleaned_data.sqlite'")
        
    except Exception as e:
        print(f"❌ ETL Pipeline failed: {str(e)}")
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 