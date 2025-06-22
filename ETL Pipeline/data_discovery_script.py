# %% [markdown]
# # Phase 1: Data Discovery and Analysis
#
# This notebook covers the initial phase of our data engineering project. The primary goal is to perform a thorough exploratory data analysis (EDA) on the four provided datasets. We will load each dataset, inspect its structure, identify data quality issues, and document our findings. This analysis will form the foundation for the design and implementation of the ETL pipeline in Phase 2.

# %% [markdown]
# ## 1. Setup and Library Imports
#
# First, let's import the necessary Python libraries for data manipulation, analysis, and visualization.

# %%
import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import seaborn as sns
import ast # For safely evaluating string representations of literals

# Set plotting style
sns.set_style('whitegrid')

# %% [markdown]
# ## 2. Data Loading
#
# We have four datasets to analyze:
# 1. `customers_messy_data.json`: Customer information.
# 2. `orders_unstructured_data.csv`: Order details.
# 3. `products_inconsistent_data.json`: Product information.
# 4. `reconciliation_challenge_data.csv`: Data for the bonus schema reconciliation task.

# %%
# File paths
customers_file = 'customers_messy_data.json'
orders_file = 'orders_unstructured_data.csv'
products_file = 'products_inconsistent_data.json'
reconciliation_file = 'reconciliation_challenge_data.csv'

# Load the datasets
try:
    # Customers data is a JSON file, but it might be JSON-lines. Let's try reading it line by line.
    with open(customers_file, 'r') as f:
        customers_data = [json.loads(line) for line in f]
    df_customers = pd.DataFrame(customers_data)

    # Products data is also a JSON file
    df_products = pd.read_json(products_file)

    # Orders and reconciliation data are CSVs
    df_orders = pd.read_csv(orders_file)
    df_reconciliation = pd.read_csv(reconciliation_file)
    
    print("All datasets loaded successfully!")
except FileNotFoundError as e:
    print(f"Error: {e}. Please ensure all data files are in the correct directory.")

# %% [markdown]
# ## 3. Analysis of Customer Data (`customers_messy_data.json`)

# %%
print("Customers Data - First 5 Rows:")
display(df_customers.head())

# %%
print("\nCustomers Data - Info:")
df_customers.info()

# %% [markdown]
# ### Findings from Customer Data:
# 1.  **Redundant Columns**: We have both `customer_id` and `id`. A quick check will see if they are identical. If so, one is redundant.
# 2.  **Missing Values**: `phone` and `address` have missing values. The `address` field is a dictionary with further nested data.
# 3.  **Inconsistent Data Types**: `registration_date` is an object (string) and likely contains multiple date formats.
# 4.  **Data Quality**: `email` should be validated. `phone` numbers could have inconsistent formatting.

# %%
# Check for redundancy between 'customer_id' and 'id'
redundancy_check = df_customers['customer_id'].equals(df_customers['id'])
print(f"Are 'customer_id' and 'id' columns identical? {redundancy_check}")

# Check for duplicate customer records
num_duplicates = df_customers.duplicated(subset=['customer_id']).sum()
print(f"Number of duplicate customer records (by customer_id): {num_duplicates}")

# %%
# Analyze registration_date formats
print("\nUnique Registration Date Formats (Top 5):")
print(df_customers['registration_date'].str.slice(0, 10).value_counts().nlargest(5))

# %%
# Visualize Country Distribution
plt.figure(figsize=(12, 6))
sns.countplot(y=df_customers['country'], order=df_customers['country'].value_counts().index)
plt.title('Customer Distribution by Country')
plt.xlabel('Number of Customers')
plt.ylabel('Country')
plt.show()

# %% [markdown]
# ### Customer Data Cleanup Plan:
# - Drop the redundant `id` column.
# - Normalize `registration_date` to a consistent `YYYY-MM-DD HH:MM:SS` format.
# - Extract nested `address` fields (street, city, state, zip_code) into separate columns.
# - Clean and standardize phone numbers.
# - Handle missing values appropriately (e.g., fill with 'N/A' or another placeholder).

# %% [markdown]
# ## 4. Analysis of Orders Data (`orders_unstructured_data.csv`)

# %%
print("Orders Data - First 5 Rows:")
display(df_orders.head())

# %%
print("\nOrders Data - Info:")
df_orders.info()

# %% [markdown]
# ### Findings from Orders Data:
#
# 1.  **Unstructured Data**: The `order_details` column is a string representation of a list of dictionaries. This needs to be parsed and flattened.
# 2.  **Mixed Data Types**: `order_date` is an object (string) and needs to be converted to a datetime type.
# 3.  **Inconsistent Enums**: The `status` column likely has inconsistent values (e.g., 'shipped' vs. 'Shipped').

# %%
# Analyze the 'status' column for inconsistent values
print("Value counts for 'status' column:")
print(df_orders['status'].value_counts())

# %%
# Attempt to parse the 'order_details' column for the first few rows to understand its structure
print("\nExample of parsed 'order_details':")
for item in df_orders['order_details'].head(2):
    try:
        # Use ast.literal_eval for safe evaluation of the string
        parsed_detail = ast.literal_eval(item)
        print(parsed_detail)
    except (ValueError, SyntaxError) as e:
        print(f"Could not parse: {item}, Error: {e}")

# %% [markdown]
# ### Orders Data Cleanup Plan:
# - Parse the `order_details` string into structured data. This will result in a new table, `order_items`, with columns like `order_id`, `product_id`, `quantity`, and `price`.
# - Standardize the `status` column to a consistent case (e.g., lowercase).
# - Convert `order_date` to a standard datetime format.
# - Create a primary `orders` table and a separate `order_items` table to achieve normalization.

# %% [markdown]
# ## 5. Analysis of Products Data (`products_inconsistent_data.json`)

# %%
print("Products Data - First 5 Rows:")
display(df_products.head())

# %%
print("\nProducts Data - Info:")
df_products.info()

# %% [markdown]
# ### Findings from Products Data:
#
# 1.  **Mixed Data Types**: `price` and `cost` are objects (strings) and contain currency symbols ('$') which must be removed before converting to a numeric type. `stock_level` is also an object and likely contains mixed types.
# 2.  **Inconsistent Categories**: The `category` column may have inconsistencies in naming or casing.
# 3.  **Date Formatting**: `added_date` is a string and needs to be standardized.

# %%
# Analyze 'category' for inconsistencies
print("Value counts for 'category' column:")
print(df_products['category'].value_counts())

# %%
# Check data types within 'stock_level'
print("\nTypes found in 'stock_level' column:")
print(df_products['stock_level'].apply(type).value_counts())

# %% [markdown]
# ### Products Data Cleanup Plan:
# - Remove currency symbols and convert `price` and `cost` columns to a float type.
# - Standardize the `category` column (e.g., consistent casing).
# - Convert `stock_level` to a consistent integer type, handling non-numeric values.
# - Convert `added_date` to a standard datetime format.

# %% [markdown]
# ## 6. Analysis of Reconciliation Data (`reconciliation_challenge_data.csv`)

# %%
print("Reconciliation Data - First 5 Rows:")
display(df_reconciliation.head())

# %% [markdown]
# ### Findings from Reconciliation Data:
#
# This dataset appears to be a separate challenge, likely for Phase 4. It contains product data with a completely different schema (`SKU`, `product_name`, `price_usd`, `quantity_on_hand`). The goal here would be to reconcile this schema with our primary `products` table schema (`product_id`, `name`, `price`, etc.). This involves mapping columns and potentially transforming data to fit our target schema.

# %% [markdown]
# ## 7. Conclusion & Next Steps
#
# Our initial data discovery has revealed several key data quality issues that need to be addressed:
#
# - **Structural Issues**: Unstructured data in `orders`, nested JSON in `customers`.
# - **Inconsistent Data Types**: Dates, numeric values (prices, stock), and identifiers are stored as strings.
# - **Redundant Data**: Duplicate `id` column in the customer data.
# - **Inconsistent Categorical Data**: The `status` and `category` fields use varied casing and naming.
# - **Missing Values**: Present across multiple datasets.
#
# The next step is to use these findings to build a robust ETL (Extract, Transform, Load) pipeline. This pipeline will programmatically perform the cleanup tasks identified above and load the clean, normalized data into a SQLite database, preparing it for analysis and visualization in the Streamlit dashboard. 