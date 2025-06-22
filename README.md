# 📊 E-commerce Data Analytics Dashboard

An interactive data analytics dashboard built using **Streamlit**, **Pandas**, **Plotly**, and **SQLite** to analyze and visualize key metrics for an e-commerce platform. This project includes an end-to-end ETL pipeline for cleaning and processing raw data into a structured format suitable for analytics.

---

## 🚀 Project Structure

├── ETL Pipeline
│ ├── etl_pipeline.py # Main ETL script to clean and transform raw data
│ ├── data_discovery_script.py # Exploratory data analysis on raw inputs
│ └── etl_summary_report.json # Summary of the ETL process
│
├── Dashboard
│ └── app.py # Streamlit dashboard application
│
├── Data
│ ├── customers_messy_data.json
│ ├── orders_unstructured_data.csv
│ ├── products_inconsistent_data.json
│ └── reconciliation_challenge_data.csv
│
└── Documentation
└── README.md # Project documentation

markdown
Copy
Edit

---

## 📈 Dashboard Features

- ✅ **Real-time KPI tracking** (customers, orders, revenue, etc.)
- 👥 **Customer analytics** (demographics, segments, status)
- 📦 **Order analysis** (statuses, payment methods, revenue by segment)
- 🛍️ **Product performance metrics** (category, price distribution, top products)
- 🧹 **Data quality insights** (missing values per table)
- 🎛 **Interactive filters** for segmenting data
- 📋 **Data tables** for exploring raw customer, product, and order data

---

## 🔧 Technologies Used

- **Python 3.9+**
- **Streamlit**
- **Pandas**
- **Plotly Express & Graph Objects**
- **NumPy**
- **SQLite3**

---

## 🧪 How to Run Locally

1. **Clone the repository:**
   ```bash
   git clone https://github.com/pratikkarde/E-commerce-Data-Analytics-Dashboard.git
   cd E-commerce-Data-Analytics-Dashboard
Install dependencies:
(Make sure you are in a virtual environment)

bash
pip install -r requirements.txt
Ensure the SQLite DB exists:
The file cleaned_data.sqlite must be present in the root directory or generated using the ETL Pipeline.

Run the Streamlit app:

bash
streamlit run Dashboard/app.py
🌐 Live Demo
🟢 Hosted on Streamlit Cloud:
[https://e-commerce-data-analytics-dashboard.streamlit.app](https://e-commerce-data-analytics-dashboard-2j56adoo83ydxy6qjzw734.streamlit.app/)

📂 Sample Screenshots
KPI Metrics	Customer Analysis	Product Insights

🧼 ETL Pipeline Overview
The ETL pipeline performs the following steps:

Cleans and standardizes customer, order, and product datasets

Handles missing values and type inconsistencies

Generates a summary report (etl_summary_report.json)

Outputs a single SQLite database: cleaned_data.sqlite

🧠 Future Improvements
🔐 Add user authentication

🧠 Incorporate machine learning for customer segmentation or churn prediction

📤 Automate data ingestion from external sources (APIs, live feeds)

📜 License
This project is licensed under the MIT License.

🙋‍♂️ Author
Pratik Karde
