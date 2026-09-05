# Supply Chain Data Platform

<img width="942" height="630" alt="image" src="https://github.com/user-attachments/assets/21884d19-e54b-4cb6-9f14-3615fd103dcb" />


An end-to-end data engineering pipeline for processing and analyzing supply chain data using **Python, PySpark, SQL, and Apache Airflow**.

The project follows a **Medallion Architecture** to transform raw operational data into validated and analytics-ready datasets.

## 🏗️ Architecture

```text
                Raw CSV Data
                     │
                     ▼
              Python Ingestion
                     │
                     ▼
               Bronze Layer
                     │
                     ▼
             PySpark Processing
                     │
                     ▼
                Silver Layer
                     │
                     ▼
             Data Quality Checks
                     │
                     ▼
                 Gold Layer
                     │
                     ▼
            Analytics-Ready Data

        Apache Airflow
        orchestrates the
        complete pipeline
```

## 🔄 Pipeline Workflow

The pipeline runs in the following order:

```text
Generate Data
      ↓
Bronze Ingestion
      ↓
Bronze → Silver
      ↓
Data Quality Validation
      ↓
Silver → Gold
```

Apache Airflow is used to automate and orchestrate the complete workflow.

## 🛠️ Technologies Used

- **Python** – Data generation, ingestion and validation
- **PySpark** – Data transformation and processing
- **SQL** – Data analysis and transformation concepts
- **Apache Airflow** – Workflow orchestration
- **PostgreSQL** – Airflow metadata database
- **Parquet** – Layered data storage
- **Medallion Architecture** – Bronze, Silver and Gold data layers

## 📂 Project Structure

```text
supply-chain-data-platform/
│
├── dags/
│   ├── supply_chain_pipeline.py
│   └── test_supply_chain.py
│
├── src/
│   ├── ingestion/
│   │   ├── generate_data.py
│   │   └── ingest_bronze.py
│   │
│   ├── spark/
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   │
│   └── validation/
│       └── data_quality.py
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── quality_reports/
│
├── sql/
├── tests/
├── config/
├── images/
│   ├── airflow_dag.png
│   ├── airflow_success.png
│   └── data_quality_report.png
│
└── README.md
```

## 🥉 Bronze Layer

The Bronze layer stores the ingested source data in Parquet format.

Datasets include:

- Suppliers
- Products
- Warehouses
- Customers
- Orders
- Inventory
- Shipments

The ingestion process validates that the datasets can be successfully loaded before continuing through the pipeline.

## 🥈 Silver Layer

The Silver layer contains cleaned and transformed data.

PySpark is used to perform operations such as:

- Data cleaning
- Null handling
- Duplicate removal
- Data validation
- Filtering invalid records
- Schema transformation

Example:

```text
Orders
100,500 records
      ↓
Silver
99,699 valid records
```

## ⚙️ Apache Airflow

Apache Airflow orchestrates the complete pipeline using the `supply_chain_pipeline` DAG.

### DAG Structure

![Airflow DAG](images/airflow_dag.png)

Task dependency:

```text
generate_data
      ↓
ingest_bronze
      ↓
bronze_to_silver
      ↓
data_quality
      ↓
silver_to_gold
```

### Successful Pipeline Run

The complete pipeline was successfully executed through Airflow, with all five tasks completing successfully.

![Successful Airflow Run](images/airflow_success.png)

## ✅ Data Quality

A dedicated data quality stage validates the Silver layer before generating Gold datasets.

The pipeline performs **22 data quality checks**, covering:

- Duplicate records
- Required columns
- Positive and non-negative values
- Referential integrity
- Orders
- Inventory
- Shipments
- Dimension tables

### Result

```text
Checks Passed: 22
Checks Failed: 0

DATA QUALITY PASSED
```

![Data Quality Report](images/data_quality_report.png)

A CSV quality report is generated under:

```text
data/quality_reports/data_quality_report.csv
```

## 🥇 Gold Layer

The Gold layer contains analytics-ready datasets designed for business analysis.

### Dimension Tables

```text
dim_customer
dim_supplier
dim_product
dim_warehouse
dim_date
```

### Fact Tables

```text
fact_orders
fact_inventory
fact_shipments
```

### Analytical Tables

```text
daily_sales
supplier_performance
warehouse_performance
inventory_metrics
```

### Gold Layer Results

| Gold Dataset | Records |
|---|---:|
| `dim_customer` | 10,000 |
| `dim_supplier` | 50 |
| `dim_product` | 500 |
| `dim_warehouse` | 10 |
| `dim_date` | 581 |
| `fact_orders` | 99,699 |
| `fact_inventory` | 5,000 |
| `fact_shipments` | 95,092 |
| `daily_sales` | 581 |
| `supplier_performance` | 50 |
| `warehouse_performance` | 10 |
| `inventory_metrics` | 500 |

These datasets can be used for downstream analytics, reporting and dashboard development.

## 📊 Pipeline Results

The final successful pipeline produced:

| Dataset | Records |
|---|---:|
| Customers | 10,000 |
| Suppliers | 50 |
| Products | 500 |
| Warehouses | 10 |
| Orders | 99,699 |
| Inventory | 5,000 |
| Shipments | 95,092 |

## 🚀 How to Run

### 1. Clone the repository

```bash
git clone <your-repository-url>
cd supply-chain-data-platform
```

### 2. Activate the Python environment

```bash
source ~/airflow-venv/bin/activate
```

### 3. Generate the data

```bash
python src/ingestion/generate_data.py
```

### 4. Run Bronze ingestion

```bash
python src/ingestion/ingest_bronze.py
```

### 5. Run Bronze → Silver transformation

```bash
spark-submit src/spark/bronze_to_silver.py
```

### 6. Run data quality checks

```bash
python src/validation/data_quality.py
```

### 7. Run Silver → Gold transformation

```bash
spark-submit src/spark/silver_to_gold.py
```

### 8. Run the complete pipeline using Airflow

Start Airflow:

```bash
airflow standalone
```

Then trigger the DAG:

```bash
airflow dags trigger supply_chain_pipeline
```

## 🎯 Key Learning Outcomes

This project demonstrates practical experience with:

- Building an end-to-end data pipeline
- Medallion Architecture
- PySpark data processing
- Batch data ingestion
- Data quality validation
- Airflow DAG development
- Workflow orchestration
- Parquet-based data storage
- Data transformation and aggregation
- Linux/WSL development environment

## 🔮 Future Enhancements

Potential improvements include:

- PostgreSQL analytics layer
- Power BI dashboard
- Incremental data processing
- Automated testing
- Cloud storage integration
- CI/CD pipeline
- Monitoring and alerting

## 👨‍💻 Author

**Keerthan HS**


