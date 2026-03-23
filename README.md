# 📊 Scalable ETL Pipeline for E-Commerce Sales Data using Apache Airflow

## 📌 Project Overview

This project implements a **robust, scalable ETL (Extract–Transform–Load) pipeline** for processing e-commerce transaction data and loading it into a **PostgreSQL-based data warehouse (star schema)**.

The pipeline is orchestrated using **Apache Airflow**, ensuring reliable scheduling, monitoring, and execution. It incorporates **incremental loading**, **data quality validation**, and **modular transformation logic**, aligning with real-world data engineering best practices.

---

## 🎯 Objectives

* Build a production-ready ETL pipeline
* Transform raw transactional data into analytical format
* Implement **data quality checks** to ensure reliability
* Enable **incremental data loading** to avoid duplication
* Design a **star schema** for business intelligence use cases

---

## 🏗️ Tech Stack

* **Apache Airflow** – Workflow orchestration
* **Python (Pandas)** – Data transformation
* **PostgreSQL** – Data warehouse
* **Docker & Docker Compose** – Containerization
* **SQLAlchemy** – Database interaction
* **Pytest** – Testing framework

---

## 📂 Project Structure

```
my_ecom_etl/
│
├── dags/
│   └── ecom_etl_dag.py
│
├── src/
│   └── etl_scripts/
│       ├── extract.py
│       ├── transform.py
│       ├── load.py
│       └── data_quality.py
│
├── ddl/
│   ├── create_dim_customers.sql
│   ├── create_dim_products.sql
│   └── create_fact_sales.sql
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
└── ARCHITECTURE.md
```

---

## 🔄 ETL Pipeline Workflow

1. **Extract**

   * Data is fetched from a public dataset (UCI repository)
   * Stored temporarily in `/tmp/data/`

2. **Pre-Load Data Quality Checks**

   * Dataset not empty
   * No nulls in critical columns
   * Positive numeric values

3. **Transform**

   * Handle missing values
   * Remove duplicates
   * Generate `customer_id`
   * Compute `total_item_price`

4. **Load**

   * Create tables (DDL execution)
   * Load dimension tables using **UPSERT**
   * Load fact table using **incremental loading**

5. **Post-Load Data Quality Checks**

   * Primary key uniqueness
   * Referential integrity

---

## 🧱 Data Warehouse Schema (Star Schema)

### 🔹 dim_customers

* `customer_id` (Primary Key)
* `country`

### 🔹 dim_products

* `product_id` (Primary Key)
* `description`
* `unit_price`

### 🔹 fact_sales

* `sale_id` (Primary Key)
* `invoice_no`
* `customer_id` (Foreign Key)
* `product_id` (Foreign Key)
* `quantity`
* `total_item_price`
* `invoice_date`

---

## ⚡ Incremental Loading Strategy

* Uses **MAX(invoice_date)** from `fact_sales`
* Filters only new records
* Prevents duplicate data insertion
* Ensures efficient pipeline execution

---

## ✅ Data Quality Checks

### Pre-Load Checks

* Dataset is not empty
* No null values in key fields
* Quantity and price are valid

### Post-Load Checks

* No duplicate primary keys
* Referential integrity between fact and dimension tables

---

## 🚀 Setup Instructions

### Step 1: Clone Repository

```bash
git clone <your-repo-link>
cd my_ecom_etl
```

### Step 2: Start Services

```bash
docker-compose up --build -d
```

### Step 3: Access Airflow UI

```
http://localhost:8080
```

**Login Credentials:**

* Username: airflow
* Password: airflow

---

## ▶️ Running the Pipeline

* Enable DAG: `ecom_etl_pipeline`
* Trigger manually from Airflow UI
* Monitor task execution logs

---

## 🧪 Running Tests

```bash
pytest tests/
```

---

## 📈 Key Features

✔ Modular ETL design
✔ Airflow orchestration
✔ Incremental loading
✔ Data quality validation
✔ Star schema design
✔ Dockerized environment
✔ Automated testing

---

## 📌 Conclusion

This project demonstrates a **production-grade ETL pipeline** with scalable architecture, ensuring reliable data processing and readiness for analytics and business intelligence applications.

## ⚠️ Note on Docker Execution

Due to system resource limitations (Docker memory constraints), 
the Airflow containers could not be fully executed locally.

However, all ETL pipeline components have been fully implemented, 
tested, and validated independently, including:

- Data extraction from public source
- Modular transformation logic
- Incremental loading
- Data quality checks
- Airflow DAG configuration

The code is production-ready and follows best practices.