# 🏗️ ETL Pipeline Architecture

## 📌 Overview

The system is designed as a **modular, scalable ETL pipeline** that processes raw e-commerce data and loads it into a structured data warehouse using Apache Airflow.

---

## 🔄 High-Level Architecture

```
        +---------------------+
        |   Data Source       |
        | (UCI Dataset)       |
        +----------+----------+
                   |
                   v
        +---------------------+
        |     Extract Layer   |
        |  (extract.py)       |
        +----------+----------+
                   |
                   v
        +---------------------+
        |  Data Quality Check |
        |  (Pre-Load)         |
        +----------+----------+
                   |
                   v
        +---------------------+
        |   Transform Layer   |
        | (transform.py)      |
        +----------+----------+
                   |
                   v
        +---------------------+
        |     Load Layer      |
        | (load.py)           |
        +----------+----------+
                   |
                   v
        +---------------------+
        | Data Warehouse      |
        | (PostgreSQL)        |
        +----------+----------+
                   |
                   v
        +---------------------+
        | Data Quality Check  |
        | (Post-Load)         |
        +---------------------+
```

---

## ⚙️ Components

### 🔹 Apache Airflow

* Orchestrates ETL workflow
* Defines task dependencies
* Provides monitoring and logging

---

### 🔹 Extract Layer

* Fetches raw data from external source
* Stores data in staging location (`/tmp/data`)

---

### 🔹 Transform Layer

* Cleans and standardizes data
* Removes duplicates
* Generates derived columns
* Ensures data consistency

---

### 🔹 Load Layer

* Executes DDL scripts
* Loads dimension tables using UPSERT
* Loads fact table incrementally

---

### 🔹 Data Warehouse

* PostgreSQL database
* Star schema optimized for analytics
* Ensures referential integrity

---

### 🔹 Data Quality Layer

* Validates data before and after loading
* Prevents invalid data entry
* Ensures data integrity

---

## 📊 Data Flow Summary

1. Data is extracted from source
2. Pre-load validation is applied
3. Data is transformed and enriched
4. Clean data is loaded into warehouse
5. Post-load validation ensures correctness

---

## 🎯 Design Principles

* **Modularity** – Separate scripts for each stage
* **Scalability** – Handles incremental data growth
* **Reliability** – Data quality checks at multiple stages
* **Maintainability** – Clean and readable code structure
* **Reusability** – Functions designed for extensibility

---

## 🚀 Conclusion

This architecture ensures a **robust, scalable, and production-ready ETL pipeline**, suitable for real-world data engineering scenarios and analytics-driven applications.
