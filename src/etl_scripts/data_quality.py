import pandas as pd
from sqlalchemy import create_engine, text

DB_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
engine = create_engine(DB_URI)


# ✅ PRE-LOAD CHECKS
def check_staged_data_quality(file_path="/tmp/data/transformed_sales.csv"):

    df = pd.read_csv(file_path)

    # 1️⃣ Row count check
    if df.empty:
        raise ValueError("❌ Data Quality Failed: Dataset is empty")

    # 2️⃣ Null check (critical columns)
    if df["InvoiceNo"].isnull().any():
        raise ValueError("❌ Data Quality Failed: Null InvoiceNo found")

    if df["StockCode"].isnull().any():
        raise ValueError("❌ Data Quality Failed: Null StockCode found")

    # 3️⃣ Positive values check
    if (df["Quantity"] <= 0).any():
        raise ValueError("❌ Data Quality Failed: Invalid Quantity values")

    if (df["UnitPrice"] < 0).any():
        raise ValueError("❌ Data Quality Failed: Negative UnitPrice found")

    print("✅ Pre-load data quality checks passed")


# ✅ POST-LOAD CHECKS
def check_loaded_data_quality():

    with engine.connect() as conn:

        # 1️⃣ PK uniqueness check (dim_customers)
        result = conn.execute(text("""
            SELECT customer_id, COUNT(*)
            FROM dim_customers
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        """))

        duplicates = result.fetchall()
        if duplicates:
            raise ValueError("❌ Duplicate customer_id found in dim_customers")

        # 2️⃣ Referential integrity check (fact → dim)
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM fact_sales f
            LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """))

        missing_fk = result.scalar()

        if missing_fk > 0:
            raise ValueError("❌ Referential integrity failed: Missing customer_id")

    print("✅ Post-load data quality checks passed")