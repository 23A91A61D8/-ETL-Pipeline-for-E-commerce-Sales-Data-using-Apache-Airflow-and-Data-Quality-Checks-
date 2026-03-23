import pandas as pd
from sqlalchemy import create_engine, text

# ✅ Database connection
DB_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
engine = create_engine(DB_URI)


# ✅ CREATE TABLES (DDL)
def create_tables():
    with engine.connect() as conn:

        # DIM CUSTOMERS
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            country VARCHAR(100),
            last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """))

        # DIM PRODUCTS
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id VARCHAR(50) PRIMARY KEY,
            description VARCHAR(255),
            unit_price NUMERIC(10,2),
            last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """))

        # FACT SALES
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_sales (
            sale_id SERIAL PRIMARY KEY,
            invoice_no VARCHAR(50) NOT NULL,
            customer_id VARCHAR(50),
            product_id VARCHAR(50),
            quantity INTEGER NOT NULL,
            total_item_price NUMERIC(10,2) NOT NULL,
            invoice_date TIMESTAMP NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
            FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
        );
        """))

    print("✅ Tables created successfully")


# ✅ LOAD DIM CUSTOMERS (UPSERT)
def load_dim_customers(file_path="/tmp/data/transformed_sales.csv"):
    df = pd.read_csv(file_path)

    customers = df[["customer_id", "Country"]].drop_duplicates()

    with engine.connect() as conn:
        for _, row in customers.iterrows():
            conn.execute(text("""
                INSERT INTO dim_customers (customer_id, country)
                VALUES (:customer_id, :country)
                ON CONFLICT (customer_id)
                DO UPDATE SET country = EXCLUDED.country
            """), {
                "customer_id": row["customer_id"],
                "country": row["Country"]
            })

    print("✅ Loaded dim_customers")


# ✅ LOAD DIM PRODUCTS (UPSERT)
def load_dim_products(file_path="/tmp/data/transformed_sales.csv"):
    df = pd.read_csv(file_path)

    products = df[["StockCode", "Description", "UnitPrice"]].drop_duplicates()

    with engine.connect() as conn:
        for _, row in products.iterrows():
            conn.execute(text("""
                INSERT INTO dim_products (product_id, description, unit_price)
                VALUES (:id, :desc, :price)
                ON CONFLICT (product_id)
                DO UPDATE SET description = EXCLUDED.description,
                              unit_price = EXCLUDED.unit_price
            """), {
                "id": row["StockCode"],
                "desc": row["Description"],
                "price": row["UnitPrice"]
            })

    print("✅ Loaded dim_products")


# ✅ LOAD FACT SALES (INCREMENTAL)
def load_fact_sales(file_path="/tmp/data/transformed_sales.csv"):
    df = pd.read_csv(file_path)

    with engine.connect() as conn:

        # Get last loaded date
        result = conn.execute(text("SELECT MAX(invoice_date) FROM fact_sales"))
        last_date = result.scalar()

        # Incremental filtering
        if last_date:
            df = df[df["InvoiceDate"] > str(last_date)]

        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO fact_sales
                (invoice_no, customer_id, product_id, quantity, total_item_price, invoice_date)
                VALUES (:inv, :cust, :prod, :qty, :price, :date)
            """), {
                "inv": row["InvoiceNo"],
                "cust": row["customer_id"],
                "prod": row["StockCode"],
                "qty": int(row["Quantity"]),
                "price": float(row["total_item_price"]),
                "date": row["InvoiceDate"]
            })

    print("✅ Loaded fact_sales incrementally")


# ✅ MAIN FUNCTION (CALL ALL)
def run_load_pipeline():
    create_tables()
    load_dim_customers()
    load_dim_products()
    load_fact_sales()