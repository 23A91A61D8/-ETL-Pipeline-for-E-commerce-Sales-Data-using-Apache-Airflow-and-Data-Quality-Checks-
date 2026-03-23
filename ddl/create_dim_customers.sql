CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    country VARCHAR(100),
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);