CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    description VARCHAR(255),
    unit_price NUMERIC(10,2),
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);