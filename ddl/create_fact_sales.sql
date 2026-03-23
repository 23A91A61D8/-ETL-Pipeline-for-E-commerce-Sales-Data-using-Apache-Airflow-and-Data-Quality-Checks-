CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,

    customer_id VARCHAR(50),
    product_id VARCHAR(50),

    quantity INTEGER NOT NULL,
    total_item_price NUMERIC(10,2) NOT NULL,
    invoice_date TIMESTAMP NOT NULL,

    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id)
        REFERENCES dim_customers(customer_id),

    CONSTRAINT fk_product
        FOREIGN KEY (product_id)
        REFERENCES dim_products(product_id)
);