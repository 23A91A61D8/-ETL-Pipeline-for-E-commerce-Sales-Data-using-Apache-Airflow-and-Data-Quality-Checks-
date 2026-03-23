import pandas as pd
import hashlib

# 1. Handle missing values
def handle_missing_values(df):
    df = df.dropna(subset=["InvoiceNo", "StockCode"])
    df["Quantity"] = df["Quantity"].fillna(0)
    df["UnitPrice"] = df["UnitPrice"].fillna(0)
    return df

# 2. Remove duplicates
def remove_duplicates(df):
    return df.drop_duplicates(subset=["InvoiceNo", "StockCode"])

# 3. Calculate total price
def calculate_total_price(df):
    df["total_item_price"] = df["Quantity"] * df["UnitPrice"]
    return df

# 4. Generate customer_id
def generate_customer_id(df):
    df["CustomerID"] = df["CustomerID"].fillna("UNKNOWN")
    df["customer_id"] = df["CustomerID"].astype(str).apply(
        lambda x: hashlib.md5(x.encode()).hexdigest()
    )
    return df

# MAIN TRANSFORM FUNCTION
def transform_sales_data(input_path="/tmp/data/raw_sales.csv",
                         output_path="/tmp/data/transformed_sales.csv"):

    df = pd.read_csv(input_path)

    df = handle_missing_values(df)
    df = remove_duplicates(df)
    df = calculate_total_price(df)
    df = generate_customer_id(df)

    # ✅ Data Quality Checks (Pre-load)
    if df["InvoiceNo"].isnull().any():
        raise ValueError("Null InvoiceNo found!")

    if (df["Quantity"] <= 0).any():
        raise ValueError("Invalid Quantity values!")

    df.to_csv(output_path, index=False)

    print("Transformation completed successfully")