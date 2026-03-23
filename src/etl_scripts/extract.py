import pandas as pd
import requests
import os

SOURCE_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"

def extract_sales_data(output_path="/tmp/data/raw_sales.csv"):
    os.makedirs("/tmp/data", exist_ok=True)

    response = requests.get(SOURCE_URL)
    response.raise_for_status()

    with open("/tmp/data/data.xlsx", "wb") as f:
        f.write(response.content)

    df = pd.read_excel("/tmp/data/data.xlsx")

    # Save as CSV
    df.to_csv(output_path, index=False)

    # ✅ Data Quality Check (Pre-load 1)
    if df.empty:
        raise ValueError("Extracted data is empty!")

    print("Extraction completed successfully")