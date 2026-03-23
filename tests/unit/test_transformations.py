import pandas as pd
from src.etl_scripts.transform import transform_sales_data

def test_transform_sales_data(tmp_path):

    # Create sample raw data
    data = {
        "InvoiceNo": ["1001", "1002"],
        "StockCode": ["A1", "B1"],
        "Description": ["Product A", "Product B"],
        "Quantity": [2, 3],
        "UnitPrice": [10.0, 20.0],
        "CustomerID": ["C1", "C2"],
        "Country": ["UK", "USA"],
        "InvoiceDate": ["2024-01-01", "2024-01-02"]
    }

    df = pd.DataFrame(data)

    raw_file = tmp_path / "raw.csv"
    transformed_file = tmp_path / "transformed.csv"

    df.to_csv(raw_file, index=False)

    # Run transformation
    transform_sales_data(str(raw_file), str(transformed_file))

    result = pd.read_csv(transformed_file)

    # Assertions
    assert "total_item_price" in result.columns
    assert result["total_item_price"].iloc[0] == 20.0
    assert "customer_id" in result.columns