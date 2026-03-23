import pandas as pd
import pytest
from src.etl_scripts.data_quality import check_staged_data_quality

def test_data_quality_pass(tmp_path):

    data = {
        "InvoiceNo": ["1001"],
        "StockCode": ["A1"],
        "Quantity": [2],
        "UnitPrice": [10.0]
    }

    df = pd.DataFrame(data)

    file = tmp_path / "data.csv"
    df.to_csv(file, index=False)

    # Should NOT raise error
    check_staged_data_quality(str(file))


def test_data_quality_fail(tmp_path):

    data = {
        "InvoiceNo": [None],   # ❌ invalid
        "StockCode": ["A1"],
        "Quantity": [-1],      # ❌ invalid
        "UnitPrice": [-10.0]   # ❌ invalid
    }

    df = pd.DataFrame(data)

    file = tmp_path / "bad.csv"
    df.to_csv(file, index=False)

    with pytest.raises(ValueError):
        check_staged_data_quality(str(file))