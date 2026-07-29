import pandas as pd
import numpy as np
import pytest
from etl_scripts.silver_transformer import (
    impute_customer_ids,
    filter_valid_records,
    transform_silver_data
)

def test_customer_id_imputation():
    """
    Test Case 1: Validating the CustomerID imputation.
    Ensures NaN, None, empty string, and whitespace-only IDs are correctly replaced with 'UNKNOWN'.
    """
    data = {
        'CustomerID': [np.nan, '12345', None, '', '   ', '12345']
    }
    df = pd.DataFrame(data)
    
    res = impute_customer_ids(df)
    
    expected = ['UNKNOWN', '12345', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', '12345']
    assert res['CustomerID'].tolist() == expected


def test_negative_and_zero_value_filtering():
    """
    Test Case 2: Verifying the negative quantity and non-positive price filtering.
    """
    data = {
        'Quantity': [10, -5, 8, 12, 15],
        'UnitPrice': [2.5, 3.0, 0.0, -1.5, 4.5]
    }
    df = pd.DataFrame(data)
    
    res = filter_valid_records(df)
    
    # Valid rows:
    # Row 0: Quantity=10, Price=2.5 (Valid)
    # Row 1: Quantity=-5 (Invalid)
    # Row 2: Price=0.0 (Invalid)
    # Row 3: Price=-1.5 (Invalid)
    # Row 4: Quantity=15, Price=4.5 (Valid)
    
    assert len(res) == 2
    assert res['Quantity'].tolist() == [10, 15]
    assert res['UnitPrice'].tolist() == [2.5, 4.5]


def test_total_price_and_aggregation():
    """
    Test Case 3: Verifying the TotalPrice arithmetic and daily sales aggregation logic.
    Calculates TotalPrice (Quantity * UnitPrice) and groups by InvoiceDate and Country.
    """
    data = {
        'InvoiceNo': ['1', '2', '3', '4'],
        'StockCode': ['A', 'B', 'C', 'D'],
        'Quantity': [2, 3, 5, 10],
        'UnitPrice': [10.0, 15.0, 20.0, 5.0],
        'InvoiceDate': [
            '2023-10-01 10:00:00',
            '2023-10-01 15:30:00',
            '2023-10-02 09:15:00',
            '2023-10-02 12:00:00'
        ],
        'Country': ['United Kingdom', 'United Kingdom', 'Germany', 'Germany'],
        'CustomerID': ['12345', '12345', '12346', '12346']
    }
    df = pd.DataFrame(data)
    
    res = transform_silver_data(df)
    
    # Total prices:
    # 10-01 UK: Row 0 (2 * 10 = 20) + Row 1 (3 * 15 = 45) = 65.0
    # 10-02 DE: Row 2 (5 * 20 = 100) + Row 3 (10 * 5 = 50) = 150.0
    
    assert len(res) == 2
    
    # Verify UK record aggregation
    uk_row = res[(res['InvoiceDate'] == '2023-10-01') & (res['Country'] == 'United Kingdom')]
    assert len(uk_row) == 1
    assert uk_row.iloc[0]['DailyTotalSales'] == 65.0
    assert uk_row.iloc[0]['CompositeKey'] == '2023-10-01_United Kingdom'
    
    # Verify Germany record aggregation
    germany_row = res[(res['InvoiceDate'] == '2023-10-02') & (res['Country'] == 'Germany')]
    assert len(germany_row) == 1
    assert germany_row.iloc[0]['DailyTotalSales'] == 150.0
    assert germany_row.iloc[0]['CompositeKey'] == '2023-10-02_Germany'
