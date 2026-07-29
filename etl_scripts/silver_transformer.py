import os
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def impute_customer_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Imputes missing or null CustomerID values with the string 'UNKNOWN'.
    """
    df_clean = df.copy()
    if 'CustomerID' in df_clean.columns:
        # Normalize various representations of null/empty to NaN
        df_clean['CustomerID'] = df_clean['CustomerID'].astype(str).replace(
            ['nan', 'NaN', 'None', '<NA>', ''], np.nan
        )
        df_clean['CustomerID'] = df_clean['CustomerID'].fillna('UNKNOWN')
        # Handle whitespace-only values
        df_clean['CustomerID'] = df_clean['CustomerID'].apply(
            lambda x: 'UNKNOWN' if str(x).strip() == '' else x
        )
    else:
        df_clean['CustomerID'] = 'UNKNOWN'
    return df_clean


def filter_valid_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out records where Quantity < 0 or UnitPrice <= 0.
    """
    df_clean = df.copy()
    df_clean['Quantity'] = pd.to_numeric(df_clean['Quantity'], errors='coerce').fillna(0).astype('int64')
    df_clean['UnitPrice'] = pd.to_numeric(df_clean['UnitPrice'], errors='coerce').fillna(0.0).astype('float64')
    return df_clean[(df_clean['Quantity'] >= 0) & (df_clean['UnitPrice'] > 0)]


def transform_silver_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies the Silver transformation business rules:
    1. Imputes null or missing CustomerID values with the string 'UNKNOWN'.
    2. Calculates a new column TotalPrice derived by Quantity * UnitPrice.
    3. Filters out rows where Quantity < 0 or UnitPrice <= 0.
    4. Groups by InvoiceDate (at the day level) and Country, aggregating to DailyTotalSales.
    5. Generates a CompositeKey (InvoiceDate_Country) for validation.
    """
    logger.info("Applying Silver transformation business logic...")
    
    # 1. Impute Customer IDs
    df_clean = impute_customer_ids(df)

    # 3. Filter valid records (Quantity >= 0 and UnitPrice > 0)
    df_clean = filter_valid_records(df_clean)

    # 2. Calculate TotalPrice
    df_clean['TotalPrice'] = df_clean['Quantity'] * df_clean['UnitPrice']

    # Parse InvoiceDate to YYYY-MM-DD
    df_clean['InvoiceDate_dt'] = pd.to_datetime(df_clean['InvoiceDate'], errors='coerce')
    df_clean = df_clean.dropna(subset=['InvoiceDate_dt'])
    df_clean['InvoiceDate'] = df_clean['InvoiceDate_dt'].dt.strftime('%Y-%m-%d')
    df_clean = df_clean.drop(columns=['InvoiceDate_dt'])

    # 4. Group by Day and Country, and aggregate TotalPrice
    grouped = df_clean.groupby(['InvoiceDate', 'Country'], as_index=False).agg(
        DailyTotalSales=('TotalPrice', 'sum')
    )

    # 5. Composite Key generation
    grouped['CompositeKey'] = grouped['InvoiceDate'] + "_" + grouped['Country']

    return grouped


def process_silver_layer(input_dir: str = "data/bronze", output_dir: str = "data/silver"):
    """
    Reads Bronze parquet data, runs transform, and writes partitioned Parquet files to Silver directory.
    """
    logger.info("Task silver_layer_transformation started.")
    try:
        if not os.path.exists(input_dir):
            error_msg = f"Bronze data directory not found at {input_dir}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        logger.info(f"Reading from Bronze layer directory: {input_dir}...")
        df = pd.read_parquet(input_dir)
        logger.info(f"Loaded {len(df)} rows from Bronze layer.")

        # Execute business logic transformations
        transformed_df = transform_silver_data(df)
        logger.info(f"Transformed and aggregated data row count: {len(transformed_df)}")

        # Partitioning columns (Year and Month) for output
        transformed_df['Year'] = pd.to_datetime(transformed_df['InvoiceDate']).dt.year
        transformed_df['Month'] = pd.to_datetime(transformed_df['InvoiceDate']).dt.month

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        logger.info(f"Writing Parquet files to Silver layer directory: {output_dir}...")
        transformed_df.to_parquet(
            output_dir,
            partition_cols=['Year', 'Month'],
            index=False,
            engine='pyarrow'
        )

        logger.info("Task silver_layer_transformation completed successfully.")
    except Exception as e:
        logger.error(f"Error during silver_layer_transformation: {str(e)}", exc_info=True)
        raise e
