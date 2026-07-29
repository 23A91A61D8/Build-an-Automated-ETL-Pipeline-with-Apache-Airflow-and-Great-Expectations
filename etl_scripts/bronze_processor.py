import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def process_bronze_layer(input_path: str = "data/raw/raw_data.csv", output_dir: str = "data/bronze"):
    """
    Reads the raw CSV file, converts columns to expected types,
    and writes it to the Bronze layer in Parquet format partitioned by Year and Month.
    """
    logger.info("Task bronze_layer_processing started.")
    try:
        if not os.path.exists(input_path):
            error_msg = f"Raw data file not found at {input_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        logger.info(f"Reading raw CSV data from {input_path}...")
        
        # Try UTF-8 first, fallback to ISO-8859-1 which is common for Online Retail dataset
        try:
            df = pd.read_csv(input_path, encoding="utf-8")
        except UnicodeDecodeError:
            logger.info("UTF-8 decoding failed, trying ISO-8859-1...")
            df = pd.read_csv(input_path, encoding="ISO-8859-1")

        logger.info(f"Successfully loaded raw CSV data. Row count: {len(df)}")

        # Convert InvoiceDate to datetime to extract Year and Month partitioning columns
        logger.info("Parsing InvoiceDate column to datetime...")
        df['InvoiceDate_dt'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
        
        # Handle cases where InvoiceDate fails to parse (NaT)
        nat_count = df['InvoiceDate_dt'].isna().sum()
        if nat_count > 0:
            logger.warning(f"Detected {nat_count} rows with missing or unparseable InvoiceDate. Defaulting to 1970-01-01.")
            df['InvoiceDate_dt'] = df['InvoiceDate_dt'].fillna(pd.Timestamp('1970-01-01'))

        # Add physical partitioning columns (Year and Month)
        df['Year'] = df['InvoiceDate_dt'].dt.year
        df['Month'] = df['InvoiceDate_dt'].dt.month

        # Remove the temporary datetime helper column
        df = df.drop(columns=['InvoiceDate_dt'])

        # Enforce column datatypes to match downstream expectations
        logger.info("Enforcing datatypes (Quantity -> int64, UnitPrice -> float64)...")
        # Ensure Quantity is int64, UnitPrice is float64
        # We use standard pd.to_numeric first, and fill NaNs to avoid conversion errors
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype('int64')
        df['UnitPrice'] = pd.to_numeric(df['UnitPrice'], errors='coerce').fillna(0.0).astype('float64')

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        logger.info(f"Writing Parquet files to Bronze layer directory: {output_dir}...")
        # Write to partitioned parquet using pyarrow
        df.to_parquet(
            output_dir,
            partition_cols=['Year', 'Month'],
            index=False,
            engine='pyarrow'
        )

        logger.info("Task bronze_layer_processing completed successfully.")
    except Exception as e:
        logger.error(f"Error during bronze_layer_processing: {str(e)}", exc_info=True)
        raise e
