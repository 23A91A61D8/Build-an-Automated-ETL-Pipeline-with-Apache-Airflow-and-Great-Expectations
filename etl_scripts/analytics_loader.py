import os
import logging
import sqlite3
import pandas as pd

logger = logging.getLogger(__name__)

def load_to_analytical_store(input_dir: str = "data/silver", db_path: str = "data/analytics.db"):
    """
    Reads Silver Parquet files and loads the data into SQLite analytics table daily_country_sales.
    Uses a 'replace' strategy to ensure idempotent execution.
    """
    logger.info("Task load_to_analytical_store started.")
    try:
        if not os.path.exists(input_dir):
            error_msg = f"Silver data directory not found: {input_dir}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        logger.info(f"Reading from Silver layer directory: {input_dir}...")
        df = pd.read_parquet(input_dir)
        logger.info(f"Loaded {len(df)} rows from Silver layer.")

        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        logger.info(f"Connecting to SQLite database at {db_path}...")
        conn = sqlite3.connect(db_path)

        logger.info("Writing table 'daily_country_sales' with 'replace' strategy for idempotency...")
        df.to_sql(
            "daily_country_sales",
            conn,
            if_exists="replace",
            index=False
        )

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM daily_country_sales")
        count = cursor.fetchone()[0]
        logger.info(f"Verification: Successfully verified {count} rows in table 'daily_country_sales'.")

        conn.close()
        logger.info("Task load_to_analytical_store completed successfully.")
    except Exception as e:
        logger.error(f"Error during load_to_analytical_store: {str(e)}", exc_info=True)
        raise e
