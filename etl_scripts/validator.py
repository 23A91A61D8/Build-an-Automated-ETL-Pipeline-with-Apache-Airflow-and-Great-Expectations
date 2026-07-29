import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def validate_layer(data_dir: str, checkpoint_name: str, asset_name: str):
    """
    Loads Parquet data, executes a Great Expectations checkpoint,
    and raises an error if validation fails.
    """
    logger.info(f"Great Expectations validation task started for: {checkpoint_name}")
    try:
        # 1. Verify and load Parquet files
        if not os.path.exists(data_dir):
            error_msg = f"Data directory not found for validation: {data_dir}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        logger.info(f"Loading files from {data_dir}...")
        df = pd.read_parquet(data_dir)
        logger.info(f"Loaded {len(df)} rows for validation.")

        # 2. Get Great Expectations context
        # Move imports inside to allow module import on environments without great_expectations
        import great_expectations as ge
        from great_expectations.core.batch import RuntimeBatchRequest

        # Locate the context directory
        context_dir = os.path.abspath("great_expectations")
        if not os.path.exists(os.path.join(context_dir, "great_expectations.yml")):
            context_dir = "/opt/airflow/great_expectations"
        
        logger.info(f"Initializing Great Expectations context from: {context_dir}")
        context = ge.get_context(context_root_dir=context_dir)

        # 3. Create Runtime Batch Request
        batch_request = RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name=asset_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": f"{asset_name}_run"}
        )

        # 4. Run Checkpoint
        logger.info(f"Executing Great Expectations checkpoint '{checkpoint_name}'...")
        results = context.run_checkpoint(
            checkpoint_name=checkpoint_name,
            validations=[{"batch_request": batch_request}]
        )

        # 5. Check validation results
        success = results.was_successful()
        logger.info(f"Validation checkpoint finished. Success status: {success}")

        # Log statistics of expectations
        for validation_result in results.list_validation_results():
            suite_name = validation_result.meta.get("expectation_suite_name")
            stats = validation_result.statistics
            logger.info(
                f"Validation Suite: {suite_name} | "
                f"Evaluated: {stats['evaluated_expectations']} | "
                f"Successful: {stats['successful_expectations']} | "
                f"Failed: {stats['unsuccessful_expectations']}"
            )

        # Trigger Circuit Breaker
        if not success:
            error_msg = f"CRITICAL: Great Expectations validation failed for checkpoint '{checkpoint_name}'!"
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info(f"Validation checkpoint '{checkpoint_name}' passed successfully.")
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}", exc_info=True)
        raise e


def validate_bronze(data_dir: str = "data/bronze"):
    """Validates Bronze layer data."""
    validate_layer(data_dir, "bronze_checkpoint", "bronze_dataset")


def validate_silver(data_dir: str = "data/silver"):
    """Validates Silver layer data."""
    validate_layer(data_dir, "silver_checkpoint", "silver_dataset")
