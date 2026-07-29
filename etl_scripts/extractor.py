import os
import logging
import requests

logger = logging.getLogger(__name__)

def download_raw_data(url: str, output_path: str = "data/raw/raw_data.csv"):
    """
    Downloads raw CSV data from the specified URL and saves it to the output path.
    Raises RuntimeError if the download fails.
    """
    logger.info("Task download_raw_data started.")
    logger.info(f"Source URL: {url}")
    logger.info(f"Destination Path: {output_path}")

    try:
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        logger.info("Sending GET request to fetch the dataset...")
        response = requests.get(url, stream=True, timeout=60)
        
        # Raise HTTPError if status code is not 200/OK
        if response.status_code != 200:
            error_msg = f"HTTP Error: Received status code {response.status_code} for URL {url}"
            logger.error(error_msg)
            raise requests.exceptions.HTTPError(error_msg)

        logger.info("Writing data chunks to file...")
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                if chunk:
                    f.write(chunk)
        
        logger.info(f"Task download_raw_data completed successfully. File saved at {output_path}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download raw data due to network/HTTP error: {str(e)}", exc_info=True)
        raise RuntimeError(f"Download failed: {str(e)}") from e
    except Exception as e:
        logger.error(f"Unexpected error occurred in download_raw_data: {str(e)}", exc_info=True)
        raise e
