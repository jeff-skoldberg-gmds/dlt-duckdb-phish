import logging.handlers
import dlt
from dlt.sources.rest_api import (
    check_connection,
)
from time import time
import logging
from phish_el import phish_dot_net_source, ship_to_mother_duck
from utilities.logging import logger

def run_dlt_pipeline(local_duckdb_name="new.db"):
    pipeline = dlt.pipeline(
        pipeline_name="phish_pipeline",
        destination=dlt.destinations.duckdb(local_duckdb_name),
        dataset_name="phish_data",
    )

    # Optional connection check
    can_connect, error_msg = check_connection(phish_dot_net_source(), "shows")
    if can_connect:
        logger.info("Connection to Phish.net API successful")
    else:
        raise ConnectionError(f"Failed to connect to Phish.net API: {error_msg}")

    # Run the pipeline
    load_info = pipeline.run(phish_dot_net_source(), loader_file_format="parquet")
    logger.info("\nPipeline load info:")
    logger.info(load_info)

def main():
    pipeline_started_at = time()
    run_dlt_pipeline()
    dlt_pipeline_completed_at = time()
    logger.info(f"DLT Pipeline completed in {(dlt_pipeline_completed_at - pipeline_started_at) / 60:.1f} minutes")

    ship_to_mother_duck(local_duckdb_name="new.db", remote_db_name="ph_land_test")
    
    logger.info(f"Total elapsed time: {(time() - pipeline_started_at) / 60:.1f} minutes")

if __name__ == "__main__":
    main()