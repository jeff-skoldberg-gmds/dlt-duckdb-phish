
import logging.handlers
import dlt
from dlt.sources.helpers import requests
from dlt.sources.rest_api import (
    rest_api_source,
    rest_api_resources,
    check_connection,
    RESTAPIConfig,
)
import os
from time import time
import logging
from phish_el import phish_dot_net_source


logger = logging.getLogger('dlt')
# Create a timed rotating file handler
file_handler = logging.handlers.TimedRotatingFileHandler(
    filename='pipeline.log',
    when='midnight',
    interval=1,
    backupCount=2
)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)
logger.info("Starting phish pipeline")


os.chdir(os.path.dirname(os.path.abspath(__file__)))



def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="phish_pipeline",
        destination=dlt.destinations.duckdb('new.db'),
        dataset_name="phish_data",
    )

    # Optional connection check
    can_connect, error_msg = check_connection(phish_dot_net_source(), "shows")
    if not can_connect:
        raise ConnectionError(f"Failed to connect to Phish.net API: {error_msg}")

    # Run the pipeline
    load_info = pipeline.run(phish_dot_net_source(), loader_file_format="parquet")
    logger.info(f"\nPipeline load info:")
    logger.info(load_info)


if __name__ == "__main__":
    start_time = time()
    run_pipeline()
    end_time = time()
    # log time in minutes rounded to 1 decimal place
    logger.info(f"Pipeline completed in {(end_time - start_time) / 60:.1f} minutes")
    import duckdb
    logger.info("Shipping to MotherDuck")
    local_con = duckdb.connect("new.db")
    local_con.sql("ATTACH 'md:'")
    local_con.sql("CREATE or replace DATABASE ph_land_test FROM CURRENT_DATABASE()")
    logger.info("Shipped!")
    logger.info(f"Total elapsed time: {(time() - start_time) / 60:.1f} minutes")