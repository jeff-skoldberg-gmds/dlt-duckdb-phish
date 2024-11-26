import logging.handlers
import dlt
from dlt.sources.helpers import requests
from dlt.sources.rest_api import (
    rest_api_resources,
    check_connection,
    RESTAPIConfig,
)
import os
from time import time
import logging

logger = logging.getLogger("dlt")
# Create a timed rotating file handler
file_handler = logging.handlers.TimedRotatingFileHandler(
    filename="pipeline.log", when="midnight", interval=1, backupCount=2
)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)
logger.info("Starting phish pipeline")


os.chdir(os.path.dirname(os.path.abspath(__file__)))


@dlt.source(name="phish_dot_net", parallelized=True)
def phish_dot_net_source():
    # Get configuration from config.toml
    config = dlt.config["source.phish_pipeline"]

    # Get API key from secrets
    api_key = dlt.secrets.get("sources.phish_pipeline.api_key")
    if not api_key:
        raise ValueError("API key not found in secrets.toml")

    basic_resources = [
        r for r in config["resources"] if r["name"] not in ["users", "user_attendance"]
    ]

    # Create the source configuration
    resource_config: RESTAPIConfig = {
        "client": {"base_url": config["base_url"]},
        "resources": basic_resources,
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "apikey": api_key,
                    **config.get("resource_defaults", {})
                    .get("endpoint", {})
                    .get("params", {}),
                }
            }
        },
    }

    yield from rest_api_resources(resource_config)

    @dlt.resource(write_disposition="replace", selected=True, parallelized=True)
    def users():
        logger.info("Fetching users...")
        response = requests.get(
            f"{config['base_url']}users/uid/0.json", params={"apikey": api_key}
        )
        yield response.json()["data"]

    @dlt.transformer(parallelized=True)
    def user_attendance(users_data):
        @dlt.defer
        def _get_attendance(user):
            response = requests.get(
                f"{config['base_url']}attendance/uid/{user['uid']}.json",
                params={"apikey": api_key},
            )
            attendance = response.json()["data"]
            for entry in attendance:
                entry["user_id"] = user["uid"]
            return attendance

        for user in users_data:
            if int(user["uid"]) % 100 == 0:
                logger.debug(f"Processing user {user['uid']}")
            yield _get_attendance(user)

    # Yield parallel user attendance pipeline
    yield users | user_attendance


def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="phish_pipeline",
        destination=dlt.destinations.duckdb("new.db"),
        dataset_name="phish_data",
    )

    # Optional connection check
    can_connect, error_msg = check_connection(phish_dot_net_source(), "shows")
    if not can_connect:
        raise ConnectionError(f"Failed to connect to Phish.net API: {error_msg}")

    # Run the pipeline
    load_info = pipeline.run(phish_dot_net_source(), loader_file_format="parquet")
    logger.info("\nPipeline load info:")
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
