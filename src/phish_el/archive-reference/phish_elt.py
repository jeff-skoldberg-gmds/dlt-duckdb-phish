import dlt
from dlt.sources.rest_api import (
    rest_api_resources,
    check_connection,
    RESTAPIConfig,
)
import os
from time import time

# change cwd to "this dir", one level up
os.chdir(os.path.dirname(os.path.abspath(__file__)))


@dlt.source(name="phish_dot_net")
def phish_dot_net_source():
    # Get configuration from config.toml
    config = dlt.config["source.phish_pipeline"]

    # Get API key from secrets
    api_key = dlt.secrets.get("sources.phish_pipeline.api_key")
    if not api_key:
        raise ValueError("API key not found in secrets.toml")

    main_config = config["resources"]

    # Create the source configuration
    resource_config: RESTAPIConfig = {
        "client": {"base_url": config["base_url"]},
        "resources": main_config,
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


def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="phish_pipeline",
        destination="duckdb",
        dataset_name="phish_data",
    )

    # Optional connection check
    can_connect, error_msg = check_connection(phish_dot_net_source(), "shows")
    if not can_connect:
        raise ConnectionError(f"Failed to connect to Phish.net API: {error_msg}")

    # Run the pipeline
    load_info = pipeline.run(phish_dot_net_source())
    print("\nPipeline load info:")
    print(load_info)


if __name__ == "__main__":
    start_time = time()
    run_pipeline()
    end_time = time()
    # log time in minutes rounded to 1 decimal place
    print(f"Pipeline completed in {(end_time - start_time) / 60:.1f} minutes")
