import argparse
import logging
import dlt
from time import time
from phish_el import phish_dot_net_source

logger = logging.getLogger("dlt")


def get_destination(local_duckdb_name="duck.db"):
    # destination.name comes from dev.config.toml / prod.config.toml depending on
    # the active profile (WORKSPACE__PROFILE or `dlthub local profile use`).
    destination_name = dlt.config.get("destination.name", str) or "duckdb"
    if destination_name == "duckdb":
        return dlt.destinations.duckdb(local_duckdb_name)
    return destination_name  # e.g. "motherduck" — credentials resolved from secrets


def run_dlt_pipeline(local_duckdb_name="duck.db", target_schema_name="phish", limit=None):
    logger.info("Starting DLT pipeline")
    pipeline = dlt.pipeline(
        pipeline_name="phish_pipeline",
        destination=get_destination(local_duckdb_name),
        dataset_name="phish_data",
    )

    # Run the pipeline
    load_info = pipeline.run(
        phish_dot_net_source(dev_limit=limit), loader_file_format="parquet"
    )
    logger.info("\nPipeline load info:")
    logger.info(load_info)


def main(local_duckdb_name="duck.db", target_schema_name="phish", limit=None):
    pipeline_started_at = time()
    logger.info("Main function started at %s", pipeline_started_at)
    run_dlt_pipeline(
        local_duckdb_name=local_duckdb_name,
        target_schema_name=target_schema_name,
        limit=limit,
    )
    logger.info(
        f"Total elapsed time: {(time() - pipeline_started_at) / 60:.1f} minutes"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap every resource to the first N items (fast dev run), e.g. --limit 1",
    )
    args = parser.parse_args()

    main(
        local_duckdb_name="duck.db",
        target_schema_name="phish",
        limit=args.limit,
    )
