import argparse
import logging
import dlt
from dlt.hub import run
from time import time
from phish_el import phish_core_source
from pipeline_common import get_destination

logger = logging.getLogger("dlt")


@run.pipeline("phish_core_pipeline")
def run_dlt_pipeline(local_duckdb_name="duck.db", target_schema_name="phish", limit=None):
    logger.info("Starting DLT core pipeline")
    pipeline = dlt.pipeline(
        pipeline_name="phish_core_pipeline",
        destination=get_destination(local_duckdb_name),
        dataset_name="phish_data",
    )

    load_info = pipeline.run(
        phish_core_source(dev_limit=limit), loader_file_format="parquet"
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
