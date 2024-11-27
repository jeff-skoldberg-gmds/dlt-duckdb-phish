import dlt
from time import time
from phish_el import phish_dot_net_source, ship_to_mother_duck
from utilities.logging import logger


def run_dlt_pipeline(local_duckdb_name="duck.db", target_schema_name="phish"):
    logger.info("Starting DLT pipeline")
    pipeline = dlt.pipeline(
        pipeline_name="phish_pipeline",
        destination=dlt.destinations.duckdb(local_duckdb_name),
        dataset_name="phish_data",
    )

    # Run the pipeline
    load_info = pipeline.run(phish_dot_net_source(), loader_file_format="parquet")
    logger.info("\nPipeline load info:")
    logger.info(load_info)


def main(local_duckdb_name="duck.db", remote_db_name="raw", target_schema_name="phish"):
    pipeline_started_at = time()
    logger.info("Main function started at %s", pipeline_started_at)
    run_dlt_pipeline(
        local_duckdb_name=local_duckdb_name, target_schema_name=target_schema_name
    )
    dlt_pipeline_completed_at = time()
    logger.info(
        f"DLT Pipeline completed in {(dlt_pipeline_completed_at - pipeline_started_at) / 60:.1f} minutes"
    )

    ship_to_mother_duck(local_duckdb_name="new.db", remote_db_name="ph_land_test")

    logger.info(
        f"Total elapsed time: {(time() - pipeline_started_at) / 60:.1f} minutes"
    )


if __name__ == "__main__":
    local_duckdb_name = "duck.db"
    target_schema_name = "phish"
    remote_db_name = "raw"
    main(
        local_duckdb_name=local_duckdb_name,
        remote_db_name=remote_db_name,
        target_schema_name=target_schema_name,
    )
