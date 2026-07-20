import logging
import subprocess

from dlt.hub import run

from dbt_profile import DBT_PROJECT_DIR, build_profile
from phish_core_pipeline import run_dlt_pipeline as run_core_pipeline

logger = logging.getLogger("dlt")


@run.pipeline("phish_dbt_transform", trigger=run_core_pipeline.success)
def run_dbt_transform(local_duckdb_name="phish.duckdb", command="build"):
    logger.info("Starting dbt transform for phish_core_pipeline")

    profiles_path, target = build_profile(local_duckdb_name=local_duckdb_name)
    logger.info("Generated %s (target: %s)", profiles_path, target)

    result = subprocess.run(
        [
            "dbt",
            command,
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROJECT_DIR,
            "--target",
            target,
        ],
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt {command} failed with exit code {result.returncode}")


if __name__ == "__main__":
    run_dbt_transform()
