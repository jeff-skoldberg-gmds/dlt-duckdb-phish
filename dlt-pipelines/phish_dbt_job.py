import subprocess

# dlt's configured logger is named after the workspace (e.g. "concerts"), NOT "dlt" —
# logging.getLogger("dlt") messages are silently dropped. This module logs through it.
from dlt.common import logger
from dlt.hub import run

from dbt_profile import DBT_PROJECT_DIR, build_profile
from phish_core_pipeline import run_dlt_pipeline as run_core_pipeline


def _dbt(args, profiles_dir):
    cmd = [
        "dbt",
        *args,
        "--project-dir",
        DBT_PROJECT_DIR,
        "--profiles-dir",
        profiles_dir,
    ]
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"{' '.join(cmd[:2])} failed with exit code {result.returncode}")


# the dbt Fusion CLI is pinned in requirements.txt — the runtime builds every job env
# from that file (a pyproject dependency group can't work here: the repo-root
# pyproject.toml sits outside the deployed workspace tree)
@run.pipeline("phish_dbt_transform", trigger=run_core_pipeline.success)
def run_dbt_transform(local_duckdb_name="phish.duckdb", command="build"):
    logger.info("Starting dbt transform for phish_core_pipeline")

    profiles_dir, target = build_profile(local_duckdb_name=local_duckdb_name)
    logger.info("Generated profiles.yml in %s (target: %s)", profiles_dir, target)

    # dbt_packages/ is generated and excluded from the deploy tarball, so install
    # packages (dbt_utils, pinned by package-lock.yml) before building
    _dbt(["deps"], profiles_dir)
    _dbt([command, "--target", target], profiles_dir)


if __name__ == "__main__":
    run_dbt_transform()
