import os

import dlt

# Explicit, container-friendly local storage for the dev duckdb file. Deliberately
# NOT relying on dltHub's profile-scoped ".dlt/data/<profile>/" resolution — that path
# depends on cwd and the active profile in ways that are surprising to anything outside
# dlt itself (e.g. dbt). Every consumer (pipelines, dbt_profile.py) resolves the same
# file via get_duckdb_path() below, so there is exactly one place this can drift.
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def get_duckdb_path(local_duckdb_name="phish.duckdb"):
    os.makedirs(DATA_DIR, exist_ok=True)
    return os.path.join(DATA_DIR, local_duckdb_name)


def get_destination(local_duckdb_name="phish.duckdb"):
    # destination.name comes from dev.config.toml / prod.config.toml depending on
    # the active profile (WORKSPACE__PROFILE or `dlthub local profile use`).
    destination_name = dlt.config.get("destination.name", str) or "duckdb"
    if destination_name == "duckdb":
        return dlt.destinations.duckdb(get_duckdb_path(local_duckdb_name))
    return destination_name  # e.g. "motherduck" — credentials resolved from secrets
