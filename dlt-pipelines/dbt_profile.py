"""Generate a dbt profiles.yml from dlt's own destination config/secrets,
so the dbt Fusion CLI reuses the same duckdb file / Motherduck database as the
dlt pipelines instead of a hand-maintained profile.

Run directly: `uv run python dbt_profile.py` (writes for the active profile: dev -> duckdb,
prod -> motherduck). The dbt project's profile name (dbt_project.yml: `profile:`) is
`dbt_project`, so the generated profiles.yml uses that same key.

profiles.yml is written to a temp directory, NOT into the dbt project: on prod it
embeds the motherduck token, and the dbt project lives inside the workspace tree that
`dlthub deploy` tarballs — a generated file there could ship secrets. Pass the returned
directory to dbt via --profiles-dir.
"""

import os
import tempfile

import dlt
import yaml

from pipeline_common import get_duckdb_path

DBT_PROFILE_NAME = "dbt_project"  # must match dbt-pipelines/dbt_project.yml `profile:`
DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "dbt-pipelines")
PROFILES_DIR = os.path.join(tempfile.gettempdir(), "phish_dbt_profiles")
PROFILES_PATH = os.path.join(PROFILES_DIR, "profiles.yml")


def build_profile(local_duckdb_name="phish.duckdb"):
    destination_name = dlt.config.get("destination.name", str) or "duckdb"

    if destination_name == "duckdb":
        target_name = "dev"
        # Same path pipeline_common.get_destination() uses — one function, no
        # separate resolution logic to drift out of sync.
        output = {
            "type": "duckdb",
            "path": get_duckdb_path(local_duckdb_name),
            "schema": "phish_data",
        }
    else:
        target_name = "prod"
        database = dlt.config["destination.motherduck.credentials.database"]
        token = dlt.secrets["destination.motherduck.credentials.password"]
        output = {
            "type": "duckdb",
            "path": f"md:{database}?motherduck_token={token}",
            "schema": "phish_data",
        }

    profile = {
        DBT_PROFILE_NAME: {
            "target": target_name,
            "outputs": {target_name: {**output, "threads": 4}},
        }
    }

    os.makedirs(PROFILES_DIR, exist_ok=True)
    with open(PROFILES_PATH, "w") as f:
        yaml.safe_dump(profile, f, sort_keys=False)

    return PROFILES_DIR, target_name


if __name__ == "__main__":
    profiles_dir, target = build_profile()
    print(f"Wrote {os.path.join(profiles_dir, 'profiles.yml')} (target: {target})")
