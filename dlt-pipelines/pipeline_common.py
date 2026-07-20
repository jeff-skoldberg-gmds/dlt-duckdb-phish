import dlt


def get_destination(local_duckdb_name="duck.db"):
    # destination.name comes from dev.config.toml / prod.config.toml depending on
    # the active profile (WORKSPACE__PROFILE or `dlthub local profile use`).
    destination_name = dlt.config.get("destination.name", str) or "duckdb"
    if destination_name == "duckdb":
        return dlt.destinations.duckdb(local_duckdb_name)
    return destination_name  # e.g. "motherduck" — credentials resolved from secrets
