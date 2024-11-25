import dlt
import duckdb
from dlt.sources.rest_api import rest_api_source


def load_phish_user_attendance() -> None:
    # Get configuration from config.toml
    config = dlt.config["source.phish_pipeline"]

    # Get API key from secrets
    api_key = dlt.secrets.get("sources.phish_pipeline.api_key")
    if not api_key:
        raise ValueError("API key not found in secrets.toml")

    # Create pipeline with same name/database as original script
    pipeline = dlt.pipeline(
        pipeline_name="phish_pipeline", destination="duckdb", dataset_name="phish_data"
    )

    # Connect to the existing DuckDB database
    con = duckdb.connect("phish_pipeline.duckdb")

    # Get users from the database
    users = con.sql("SELECT uid FROM phish_data.users").fetchall()

    # For each user, create an attendance source
    for (user_id,) in users:  # Note: fetchall returns tuples
        attendance_config = {
            "client": {"base_url": config["base_url"]},
            "resources": [
                {
                    "name": "user_attendance",
                    "endpoint": {
                        "path": f"attendance/uid/{user_id}.json",
                        "params": {"apikey": api_key},
                    },
                }
            ],
        }

        attendance_source = rest_api_source(attendance_config)
        info = pipeline.run(attendance_source)
        print(f"Loaded attendance for user {info}")

    con.close()


if __name__ == "__main__":
    load_phish_user_attendance()
