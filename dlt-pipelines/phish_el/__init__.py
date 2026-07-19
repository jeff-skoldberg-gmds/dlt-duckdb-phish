"""
shows, setlists, songs, venues are called with full load in parallel.
users and user_attendance are called seperately so that user_attendance can be parallelized based on the full users response.
1000 user setlists come back in about 20 seconds.  Compared to "slow_way.py" which takes 2 minutes to fetch 1000 user setlists.
"""

import logging
import dlt
from dlt.sources.helpers import requests
from dlt.sources.rest_api import (
    rest_api_resources,
    RESTAPIConfig,
)

logger = logging.getLogger("dlt")


@dlt.source(name="phish_dot_net", parallelized=True)
def phish_dot_net_source(dev_limit: int = None):
    """
    dev_limit: when set, caps every resource to the first `dev_limit` items
    (i.e. the first page or two) for fast local iteration. Leave unset for a full load.
    Falls back to sources.phish_pipeline.dev_limit in config.toml if not passed explicitly.
    """
    # Get configuration from config.toml (per-key lookup — dict-style section access
    # only merges from secrets, not config.toml, so avoid dlt.config["sources.phish_pipeline"])
    base_url = dlt.config["sources.phish_pipeline.base_url"]
    resources = dlt.config["sources.phish_pipeline.resources"]
    resource_default_params = dlt.config.get(
        "sources.phish_pipeline.resource_defaults.endpoint.params", dict
    ) or {}

    if dev_limit is None:
        dev_limit = dlt.config.get("sources.phish_pipeline.dev_limit")

    if dev_limit:
        # these endpoints return everything in a single (unpaginated) response, so
        # dlt's add_limit() can't truncate them — it only checks after a full page/yield
        # is already extracted. Ask the API itself to return fewer rows instead.
        resource_default_params = {**resource_default_params, "limit": dev_limit}

    # Get API key from secrets
    api_key = dlt.secrets.get("sources.phish_pipeline.api_key")
    if not api_key:
        raise ValueError("API key not found in secrets.toml")

    basic_resources = [
        r for r in resources if r["name"] not in ["users", "user_attendance"]
    ]

    # Create the source configuration
    resource_config: RESTAPIConfig = {
        "client": {"base_url": base_url},
        "resources": basic_resources,
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "apikey": api_key,
                    **resource_default_params,
                }
            }
        },
    }

    yield from rest_api_resources(resource_config)

    enable_user_attendance = dlt.config.get(
        "sources.phish_pipeline.enable_user_attendance", bool
    )
    if not enable_user_attendance:
        logger.info(
            "users/user_attendance is disabled "
            "(sources.phish_pipeline.enable_user_attendance = false in config.toml) — "
            "see docs/user-attendance-incremental-options.md"
        )
        return

    @dlt.resource(write_disposition="replace", selected=True, parallelized=True)
    def users():
        logger.info("Fetching users...")
        response = requests.get(
            f"{base_url}users/uid/0.json", params={"apikey": api_key}
        )
        users_data = response.json()["data"]
        # this endpoint returns all users as one JSON list, so add_limit() (which counts
        # generator yields, not list elements) can't cap it — slice explicitly instead
        yield users_data[:dev_limit] if dev_limit else users_data

    @dlt.transformer(parallelized=True)
    def user_attendance(users_data):
        @dlt.defer
        def _get_attendance(user):
            response = requests.get(
                f"{base_url}attendance/uid/{user['uid']}.json",
                params={"apikey": api_key},
            )
            attendance = response.json()["data"]
            for entry in attendance:
                entry["user_id"] = user["uid"]
            return attendance

        for user in users_data:
            if int(user["uid"]) % 500 == 0:
                logger.info(f"Processing user {user['uid']}")
            if int(user["uid"]) % 100 == 0:
                logger.debug(f"Processing user {user['uid']}")
            yield _get_attendance(user)

    # Yield parallel user attendance pipeline (users() already slices to dev_limit above)
    yield users | user_attendance
