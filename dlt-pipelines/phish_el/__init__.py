"""
shows, setlists, songs, venues are called with full load in parallel (phish_core_source).
users and user_attendance are a separate source (phish_user_source) so that user_attendance
can be parallelized based on the full users response, and so the two can be deployed,
scheduled, and rate-limited independently — see docs/user-attendance-incremental-options.md.
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


def _get_base_url():
    return dlt.config["sources.phish_pipeline.base_url"]


def _get_api_key():
    api_key = dlt.secrets.get("sources.phish_pipeline.api_key")
    if not api_key:
        raise ValueError("API key not found in secrets.toml")
    return api_key


@dlt.source(name="phish_dot_net_core", parallelized=True)
def phish_core_source(dev_limit: int = None):
    """
    shows, setlists, songs, venues.

    dev_limit: when set, caps every resource to the first `dev_limit` items
    (i.e. the first page or two) for fast local iteration. Leave unset for a full load.
    Falls back to sources.phish_pipeline.dev_limit in config.toml if not passed explicitly.
    """
    base_url = _get_base_url()
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

    resource_config: RESTAPIConfig = {
        "client": {"base_url": base_url},
        "resources": resources,
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "apikey": _get_api_key(),
                    **resource_default_params,
                }
            }
        },
    }

    yield from rest_api_resources(resource_config)


@dlt.source(name="phish_dot_net_users", parallelized=True)
def phish_user_source(dev_limit: int = None, full_sweep_attendance: bool = None):
    """
    users + user_attendance.

    dev_limit: see phish_core_source. Falls back to sources.phish_pipeline.dev_limit.

    full_sweep_attendance: see docs/user-attendance-incremental-options.md (Option A).
    Falls back to sources.phish_pipeline.full_sweep_attendance in config.toml if not passed
    explicitly (default False if unset there too).
    False (default, daily cadence): `users` is pulled incrementally by uid, so
    `user_attendance` is only fetched for newly-created accounts.
    True (slower cadence, e.g. weekly cron): re-pulls attendance for ALL users to catch
    backfilled attendance on existing accounts. Both modes merge on (uid, showid), so
    re-pulls are idempotent.
    """
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

    base_url = _get_base_url()
    api_key = _get_api_key()

    if dev_limit is None:
        dev_limit = dlt.config.get("sources.phish_pipeline.dev_limit")

    if full_sweep_attendance is None:
        full_sweep_attendance = (
            dlt.config.get("sources.phish_pipeline.full_sweep_attendance", bool) or False
        )

    def _fetch_users():
        logger.info("Fetching users...")
        response = requests.get(
            f"{base_url}users/uid/0.json", params={"apikey": api_key}
        )
        users_data = response.json()["data"]
        # attendance record's own "uid" field is the user id — cast so the incremental
        # cursor compares numerically (uid comes back as a string, and lexicographic
        # ordering breaks once digit counts differ, e.g. "9" > "10")
        for u in users_data:
            u["uid"] = int(u["uid"])
        # this endpoint returns all users as one JSON list, so add_limit() (which counts
        # generator yields, not list elements) can't cap it — slice explicitly instead
        return users_data[:dev_limit] if dev_limit else users_data

    # attendance has no timestamp/cursor field, so it's re-fetched per-user via merge
    # rather than truly incremental — see docs/user-attendance-incremental-options.md
    @dlt.transformer(write_disposition="merge", primary_key=("uid", "showid"), parallelized=True)
    def user_attendance(users_data):
        @dlt.defer
        def _get_attendance(user):
            response = requests.get(
                f"{base_url}attendance/uid/{user['uid']}.json",
                params={"apikey": api_key},
            )
            return response.json()["data"]

        for user in users_data:
            if int(user["uid"]) % 500 == 0:
                logger.info(f"Processing user {user['uid']}")
            if int(user["uid"]) % 100 == 0:
                logger.debug(f"Processing user {user['uid']}")
            yield _get_attendance(user)

    if full_sweep_attendance:
        # ignores/does not touch the daily incremental cursor below — pulls attendance
        # for every user, on a slower cadence, as the safety net for backfilled shows
        @dlt.resource(name="users", write_disposition="merge", primary_key="uid", selected=True, parallelized=True)
        def users():
            yield _fetch_users()
    else:
        @dlt.resource(name="users", write_disposition="merge", primary_key="uid", selected=True, parallelized=True)
        def users(uid=dlt.sources.incremental("uid", initial_value=0)):
            yield _fetch_users()

    # Yield parallel user attendance pipeline (users() already slices to dev_limit above)
    yield users | user_attendance
