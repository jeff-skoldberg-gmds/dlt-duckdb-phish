"""
shows, setlists, songs, venues, artists are called with full load in parallel (phish_core_source).
users and user_attendance are a separate source (phish_user_source) so that user_attendance
can be parallelized based on the full users response, and so the two can be deployed,
scheduled, and rate-limited independently — see docs/user-attendance-incremental-options.md.
1000 user setlists come back in about 20 seconds.  Compared to "slow_way.py" which takes 2 minutes to fetch 1000 user setlists.
"""

import dlt
from dlt.common import pendulum
# dlt's configured logger is named after the workspace (e.g. "concerts"), NOT "dlt" —
# logging.getLogger("dlt") messages are silently dropped. This module logs through it.
from dlt.common import logger
from dlt.sources.helpers import requests
from dlt.sources.rest_api import (
    rest_api_resources,
    RESTAPIConfig,
)

# state["sources"][SOURCE_STATE_KEY][SWEEP_STATE_KEY] holds full-sweep progress for
# user_attendance. It sits next to (not inside) the "resources" subtree, so it can
# never collide with the daily incremental cursor at resources.users.incremental.uid.
SOURCE_STATE_KEY = "phish_dot_net_users"  # == the @dlt.source/schema name below
SWEEP_STATE_KEY = "attendance_sweep"


def _get_base_url():
    return dlt.config["sources.phish_pipeline.base_url"]


def _get_api_key():
    api_key = dlt.secrets.get("sources.phish_pipeline.api_key")
    if not api_key:
        raise ValueError("API key not found in secrets.toml")
    return api_key


def fetch_all_users(dev_limit: int = None):
    """All phish.net users in one call, uid cast to int, sorted ascending by uid.

    Also used directly by the chunked full-sweep driver in phish_user_pipeline.py,
    which slices this list into chunks and passes them back via users_chunk.
    """
    logger.info("Fetching users...")
    response = requests.get(
        f"{_get_base_url()}users/uid/0.json", params={"apikey": _get_api_key()}
    )
    users_data = response.json()["data"]
    # attendance record's own "uid" field is the user id — cast so uid comparisons
    # and sort order are numeric (uid comes back as a string, and lexicographic
    # ordering breaks once digit counts differ, e.g. "9" > "10")
    for u in users_data:
        u["uid"] = int(u["uid"])
    # sorted so chunk boundaries in the full-sweep driver are deterministic
    users_data.sort(key=lambda u: u["uid"])
    # this endpoint returns all users as one JSON list, so add_limit() (which counts
    # generator yields, not list elements) can't cap it — slice explicitly instead
    return users_data[:dev_limit] if dev_limit else users_data


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
            # every endpoint returns a full snapshot, so each run replaces the
            # table — the default "append" duplicates rows across runs
            "write_disposition": "replace",
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
def phish_user_source(
    dev_limit: int = None,
    full_sweep_attendance: bool = None,
    users_chunk: list = None,
    sweep_state: dict = None,
):
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

    users_chunk + sweep_state: chunked full-sweep mode, driven by phish_user_pipeline.py.
    When users_chunk is not None, no user list is fetched here — attendance is pulled for
    exactly those user records, and sweep_state is persisted to
    dlt state (sources.phish_dot_net_users.attendance_sweep) by the sweep_log resource,
    so the sweep watermark commits atomically with this chunk's load package.
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

    # attendance has no timestamp/cursor field, so it's re-fetched per-user via merge
    # rather than truly incremental — see docs/user-attendance-incremental-options.md.
    # merge_key="uid" deletes ALL existing rows for each fetched uid before insert, so a
    # re-fetch fully replaces that user's attendance (handles un-marked shows). Caveat:
    # a user whose fetch returns zero rows leaves stale rows behind — deletes are driven
    # by staged rows.
    @dlt.transformer(write_disposition="merge", primary_key=("uid", "showid"), merge_key="uid", parallelized=True)
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

    if users_chunk is not None:
        # chunked full sweep: the driver (phish_user_pipeline.py) fetched the user list,
        # picked this chunk, and computed sweep_state. No incremental cursor is bound
        # here, so the daily-mode cursor at resources.users.incremental.uid is untouched.
        @dlt.resource(name="users", write_disposition="merge", primary_key="uid", selected=True, parallelized=True)
        def users():
            if users_chunk:
                yield users_chunk

        @dlt.resource(name="sweep_log", write_disposition="append")
        def sweep_log():
            # resource-function bodies are the one supported place to write dlt state;
            # it travels in this chunk's load package and rolls back if extract fails
            dlt.current.source_state()[SWEEP_STATE_KEY] = sweep_state
            yield {
                "sweep_started_at": sweep_state["sweep_started_at"],
                "chunk_start_uid": users_chunk[0]["uid"] if users_chunk else None,
                "chunk_end_uid": users_chunk[-1]["uid"] if users_chunk else None,
                "user_count": len(users_chunk),
                "sweep_completed": sweep_state.get("completed_at") is not None,
                "recorded_at": pendulum.now("UTC").isoformat(),
            }

        # yield the resource AND the pipe so the user records themselves are merged
        # into the users table chunk-by-chunk (extracted once, fanned out to both)
        yield users
        yield users | user_attendance
        yield sweep_log
        return

    if full_sweep_attendance:
        # ignores/does not touch the daily incremental cursor below — pulls attendance
        # for every user, on a slower cadence, as the safety net for backfilled shows.
        # NOTE: prefer the chunked driver (phish_user_pipeline.py --full-sweep-attendance),
        # which runs this sweep in resumable chunks instead of one giant run.
        @dlt.resource(name="users", write_disposition="merge", primary_key="uid", selected=True, parallelized=True)
        def users():
            yield fetch_all_users(dev_limit)
    else:
        @dlt.resource(name="users", write_disposition="merge", primary_key="uid", selected=True, parallelized=True)
        def users(uid=dlt.sources.incremental("uid", initial_value=0)):
            yield fetch_all_users(dev_limit)

    # Yield parallel user attendance pipeline (users() already slices to dev_limit above)
    yield users | user_attendance
