import argparse
import dlt
from dlt.common import pendulum
# dlt's configured logger is named after the workspace (e.g. "concerts"), NOT "dlt" —
# logging.getLogger("dlt") messages are silently dropped. This module logs through it.
from dlt.common import logger
from dlt.hub import run
from time import time
from phish_el import (
    phish_user_source,
    fetch_all_users,
    SOURCE_STATE_KEY,
    SWEEP_STATE_KEY,
)
from pipeline_common import get_destination


def _build_pipeline(local_duckdb_name):
    return dlt.pipeline(
        pipeline_name="phish_user_pipeline",
        destination=get_destination(local_duckdb_name),
        dataset_name="phish_data",
    )


def _read_sweep_state(pipeline) -> dict:
    # pipeline.state re-reads state.json on every access, so this sees exactly what
    # the last completed chunk run committed
    return (
        pipeline.state.get("sources", {})
        .get(SOURCE_STATE_KEY, {})
        .get(SWEEP_STATE_KEY, {})
        or {}
    )


def _run_chunked_sweep(pipeline, chunk_size, time_budget_minutes, max_chunks, dev_limit):
    """Full attendance sweep as a series of small pipeline.run() calls.

    Each chunk's load package carries both its attendance data and the updated sweep
    watermark (written inside the source), so progress is durable per chunk: a killed
    run resumes from the last completed chunk instead of starting over. The time budget
    lets a run end cleanly before the platform's 2-hour job limit; the next invocation
    picks up where this one left off.
    """
    started = time()
    budget_s = (time_budget_minutes or 0) * 60  # 0 = run to completion

    # a run killed mid-load leaves a pending package holding the chunk's data AND its
    # sweep state doc; pipeline.run() with pending data loads it and returns WITHOUT
    # extracting anything new, so flush before the loop or chunk 1 would be ignored
    if pipeline.has_pending_data:
        logger.info("Flushing pending load package from a previous interrupted run")
        pipeline.run()

    # an ephemeral runtime box has no local state.json, and the driver reads the sweep
    # watermark BEFORE the first pipeline.run() (which is when dlt would restore state
    # from the destination) — without this sync every platform run would restart the
    # sweep from scratch. No-op when local state is already current.
    try:
        pipeline.sync_destination()
    except Exception as e:
        logger.warning("Could not sync state from destination, using local state: %s", e)

    users = fetch_all_users(dev_limit=dev_limit)
    if not users:
        logger.info("User list is empty — nothing to sweep")
        return

    sweep = _read_sweep_state(pipeline)
    if sweep.get("watermark_uid") is None:
        # never ran, or the previous sweep completed — start a fresh sweep
        sweep = {
            "sweep_started_at": pendulum.now("UTC").isoformat(),
            "watermark_uid": -1,
            "completed_at": None,
            "chunks_completed": 0,
            "last_sweep_completed_at": sweep.get("last_sweep_completed_at"),
        }
        logger.info("Starting new attendance sweep at %s", sweep["sweep_started_at"])
    else:
        logger.info(
            "Resuming sweep started %s from uid > %s (%d chunks already done)",
            sweep["sweep_started_at"],
            sweep["watermark_uid"],
            sweep.get("chunks_completed", 0),
        )

    chunks_this_run = 0
    last_chunk_s = 0.0
    while True:
        if max_chunks and chunks_this_run >= max_chunks:
            logger.info("Stopping: --max-chunks %d reached, sweep still open", max_chunks)
            break
        if budget_s and (time() - started) + 1.5 * last_chunk_s > budget_s:
            logger.info(
                "Stopping cleanly %.1f min in: %d-minute time budget nearly exhausted; "
                "next run resumes from uid > %s",
                (time() - started) / 60,
                time_budget_minutes,
                sweep["watermark_uid"],
            )
            break

        remaining = [u for u in users if u["uid"] > sweep["watermark_uid"]]
        chunk = remaining[:chunk_size]
        is_final = len(remaining) <= chunk_size
        now = pendulum.now("UTC").isoformat()
        new_state = {
            "sweep_started_at": sweep["sweep_started_at"],
            "watermark_uid": None if is_final else chunk[-1]["uid"],
            "completed_at": now if is_final else None,
            "chunks_completed": sweep.get("chunks_completed", 0) + 1,
            "last_sweep_completed_at": (
                now if is_final else sweep.get("last_sweep_completed_at")
            ),
        }

        t0 = time()
        # fresh source instance every iteration — a DltSource is single-use
        pipeline.run(
            phish_user_source(users_chunk=chunk, sweep_state=new_state),
            loader_file_format="parquet",
        )
        last_chunk_s = time() - t0
        chunks_this_run += 1
        logger.info(
            "Chunk %d done: uids (%s, %s], %d users, %d/%d users swept, %.0fs",
            new_state["chunks_completed"],
            sweep["watermark_uid"],
            chunk[-1]["uid"] if chunk else sweep["watermark_uid"],
            len(chunk),
            len(users) - (len(remaining) - len(chunk)),
            len(users),
            last_chunk_s,
        )

        if is_final:
            logger.info(
                "Sweep complete: %d users in %d chunks",
                len(users),
                new_state["chunks_completed"],
            )
            break

        committed = _read_sweep_state(pipeline)
        if committed.get("watermark_uid") != new_state["watermark_uid"]:
            raise RuntimeError(
                "Sweep watermark did not advance after chunk run "
                f"(expected {new_state['watermark_uid']}, "
                f"state has {committed.get('watermark_uid')}) — "
                "aborting to avoid re-running the same chunk forever"
            )
        sweep = committed


def print_sweep_status(local_duckdb_name="phish.duckdb"):
    """Show sweep progress from dlt state without running anything."""
    pipeline = _build_pipeline(local_duckdb_name)
    try:
        pipeline.sync_destination()
    except Exception as e:
        logger.warning("Could not sync state from destination, showing local state: %s", e)
    src = pipeline.state.get("sources", {}).get(SOURCE_STATE_KEY, {})
    print("attendance sweep:", src.get(SWEEP_STATE_KEY) or "never run")
    cursor = src.get("resources", {}).get("users", {}).get("incremental", {}).get("uid", {})
    print("incremental uid cursor (daily new-user mode):", cursor.get("last_value"))
    print("pending load packages:", pipeline.has_pending_data)


@run.pipeline("phish_user_pipeline")
def run_dlt_pipeline(
    local_duckdb_name="phish.duckdb",
    target_schema_name="phish",
    limit=None,
    full_sweep_attendance=None,
    chunk_size=500,
    time_budget_minutes=100,
    max_chunks=None,
):
    logger.info("Starting DLT user pipeline")
    pipeline = _build_pipeline(local_duckdb_name)

    # the chunked driver needs the mode before building a source, so resolve the
    # config fallbacks the source would otherwise apply itself
    if full_sweep_attendance is None:
        full_sweep_attendance = (
            dlt.config.get("sources.phish_pipeline.full_sweep_attendance", bool) or False
        )

    if full_sweep_attendance:
        if not dlt.config.get("sources.phish_pipeline.enable_user_attendance", bool):
            logger.info(
                "users/user_attendance is disabled "
                "(sources.phish_pipeline.enable_user_attendance = false) — skipping sweep"
            )
            return
        _run_chunked_sweep(
            pipeline,
            chunk_size=chunk_size,
            time_budget_minutes=time_budget_minutes,
            max_chunks=max_chunks,
            dev_limit=limit,
        )
    else:
        load_info = pipeline.run(
            phish_user_source(dev_limit=limit, full_sweep_attendance=False),
            loader_file_format="parquet",
        )
        logger.info("\nPipeline load info:")
        logger.info(load_info)


def main(
    local_duckdb_name="phish.duckdb",
    target_schema_name="phish",
    limit=None,
    full_sweep_attendance=False,
    chunk_size=500,
    time_budget_minutes=100,
    max_chunks=None,
):
    pipeline_started_at = time()
    logger.info("Main function started at %s", pipeline_started_at)
    run_dlt_pipeline(
        local_duckdb_name=local_duckdb_name,
        target_schema_name=target_schema_name,
        limit=limit,
        full_sweep_attendance=full_sweep_attendance,
        chunk_size=chunk_size,
        time_budget_minutes=time_budget_minutes,
        max_chunks=max_chunks,
    )
    logger.info(
        f"Total elapsed time: {(time() - pipeline_started_at) / 60:.1f} minutes"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap every resource to the first N items (fast dev run), e.g. --limit 1",
    )
    parser.add_argument(
        "--full-sweep-attendance",
        action="store_true",
        help=(
            "Re-pull attendance for ALL users in resumable chunks instead of just "
            "newly-created ones (slower cadence, e.g. weekly cron) — see "
            "docs/user-attendance-incremental-options.md"
        ),
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Users per chunk in full-sweep mode; each chunk is one durable checkpoint",
    )
    parser.add_argument(
        "--time-budget-minutes",
        type=int,
        default=100,
        help=(
            "Stop starting new chunks after this many minutes so the run ends cleanly "
            "before the platform's 2h job limit; 0 = run the sweep to completion"
        ),
    )
    parser.add_argument(
        "--max-chunks",
        type=int,
        default=None,
        help="Stop after N chunks this run (mainly for testing resume behavior)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print sweep progress and incremental cursor from dlt state, then exit",
    )
    args = parser.parse_args()

    if args.status:
        print_sweep_status()
    else:
        main(
            local_duckdb_name="phish.duckdb",
            target_schema_name="phish",
            limit=args.limit,
            full_sweep_attendance=args.full_sweep_attendance,
            chunk_size=args.chunk_size,
            time_budget_minutes=args.time_budget_minutes,
            max_chunks=args.max_chunks,
        )
