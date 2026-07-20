# phish_el

dlt sources for the [phish.net API](https://phish.net/api/docs). Two independent
sources, two independent pipelines — split so "core" show data and "user" attendance
data can be run, scheduled, and rate-limited on their own:

| Source (`phish_el/__init__.py`) | Pipeline entrypoint                        | dltHub job name                   | What it loads |
|----------------------------------|---------------------------------------------|------------------------------------|---------------|
| `phish_core_source`              | `../phish_core_pipeline.py`                 | `phish_core_pipeline.run_dlt_pipeline` | `shows`, `setlists`, `songs`, `venues` |
| `phish_user_source`              | `../phish_user_pipeline.py`                 | `phish_user_pipeline.run_dlt_pipeline` | `users`, `user_attendance` |

Both write to the same `phish_data` dataset, just via separate pipelines (separate
dlt pipeline state / dltHub jobs).

## Running locally

```bash
cd dlt-pipelines   # dlt resolves .dlt/config.toml relative to cwd

uv run python phish_core_pipeline.py                    # full core load
uv run python phish_core_pipeline.py --limit 5           # dev: cap every resource to 5 items

uv run python phish_user_pipeline.py                     # daily mode: only new users
uv run python phish_user_pipeline.py --limit 5            # dev: cap to 5 users
uv run python phish_user_pipeline.py --full-sweep-attendance   # re-pull ALL users' attendance,
                                                               # in resumable chunks
uv run python phish_user_pipeline.py --full-sweep-attendance \
    --chunk-size 500 --time-budget-minutes 0                  # sweep to completion locally
uv run python phish_user_pipeline.py --status                  # sweep progress, no data run
```

Requires `dlt-pipelines/.dlt/secrets.toml` with:

```toml
[sources.phish_pipeline]
api_key = "your-key-here"
```

Locally these run on the `dev` profile (`.dlt/dev.config.toml`) → local DuckDB at
`.dlt/data/dev/duck.db`.

## `users`/`user_attendance` is gated off by default

`sources.phish_pipeline.enable_user_attendance` in `.dlt/config.toml` defaults to
`false` — `phish_user_pipeline.py` will run but log a no-op and load nothing until you
flip it to `true` (globally in `config.toml`, or per-profile in `dev.config.toml` /
`prod.config.toml`). It's off by default because `user_attendance` is a full
user×show join — potentially tens/hundreds of millions of rows, one HTTP request per
user. See `../docs/user-attendance-incremental-options.md` for the full design
rationale before turning it on.

## Incremental behavior — what's a real cursor and what isn't

- `shows` / `setlists` / `songs` / `venues`: incremental via the API's `updated_at`
  field (config'd per-resource in `.dlt/config.toml`'s `resources` list).
- `users`: incremental via `dlt.sources.incremental("uid")` — each run only pulls
  users with a `uid` higher than the last seen. Merged on `uid`.
- `user_attendance`: **no timestamp field exists anywhere in the API** for this data
  (not on the attendance record, not on the user object) — there is no real cursor.
  Instead:
  - **Default mode**: only fetches attendance for the uids `users` just found as new
    (piggybacks on the `users` incremental cursor). Cheap, but blind to users who
    backfill attendance for old shows on an existing account.
  - **`--full-sweep-attendance`** (or `full_sweep_attendance = true` under
    `[sources.phish_pipeline]` in a profile config): re-fetches attendance for every
    user, ignoring the `users` cursor. Idempotent — merges on `(uid, showid)`, so
    re-running it never duplicates rows. Run this on a slower cadence (e.g. weekly) as
    the backfill safety net; staleness for backfills is bounded to "at most one sweep
    interval."
  - The sweep runs in **resumable chunks**: one `pipeline.run()` per `--chunk-size`
    users (default 500 — sized so a chunk still finishes inside the time budget even when
    the API is heavily rate-limiting), each committing a watermark to dlt state alongside its data.
    A killed run (Ctrl+C, crash, the platform's 2h job limit) resumes from the last
    completed chunk on the next invocation; `--time-budget-minutes` (default 100)
    stops a run cleanly before the platform kill so the next scheduled run continues
    the same sweep. Each chunk also appends a row to the `sweep_log` table, and
    `--status` prints the current watermark. When a sweep completes, the next
    full-sweep invocation starts a fresh one. See the "Chunked sweep" section of
    `../docs/user-attendance-incremental-options.md`.

Full design writeup: `../docs/user-attendance-incremental-options.md`.

## Rate limiting

`phish.net` rate-limits `attendance/uid/{uid}.json`, and a full sweep fires one
request per user. Two knobs in `.dlt/config.toml` control this:

```toml
[runtime]
request_max_attempts = 10     # retries per request on 429/5xx (dlt default: 5)
request_backoff_factor = 2    # exponential backoff multiplier (dlt default: 1)

[extract]
max_parallel_items = 5        # caps concurrent in-flight @dlt.defer futures
                               # (independent of `workers` thread count)
```

If a full sweep still dies with repeated `429 Too Many Requests`, raise
`request_max_attempts`/`request_backoff_factor` further and/or lower
`max_parallel_items` before retrying.

## Deploying to dltHub Platform

`__deployment__.py` (repo root of `dlt-pipelines/`) exposes both pipelines as
separate jobs:

```python
from phish_core_pipeline import run_dlt_pipeline as run_core_pipeline
from phish_user_pipeline import run_dlt_pipeline as run_user_pipeline
```

```bash
cd dlt-pipelines
uv run dlthub deploy --dry-run   # preview: which jobs are new/updated/archived
uv run dlthub deploy             # sync __deployment__.py to the platform as jobs
```

Deployed batch jobs run on the `prod` profile (`.dlt/prod.config.toml`) —
`enable_user_attendance` and `full_sweep_attendance` can be set there independently of
your local `dev` profile.

### Run a job on the cloud

```bash
uv run dlthub job list                                    # see registered jobs
uv run dlthub run jobs.phish_core_pipeline.run_dlt_pipeline -f   # run + stream logs
uv run dlthub run jobs.phish_user_pipeline.run_dlt_pipeline -f
```

### Simulate a job locally before deploying (uses prod credentials, runs on your machine)

```bash
uv run dlthub local run jobs.phish_core_pipeline.run_dlt_pipeline --profile prod --dry-run
uv run dlthub local run jobs.phish_user_pipeline.run_dlt_pipeline --profile prod
```

### One-off full attendance sweep on the cloud

There's no `--config` override for cloud `dlthub run` — the deployed entrypoint takes
no CLI args, so the flag has to come from config. To trigger a one-off full sweep:

1. Set `full_sweep_attendance = true` under `[sources.phish_pipeline]` in
   `.dlt/prod.config.toml`.
2. `uv run dlthub deploy && uv run dlthub run jobs.phish_user_pipeline.run_dlt_pipeline -f`
3. **Revert step 1 back to `false`/remove it and redeploy** once done, so the regular
   schedule goes back to cheap incremental-only daily runs instead of sweeping on
   every trigger.

A sweep bigger than one 2-hour job is fine: each run stops cleanly at its
`time_budget_minutes` (default 100) and the next run of the same job resumes the
sweep from the committed watermark — just trigger/schedule the job repeatedly until
the logs report "Sweep complete" (or check the `sweep_log` table / `--status`
locally). The driver knobs (`chunk_size`, `time_budget_minutes`, `max_chunks`) are
job config on the platform — settable under
`[jobs.phish_user_pipeline.run_dlt_pipeline]` in `prod.config.toml`, no code change
needed.

### Debugging a deployed run

```bash
uv run dlthub job logs phish_core_pipeline.run_dlt_pipeline       # last run's logs
uv run dlthub job logs phish_user_pipeline.run_dlt_pipeline -f    # stream live
uv run dlthub show                                                 # open the web UI
```

### Scheduling (cron)

Add a `trigger=` to the `@run.pipeline` decorator in `phish_core_pipeline.py` /
`phish_user_pipeline.py`, then `dlthub deploy`. Example — core daily, user daily
(default incremental) + a separate weekly full sweep as a second scheduled job:

```python
from dlt.hub.run import trigger

@run.pipeline("phish_core_pipeline", trigger=trigger.schedule("0 6 * * *"))  # 6am UTC daily
def run_dlt_pipeline(...): ...
```

Triggers declared in code are the source of truth — there's no CLI command to add/remove
schedules; only `dlthub deploy` reconciles them.

## Files

- `__init__.py` — `phish_core_source` and `phish_user_source` (`@dlt.source`s).
- `sql-etc/` — SQL/notebook scratch work for exploring loaded data.
- `archive_reference/` — an older, slower implementation kept for reference only. Not
  used by either pipeline.
