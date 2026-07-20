# Incremental refresh of `user_attendance`

## The problem

`user_attendance` is a join of every phish.net user to every show they say
they attended — potentially tens or hundreds of millions of rows at full
scale. Users can create an account today and backfill attendance for a show
from decades ago at any time, and generally do so weeks after the fact
rather than same-day. Pulling `shows`/`setlists` daily only captures the
setlist itself, not who was there — that trickles in slowly and
unpredictably (~1% of eventual attendees same-day, single-digit percent
after weeks, more over months/years).

Checked the actual API/schema: `shows` has an `updated_at` field, so
incremental refresh of shows/setlists is legitimate. **`user_attendance` has
no timestamp field anywhere** — not on the attendance record, not on the
user object — and the API only exposes attendance per-user
(`attendance/uid/{uid}.json`, full history, no date-range or "since"
param). There is no row-level incremental cursor for attendance. Any
"incremental" claim at the attendance-record level would be fabricated.

## Reframing the cost

The real cost driver isn't row count, it's request count: attendance is
fetched one full history per user per HTTP call, not paginated by
attendance row. So "hundreds of millions of rows" is really "N users × 1
request each." The tractable question isn't "which rows changed" (no
cursor exists) but "which users are worth re-polling."

## Option A — user-level incremental + merge, full sweep as safety net

- Incrementally pull *new* users daily off `users/uid/0.json` (small,
  append-only by uid) and fetch attendance for just those new users.
- Separately, re-pull attendance for **all** users on a slower cadence
  (e.g. weekly) using `write_disposition="merge"` keyed on
  `(uid, showid)` — cheap on the destination since merge only touches
  changed/new rows, bounded on extraction since it's N requests where N is
  user count, not attendance-row count.
- Staleness for backfills is bounded to "at most one sweep interval,"
  which is honest rather than pretending true incrementality.

## Option B — accept and document incompleteness by design

Same daily-new-user + periodic-full-sweep pattern as Option A, plus:

- Track a `last_synced_at` per user.
- Surface a `last_full_synced_at` watermark to downstream consumers so
  attendance counts are understood as a lower bound as of a given sync
  date, not a final fact. This matters because attendance for a given show
  creeps up for years — anyone querying "who went to last night's show"
  needs to know they're seeing ~1% coverage, not a stalled pipeline.

## Recommendation

Do both — B is just being honest about A's limits, not a separate choice.
Use `merge` write disposition keyed on `(uid, showid)` regardless of pull
cadence so re-pulls are idempotent, incrementally load new users daily,
full-resync all users on a cron (weekly is likely fine given how slowly
backfills trickle in), and expose `last_synced_at` / `last_full_synced_at`
so consumers know the numbers are a lower bound.

## Status

Option A implemented. `users` is pulled with `dlt.sources.incremental("uid")`
and merged on `uid`; `user_attendance` merges on `(uid, showid)`, so re-pulls
are idempotent. By default a run only fetches attendance for newly-created
users. Pass `--full-sweep-attendance` (or `full_sweep_attendance=True` to
`phish_user_source`) to re-pull attendance for all users instead — run
this on a slower cadence (e.g. weekly) as the backfill safety net. See
[phish_user_pipeline.py](../phish_user_pipeline.py) / [phish_el/__init__.py](../phish_el/__init__.py).

Option B's `last_synced_at` / `last_full_synced_at` watermarks are not
implemented — not yet needed unless downstream consumers require an explicit
staleness signal.

The resources are still gated off by default via
`sources.phish_pipeline.enable_user_attendance` in `config.toml`; flip it to
`true` to turn them on.
