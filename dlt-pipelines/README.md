# dlt-pipelines

Extract & load side of the project: [dlt](https://dlthub.com) pipelines that pull
phish.net API data into DuckDB (and ship it to MotherDuck), plus the dbt job that
transforms it afterward.

For source/resource-level detail (incremental strategy, rate limiting, per-source
deployment options), see [phish_el/README.md](phish_el/README.md). This file is the
front door: setup, running things locally, and how dlt and dbt fit together.

## Setup

```bash
uv sync
```

Add your phish.net API key to `.dlt/secrets.toml` (git ignored):

```toml
[sources.phish_pipeline]
api_key = "your-key-here"
```

All commands below assume `cd dlt-pipelines` first — dlt resolves `.dlt/config.toml`
relative to cwd.

## Running the pipelines

```bash
uv run python phish_core_pipeline.py            # shows, setlists, songs, venues
uv run python phish_core_pipeline.py --limit 5   # dev sample

uv run python phish_user_pipeline.py             # users, user attendance (gated, see phish_el/README.md)
```

Locally these run on the `dev` profile → DuckDB at `dlt-pipelines/data/phish.duckdb`
(see `pipeline_common.get_duckdb_path()` — a fixed, explicit path, not dlt's
profile-scoped default, so anything outside dlt, like dbt, can find the same file
without asking dlt where it put it).

## Running dbt through dlt

The dbt project lives in [`dbt-pipelines/`](dbt-pipelines) — **inside** this
workspace, so `dlthub deploy` ships it to the platform (the runtime only syncs this
directory; a sibling location could never run there). It transforms whatever's
currently in the `phish_data` dataset, via the **dbt Fusion** CLI (`dbt`), not dlt's
built-in `dlt.helpers.dbt` runner — that runner pip-installs classic dbt-core into a
venv, which isn't how Fusion works. Instead:

- **`dbt_profile.py`** generates a `profiles.yml` from dlt's own destination config —
  the same duckdb path (`get_duckdb_path()`) locally, or the Motherduck
  `database`/`password` credentials on `prod`. No hand-maintained profile, no separate
  credentials to keep in sync. The file is written to a **temp directory** (returned
  by `build_profile()`), never into the dbt project: on prod it embeds the motherduck
  token, and generated files inside the workspace could ship in the deploy tarball.
- **`phish_dbt_job.py`** regenerates that profile, runs `dbt deps` (dbt_packages/ is
  generated, gitignored, and excluded from deploys), then
  `dbt build --project-dir dbt-pipelines --profiles-dir <tmp> --target <dev|prod>`.

Run it directly:

```bash
uv run python phish_dbt_job.py                          # dev → local duckdb
WORKSPACE__PROFILE=prod uv run python phish_dbt_job.py  # prod → Motherduck
```

Or drive dbt yourself, e.g. to run/debug a single model:

```bash
uv run python dbt_profile.py   # writes profiles.yml to a temp dir, prints the path
dbt debug   --project-dir dbt-pipelines --profiles-dir <printed dir> --target dev
dbt run     --project-dir dbt-pipelines --profiles-dir <printed dir> --target dev --select stg_phishapi__shows
```

`phish_dbt_job.py` is decorated `@run.pipeline("phish_dbt_transform",
trigger=run_core_pipeline.success)` — once deployed, it only fires after
`phish_core_pipeline` succeeds, no polling. It's registered in `__deployment__.py`
alongside the extract jobs, so on the platform it shows up as its own job with its own
logs/run history, separate from extraction.

Two packaging notes for the platform:

- The `dbt` Fusion CLI is pinned in **`requirements.txt`** — the runtime builds every
  job env from that file. (A pyproject `[dependency-groups]` entry can't work here:
  the repo-root `pyproject.toml` sits outside the deployed workspace tree. Keep the
  two pins in sync.)
- **`.gitignore` in this directory doubles as the deploy exclusion list** — `dlthub
  deploy` tarballs this workspace using gitignore-style patterns from that one file
  (nested `.gitignore`s are ignored). Deploy-sensitive patterns (`data/`,
  `profiles.yml`, dbt artifacts) must live there.

## Deploying to dltHub Platform

```bash
uv run dlthub deploy --dry-run   # preview: which jobs are new/updated/archived
uv run dlthub deploy             # sync __deployment__.py (3 jobs: core, user, dbt transform)
```

Deployed jobs run on the `prod` profile (`.dlt/prod.config.toml`). See
[phish_el/README.md](phish_el/README.md#deploying-to-dlthub-platform) for job
run/log/schedule commands.
