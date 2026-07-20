# Phish.net Data Platform

## About

This repo turns the [phish.net API](https://phish.net/api/docs) into a queryable
dataset, in two stages:

1. **Extract & load** ([dlt](https://dlthub.com)) — pulls shows, setlists, songs,
   venues, users, and user attendance into a local DuckDB, and ships it to MotherDuck
   for sharing/analysis.
2. **Transform** ([dbt](https://www.getdbt.com), via the Fusion engine) — models that
   raw data into staging → intermediate → reporting tables.

Both stages run locally against the same DuckDB file, and both deploy as independent,
separately-scheduled jobs on the dltHub Platform.

## Layout

```
dlt-pipelines/   extract & load — dlt pipelines, sources, dltHub deployment config
dbt-pipelines/   transform — dbt project (staging/intermediate/reporting models)
```

- **[dlt-pipelines/README.md](dlt-pipelines/README.md)** — running the pipelines,
  running dbt through dlt, deploying to dltHub Platform.
- **[dbt-pipelines/README.md](dbt-pipelines/README.md)** — what the dbt project
  contains.

## Quickstart

```bash
# 1. install uv: https://docs.astral.sh/uv/getting-started/installation/
uv sync

# 2. add your phish.net API key
#    edit dlt-pipelines/.dlt/secrets.toml (git ignored):
#    [sources.phish_pipeline]
#    api_key = "your-key-here"

# 3. extract, load, transform
cd dlt-pipelines
uv run python phish_core_pipeline.py --limit 5   # shows, setlists, songs, venues (dev sample)
uv run python phish_dbt_job.py                    # regenerate dbt profile + `dbt build`
```

See [dlt-pipelines/README.md](dlt-pipelines/README.md) for the full pipeline set
(including `phish_user_pipeline.py`), incremental loading, rate limiting, and
deploying/scheduling on dltHub Platform.
