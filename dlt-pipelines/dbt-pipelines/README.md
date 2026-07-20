# dbt-pipelines

dbt project (dbt Fusion) that transforms phish.net data loaded by the dlt pipelines in
the [parent workspace](..) — see that project's
[README](../README.md#running-dbt-through-dlt) for how to actually run this (profile
generation, `dbt build`, dltHub deployment). This file just orients you inside the dbt
project itself.

This directory lives **inside** `dlt-pipelines/` on purpose: `dlthub deploy` only
ships the workspace directory, so nesting here is what lets `phish_dbt_transform` run
on the dltHub Platform.

## Models

```
models/_raw_sources/PHISH.yml   source defs — points at dlt's phish_data schema
models/staging/                 1:1 cleanup of raw source tables
models/intermediate/            joins/aggregations not yet report-ready
models/reporting/                final tables: phish_setlists, phish_song_stats
seeds/phish_eras.csv             static era lookup (used by stg_phishapi__eras)
```

Materializations are set in `dbt_project.yml`: staging/intermediate as views,
reporting as tables.

## Formerly known gaps (fixed)

- **`stg_phishapi__artists`** — dlt's `phish_core_pipeline` now extracts an `artists`
  resource, so this model and its source tests pass.
- **`phish_setlists.sql`** — the VARCHAR/DATE `COALESCE` type mismatch has been fixed;
  the model builds cleanly.

Full `dbt build` is green (10 models, 12 tests, 1 seed) on both dev (local DuckDB)
and prod (Motherduck) targets.

## Origin

Originally built against Airbyte-loaded Snowflake tables — source config has since
been repointed at dlt's DuckDB output, but some model logic still reflects that
history (e.g. the two gaps above).
