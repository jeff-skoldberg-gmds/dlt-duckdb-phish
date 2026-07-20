# dbt-pipelines

dbt project (dbt Fusion) that transforms phish.net data loaded by
[`../dlt-pipelines`](../dlt-pipelines) — see that project's
[README](../dlt-pipelines/README.md#running-dbt-through-dlt) for how to actually run
this (profile generation, `dbt build`, dltHub deployment). This file just orients you
inside the dbt project itself.

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

## Known gaps

- **`stg_phishapi__artists`** — sourced from `PHISH.ARTISTS`, but dlt's
  `phish_core_pipeline` doesn't extract an `artists` resource (only
  shows/setlists/songs/venues). This model and its source tests will fail until
  either an `artists` resource is added to dlt or this model is removed.
- **`phish_setlists.sql`** — fails with a VARCHAR/DATE type mismatch in a `COALESCE`;
  needs an explicit cast. Pre-existing, unrelated to the dlt/dbt wiring.

## Origin

Originally built against Airbyte-loaded Snowflake tables — source config has since
been repointed at dlt's DuckDB output, but some model logic still reflects that
history (e.g. the two gaps above).
