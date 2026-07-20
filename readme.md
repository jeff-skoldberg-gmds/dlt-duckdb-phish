# phish.net api shtuff
## About
This repo does the following:
- Loads the following phish.net endpoints to a local duckdb:
  - shows (shows.json)
  - songs (songs.json)
  - venues (venues.json)
  - setlists (setlists.json)
  - users (users/uid/0.json)
  - user attendance (attendance/uid/{user_id}.json)
- Ships the local duckdb to MotherDuck for sharing and analysis.

It does this using dlt and is very fast considering the number of API calls it makes.

The entire package runs in about 20 minutes.

## getting started
### set up the env
install `uv` per [the docs](https://docs.astral.sh/uv/getting-started/installation/)  
That should be all you need to do.  
You can run `uv sync` just to make sure it is all working, but you shouldn't have to.  


### set up your api secret
edit `dlt-pipelines/.dlt/secrets.toml` (git ignored) and add your API key under `[sources.phish_pipeline]`:
```toml
[sources.phish_pipeline]
api_key = "your-key-here"
```

### run the pipeline
```
cd dlt-pipelines
uv run python phish_core_pipeline.py   # shows, setlists, songs, venues
uv run python phish_user_pipeline.py   # users, user attendance (gated by enable_user_attendance)
```
dlt resolves `.dlt/config.toml` and `.dlt/secrets.toml` relative to the directory you run from, so you must `cd dlt-pipelines` first (this repo is a monorepo — `dlt-pipelines/` is one project among others, e.g. a future `dbt/` or app folder).

## Notes on files and directories
All pipeline code lives under `dlt-pipelines/`:
1. `dlt-pipelines/phish_core_pipeline.py`: Runs shows/setlists/songs/venues — the fast, no-user-data pipeline. It requires you set up `dlt-pipelines/.dlt/secrets.toml`.
2. `dlt-pipelines/phish_user_pipeline.py`: Runs users + user attendance (fetching every show for every user of the platform) as its own pipeline, so it can be scheduled/rate-limited independently of core data. See `--full-sweep-attendance` and `dlt-pipelines/docs/user-attendance-incremental-options.md`.
3. `dlt-pipelines/pipeline_common.py`: Shared `get_destination()` helper for both pipelines.
4. `dlt-pipelines/slow_way.py`: You can ignore it. It is demonstrating how I did this before getting dlt concurrency to work. The approach is still neat, so I kept it for reference. It uses dlt's "resolve" to resolve the user/shows combinations.
5. `dlt-pipelines/phish_el/`: This is where the dlt `source`s and `resources` are (in `__init__.py`) — `phish_core_source` and `phish_user_source`.
6. `dlt-pipelines/phish_el/archive_reference/`: Stuff I'm not using anymore but wanted to keep for reference.
7. `dlt-pipelines/.dlt/config.toml` / `dlt-pipelines/.dlt/secrets.toml`: pipeline config and credentials for this sub-project.


## to-do:
Add user's motherduck token to the secrets.toml file and create a way for it to be passed to the function!
Right now this only works if their MD token is stored in env vars.
Need to add the "create view" step before shipping to MotherDuck.
Create the the view of user||shows||setlists