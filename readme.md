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

Or, if you a masochist, you can:
```
python -m venv .venv
source .venv/bin/Activate
pip install -r requirements
```

### set up your api secret
edit `dlt-pipelines/.dlt/secrets.toml` (git ignored) and add your API key under `[sources.phish_pipeline]`:
```toml
[sources.phish_pipeline]
api_key = "your-key-here"
```

### run the pipeline
```
cd dlt-pipelines
uv run python phish_pipeline.py
```
dlt resolves `.dlt/config.toml` and `.dlt/secrets.toml` relative to the directory you run from, so you must `cd dlt-pipelines` first (this repo is a monorepo — `dlt-pipelines/` is one project among others, e.g. a future `dbt/` or app folder).

## Notes on files and directories
All pipeline code lives under `dlt-pipelines/`:
1. `dlt-pipelines/phish_pipeline.py`: Runs the phish.net APIs, including fetching every show for every user of the platform. It requires you set up `dlt-pipelines/.dlt/secrets.toml`.
2. `dlt-pipelines/slow_way.py`: You can ignore it. It is demonstrating how I did this before getting dlt concurrency to work. The approach is still neat, so I kept it for reference. It uses dlt's "resolve" to resolve the user/shows combinations.
3. `dlt-pipelines/phish_el/`: This is where the dlt `source` and `resources` are (in `__init__.py`).
4. `dlt-pipelines/phish_el/archive_reference/`: Stuff I'm not using anymore but wanted to keep for reference.
5. `dlt-pipelines/.dlt/config.toml` / `dlt-pipelines/.dlt/secrets.toml`: pipeline config and credentials for this sub-project.


## to-do:
Add user's motherduck token to the secrets.toml file and create a way for it to be passed to the function!
Right now this only works if their MD token is stored in env vars.
Need to add the "create view" step before shipping to MotherDuck.
Create the the view of user||shows||setlists