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
locate `.dlt/secrets.toml.example` and rename it to `.dlt/secrets.toml` (which is git ignored.)

pop in our API key.

### run the pipeline
```
cd src
# phish_pipeline.py must be run from the src directory!
python phish_pipeline.py
```

## Notes on files and directories
1. `src\phish_pipeline.py`: Runs the import phish.net APIs, including fetching every show for every user of the platform.  It requires you set up .dlt/secrets.toml
2. `slow_way.py`: You can ignore it.  It is demonstrating how I did this before getting dlt concurrency to work.  The approach is still neat, so I kept it for reference.  It uses "resolves" the user/shows combination by calling the 
3. `src\utilities\`: This is where the logging is set up and where the log files end up.
4. `src\phish_el\`: This is where the dlt `source` and `resources` are (in `__init__.py`). This is also where your duckdb ends up.
5. `src\phish_el\archive_reference\`: Stuff I'm not using anymore but wanted to keep for reference.


## to-do:
Need to add the "create view" step before shipping to MotherDuck.
Create the the view of user||shows||setlists