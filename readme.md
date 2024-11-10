# phish api shtuff

## getting started
### set up the env
```
# mac, use python3 and bin/activate.
# windwos, use python and Scripts/activate
python3 -m venv .venv
. .venv/bin/activate
```

### set up your api secret
locate `.dlt/secrets.toml.example` and rename it to `.dlt/secrets.toml` (which is git ignored.)

pop in our API key.

## files
1. phish_elt.py: extracts the basic phish apis using dlt.  It requires you set up .dlt/secrets.toml
2. phish_users_attendance_elt.py: uses dlt to pass each user ID to the attendance API.
3. explore-phish-data.ipynb: A juypter notbook that helps you see inside the duckdb.  Also, the view which joins users to setlists is created here.
4. user_setlists_jsonl_to_local_files.py: Spits "event data" of a user seeing a song at a show to jsonl locally.  No, I did not create event ID, etc.
5. user_setlists_jsonl_to_s3.py: same thing but shoots it to S3 and also uses concurrency to gain some speed.