"""Phish.net ingestion — core (shows, setlists, songs, venues) and user (users, user attendance) load to DuckDB/Motherduck as independent pipelines."""

from phish_core_pipeline import run_dlt_pipeline as run_core_pipeline
from phish_user_pipeline import run_dlt_pipeline as run_user_pipeline

__all__ = ["run_core_pipeline", "run_user_pipeline"]
