import duckdb
import boto3
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ChunkResult:
    chunk_num: int
    rows_processed: int
    output_file: str
    duration: float
    start_time: datetime

def get_aws_credentials():
    """Get credentials from AWS SSO session"""
    session = boto3.Session()
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()
    return {
        'access_key_id': credentials.access_key,
        'secret_access_key': credentials.secret_key,
        'session_token': credentials.token
    }

def process_chunk(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    s3_path: str,
    chunk: int,
    batch_size: int,
    total_rows: int
) -> ChunkResult:
    """Process a single chunk of data"""
    start_time = datetime.now()
    output_file = f"{s3_path}/part_{chunk + 1:05d}.json"
    
    query = f"""
    COPY (
        SELECT *
        FROM {view_name}
        LIMIT {batch_size}
        OFFSET {chunk * batch_size}
    ) TO '{output_file}' (FORMAT JSON)
    """
    
    con.sql(query)
    duration = time.time() - start_time.timestamp()
    rows_processed = min((chunk + 1) * batch_size, total_rows)
    
    return ChunkResult(
        chunk_num=chunk + 1,
        rows_processed=rows_processed,
        output_file=output_file,
        duration=duration,
        start_time=start_time
    )

def write_chunked_json_to_s3(
    db_name: str,
    view_name: str,
    s3_path: str,
    batch_size: int = 1_000_000,
    max_workers: int = 4
):
    """
    Write a DuckDB view to multiple JSON files directly to S3 using concurrent processing
    
    Args:
        db_name: Name of DuckDB database
        view_name: Name of the view to export
        s3_path: S3 path prefix (e.g. 's3://my-bucket/path/to/files')
        batch_size: Number of rows per file
        max_workers: Maximum number of concurrent threads
    """
    # Connect to DuckDB
    con = duckdb.connect(db_name)
    
    # Install and load httpfs extension for S3 support
    con.sql("INSTALL httpfs;")
    con.sql("LOAD httpfs;")
    
    # Get AWS credentials from SSO session
    creds = get_aws_credentials()
    
    # Configure S3 credentials
    con.sql(f"""
        SET s3_region='us-east-1';
        SET s3_access_key_id='{creds['access_key_id']}';
        SET s3_secret_access_key='{creds['secret_access_key']}';
        SET s3_session_token='{creds['session_token']}';
    """)
    
    # Get total count for progress tracking
    total_rows = con.sql(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
    print(f"Starting export of {total_rows:,} rows at {datetime.now()}")
    
    # Calculate number of chunks
    num_chunks = (total_rows + batch_size - 1) // batch_size
    start_time = time.time()
    completed_chunks = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chunks to the executor
        future_to_chunk = {
            executor.submit(
                process_chunk, 
                con, 
                view_name, 
                s3_path, 
                chunk, 
                batch_size, 
                total_rows
            ): chunk 
            for chunk in range(num_chunks)
        }
        
        # Process completed chunks
        results = []
        for future in as_completed(future_to_chunk):
            chunk_num = future_to_chunk[future]
            try:
                result = future.result()
                results.append(result)
                completed_chunks += 1
                
                # Print progress
                elapsed = time.time() - start_time
                avg_time = elapsed / completed_chunks
                est_remaining = avg_time * (num_chunks - completed_chunks)
                
                print(f"Chunk {result.chunk_num}/{num_chunks} complete: "
                      f"{result.rows_processed:,} rows in {result.duration:.2f}s | "
                      f"Est. remaining: {est_remaining:.0f}s")
                
            except Exception as e:
                print(f"Chunk {chunk_num + 1} failed: {str(e)}")
                raise
    
    # Final stats
    total_duration = time.time() - start_time
    results.sort(key=lambda x: x.chunk_num)  # Sort for consistent output
    
    print(f"\nExport complete at {datetime.now()}")
    print(f"Wrote {total_rows:,} rows to {num_chunks} files in {total_duration:.2f}s")
    print(f"Average time per chunk: {total_duration/num_chunks:.2f}s")
    print(f"Files written to: {s3_path}/")
    
    con.close()

if __name__ == "__main__":
    write_chunked_json_to_s3(
        db_name="phish_pipeline.duckdb",
        view_name="main.user_setlists_v",
        s3_path="s3://phish-user-attendance-setlists",
        batch_size=100_000,
        max_workers=8
    )