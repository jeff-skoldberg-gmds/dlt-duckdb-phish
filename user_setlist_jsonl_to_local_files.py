import duckdb
import os

def write_chunked_jsonl(
    db_name: str,
    view_name: str,
    output_dir: str,
    batch_size: int = 100_000
):
    """
    Write a DuckDB view to multiple JSONL files using DuckDB's SQL interface
    
    Args:
        db_name: Name of DuckDB database
        view_name: Name of the view to export
        output_dir: Directory to write JSONL files
        batch_size: Number of rows to process at once
    """
    # Connect to DuckDB
    con = duckdb.connect(db_name)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get total count for progress tracking
    total_rows = con.sql(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
    print(f"Total rows to process: {total_rows:,}")
    
    # Calculate number of chunks
    num_chunks = (total_rows + batch_size - 1) // batch_size
    
    for chunk in range(num_chunks):
        output_file = f"{output_dir}/part_{chunk + 1:05d}.jsonl"
        
        query = f"""
        COPY (
            SELECT *
            FROM {view_name}
            LIMIT {batch_size}
            OFFSET {chunk * batch_size}
        ) TO '{output_file}' (FORMAT JSON)
        """
        
        con.sql(query)
        rows_processed = min((chunk + 1) * batch_size, total_rows)
        print(f"Processed {rows_processed:,}/{total_rows:,} rows. Wrote file: {output_file}")
    
    con.close()
    print(f"\nComplete! Wrote {total_rows:,} rows to {num_chunks} files in {output_dir}")

# Example usage
if __name__ == "__main__":
    write_chunked_jsonl(
        db_name="phish_pipeline.duckdb",
        view_name="main.user_setlists_v",
        output_dir="output_jsonl",
        batch_size=500_000 
    )