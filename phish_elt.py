import dlt
from dlt.sources.rest_api import rest_api_source, check_connection

def load_phish_data() -> None:
    # Get configuration from config.toml
    config = dlt.config['source.phish_pipeline']
    
    # Get API key from secrets
    api_key = dlt.secrets.get('sources.phish_pipeline.api_key')
    if not api_key:
        raise ValueError("API key not found in secrets.toml")
    
    # Create the source configuration
    source_config = {
        "client": {
            "base_url": config['base_url']
        },
        "resources": config['resources'],
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "apikey": api_key,
                    **config.get('resource_defaults', {}).get('endpoint', {}).get('params', {})
                }
            }
        }
    }

    phish_source = rest_api_source(source_config)

    pipeline = dlt.pipeline(
        pipeline_name="phish_pipeline",
        destination='duckdb',
        dataset_name="phish_data",
    )

    # Optional connection check
    can_connect, error_msg = check_connection(phish_source, "shows")
    if not can_connect:
        raise ConnectionError(f"Failed to connect to Phish.net API: {error_msg}")

    # Run the pipeline
    load_info = pipeline.run(phish_source)
    print(f"\nPipeline load info:")
    print(load_info)

if __name__ == "__main__":
    load_phish_data()