import dlt
@dlt.resource(write_disposition="merge", primary_key='id')
def get_users_data(updated_at=dlt.sources.incremental("updated_at", initial_value="2025-01-01T00:00:00Z")):
    # Sample data source
    data = [
        {"id": 3, "name": "Alice", "updated_at": "2025-05-31 14:50:00"},
        {"id": 4, "name": "Bob", "updated_at": "2025-06-01 14:55:00"},
        {"id": 5, "name": "Charlie", "updated_at": "2025-06-02 15:00:00", "is_active": True}
    ]
    # Filter data based on the incremental 'updated_at' value
    for row in data:
        yield row

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="elasticsearch_pipeline",
        destination=dlt.destinations.elasticsearch(credentials={
            "protocol": "http",
            "host": "localhost",
            "port": 9200,
            "username": "elastic",
            "password": "changeme"
        }
        )
    )

    # Run the pipeline with the incremental resource
    pipeline.run(get_users_data(),  table_name="users")
