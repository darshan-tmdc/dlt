import dlt
@dlt.resource(write_disposition="merge")
def get_users_data(updated_at=dlt.sources.incremental("updated_at", initial_value="2025-01-01T00:00:00Z")):
    # Sample data source
    data = [
        {"id": 3, "name": "Alice", "updated_at": "2025-05-31 14:50:00"},
        {"id": 4, "name": "Bob", "updated_at": "2025-05-31 14:55:00"}
    ]
    # Filter data based on the incremental 'updated_at' value
    for row in data:
        yield row

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="elasticsearch_pipeline",
        destination=dlt.destinations.elasticsearch(
            host="http://localhost:9200",
            username="elastic",
            password="changeme"
        )
    )

    # Run the pipeline with the incremental resource
    pipeline.run(get_users_data(),  table_name="users")
