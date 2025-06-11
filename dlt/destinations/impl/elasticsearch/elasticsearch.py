from types import TracebackType
from typing import Optional, List, Dict, Type, Iterable, Any
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError
from elasticsearch.helpers import BulkIndexError
from dlt.common.pendulum import pendulum
from dlt.common.destination.client import StateInfo, StorageSchemaInfo
from dlt.common.schema.typing import TStoredSchema
import json

from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.utils import (
    loads_table,
    normalize_table_identifiers,
    version_table,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    PreparedTableSchema,
    JobClientBase,
    WithStateSync,
    LoadJob,
)
from dlt.destinations.impl.elasticsearch.job_client import LoadElasticJob

from dlt.destinations.utils import get_pipeline_state_query_columns
from dlt.destinations.impl.elasticsearch.configuration import (
    ElasticsearchClientConfiguration,
)

class ElasticsearchClient(JobClientBase, WithStateSync):
    """Elasticsearch destination job client."""

    def __init__(
        self,
        schema: Schema,
        config: ElasticsearchClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        version_table_ = normalize_table_identifiers(version_table(), schema.naming)
        self.version_table_fields = list(version_table_["columns"].keys())
        loads_table_ = normalize_table_identifiers(loads_table(), schema.naming)
        self.loads_table_fields = list(loads_table_["columns"].keys())
        state_table_ = normalize_table_identifiers(
            get_pipeline_state_query_columns(), schema.naming
        )
        self.state_table_fields = list(state_table_["columns"].keys())
        self.config: ElasticsearchClientConfiguration = config
        self.es = None

    @property
    def dataset_prefix(self) -> str:
        return (self.config.dataset_name + "_") if self.config.dataset_name else ""

    def _index_name(self, table_name: str) -> str:
        # Remove leading underscore if present
        table_name = table_name.lstrip('_')
        return f"{self.dataset_prefix}{table_name}"

    # Connection helpers
    def connect(self) -> None:
        from elasticsearch import Elasticsearch

        auth = None
        if self.config.credentials.username:
            auth = (self.config.credentials.username, self.config.credentials.password)

        url = f"{self.config.credentials.protocol}://{self.config.credentials.host}:{self.config.credentials.port}"
        self.es = Elasticsearch(url, basic_auth=auth)

    def close(self) -> None:
        if self.es:
            self.es.close()
            self.es = None

    # Bulk helper
    def bulk(self, actions: List[Dict[str, Any]]) -> None:
        try:
            success, failed = helpers.bulk(
                self.es,
                actions,
                raise_on_error=False,
                raise_on_exception=False,
                max_retries=self.config.max_retries
            )

            if failed:
                # Log failed documents
                for item in failed:
                    print(f"Failed to index document: {item}")

        except BulkIndexError as e:
            print(f"Bulk indexing error: {str(e)}")
            raise
        except ConnectionError as e:
            print(f"Connection error: {str(e)}")
            raise
        except Exception as e:
            print(f"Unexpected error during bulk indexing: {str(e)}")
            raise

    # JobClientBase methods
    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        """Initialize storage and ensure schema is created and stored.

        Args:
            truncate_tables: List of tables to truncate. Defaults to None.
        """
        self.connect()

        # Create system tables if they don't exist
        system_tables = [
            self.schema.version_table_name,
            self.schema.loads_table_name,
            self.schema.state_table_name
        ]

        # Get normalized table definitions
        version_table_ = normalize_table_identifiers(version_table(), self.schema.naming)
        loads_table_ = normalize_table_identifiers(loads_table(), self.schema.naming)
        state_table_ = normalize_table_identifiers(get_pipeline_state_query_columns(), self.schema.naming)

        for table in system_tables:
            index = self._index_name(table)
            if not self.es.indices.exists(index=index):
                # Define index settings
                settings = {
                    "number_of_shards": self.config.number_of_shards,
                    "number_of_replicas": self.config.number_of_replicas,
                    "refresh_interval": self.config.refresh_interval
                }

                # Define mappings based on table type
                mappings = {
                    "properties": {}
                }

                # Add common timestamp fields
                mappings["properties"].update({
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"}
                })

                # Add table-specific fields based on normalized table definitions
                if table == self.schema.version_table_name:
                    for field, field_type in version_table_["columns"].items():
                        mappings["properties"][field] = self._get_elasticsearch_type(field_type)
                elif table == self.schema.loads_table_name:
                    for field, field_type in loads_table_["columns"].items():
                        mappings["properties"][field] = self._get_elasticsearch_type(field_type)
                elif table == self.schema.state_table_name:
                    for field, field_type in state_table_["columns"].items():
                        mappings["properties"][field] = self._get_elasticsearch_type(field_type)

                self.es.indices.create(
                    index=index,
                    settings=settings,
                    mappings=mappings,
                    ignore=400
                )

        # Ensure schema is stored
        if hasattr(self, 'schema'):
            # Store the schema
            schema_doc = {
                "version": self.schema.version,
                "engine_version": self.schema.ENGINE_VERSION,
                "schema_name": self.schema.name,
                "version_hash": self.schema.stored_version_hash,
                "schema": json.dumps(self.schema.to_dict()),
                "inserted_at": pendulum.now().isoformat()
            }

            try:
                # Index the schema document
                self.es.index(
                    index=self._index_name(self.schema.version_table_name),
                    document=schema_doc,
                    refresh=True  # Force refresh to make the document immediately searchable
                )
                print(f"Schema with hash {self.schema.stored_version_hash} stored in Elasticsearch")

                # Store schema locally using parent implementation
                super().update_stored_schema()

            except Exception as e:
                print(f"Error storing schema in Elasticsearch: {str(e)}")
                raise

        # Handle truncate tables if specified
        if truncate_tables:
            for table in truncate_tables:
                index = self._index_name(table)
                if self.es.indices.exists(index=index):
                    self.es.indices.delete(index=index)

                # Define index settings and mappings
                settings = {
                    "number_of_shards": self.config.number_of_shards,
                    "number_of_replicas": self.config.number_of_replicas,
                    "refresh_interval": self.config.refresh_interval
                }

                mappings = {
                    "properties": {
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"}
                    }
                }

                self.es.indices.create(
                    index=index,
                    settings=settings,
                    mappings=mappings,
                    ignore=400
                )

    def _get_elasticsearch_type(self, field_type: dict) -> dict:
        """Convert DLT field type to Elasticsearch type.

        Args:
            field_type: DLT field type dictionary containing 'type' and other properties

        Returns:
            Elasticsearch type mapping
        """
        type_mapping = {
            "text": {"type": "text"},
            "bigint": {"type": "long"},
            "double": {"type": "double"},
            "timestamp": {"type": "date"},
            "bool": {"type": "boolean"},
            "complex": {"type": "object"},
            "decimal": {"type": "double"},
            "wei": {"type": "long"},
            "date": {"type": "date"},
            "time": {"type": "date"},
            "binary": {"type": "binary"},
            "json": {"type": "object"}
        }

        # Get the base type from the field type dictionary
        base_type = field_type.get("type", "").lower()

        # Get the Elasticsearch type mapping
        es_type = type_mapping.get(base_type, {"type": "keyword"})

        # Add any additional properties from the field type
        if "nullable" in field_type:
            es_type["null_value"] = None

        return es_type

    def is_storage_initialized(self) -> bool:
        """Check if storage is initialized and create necessary tables if not.

        Returns:
            bool: True if storage is initialized, False otherwise
        """
        self.connect()
        try:
            # Check if Elasticsearch is accessible
            self.es.info()

            # Check if version table exists
            version_table = self._index_name(self.schema.version_table_name)
            if not self.es.indices.exists(index=version_table):
                # Create version table
                settings = {
                    "number_of_shards": self.config.number_of_shards,
                    "number_of_replicas": self.config.number_of_replicas,
                    "refresh_interval": self.config.refresh_interval
                }
                mappings = {
                    "properties": {
                        "version": {"type": "keyword"},
                        "engine_version": {"type": "keyword"},
                        "schema_name": {"type": "keyword"},
                        "version_hash": {"type": "keyword"},
                        "schema": {"type": "text"},
                        "inserted_at": {"type": "date"}
                    }
                }
                self.es.indices.create(
                    index=version_table,
                    settings=settings,
                    mappings=mappings,
                    ignore=400
                )

                # If we have a schema, store it immediately
                if hasattr(self, 'schema'):
                    schema_doc = {
                        "version": self.schema.version,
                        "engine_version": self.schema.ENGINE_VERSION,
                        "schema_name": self.schema.name,
                        "version_hash": self.schema.stored_version_hash,
                        "schema": json.dumps(self.schema.to_dict()),
                        "inserted_at": pendulum.now().isoformat()
                    }

                    try:
                        self.es.index(
                            index=version_table,
                            document=schema_doc,
                            refresh=True
                        )
                        print(f"Schema with hash {self.schema.stored_version_hash} stored in Elasticsearch")

                    except Exception as e:
                        print(f"Error storing schema in Elasticsearch: {str(e)}")
                        return False

            # Check if loads table exists
            loads_table = self._index_name(self.schema.loads_table_name)
            if not self.es.indices.exists(index=loads_table):
                settings = {
                    "number_of_shards": self.config.number_of_shards,
                    "number_of_replicas": self.config.number_of_replicas,
                    "refresh_interval": self.config.refresh_interval
                }
                mappings = {
                    "properties": {
                        "load_id": {"type": "keyword"},
                        "schema_name": {"type": "keyword"},
                        "status": {"type": "integer"},
                        "inserted_at": {"type": "date"},
                        "schema_version_hash": {"type": "keyword"}
                    }
                }
                self.es.indices.create(
                    index=loads_table,
                    settings=settings,
                    mappings=mappings,
                    ignore=400
                )

            # Check if state table exists
            state_table = self._index_name(self.schema.state_table_name)
            if not self.es.indices.exists(index=state_table):
                settings = {
                    "number_of_shards": self.config.number_of_shards,
                    "number_of_replicas": self.config.number_of_replicas,
                    "refresh_interval": self.config.refresh_interval
                }
                mappings = {
                    "properties": {
                        "version": {"type": "keyword"},
                        "engine_version": {"type": "keyword"},
                        "pipeline_name": {"type": "keyword"},
                        "state": {"type": "text"},
                        "created_at": {"type": "date"},
                        "_dlt_load_id": {"type": "keyword"}
                    }
                }
                self.es.indices.create(
                    index=state_table,
                    settings=settings,
                    mappings=mappings,
                    ignore=400
                )

            return True
        except Exception as e:
            print(f"Error checking storage initialization: {str(e)}")
            return False

    def drop_storage(self) -> None:
        self.connect()
        if self.config.dataset_name:
            prefix = self.dataset_prefix
            indices = self.es.indices.get(f"{prefix}*").keys()
            for idx in indices:
                self.es.indices.delete(index=idx)
        self.close()

    def get_stored_schema(self, schema_name: str = None) -> Optional[StorageSchemaInfo]:
        """Get the latest stored schema for the given schema name.

        Args:
            schema_name: Name of the schema to retrieve. If None, returns the latest schema.

        Returns:
            The stored schema or None if not found
        """
        # First try to get schema from local storage
        if hasattr(self, 'schema'):
            try:
                # Get schema from parent implementation
                schema = super().get_stored_schema(schema_name)
                if schema is not None:
                    return schema
            except Exception as e:
                print(f"Error reading local schema: {str(e)}")

        # If not found locally, try Elasticsearch
        loads_table = self._index_name(self.schema.loads_table_name)
        version_table = self._index_name(self.schema.version_table_name)

        try:
            # First, get the latest schema version hash from loads table
            loads_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"schema_name": schema_name or self.schema.name}},
                            {"term": {"status": 0}}  # Completed loads
                        ]
                    }
                },
                "sort": [
                    {"inserted_at": {"order": "desc"}}
                ],
                "size": 1
            }

            loads_response = self.es.search(
                index=loads_table,
                body=loads_query
            )

            loads_hits = loads_response["hits"]["hits"]
            if not loads_hits:
                print(f"No completed loads found for schema {schema_name}")
                return None

            # Get the schema version hash
            schema_version_hash = loads_hits[0]["_source"]["schema_version_hash"]

            # Now get the actual schema from version table
            version_query = {
                "query": {
                    "term": {
                        "version_hash": schema_version_hash
                    }
                },
                "size": 1
            }

            version_response = self.es.search(
                index=version_table,
                body=version_query
            )

            version_hits = version_response["hits"]["hits"]
            if not version_hits:
                print(f"No schema found for version hash {schema_version_hash}")
                return None

            # Get the schema document
            schema_doc = version_hits[0]["_source"]
            if "schema" not in schema_doc:
                print(f"Schema document does not contain schema data: {schema_doc}")
                return None

            # Create StorageSchemaInfo with schema as JSON string
            schema_info = StorageSchemaInfo(
                version_hash=schema_doc["version_hash"],
                schema_name=schema_doc["schema_name"],
                version=schema_doc["version"],
                engine_version=schema_doc["engine_version"],
                inserted_at=pendulum.parse(schema_doc["inserted_at"]),
                schema=schema_doc["schema"]  # Already a JSON string from Elasticsearch
            )

            # Store the schema locally using parent implementation
            if hasattr(self, 'schema'):
                try:
                    super().update_stored_schema()
                except Exception as e:
                    print(f"Error storing schema locally: {str(e)}")

            return schema_info

        except Exception as e:
            print(f"Error retrieving schema from Elasticsearch: {str(e)}")
            return None

    def get_stored_schema_by_hash(self, schema_hash: str) -> Optional[TStoredSchema]:
        """Get a stored schema by its hash.

        Args:
            schema_hash: Hash of the schema to retrieve

        Returns:
            The stored schema or None if not found
        """
        version_table = self._index_name(self.schema.version_table_name)

        # Build the query
        query = {
            "query": {
                "term": {
                    "version_hash": schema_hash
                }
            },
            "size": 1
        }

        try:
            response = self.es.search(
                index=version_table,
                body=query
            )

            hits = response["hits"]["hits"]
            if not hits:
                return None

            # Get the schema document
            schema_doc = hits[0]["_source"]
            return json.loads(schema_doc["schema"])

        except Exception as e:
            print(f"Error retrieving schema from Elasticsearch: {str(e)}")
            return None

    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Retrieves the latest completed state for a pipeline from Elasticsearch.

        Args:
            pipeline_name: Name of the pipeline to get state for

        Returns:
            StateInfo object containing the latest state, or None if no state found
        """
        # Get the state table name
        state_table = self._index_name(self.schema.state_table_name)

        # Build the query to get the latest state
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"pipeline_name": pipeline_name}}
                    ]
                }
            },
            "sort": [
                {"created_at": {"order": "desc"}}
            ],
            "size": 1
        }

        try:
            # Search in the state table
            response = self.es.search(
                index=state_table,
                body=query
            )

            hits = response["hits"]["hits"]
            if not hits:
                return None

            # Get the latest state document
            state_doc = hits[0]["_source"]

            # Create StateInfo object
            return StateInfo(
                version=state_doc["version"],
                engine_version=state_doc["engine_version"],
                pipeline_name=state_doc["pipeline_name"],
                state=state_doc["state"],
                created_at=pendulum.parse(state_doc["created_at"]),
                version_hash=state_doc.get("version_hash"),
                _dlt_load_id=state_doc.get("_dlt_load_id")
            )

        except Exception as e:
            print(f"Error retrieving state from Elasticsearch: {str(e)}")
            return None

    def _update_index_mappings(self, table_name: str, new_mappings: dict) -> None:
        """Update the mappings for an existing index.

        Args:
            table_name: Name of the table/index to update
            new_mappings: New mappings to apply
        """
        index = self._index_name(table_name)
        if not self.es.indices.exists(index=index):
            return

        try:
            # Get current mappings
            current_mappings = self.es.indices.get_mapping(index=index)[index]['mappings']
            current_properties = current_mappings.get('properties', {})

            # Only add new fields, preserve existing field types
            new_properties = {}
            for field, mapping in new_mappings.get('properties', {}).items():
                if field not in current_properties:
                    new_properties[field] = mapping

            if new_properties:
                # Update the mappings with only new fields
                self.es.indices.put_mapping(
                    index=index,
                    body={'properties': new_properties}
                )
                print(f"Added new fields to index {index}: {list(new_properties.keys())}")
            else:
                print(f"No new fields to add to index {index}")

        except Exception as e:
            print(f"Error updating mappings for index {index}: {str(e)}")
            raise

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> Optional[TSchemaTables]:
        """Updates storage to the current schema.

        Args:
            only_tables: Updates only listed tables. Defaults to None.
            expected_update: Update that is expected to be applied to the destination

        Returns:
            Optional[TSchemaTables]: Returns an update that was applied at the destination.
        """
        # Call parent implementation first to ensure local schema storage
        super().update_stored_schema(only_tables, expected_update)

        # Get the schema version table name
        version_table = self._index_name(self.schema.version_table_name)

        # Check if schema with this hash already exists
        schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is not None:
            print(f"Schema with hash {self.schema.stored_version_hash} already exists in storage")
            # Even if schema exists, we should update mappings for new fields
            for table_name, table in self.schema.tables.items():
                if only_tables and table_name not in only_tables:
                    continue
                    
                # Create mappings for the table
                mappings = {
                    "properties": {}
                }
                
                # Add table-specific fields
                for field, field_type in table["columns"].items():
                    mappings["properties"][field] = self._get_elasticsearch_type(field_type)
                
                # Update the index mappings
                self._update_index_mappings(table_name, mappings)
            return expected_update

        # Store the schema
        schema_doc = {
            "version": self.schema.version,
            "engine_version": self.schema.ENGINE_VERSION,
            "schema_name": self.schema.name,
            "version_hash": self.schema.stored_version_hash,
            "schema": json.dumps(self.schema.to_dict()),
            "inserted_at": pendulum.now().isoformat()
        }

        try:
            # Index the schema document
            self.es.index(
                index=version_table,
                document=schema_doc,
                refresh=True  # Force refresh to make the document immediately searchable
            )
            print(f"Schema with hash {self.schema.stored_version_hash} stored in Elasticsearch")

            # Update mappings for all tables in the schema
            for table_name, table in self.schema.tables.items():
                if only_tables and table_name not in only_tables:
                    continue
                    
                # Create mappings for the table
                mappings = {
                    "properties": {}
                }
                
                # Add table-specific fields
                for field, field_type in table["columns"].items():
                    mappings["properties"][field] = self._get_elasticsearch_type(field_type)
                
                # Update the index mappings
                self._update_index_mappings(table_name, mappings)

        except Exception as e:
            print(f"Error storing schema: {str(e)}")
            raise

        return expected_update

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        return LoadElasticJob(file_path, self._index_name(table["name"]))


    def complete_load(self, load_id: str) -> None:
        from datetime import datetime
        values = [
            load_id,
            self.schema.name,
            0,
            datetime.utcnow().isoformat(),
            self.schema.version_hash,
        ]
        assert len(values) == len(self.loads_table_fields)
        doc = {k: v for k, v in zip(self.loads_table_fields, values)}
        self.es.index(index=self._index_name(self.schema.loads_table_name), document=doc)

    def __enter__(self) -> "ElasticsearchClient":
        self.connect()
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        self.close()

