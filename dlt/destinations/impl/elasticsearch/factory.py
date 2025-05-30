import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.elasticsearch.configuration import (
    ElasticsearchCredentials,
    ElasticsearchClientConfiguration,
)

if t.TYPE_CHECKING:
    from dlt.destinations.impl.elasticsearch.elasticsearch import (
        ElasticsearchClient,
    )


class elasticsearch(Destination[ElasticsearchClientConfiguration, "ElasticsearchClient"]):
    spec = ElasticsearchClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "jsonl"
        caps.supported_loader_file_formats = ["jsonl"]
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.supports_ddl_transactions = False
        caps.supported_replace_strategies = ["truncate-and-insert"]
        caps.supported_merge_strategies = ["delete-insert"]
        return caps

    @property
    def client_class(self) -> t.Type["ElasticsearchClient"]:
        from dlt.destinations.impl.elasticsearch.elasticsearch import (
            ElasticsearchClient,
        )

        return ElasticsearchClient

    def __init__(
        self,
        credentials: t.Union[ElasticsearchCredentials, t.Dict[str, t.Any], str] = None,
        dataset_name: str = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            dataset_name=dataset_name,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )

elasticsearch.register()