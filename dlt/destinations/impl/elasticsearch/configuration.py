import dataclasses
from typing import Optional, Final

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination.client import DestinationClientConfiguration
from dlt.common.utils import digest128


@configspec
class ElasticsearchCredentials(CredentialsConfiguration):
    host: str = "localhost"
    port: int = 9200
    protocol: str = "http"
    username: str = None
    password: str = None
    verify_certs: bool = True

    def __str__(self) -> str:
        """Return connection string in format: elasticsearch://[username:***@]host[:port]"""
        conn_str = "elasticsearch://"
        if self.username:
            conn_str += self.username
            if self.password:
                conn_str += ":***"  # Mask password for security
            conn_str += "@"
        conn_str += f"{self.host}:{self.port}"

        return conn_str


@configspec
class ElasticsearchClientConfiguration(DestinationClientConfiguration):
    destination_type: Final[str] = dataclasses.field(
        default="elasticsearch", init=False, repr=False, compare=False
    )

    dataset_name: Optional[str] = None
    batch_size: int = 1000
    credentials: ElasticsearchCredentials = None
    number_of_shards: int = 1
    number_of_replicas: int = 1
    refresh_interval: str = "1s"
    max_retries: int = 3
    retry_on_timeout: bool = True
    timeout: int = 30

    def fingerprint(self) -> str:
        return digest128(self.credentials.host)