from typing import List, Dict, Any, Optional
from datetime import date, datetime
import pytz

from dlt.common.json import json
from dlt.common.schema.utils import (
    get_columns_names_with_prop
)
from dlt.common.destination.client import (
    RunnableLoadJob,

)
from dlt.common.storages import FileStorage

class LoadElasticJob(RunnableLoadJob):
    def __init__(self, file_path: str, index_name: str) -> None:
        super().__init__(file_path)
        self._index_name = index_name
        self._job_client: "ElasticsearchClient" = None

    @staticmethod
    def convert_doc_fields(doc: Dict[str, Any]) -> Dict[str, Any]:
        """Convert document fields to Elasticsearch compatible format."""
        for k, v in doc.items():
            if isinstance(v, (datetime, date)):
                # Ensure datetime is timezone aware
                if isinstance(v, datetime) and v.tzinfo is None:
                    v = v.replace(tzinfo=pytz.UTC)
                doc[k] = v.isoformat()
        return doc

    def generate_document_id(self, doc: Dict[str, Any]) -> Optional[str]:
        """Generate a document ID based on primary keys or unique constraints."""
        if self._load_table.get("write_disposition") != "merge":
            return None

        keys = get_columns_names_with_prop(self._load_table, "primary_key")
        if not keys:
            keys = get_columns_names_with_prop(self._load_table, "unique")
        
        if not keys:
            return None

        # Create a deterministic ID from the key values
        key_values = [str(doc.get(k, "")) for k in keys]
        return "_".join(key_values)

    def run(self) -> None:
        es = self._job_client.es
        actions: List[Dict[str, Any]] = []
        
        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            for line in f:
                doc = json.loads(line)
                doc = self.convert_doc_fields(doc)
                
                action = {
                    "_index": self._index_name,
                    "_source": doc
                }
                
                # Generate document ID if needed
                doc_id = self.generate_document_id(doc)
                if doc_id:
                    action["_id"] = doc_id
                
                actions.append(action)
                
                if len(actions) >= self._job_client.config.batch_size:
                    self._job_client.bulk(actions)
                    actions.clear()
        
        if actions:
            self._job_client.bulk(actions)