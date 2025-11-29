from ..base import BasePipeline
from ..tasks.ingest import IngestTask
from ..tasks.write import WriteDeltaTask

class BronzeRawIngestionPipeline(BasePipeline):
    """
    Ingests any source via named ingestor → writes raw bronze Delta with _ingested_at + source provenance
    """
    def build(self) -> 'BasePipeline':
        # 1. Ingest
        self.add_task(IngestTask("ingest_source", {
            "ingestor": self.config.ingestor,
            "ingestor_config": self.config.extra_config.get("ingestor_config", {})
        }))
        
        # 2. Write to Bronze
        self.add_task(WriteDeltaTask("write_bronze", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "write_mode": "append",
            "partition_columns": self.config.partition_columns
        }))
        
        return self
