from ..base import BasePipeline
from ..tasks.ingest import IngestTask
from ..tasks.write import WriteDeltaTask

class BronzeStreamingIngestionPipeline(BasePipeline):
    """
    Same as BronzeRawIngestionPipeline but Structured Streaming with checkpointing and exactly-once to bronze
    """
    def build(self) -> 'BasePipeline':
        # 1. Ingest (Streaming)
        self.add_task(IngestTask("ingest_source_stream", {
            "ingestor": self.config.ingestor,
            "ingestor_config": {
                **self.config.extra_config.get("ingestor_config", {}),
                "streaming": True
            }
        }))
        
        # 2. Write to Bronze (Streaming)
        self.add_task(WriteDeltaTask("write_bronze_stream", {
            "destination_catalog": self.config.destination_catalog,
            "destination_schema": self.config.destination_schema,
            "destination_table": self.config.destination_table,
            "write_mode": "append",
            "partition_columns": self.config.partition_columns,
            "checkpoint_location": self.config.checkpoint_location
        }))
        
        return self
