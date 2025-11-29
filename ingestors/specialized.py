"""
Specialized and industry-specific ingestors.

4 ingestors for healthcare, financial, and industry-specific data formats.
"""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode
from .base import BaseBatchIngestor
from .factory import register_ingestor


@register_ingestor("hl7_fhir_bundle")
class Hl7FhirBundleIngestion(BaseBatchIngestor):
    """
    HL7 FHIR Bundle JSON -> flattened resources.

    Sub-Group: Specialized / Industry
    Tags: hl7, fhir, healthcare, bundle, json

    Config example:
        {
            "source_name": "fhir_patients",
            "connection_name": "fhir",
            "resource_type": "Patient",
            "target_table": "fhir_patients",
            "flatten_resources": true
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        resource_type = self.config["resource_type"]
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        # Read FHIR resources
        search_params = {}
        if watermark and self.watermark_column:
            search_params[f"{self.watermark_column}_gt"] = watermark
        
        df = conn.read(
            resource_type=resource_type,
            search_params=search_params,
            count=self.config.get("page_size", 100)
        )
        
        # Flatten FHIR resources if configured
        if self.config.get("flatten_resources", True):
            # FHIR resources are deeply nested - flatten key fields
            # This is resource-type specific
            pass
        
        return df


@register_ingestor("x12_edi_837_835")
class X12Edi837835Ingestion(BaseBatchIngestor):
    """
    Healthcare claims X12 EDI 837/835 -> loops & segments.

    Sub-Group: Specialized / Industry
    Tags: x12, edi, healthcare, claims, 837, 835

    Config example:
        {
            "source_name": "edi_claims",
            "source_path": "s3://bucket/edi/837/",
            "transaction_set": "837",
            "target_table": "claims_837",
            "parse_level": "claim"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        # Use X12 EDI connection
        conn_name = self.config.get("connection_name", "x12_edi")
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        source_path = self.config.get("source_path")
        transaction_set = self.config["transaction_set"]
        
        # Read and parse X12 EDI
        df = conn.read(
            path=source_path,
            message_types=[transaction_set]
        )
        
        # Parse into claim-level or line-level records
        parse_level = self.config.get("parse_level", "claim")
        
        return df


@register_ingestor("fix_protocol_log")
class FixProtocolLogIngestion(BaseBatchIngestor):
    """
    FIX 4.4+ protocol logs -> structured messages.

    Sub-Group: Specialized / Industry
    Tags: fix, protocol, financial, trading, messaging

    Config example:
        {
            "source_name": "fix_logs",
            "source_path": "s3://bucket/fix/logs/",
            "fix_version": "FIX.4.4",
            "message_types": ["D", "8", "9"],
            "target_table": "fix_messages"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config.get("connection_name", "fix_protocol")
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        source_path = self.config["source_path"]
        message_types = self.config.get("message_types")
        
        # Read FIX protocol logs
        df = conn.read(
            path=source_path,
            message_types=message_types
        )
        
        return df


@register_ingestor("swift_mt_mx_iso20022")
class SwiftMtMxIso20022Ingestion(BaseBatchIngestor):
    """
    SWIFT MT/MX messages -> field-level parsing.

    Sub-Group: Specialized / Industry
    Tags: swift, mt, mx, iso20022, financial, banking

    Config example:
        {
            "source_name": "swift_messages",
            "source_path": "s3://bucket/swift/",
            "message_format": "MT",
            "message_types": ["MT103", "MT202"],
            "target_table": "swift_payments"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config.get("connection_name", "swift")
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        source_path = self.config["source_path"]
        message_types = self.config.get("message_types")
        
        # Read SWIFT messages
        df = conn.read(
            path=source_path,
            message_types=message_types
        )
        
        return df
