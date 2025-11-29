"""
Generic API ingestion patterns.

4 flexible API ingestors for REST, GraphQL, and webhook patterns.
"""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from .base import BaseBatchIngestor, BaseStreamingIngestor
from .factory import register_ingestor
import requests
import time


@register_ingestor("paginated_rest_json")
class PaginatedRestJsonIngestion(BaseBatchIngestor):
    """
    Generic paginated REST API with JSON response.

    Sub-Group: Generic API
    Tags: rest-api, json, pagination, cursor, offset, generic

    Supports cursor, offset, and page-number pagination.

    Config example:
        {
            "source_name": "generic_api_data",
            "api_url": "https://api.example.com/v1/data",
            "auth_type": "bearer",
            "auth_token": "${API_TOKEN}",
            "pagination_type": "cursor",
            "pagination_param": "cursor",
            "limit_param": "limit",
            "limit_value": 100,
            "target_table": "api_data",
            "watermark_column": "updated_at"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        api_url = self.config["api_url"]
        auth_type = self.config.get("auth_type", "bearer")
        auth_token = self.config.get("auth_token")
        pagination_type = self.config.get("pagination_type", "cursor")
        pagination_param = self.config.get("pagination_param", "cursor")
        limit_param = self.config.get("limit_param", "limit")
        limit_value = self.config.get("limit_value", 100)
        
        # Build headers
        headers = {"Content-Type": "application/json"}
        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {auth_token}"
        
        all_records = []
        cursor = None
        page = 1
        
        while True:
            # Build params
            params = {limit_param: limit_value}
            
            if pagination_type == "cursor" and cursor:
                params[pagination_param] = cursor
            elif pagination_type == "offset":
                params[pagination_param] = (page - 1) * limit_value
            elif pagination_type == "page":
                params[pagination_param] = page
            
            # Add watermark filter
            if watermark and self.watermark_column:
                params[f"{self.watermark_column}_gt"] = watermark
            
            # Make request
            response = requests.get(api_url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract records (configurable path)
            records_path = self.config.get("records_path", "data")
            records = data.get(records_path, [])
            
            if not records:
                break
            
            all_records.extend(records)
            
            # Get next cursor/page
            if pagination_type == "cursor":
                next_cursor_path = self.config.get("next_cursor_path", "next_cursor")
                cursor = data.get(next_cursor_path)
                if not cursor:
                    break
            else:
                page += 1
                # Check if we have more pages
                has_more = data.get("has_more", len(records) >= limit_value)
                if not has_more:
                    break
            
            # Rate limiting
            time.sleep(self.config.get("rate_limit_delay", 0.1))
        
        # Convert to DataFrame
        if all_records:
            df = self.spark.createDataFrame(all_records)
        else:
            df = self.spark.createDataFrame([], "dummy STRING")
        
        return df


@register_ingestor("paginated_rest_csv")
class PaginatedRestCsvIngestion(BaseBatchIngestor):
    """
    Generic REST API returning CSV.

    Sub-Group: Generic API
    Tags: rest-api, csv, generic, export

    Config example:
        {
            "source_name": "csv_api_data",
            "api_url": "https://api.example.com/v1/export",
            "auth_token": "${API_TOKEN}",
            "target_table": "csv_api_data",
            "csv_options": {"header": "true", "inferSchema": "true"}
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        import pandas as pd
        from io import StringIO
        
        api_url = self.config["api_url"]
        auth_token = self.config.get("auth_token")
        
        headers = {"Authorization": f"Bearer {auth_token}"}
        
        # Add watermark to query
        params = {}
        if watermark and self.watermark_column:
            params[f"{self.watermark_column}_gt"] = watermark
        
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()
        
        # Parse CSV
        csv_data = StringIO(response.text)
        pandas_df = pd.read_csv(csv_data)
        
        # Convert to Spark DataFrame
        df = self.spark.createDataFrame(pandas_df)
        
        return df


@register_ingestor("graphql_paginated")
class GraphQLPaginatedIngestion(BaseBatchIngestor):
    """
    Generic GraphQL API with cursor pagination.

    Sub-Group: Generic API
    Tags: graphql, pagination, cursor, generic

    Config example:
        {
            "source_name": "graphql_data",
            "graphql_url": "https://api.example.com/graphql",
            "query_template": "query($cursor: String) { items(after: $cursor) { nodes { id name } pageInfo { endCursor hasNextPage } } }",
            "auth_token": "${GRAPHQL_TOKEN}",
            "target_table": "graphql_data",
            "records_path": "data.items.nodes",
            "cursor_path": "data.items.pageInfo.endCursor",
            "has_next_path": "data.items.pageInfo.hasNextPage"
        }
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        graphql_url = self.config["graphql_url"]
        query_template = self.config["query_template"]
        auth_token = self.config.get("auth_token")
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {auth_token}"
        }
        
        all_records = []
        cursor = None
        
        while True:
            # Build GraphQL variables
            variables = {}
            if cursor:
                variables["cursor"] = cursor
            if watermark:
                variables["watermark"] = watermark
            
            # Make GraphQL request
            payload = {
                "query": query_template,
                "variables": variables
            }
            
            response = requests.post(graphql_url, headers=headers, json=payload)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract records using path
            records_path = self.config.get("records_path", "data.items")
            records = self._get_nested(data, records_path)
            
            if not records:
                break
            
            all_records.extend(records)
            
            # Get next cursor
            cursor_path = self.config.get("cursor_path", "data.pageInfo.endCursor")
            has_next_path = self.config.get("has_next_path", "data.pageInfo.hasNextPage")
            
            cursor = self._get_nested(data, cursor_path)
            has_next = self._get_nested(data, has_next_path)
            
            if not has_next:
                break
            
            time.sleep(0.1)
        
        # Convert to DataFrame
        if all_records:
            df = self.spark.createDataFrame(all_records)
        else:
            df = self.spark.createDataFrame([], "dummy STRING")
        
        return df
    
    def _get_nested(self, data: dict, path: str):
        """Get nested value from dict using dot notation."""
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value


@register_ingestor("webhook_to_delta")
class WebhookToDeltaIngestion(BaseStreamingIngestor):
    """
    HTTP webhook receiver -> raw bronze table.

    Sub-Group: Generic API
    Tags: webhook, streaming, kafka, delta, http

    Note: This requires a separate webhook receiver service
    that writes to a Kafka topic or Delta table.

    Config example:
        {
            "source_name": "webhooks",
            "connection_name": "kafka",
            "topic": "webhooks-raw",
            "target_table": "webhooks_raw",
            "checkpoint_location": "/mnt/checkpoints/webhooks"
        }
    """
    
    def read_stream(self) -> DataFrame:
        from sparkle.connections import Connection
        
        conn_name = self.config["connection_name"]
        topic = self.config["topic"]
        
        conn = Connection.get(conn_name, self.spark, env=self.config.get("connection_env", self.env))
        
        # Read from Kafka topic fed by webhook receiver
        stream_df = conn.read_stream(topic=topic)
        
        # Add ingestion timestamp
        stream_df = stream_df.withColumn("ingestion_time", current_timestamp())
        
        return stream_df
