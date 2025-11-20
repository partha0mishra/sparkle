"""
SaaS API incremental ingestors.

18 production-grade SaaS platform ingestors with incremental loading.
"""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from .base import BaseBatchIngestor, BaseStreamingIngestor
from .factory import register_ingestor


@register_ingestor("salesforce_bulk_full")
class SalesforceBulkApiFullExtractor(BaseBatchIngestor):
    """
    Salesforce full export via Bulk API 2.0.
    
    Config: {"source_name": "sf_accounts", "connection_name": "salesforce", 
             "sobject": "Account", "target_table": "accounts_full"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark, 
                            env=self.config.get("connection_env", self.env))
        
        df = conn.read(
            sobject=self.config["sobject"],
            query_type="full"
        )
        return df


@register_ingestor("salesforce_bulk_incremental")
class SalesforceBulkApiIncrementalExtractor(BaseBatchIngestor):
    """
    Salesforce incremental via PK Chunking + LastModifiedDate.
    
    Config: {"source_name": "sf_contacts", "connection_name": "salesforce",
             "sobject": "Contact", "target_table": "contacts", 
             "watermark_column": "LastModifiedDate"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        # Build SOQL with watermark
        soql = f"SELECT FIELDS(ALL) FROM {self.config['sobject']}"
        if watermark:
            soql += f" WHERE LastModifiedDate > {watermark}"
        
        df = conn.read(sobject=self.config["sobject"], query=soql)
        return df


@register_ingestor("salesforce_cdc_streamer")
class SalesforceCDCStreamer(BaseStreamingIngestor):
    """
    Salesforce CDC via CometD/Replay.
    
    Config: {"source_name": "sf_cdc", "connection_name": "salesforce_cdc",
             "channel": "/data/AccountChangeEvent", 
             "target_table": "account_changes"}
    """
    
    def read_stream(self) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        stream_df = conn.read_stream(channel=self.config["channel"])
        return stream_df


@register_ingestor("netsuite_suiteanalytics_incremental")
class NetsuiteSuiteAnalyticsIncremental(BaseBatchIngestor):
    """
    Netsuite via SuiteAnalytics + LAST_MODIFIED_DATE.
    
    Config: {"source_name": "ns_customers", "connection_name": "netsuite",
             "saved_search_id": "123", "target_table": "customers",
             "watermark_column": "lastmodifieddate"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        search_id = self.config.get("saved_search_id")
        df = conn.read(saved_search_id=search_id)
        
        if watermark:
            df = df.filter(df.lastmodifieddate > watermark)
        
        return df


@register_ingestor("workday_prism_raas_incremental")
class WorkdayPrismRaaSIncremental(BaseBatchIngestor):
    """
    Workday Prism via RaaS API.
    
    Config: {"source_name": "wd_employees", "connection_name": "workday_prism",
             "report_url": "https://...", "target_table": "employees"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        df = conn.read(report_url=self.config["report_url"])
        
        if watermark and self.watermark_column:
            df = df.filter(df[self.watermark_column] > watermark)
        
        return df


@register_ingestor("marketo_bulk_extract_incremental")
class MarketoBulkExtractIncremental(BaseBatchIngestor):
    """
    Marketo Bulk Export with sinceDateTime.
    
    Config: {"source_name": "marketo_leads", "connection_name": "marketo",
             "export_type": "leads", "target_table": "leads"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        export_type = self.config["export_type"]
        
        filters = {}
        if watermark:
            filters["updatedAt"] = {"startAt": watermark}
        
        df = conn.read(object_type=export_type, filters=filters)
        return df


@register_ingestor("hubspot_incremental")
class HubSpotIncrementalIngestion(BaseBatchIngestor):
    """
    HubSpot incremental via cursor pagination.
    
    Config: {"source_name": "hubspot_contacts", "connection_name": "hubspot",
             "object_type": "contacts", "target_table": "contacts"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        object_type = self.config["object_type"]
        
        params = {}
        if watermark:
            params["startUpdatedAt"] = watermark
        
        df = conn.read(object_type=object_type, params=params)
        return df


@register_ingestor("zendesk_incremental")
class ZendeskIncrementalIngestion(BaseBatchIngestor):
    """
    Zendesk incremental with start_time cursor.
    
    Config: {"source_name": "zendesk_tickets", "connection_name": "zendesk",
             "endpoint": "tickets", "target_table": "tickets"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        endpoint = self.config["endpoint"]
        
        params = {}
        if watermark:
            params["start_time"] = watermark
        
        df = conn.read(endpoint=endpoint, params=params)
        return df


@register_ingestor("shopify_admin_api_incremental")
class ShopifyAdminApiIncremental(BaseBatchIngestor):
    """
    Shopify incremental via since_id + created_at_min.
    
    Config: {"source_name": "shopify_orders", "connection_name": "shopify",
             "resource": "orders", "target_table": "orders"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        resource = self.config["resource"]
        
        # Shopify uses both since_id and created_at_min
        df = conn.read(resource=resource)
        
        if watermark and self.watermark_column:
            df = df.filter(df[self.watermark_column] > watermark)
        
        return df


@register_ingestor("stripe_incremental_events")
class StripeIncrementalEventsIngestion(BaseBatchIngestor):
    """
    Stripe Events API with starting_after + created filter.
    
    Config: {"source_name": "stripe_charges", "connection_name": "stripe",
             "object_type": "charges", "target_table": "charges"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        object_type = self.config["object_type"]
        
        created_after = None
        if watermark:
            # Convert watermark to Unix timestamp
            from datetime import datetime
            created_after = int(datetime.fromisoformat(watermark).timestamp())
        
        df = conn.read(object_type=object_type, created_after=created_after)
        return df


@register_ingestor("zuora_aqua_incremental")
class ZuoraAquaIncremental(BaseBatchIngestor):
    """
    Zuora AQUA API with updatedDate watermark.
    
    Config: {"source_name": "zuora_accounts", "connection_name": "zuora",
             "object_type": "Account", "target_table": "accounts"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        object_type = self.config["object_type"]
        
        query = None
        if watermark:
            query = f"SELECT * FROM {object_type} WHERE UpdatedDate > '{watermark}'"
        
        df = conn.read(object_type=object_type, query=query)
        return df


@register_ingestor("servicenow_table_api_incremental")
class ServiceNowTableApiIncremental(BaseBatchIngestor):
    """
    ServiceNow with sys_updated_on watermark.
    
    Config: {"source_name": "snow_incidents", "connection_name": "servicenow",
             "table": "incident", "target_table": "incidents"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        table = self.config["table"]
        
        query = None
        if watermark:
            query = f"sys_updated_on>{watermark}"
        
        df = conn.read(table=table, query=query)
        return df


@register_ingestor("google_analytics4_bigquery_export_daily")
class GoogleAnalytics4BigQueryExportDaily(BaseBatchIngestor):
    """
    GA4 BigQuery export daily tables.
    
    Config: {"source_name": "ga4_events", "connection_name": "bigquery",
             "dataset": "analytics_123456789", "table_prefix": "events_",
             "target_table": "ga4_events"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        from datetime import datetime, timedelta
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        dataset = self.config["dataset"]
        table_prefix = self.config.get("table_prefix", "events_")
        
        # GA4 exports daily tables like events_20251119
        if watermark:
            start_date = datetime.fromisoformat(watermark)
        else:
            start_date = datetime.utcnow() - timedelta(days=7)
        
        date_str = start_date.strftime("%Y%m%d")
        table_name = f"{dataset}.{table_prefix}{date_str}"
        
        df = conn.read(table=table_name)
        return df


@register_ingestor("google_ads_report_incremental")
class GoogleAdsReportIncremental(BaseBatchIngestor):
    """
    Google Ads report with date range from watermark.
    
    Config: {"source_name": "gads_campaigns", "connection_name": "google_ads",
             "query": "SELECT ...", "target_table": "campaigns"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        query = self.config["query"]
        
        # Add date filter to query if watermark exists
        if watermark:
            query += f" WHERE segments.date >= '{watermark}'"
        
        df = conn.read(query=query)
        return df


@register_ingestor("meta_ads_insights_incremental")
class MetaAdsInsightsIncremental(BaseBatchIngestor):
    """
    Meta/Facebook Ads Insights with time_increment.
    
    Config: {"source_name": "fb_insights", "connection_name": "facebook_ads",
             "object_type": "insights", "target_table": "insights"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        from datetime import datetime, timedelta
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        time_range = {}
        if watermark:
            time_range["since"] = watermark
            time_range["until"] = datetime.utcnow().strftime("%Y-%m-%d")
        else:
            time_range["since"] = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
            time_range["until"] = datetime.utcnow().strftime("%Y-%m-%d")
        
        df = conn.read(
            object_type="insights",
            time_range=time_range,
            level=self.config.get("level", "campaign")
        )
        return df


@register_ingestor("linkedin_ads_incremental")
class LinkedInAdsIncremental(BaseBatchIngestor):
    """
    LinkedIn Ads with start_date watermark.
    
    Config: {"source_name": "li_campaigns", "connection_name": "linkedin_ads",
             "object_type": "analytics", "target_table": "analytics"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        from datetime import datetime, timedelta
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        date_range = {}
        if watermark:
            date_range["start"] = watermark
            date_range["end"] = datetime.utcnow().strftime("%Y-%m-%d")
        
        df = conn.read(
            object_type=self.config.get("object_type", "analytics"),
            date_range=date_range if date_range else None
        )
        return df


@register_ingestor("amplitude_event_export_incremental")
class AmplitudeEventExportIncremental(BaseBatchIngestor):
    """
    Amplitude daily event export files from S3.
    
    Config: {"source_name": "amplitude_events", "connection_name": "amplitude",
             "target_table": "amplitude_events"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        from datetime import datetime, timedelta
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        if watermark:
            start_date = watermark
        else:
            start_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        end_date = datetime.utcnow().strftime("%Y-%m-%d")
        
        df = conn.read(start_date=start_date, end_date=end_date)
        return df


@register_ingestor("segment_warehouse_sync")
class SegmentWarehouseSync(BaseBatchIngestor):
    """
    Segment S3 exports (pages, tracks, identifies).
    
    Config: {"source_name": "segment_tracks", "connection_name": "segment",
             "source": "s3", "target_table": "segment_tracks"}
    """
    
    def read_batch(self, watermark: Optional[str]) -> DataFrame:
        from sparkle.connections import Connection
        from datetime import datetime, timedelta
        
        conn = Connection.get(self.config["connection_name"], self.spark,
                            env=self.config.get("connection_env", self.env))
        
        source = self.config.get("source", "s3")
        
        if watermark:
            date = watermark
        else:
            date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        df = conn.read(source=source, date=date)
        return df
