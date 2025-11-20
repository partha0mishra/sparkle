"""
SaaS platform connections.

Supports: Salesforce, Salesforce CDC, Netsuite, Workday, ServiceNow, Marketo,
HubSpot, Zendesk, Shopify, Stripe, Zuora, Google Analytics 4, Google Ads,
Facebook/Meta Ads, LinkedIn Ads, Amplitude, Segment.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
import requests
import time

from .base import APIConnection
from .factory import register_connection


@register_connection("salesforce_cdc")
@register_connection("sf_cdc")
class SalesforceCDCConnection(APIConnection):
    """
    Salesforce Change Data Capture (CDC) streaming connection.

    Example config:
        {
            "instance_url": "https://mycompany.my.salesforce.com",
            "username": "${SF_USERNAME}",
            "password": "${SF_PASSWORD}",
            "security_token": "${SF_SECURITY_TOKEN}",
            "channel": "/data/ChangeEvents",
            "replay_id": -1
        }

    Usage:
        >>> conn = Connection.get("salesforce_cdc", spark, env="prod")
        >>> stream_df = conn.read_stream(channel="/data/Account ChangeEvent")
    """

    def get_headers(self) -> Dict[str, str]:
        """Get Salesforce auth headers."""
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json"
        }

    def _get_access_token(self) -> str:
        """Authenticate and get access token."""
        if hasattr(self, '_access_token'):
            return self._access_token

        login_url = "https://login.salesforce.com/services/oauth2/token"

        response = requests.post(login_url, data={
            "grant_type": "password",
            "client_id": self.config.get("client_id", ""),
            "client_secret": self.config.get("client_secret", ""),
            "username": self.config["username"],
            "password": f"{self.config['password']}{self.config.get('security_token', '')}"
        })

        response.raise_for_status()
        self._access_token = response.json()["access_token"]
        return self._access_token

    def test(self) -> bool:
        """Test Salesforce CDC connection."""
        try:
            self._get_access_token()
            return True
        except Exception as e:
            self.logger.error(f"Salesforce CDC connection test failed: {e}")
            return False

    def read(self, **kwargs) -> DataFrame:
        """
        Salesforce CDC is streaming-only. Use read_stream().
        """
        raise NotImplementedError(
            "Salesforce CDC is streaming-only. Use read_stream() instead."
        )

    def write(self, df: DataFrame, **kwargs) -> None:
        """CDC is read-only."""
        raise NotImplementedError("Salesforce CDC is read-only")


@register_connection("netsuite")
class NetsuiteConnection(APIConnection):
    """
    Oracle Netsuite connection via SuiteAnalytics Connect / REST API.

    Example config:
        {
            "account_id": "1234567",
            "consumer_key": "${NETSUITE_CONSUMER_KEY}",
            "consumer_secret": "${NETSUITE_CONSUMER_SECRET}",
            "token_id": "${NETSUITE_TOKEN_ID}",
            "token_secret": "${NETSUITE_TOKEN_SECRET}",
            "base_url": "https://1234567.suitetalk.api.netsuite.com"
        }

    Usage:
        >>> conn = Connection.get("netsuite", spark, env="prod")
        >>> df = conn.read(record_type="customer", fields=["entityId", "email"])
    """

    def get_headers(self) -> Dict[str, str]:
        """Get Netsuite OAuth headers."""
        import hashlib
        import hmac
        import base64
        from datetime import datetime
        import secrets

        timestamp = str(int(datetime.utcnow().timestamp()))
        nonce = secrets.token_hex(16)

        # Build OAuth signature (simplified)
        signature_base = f"{self.config['consumer_key']}&{timestamp}&{nonce}"

        signature = hmac.new(
            self.config["consumer_secret"].encode(),
            signature_base.encode(),
            hashlib.sha256
        ).digest()

        encoded_signature = base64.b64encode(signature).decode()

        return {
            "Authorization": f'OAuth realm="{self.config["account_id"]}", '
                           f'oauth_consumer_key="{self.config["consumer_key"]}", '
                           f'oauth_token="{self.config["token_id"]}", '
                           f'oauth_signature_method="HMAC-SHA256", '
                           f'oauth_timestamp="{timestamp}", '
                           f'oauth_nonce="{nonce}", '
                           f'oauth_signature="{encoded_signature}"',
            "Content-Type": "application/json"
        }

    def test(self) -> bool:
        """Test Netsuite connection."""
        try:
            response = requests.get(
                f"{self.base_url}/services/rest/record/v1/metadata-catalog",
                headers=self.get_headers(),
                timeout=30
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Netsuite connection test failed: {e}")
            return False

    def read(
        self,
        record_type: str,
        fields: Optional[List[str]] = None,
        query: Optional[str] = None,
        limit: int = 1000,
        **kwargs
    ) -> DataFrame:
        """
        Read Netsuite records.

        Args:
            record_type: Record type (customer, transaction, etc.)
            fields: List of fields to retrieve
            query: SuiteQL query
            limit: Max records
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        if query:
            url = f"{self.base_url}/services/rest/query/v1/suiteql"
            payload = {"q": query}
        else:
            url = f"{self.base_url}/services/rest/record/v1/{record_type}"
            payload = {}
            if fields:
                payload["fields"] = fields

        headers = self.get_headers()

        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        data = response.json()
        items = data.get("items", [data])

        pdf = pd.DataFrame(items[:limit])
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"record_type": record_type, "records": len(items)})

        return df

    def write(self, df: DataFrame, record_type: str, **kwargs) -> None:
        """Write to Netsuite (creates/updates records)."""
        headers = self.get_headers()
        url = f"{self.base_url}/services/rest/record/v1/{record_type}"

        records = [row.asDict() for row in df.collect()]

        for record in records:
            response = requests.post(url, headers=headers, json=record)
            response.raise_for_status()

        self.emit_lineage("write", {"record_type": record_type, "records": len(records)})


@register_connection("workday")
@register_connection("workday_prism")
class WorkdayPrismConnection(APIConnection):
    """
    Workday Prism Analytics connection.

    Example config:
        {
            "tenant_url": "https://wd2-impl.workday.com",
            "username": "${WORKDAY_USER}",
            "password": "${WORKDAY_PASSWORD}",
            "client_id": "${WORKDAY_CLIENT_ID}",
            "client_secret": "${WORKDAY_CLIENT_SECRET}",
            "refresh_token": "${WORKDAY_REFRESH_TOKEN}"
        }

    Usage:
        >>> conn = Connection.get("workday", spark, env="prod")
        >>> df = conn.read(dataset="Employee_Data")
    """

    def get_headers(self) -> Dict[str, str]:
        """Get Workday OAuth headers."""
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json"
        }

    def _get_access_token(self) -> str:
        """Get OAuth access token."""
        if hasattr(self, '_access_token'):
            return self._access_token

        token_url = f"{self.base_url}/ccx/oauth2/{self.config['tenant_id']}/token"

        response = requests.post(token_url, data={
            "grant_type": "refresh_token",
            "refresh_token": self.config["refresh_token"],
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"]
        })

        response.raise_for_status()
        self._access_token = response.json()["access_token"]
        return self._access_token

    def test(self) -> bool:
        """Test Workday connection."""
        try:
            self._get_access_token()
            return True
        except Exception as e:
            self.logger.error(f"Workday connection test failed: {e}")
            return False

    def read(self, dataset: str, limit: int = 10000, **kwargs) -> DataFrame:
        """Read from Workday Prism dataset."""
        headers = self.get_headers()
        url = f"{self.base_url}/ccx/api/prismAnalytics/v2/{self.config['tenant_id']}/datasets/{dataset}/data"

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        records = data.get("data", [])

        pdf = pd.DataFrame(records[:limit])
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"dataset": dataset, "records": len(records)})

        return df

    def write(self, df: DataFrame, dataset: str, **kwargs) -> None:
        """Upload data to Workday Prism."""
        raise NotImplementedError("Workday Prism write support coming soon")


@register_connection("servicenow")
class ServiceNowConnection(APIConnection):
    """
    ServiceNow connection via REST API.

    Example config:
        {
            "instance": "mycompany.service-now.com",
            "username": "${SERVICENOW_USER}",
            "password": "${SERVICENOW_PASSWORD}",
            "base_url": "https://mycompany.service-now.com"
        }

    Usage:
        >>> conn = Connection.get("servicenow", spark, env="prod")
        >>> df = conn.read(table="incident", query="active=true")
        >>> df = conn.read(table="cmdb_ci_server")
    """

    def get_headers(self) -> Dict[str, str]:
        """Get ServiceNow headers."""
        import base64
        credentials = f"{self.config['username']}:{self.config['password']}"
        encoded = base64.b64encode(credentials.encode()).decode()

        return {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def test(self) -> bool:
        """Test ServiceNow connection."""
        try:
            response = requests.get(
                f"{self.base_url}/api/now/table/sys_user?sysparm_limit=1",
                headers=self.get_headers(),
                timeout=30
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"ServiceNow connection test failed: {e}")
            return False

    def read(
        self,
        table: str,
        query: Optional[str] = None,
        fields: Optional[List[str]] = None,
        limit: int = 10000,
        **kwargs
    ) -> DataFrame:
        """
        Read ServiceNow table.

        Args:
            table: Table name (incident, cmdb_ci, etc.)
            query: Encoded query string
            fields: List of fields to return
            limit: Max records
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        url = f"{self.base_url}/api/now/table/{table}"
        headers = self.get_headers()

        params = {"sysparm_limit": limit}
        if query:
            params["sysparm_query"] = query
        if fields:
            params["sysparm_fields"] = ",".join(fields)

        all_records = []
        offset = 0

        while len(all_records) < limit:
            params["sysparm_offset"] = offset

            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()
            records = data.get("result", [])

            if not records:
                break

            all_records.extend(records)
            offset += len(records)

            time.sleep(0.1)  # Rate limiting

        pdf = pd.DataFrame(all_records[:limit])
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"table": table, "records": len(all_records)})

        return df

    def write(self, df: DataFrame, table: str, **kwargs) -> None:
        """Write records to ServiceNow table."""
        headers = self.get_headers()
        url = f"{self.base_url}/api/now/table/{table}"

        records = [row.asDict() for row in df.collect()]

        for record in records:
            response = requests.post(url, headers=headers, json=record)
            response.raise_for_status()
            time.sleep(0.1)  # Rate limiting

        self.emit_lineage("write", {"table": table, "records": len(records)})


@register_connection("marketo")
class MarketoConnection(APIConnection):
    """
    Marketo connection via Bulk Extract API.

    Example config:
        {
            "munchkin_id": "123-ABC-456",
            "client_id": "${MARKETO_CLIENT_ID}",
            "client_secret": "${MARKETO_CLIENT_SECRET}",
            "base_url": "https://123-ABC-456.mktorest.com"
        }

    Usage:
        >>> conn = Connection.get("marketo", spark, env="prod")
        >>> df = conn.read(object_type="leads", fields=["email", "firstName", "lastName"])
    """

    def get_headers(self) -> Dict[str, str]:
        """Get Marketo headers with access token."""
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json"
        }

    def _get_access_token(self) -> str:
        """Get Marketo access token."""
        if hasattr(self, '_access_token'):
            return self._access_token

        token_url = f"{self.base_url}/identity/oauth/token"

        response = requests.get(token_url, params={
            "grant_type": "client_credentials",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"]
        })

        response.raise_for_status()
        self._access_token = response.json()["access_token"]
        return self._access_token

    def test(self) -> bool:
        """Test Marketo connection."""
        try:
            self._get_access_token()
            return True
        except Exception as e:
            self.logger.error(f"Marketo connection test failed: {e}")
            return False

    def read(
        self,
        object_type: str = "leads",
        fields: Optional[List[str]] = None,
        filter_type: Optional[str] = None,
        filter_values: Optional[List[str]] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read from Marketo via Bulk Extract API.

        Args:
            object_type: leads, activities, program_members, etc.
            fields: List of fields to extract
            filter_type: Filter type (createdAt, updatedAt, etc.)
            filter_values: Filter values
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        headers = self.get_headers()

        # Create extract job
        create_url = f"{self.base_url}/bulk/v1/{object_type}/export/create.json"

        payload = {}
        if fields:
            payload["fields"] = fields
        if filter_type and filter_values:
            payload["filter"] = {
                filter_type: filter_values
            }

        response = requests.post(create_url, headers=headers, json=payload)
        response.raise_for_status()

        export_id = response.json()["result"][0]["exportId"]

        # Enqueue job
        enqueue_url = f"{self.base_url}/bulk/v1/{object_type}/export/{export_id}/enqueue.json"
        requests.post(enqueue_url, headers=headers).raise_for_status()

        # Poll for completion
        status_url = f"{self.base_url}/bulk/v1/{object_type}/export/{export_id}/status.json"

        while True:
            response = requests.get(status_url, headers=headers)
            response.raise_for_status()
            status = response.json()["result"][0]["status"]

            if status == "Completed":
                break
            elif status == "Failed":
                raise Exception("Marketo export failed")

            time.sleep(5)

        # Download results
        file_url = f"{self.base_url}/bulk/v1/{object_type}/export/{export_id}/file.json"
        response = requests.get(file_url, headers=headers)
        response.raise_for_status()

        # Parse CSV
        from io import StringIO
        csv_data = StringIO(response.text)
        pdf = pd.read_csv(csv_data)
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"object_type": object_type, "records": df.count()})

        return df

    def write(self, df: DataFrame, **kwargs) -> None:
        """Marketo bulk import not supported via REST API."""
        raise NotImplementedError("Marketo write via Spark not supported")


@register_connection("hubspot")
class HubSpotConnection(APIConnection):
    """
    HubSpot CRM connection via REST API v3.

    Example config:
        {
            "base_url": "https://api.hubapi.com",
            "api_token": "${HUBSPOT_API_TOKEN}",
            "auth_type": "bearer"
        }

    Usage:
        >>> conn = Connection.get("hubspot", spark, env="prod")
        >>> df = conn.read(object_type="contacts", properties=["email", "firstname", "lastname"])
        >>> df = conn.read(object_type="companies")
        >>> df = conn.read(object_type="deals")
    """

    def get_headers(self) -> Dict[str, str]:
        """Get HubSpot headers."""
        return {
            "Authorization": f"Bearer {self.config['api_token']}",
            "Content-Type": "application/json"
        }

    def test(self) -> bool:
        """Test HubSpot connection."""
        try:
            response = requests.get(
                f"{self.base_url}/crm/v3/objects/contacts?limit=1",
                headers=self.get_headers(),
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"HubSpot connection test failed: {e}")
            return False

    def read(
        self,
        object_type: str,
        properties: Optional[List[str]] = None,
        limit: int = 10000,
        **kwargs
    ) -> DataFrame:
        """
        Read HubSpot CRM objects.

        Args:
            object_type: Object type (contacts, companies, deals, tickets, etc.)
            properties: List of properties to retrieve
            limit: Max records
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        url = f"{self.base_url}/crm/v3/objects/{object_type}"
        headers = self.get_headers()

        all_records = []
        after = None

        while len(all_records) < limit:
            params = {"limit": min(100, limit - len(all_records))}
            if after:
                params["after"] = after
            if properties:
                params["properties"] = ",".join(properties)

            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])

            # Flatten properties
            for item in results:
                props = item.get("properties", {})
                props["id"] = item.get("id")
                all_records.append(props)

            # Check for more pages
            paging = data.get("paging", {})
            after = paging.get("next", {}).get("after")

            if not after or not results:
                break

            time.sleep(0.1)  # Rate limiting

        pdf = pd.DataFrame(all_records[:limit])
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"object_type": object_type, "records": len(all_records)})

        return df

    def write(self, df: DataFrame, object_type: str, **kwargs) -> None:
        """
        Write records to HubSpot CRM.

        Args:
            df: DataFrame to write
            object_type: Object type (contacts, companies, deals, etc.)
            **kwargs: Additional options
        """
        url = f"{self.base_url}/crm/v3/objects/{object_type}/batch/create"
        headers = self.get_headers()

        records = [row.asDict() for row in df.collect()]

        # HubSpot batch API accepts max 100 records
        for i in range(0, len(records), 100):
            batch = records[i:i + 100]

            payload = {
                "inputs": [{"properties": record} for record in batch]
            }

            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()

            time.sleep(0.1)  # Rate limiting

        self.emit_lineage("write", {"object_type": object_type, "records": len(records)})


@register_connection("zendesk")
class ZendeskConnection(APIConnection):
    """
    Zendesk connection via REST API.

    Example config:
        {
            "subdomain": "mycompany",
            "email": "${ZENDESK_EMAIL}",
            "api_token": "${ZENDESK_API_TOKEN}",
            "base_url": "https://mycompany.zendesk.com"
        }

    Usage:
        >>> conn = Connection.get("zendesk", spark, env="prod")
        >>> df = conn.read(endpoint="tickets", params={"status": "open"})
        >>> df = conn.read(endpoint="users")
    """

    def get_headers(self) -> Dict[str, str]:
        """Get Zendesk headers."""
        import base64
        credentials = f"{self.config['email']}/token:{self.config['api_token']}"
        encoded = base64.b64encode(credentials.encode()).decode()

        return {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/json"
        }

    def test(self) -> bool:
        """Test Zendesk connection."""
        try:
            response = requests.get(
                f"{self.base_url}/api/v2/users/me.json",
                headers=self.get_headers(),
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Zendesk connection test failed: {e}")
            return False

    def read(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        limit: int = 10000,
        **kwargs
    ) -> DataFrame:
        """
        Read from Zendesk endpoint.

        Args:
            endpoint: API endpoint (tickets, users, organizations, etc.)
            params: Query parameters
            limit: Max records
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        url = f"{self.base_url}/api/v2/{endpoint}.json"
        headers = self.get_headers()

        all_records = []
        page_url = url

        while len(all_records) < limit:
            response = requests.get(page_url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()

            # Extract records (endpoint name is usually the key)
            records = data.get(endpoint, data.get("results", []))
            all_records.extend(records)

            # Check for next page
            next_page = data.get("next_page")
            if not next_page or not records:
                break

            page_url = next_page
            params = None  # Clear params for subsequent pages

            time.sleep(0.1)  # Rate limiting

        pdf = pd.DataFrame(all_records[:limit])
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"endpoint": endpoint, "records": len(all_records)})

        return df

    def write(self, df: DataFrame, endpoint: str, **kwargs) -> None:
        """Write records to Zendesk."""
        headers = self.get_headers()
        url = f"{self.base_url}/api/v2/{endpoint}.json"

        records = [row.asDict() for row in df.collect()]

        for record in records:
            response = requests.post(url, headers=headers, json={endpoint.rstrip("s"): record})
            response.raise_for_status()
            time.sleep(0.1)  # Rate limiting

        self.emit_lineage("write", {"endpoint": endpoint, "records": len(records)})


@register_connection("shopify")
class ShopifyConnection(APIConnection):
    """
    Shopify connection via GraphQL Admin API.

    Example config:
        {
            "shop": "mystore.myshopify.com",
            "access_token": "${SHOPIFY_ACCESS_TOKEN}",
            "api_version": "2024-01",
            "base_url": "https://mystore.myshopify.com"
        }

    Usage:
        >>> conn = Connection.get("shopify", spark, env="prod")
        >>> df = conn.read(resource="orders", fields=["id", "name", "totalPrice"])
        >>> df = conn.read(resource="products")
    """

    def get_headers(self) -> Dict[str, str]:
        """Get Shopify headers."""
        return {
            "X-Shopify-Access-Token": self.config["access_token"],
            "Content-Type": "application/json"
        }

    def test(self) -> bool:
        """Test Shopify connection."""
        try:
            response = requests.get(
                f"{self.base_url}/admin/api/{self.config.get('api_version', '2024-01')}/shop.json",
                headers=self.get_headers(),
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Shopify connection test failed: {e}")
            return False

    def read(
        self,
        resource: str,
        fields: Optional[List[str]] = None,
        limit: int = 250,
        **kwargs
    ) -> DataFrame:
        """
        Read from Shopify REST API.

        Args:
            resource: Resource type (orders, products, customers, etc.)
            fields: List of fields to return
            limit: Max records (max 250 per request)
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        api_version = self.config.get("api_version", "2024-01")
        url = f"{self.base_url}/admin/api/{api_version}/{resource}.json"
        headers = self.get_headers()

        params = {"limit": min(250, limit)}
        if fields:
            params["fields"] = ",".join(fields)

        all_records = []

        while len(all_records) < limit:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()
            records = data.get(resource, [])
            all_records.extend(records)

            # Check for next page via Link header
            link_header = response.headers.get("Link", "")
            if "rel=\"next\"" not in link_header or not records:
                break

            # Parse next page URL from Link header
            import re
            match = re.search(r'<([^>]+)>; rel="next"', link_header)
            if match:
                url = match.group(1)
                params = {}  # URL already has params

            time.sleep(0.5)  # Rate limiting (2 requests/second)

        pdf = pd.DataFrame(all_records[:limit])
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"resource": resource, "records": len(all_records)})

        return df

    def write(self, df: DataFrame, resource: str, **kwargs) -> None:
        """Write records to Shopify."""
        api_version = self.config.get("api_version", "2024-01")
        url = f"{self.base_url}/admin/api/{api_version}/{resource}.json"
        headers = self.get_headers()

        records = [row.asDict() for row in df.collect()]

        for record in records:
            response = requests.post(
                url,
                headers=headers,
                json={resource.rstrip("s"): record}
            )
            response.raise_for_status()
            time.sleep(0.5)  # Rate limiting

        self.emit_lineage("write", {"resource": resource, "records": len(records)})


@register_connection("stripe")
class StripeConnection(APIConnection):
    """
    Stripe connection via REST API.

    Example config:
        {
            "api_key": "secret://prod-secrets/stripe-key",
            "base_url": "https://api.stripe.com"
        }

    Usage:
        >>> conn = Connection.get("stripe", spark, env="prod")
        >>> df = conn.read(object_type="charges", limit=1000)
        >>> df = conn.read(object_type="customers")
        >>> df = conn.read(object_type="subscriptions")
    """

    def get_headers(self) -> Dict[str, str]:
        """Get Stripe headers."""
        return {
            "Authorization": f"Bearer {self.config['api_key']}",
            "Content-Type": "application/x-www-form-urlencoded"
        }

    def test(self) -> bool:
        """Test Stripe connection."""
        try:
            response = requests.get(
                f"{self.base_url}/v1/account",
                headers=self.get_headers(),
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Stripe connection test failed: {e}")
            return False

    def read(
        self,
        object_type: str,
        limit: int = 100,
        created_after: Optional[int] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read from Stripe API.

        Args:
            object_type: Object type (charges, customers, subscriptions, etc.)
            limit: Max records per request (max 100)
            created_after: Unix timestamp for filtering
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        url = f"{self.base_url}/v1/{object_type}"
        headers = self.get_headers()

        params = {"limit": min(100, limit)}
        if created_after:
            params["created[gte]"] = created_after

        all_records = []
        has_more = True

        while has_more and len(all_records) < limit:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()
            records = data.get("data", [])
            all_records.extend(records)

            has_more = data.get("has_more", False)

            if has_more and records:
                params["starting_after"] = records[-1]["id"]

            time.sleep(0.1)  # Rate limiting

        pdf = pd.DataFrame(all_records)
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"object_type": object_type, "records": len(all_records)})

        return df

    def write(self, df: DataFrame, object_type: str, **kwargs) -> None:
        """Write to Stripe (create objects)."""
        url = f"{self.base_url}/v1/{object_type}"
        headers = self.get_headers()

        records = [row.asDict() for row in df.collect()]

        for record in records:
            response = requests.post(url, headers=headers, data=record)
            response.raise_for_status()
            time.sleep(0.1)  # Rate limiting

        self.emit_lineage("write", {"object_type": object_type, "records": len(records)})


@register_connection("zuora")
class ZuoraConnection(APIConnection):
    """
    Zuora subscription billing connection via AQuA API.

    Example config:
        {
            "base_url": "https://rest.zuora.com",
            "client_id": "${ZUORA_CLIENT_ID}",
            "client_secret": "${ZUORA_CLIENT_SECRET}",
            "entity_id": "${ZUORA_ENTITY_ID}"
        }

    Usage:
        >>> conn = Connection.get("zuora", spark, env="prod")
        >>> df = conn.read(object_type="Account")
        >>> df = conn.read(object_type="Subscription")
    """

    def get_headers(self) -> Dict[str, str]:
        """Get Zuora headers with OAuth token."""
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json"
        }

    def _get_access_token(self) -> str:
        """Get Zuora OAuth access token."""
        if hasattr(self, '_access_token'):
            return self._access_token

        token_url = f"{self.base_url}/oauth/token"

        response = requests.post(token_url, data={
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "client_credentials"
        })

        response.raise_for_status()
        self._access_token = response.json()["access_token"]
        return self._access_token

    def test(self) -> bool:
        """Test Zuora connection."""
        try:
            self._get_access_token()
            return True
        except Exception as e:
            self.logger.error(f"Zuora connection test failed: {e}")
            return False

    def read(self, object_type: str, query: Optional[str] = None, **kwargs) -> DataFrame:
        """
        Read from Zuora via AQuA Export API.

        Args:
            object_type: Object type (Account, Subscription, etc.)
            query: ZOQL query
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        headers = self.get_headers()

        # Create export job
        create_url = f"{self.base_url}/v1/batch-query/"

        if query is None:
            query = f"SELECT * FROM {object_type}"

        payload = {
            "format": "csv",
            "version": "1.2",
            "name": f"Spark Export {object_type}",
            "encrypted": "none",
            "useQueryLabels": "true",
            "dateTimeUtc": "true",
            "queries": [
                {
                    "name": object_type,
                    "query": query,
                    "type": "zoqlexport"
                }
            ]
        }

        response = requests.post(create_url, headers=headers, json=payload)
        response.raise_for_status()

        job_id = response.json()["id"]

        # Poll for completion
        status_url = f"{self.base_url}/v1/batch-query/jobs/{job_id}"

        while True:
            response = requests.get(status_url, headers=headers)
            response.raise_for_status()
            status = response.json()["batches"][0]["status"]

            if status == "completed":
                break
            elif status == "aborted":
                raise Exception("Zuora export failed")

            time.sleep(5)

        # Download results
        file_id = response.json()["batches"][0]["fileId"]
        file_url = f"{self.base_url}/v1/files/{file_id}"

        response = requests.get(file_url, headers=headers)
        response.raise_for_status()

        # Parse CSV
        from io import StringIO
        csv_data = StringIO(response.text)
        pdf = pd.read_csv(csv_data)
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"object_type": object_type, "records": df.count()})

        return df

    def write(self, df: DataFrame, **kwargs) -> None:
        """Zuora write not supported via AQuA."""
        raise NotImplementedError("Zuora write via Spark not supported")


@register_connection("google_analytics")
@register_connection("ga4")
class GoogleAnalytics4Connection(APIConnection):
    """
    Google Analytics 4 connection.

    Note: Prefer BigQuery export for large-scale analytics.

    Example config:
        {
            "property_id": "123456789",
            "credentials_file": "/path/to/service-account.json",
            "base_url": "https://analyticsdata.googleapis.com"
        }

    Usage:
        >>> conn = Connection.get("ga4", spark, env="prod")
        >>> df = conn.read(
        ...     metrics=["activeUsers", "sessions"],
        ...     dimensions=["date", "country"],
        ...     start_date="2024-01-01",
        ...     end_date="2024-01-31"
        ... )
    """

    def get_headers(self) -> Dict[str, str]:
        """Get Google Analytics headers."""
        from google.oauth2 import service_account
        import google.auth.transport.requests

        credentials = service_account.Credentials.from_service_account_file(
            self.config["credentials_file"],
            scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )

        request = google.auth.transport.requests.Request()
        credentials.refresh(request)

        return {
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json"
        }

    def test(self) -> bool:
        """Test GA4 connection."""
        try:
            self.get_headers()
            return True
        except Exception as e:
            self.logger.error(f"GA4 connection test failed: {e}")
            return False

    def read(
        self,
        metrics: List[str],
        dimensions: List[str],
        start_date: str,
        end_date: str,
        **kwargs
    ) -> DataFrame:
        """
        Read from Google Analytics 4.

        Args:
            metrics: List of metrics (e.g., ["activeUsers", "sessions"])
            dimensions: List of dimensions (e.g., ["date", "country"])
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        url = f"{self.base_url}/v1beta/properties/{self.config['property_id']}:runReport"
        headers = self.get_headers()

        payload = {
            "dateRanges": [{"startDate": start_date, "endDate": end_date}],
            "metrics": [{"name": m} for m in metrics],
            "dimensions": [{"name": d} for d in dimensions]
        }

        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        data = response.json()

        # Parse response
        rows = data.get("rows", [])
        records = []

        for row in rows:
            record = {}
            for i, dim in enumerate(dimensions):
                record[dim] = row["dimensionValues"][i]["value"]
            for i, metric in enumerate(metrics):
                record[metric] = row["metricValues"][i]["value"]
            records.append(record)

        pdf = pd.DataFrame(records)
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"metrics": metrics, "dimensions": dimensions, "records": len(records)})

        return df

    def write(self, df: DataFrame, **kwargs) -> None:
        """GA4 is read-only."""
        raise NotImplementedError("Google Analytics is read-only")


@register_connection("google_ads")
class GoogleAdsConnection(APIConnection):
    """
    Google Ads connection via Google Ads API.

    Example config:
        {
            "developer_token": "${GOOGLE_ADS_DEVELOPER_TOKEN}",
            "client_id": "${GOOGLE_ADS_CLIENT_ID}",
            "client_secret": "${GOOGLE_ADS_CLIENT_SECRET}",
            "refresh_token": "${GOOGLE_ADS_REFRESH_TOKEN}",
            "customer_id": "1234567890",
            "login_customer_id": "9876543210"
        }

    Usage:
        >>> conn = Connection.get("google_ads", spark, env="prod")
        >>> df = conn.read(query="SELECT campaign.id, campaign.name, metrics.clicks FROM campaign")
    """

    def get_headers(self) -> Dict[str, str]:
        """Get Google Ads headers."""
        return {
            "developer-token": self.config["developer_token"],
            "login-customer-id": self.config.get("login_customer_id", ""),
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json"
        }

    def _get_access_token(self) -> str:
        """Get OAuth access token."""
        if hasattr(self, '_access_token'):
            return self._access_token

        token_url = "https://oauth2.googleapis.com/token"

        response = requests.post(token_url, data={
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "refresh_token": self.config["refresh_token"],
            "grant_type": "refresh_token"
        })

        response.raise_for_status()
        self._access_token = response.json()["access_token"]
        return self._access_token

    def test(self) -> bool:
        """Test Google Ads connection."""
        try:
            self._get_access_token()
            return True
        except Exception as e:
            self.logger.error(f"Google Ads connection test failed: {e}")
            return False

    def read(self, query: str, **kwargs) -> DataFrame:
        """
        Read from Google Ads via GAQL query.

        Args:
            query: Google Ads Query Language (GAQL) query
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        url = f"https://googleads.googleapis.com/v15/customers/{self.config['customer_id']}/googleAds:searchStream"
        headers = self.get_headers()

        payload = {"query": query}

        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        data = response.json()

        # Parse results
        records = []
        for result in data.get("results", []):
            records.append(self._flatten_dict(result))

        pdf = pd.DataFrame(records)
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"query": query, "records": len(records)})

        return df

    def _flatten_dict(self, d: Dict, parent_key: str = "", sep: str = "_") -> Dict:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def write(self, df: DataFrame, **kwargs) -> None:
        """Google Ads write not supported."""
        raise NotImplementedError("Google Ads write via Spark not supported")


@register_connection("facebook_ads")
@register_connection("meta_ads")
class FacebookAdsConnection(APIConnection):
    """
    Facebook / Meta Ads Marketing API connection.
    
    Reads campaign, adset, and ad performance data from Meta Marketing API.
    
    Example config:
        facebook_ads_prod:
          type: facebook_ads
          access_token: secret://facebook/access_token
          account_id: act_1234567890
          api_version: v18.0
    
    Usage:
        >>> conn = Connection.get("facebook_ads", spark, env="prod")
        >>> campaigns_df = conn.read(object_type="campaigns", fields=["id", "name", "status"])
        >>> insights_df = conn.read(
        ...     object_type="insights",
        ...     level="campaign",
        ...     time_range={"since": "2024-01-01", "until": "2024-01-31"}
        ... )
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.access_token = self.config["access_token"]
        self.account_id = self.config["account_id"]
        self.api_version = self.config.get("api_version", "v18.0")
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
    
    def test(self) -> bool:
        """Test Facebook Ads API connection."""
        try:
            url = f"{self.base_url}/me/adaccounts"
            response = requests.get(
                url,
                params={"access_token": self.access_token}
            )
            response.raise_for_status()
            self.logger.info(f"✓ Facebook Ads API connection successful")
            return True
        except Exception as e:
            self.logger.error(f"✗ Facebook Ads API test failed: {e}")
            return False
    
    def read(
        self,
        object_type: str = "campaigns",
        fields: Optional[List[str]] = None,
        filtering: Optional[List[Dict]] = None,
        time_range: Optional[Dict[str, str]] = None,
        level: Optional[str] = None,
        limit: int = 1000,
        **kwargs
    ) -> DataFrame:
        """
        Read data from Facebook Ads API.
        
        Args:
            object_type: One of 'campaigns', 'adsets', 'ads', 'insights'
            fields: List of fields to retrieve
            filtering: Facebook Ads filtering spec
            time_range: {'since': 'YYYY-MM-DD', 'until': 'YYYY-MM-DD'}
            level: For insights: 'account', 'campaign', 'adset', 'ad'
            limit: Max records per page
        """
        if object_type == "insights":
            endpoint = f"/{self.account_id}/insights"
            default_fields = ["impressions", "clicks", "spend", "reach"]
        elif object_type == "campaigns":
            endpoint = f"/{self.account_id}/campaigns"
            default_fields = ["id", "name", "status", "objective"]
        elif object_type == "adsets":
            endpoint = f"/{self.account_id}/adsets"
            default_fields = ["id", "name", "status", "campaign_id"]
        elif object_type == "ads":
            endpoint = f"/{self.account_id}/ads"
            default_fields = ["id", "name", "status", "adset_id"]
        else:
            raise ValueError(f"Unsupported object_type: {object_type}")
        
        fields = fields or default_fields
        
        params = {
            "access_token": self.access_token,
            "fields": ",".join(fields),
            "limit": limit
        }
        
        if filtering:
            import json
            params["filtering"] = json.dumps(filtering)
        
        if time_range and object_type == "insights":
            import json
            params["time_range"] = json.dumps(time_range)
        
        if level and object_type == "insights":
            params["level"] = level
        
        all_data = []
        url = f"{self.base_url}{endpoint}"
        
        while url:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            result = response.json()
            all_data.extend(result.get("data", []))
            
            # Get next page URL from paging.next
            paging = result.get("paging", {})
            url = paging.get("next")
            params = {}  # Next URL has all params embedded
            
            time.sleep(0.2)  # Rate limiting
        
        self.logger.info(f"Fetched {len(all_data)} {object_type} from Facebook Ads")
        
        return self.spark.createDataFrame(all_data) if all_data else self.spark.createDataFrame([], f"id STRING")


@register_connection("linkedin_ads")
class LinkedInAdsConnection(APIConnection):
    """
    LinkedIn Ads Marketing API connection.
    
    Reads campaign and ad analytics from LinkedIn Marketing API.
    
    Example config:
        linkedin_ads_prod:
          type: linkedin_ads
          access_token: secret://linkedin/access_token
          account_id: "123456789"
    
    Usage:
        >>> conn = Connection.get("linkedin_ads", spark, env="prod")
        >>> campaigns_df = conn.read(object_type="campaigns")
        >>> analytics_df = conn.read(
        ...     object_type="analytics",
        ...     date_range={"start": "2024-01-01", "end": "2024-01-31"}
        ... )
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.access_token = self.config["access_token"]
        self.account_id = self.config["account_id"]
        self.base_url = "https://api.linkedin.com/v2"
    
    def _get_headers(self) -> Dict[str, str]:
        """Get LinkedIn API headers."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "Content-Type": "application/json"
        }
    
    def test(self) -> bool:
        """Test LinkedIn Ads API connection."""
        try:
            url = f"{self.base_url}/adAccountsV2/{self.account_id}"
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            self.logger.info(f"✓ LinkedIn Ads API connection successful")
            return True
        except Exception as e:
            self.logger.error(f"✗ LinkedIn Ads API test failed: {e}")
            return False
    
    def read(
        self,
        object_type: str = "campaigns",
        date_range: Optional[Dict[str, str]] = None,
        fields: Optional[List[str]] = None,
        limit: int = 100,
        **kwargs
    ) -> DataFrame:
        """
        Read data from LinkedIn Ads API.
        
        Args:
            object_type: One of 'campaigns', 'creatives', 'analytics'
            date_range: {'start': 'YYYY-MM-DD', 'end': 'YYYY-MM-DD'}
            fields: Fields to retrieve
            limit: Records per page
        """
        if object_type == "campaigns":
            endpoint = "/adCampaignsV2"
            params = {
                "q": "search",
                "search.account.values[0]": f"urn:li:sponsoredAccount:{self.account_id}",
                "count": limit
            }
        elif object_type == "creatives":
            endpoint = "/adCreativesV2"
            params = {
                "q": "search",
                "search.account.values[0]": f"urn:li:sponsoredAccount:{self.account_id}",
                "count": limit
            }
        elif object_type == "analytics":
            endpoint = "/adAnalyticsV2"
            params = {
                "q": "analytics",
                "pivot": "CAMPAIGN",
                "accounts[0]": f"urn:li:sponsoredAccount:{self.account_id}",
                "fields": fields or "clicks,impressions,costInLocalCurrency"
            }
            if date_range:
                params["dateRange.start.day"] = int(date_range["start"].split("-")[2])
                params["dateRange.start.month"] = int(date_range["start"].split("-")[1])
                params["dateRange.start.year"] = int(date_range["start"].split("-")[0])
                params["dateRange.end.day"] = int(date_range["end"].split("-")[2])
                params["dateRange.end.month"] = int(date_range["end"].split("-")[1])
                params["dateRange.end.year"] = int(date_range["end"].split("-")[0])
        else:
            raise ValueError(f"Unsupported object_type: {object_type}")
        
        all_data = []
        start = 0
        
        while True:
            params["start"] = start
            url = f"{self.base_url}{endpoint}"
            
            response = requests.get(url, headers=self._get_headers(), params=params)
            response.raise_for_status()
            
            result = response.json()
            elements = result.get("elements", [])
            
            if not elements:
                break
            
            all_data.extend(elements)
            
            # Check if there are more pages
            paging = result.get("paging", {})
            if "next" not in paging.get("links", []):
                break
            
            start += limit
            time.sleep(0.3)  # Rate limiting
        
        self.logger.info(f"Fetched {len(all_data)} {object_type} from LinkedIn Ads")
        
        return self.spark.createDataFrame(all_data) if all_data else self.spark.createDataFrame([], "id STRING")


@register_connection("amplitude")
class AmplitudeConnection(APIConnection):
    """
    Amplitude Analytics Export API connection.
    
    Reads event data from Amplitude Export API.
    
    Example config:
        amplitude_prod:
          type: amplitude
          api_key: secret://amplitude/api_key
          secret_key: secret://amplitude/secret_key
          project_id: "12345"
    
    Usage:
        >>> conn = Connection.get("amplitude", spark, env="prod")
        >>> events_df = conn.read(
        ...     start_date="2024-01-01",
        ...     end_date="2024-01-31"
        ... )
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.api_key = self.config["api_key"]
        self.secret_key = self.config["secret_key"]
        self.project_id = self.config.get("project_id")
        self.base_url = "https://amplitude.com/api/2"
    
    def test(self) -> bool:
        """Test Amplitude API connection."""
        try:
            import requests
            from requests.auth import HTTPBasicAuth
            
            url = f"{self.base_url}/events/list"
            response = requests.get(
                url,
                auth=HTTPBasicAuth(self.api_key, self.secret_key)
            )
            response.raise_for_status()
            self.logger.info(f"✓ Amplitude API connection successful")
            return True
        except Exception as e:
            self.logger.error(f"✗ Amplitude API test failed: {e}")
            return False
    
    def read(
        self,
        start_date: str,
        end_date: str,
        event_type: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read events from Amplitude Export API.
        
        Args:
            start_date: Start date in YYYYMMDD format (or YYYY-MM-DD)
            end_date: End date in YYYYMMDD format (or YYYY-MM-DD)
            event_type: Optional specific event type to filter
        
        Returns:
            DataFrame with Amplitude event data
        """
        from requests.auth import HTTPBasicAuth
        import gzip
        import json
        from datetime import datetime, timedelta
        
        # Convert date format if needed
        start_date = start_date.replace("-", "")
        end_date = end_date.replace("-", "")
        
        # Amplitude Export API provides daily export files
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        
        all_events = []
        current_dt = start_dt
        
        while current_dt <= end_dt:
            date_str = current_dt.strftime("%Y%m%d")
            hour = "0"  # Can iterate 0-23 for hourly data
            
            url = f"{self.base_url}/export"
            params = {
                "start": date_str,
                "end": date_str
            }
            
            self.logger.info(f"Fetching Amplitude data for {date_str}")
            
            response = requests.get(
                url,
                params=params,
                auth=HTTPBasicAuth(self.api_key, self.secret_key),
                stream=True
            )
            
            if response.status_code == 200:
                # Response is gzipped JSON lines
                for line in gzip.decompress(response.content).decode('utf-8').split('\n'):
                    if line.strip():
                        event = json.loads(line)
                        if event_type is None or event.get("event_type") == event_type:
                            all_events.append(event)
            elif response.status_code == 404:
                self.logger.warning(f"No data available for {date_str}")
            else:
                response.raise_for_status()
            
            current_dt += timedelta(days=1)
            time.sleep(0.5)  # Rate limiting
        
        self.logger.info(f"Fetched {len(all_events)} events from Amplitude")
        
        if all_events:
            return self.spark.createDataFrame(all_events)
        else:
            # Return empty DataFrame with basic schema
            from pyspark.sql.types import StructType, StructField, StringType, LongType
            schema = StructType([
                StructField("event_type", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("event_time", LongType(), True)
            ])
            return self.spark.createDataFrame([], schema)


@register_connection("segment")
class SegmentConnection(APIConnection):
    """
    Segment CDP connection via S3 export or Destinations.
    
    Reads event data from Segment S3 exports or via Personas/Profiles API.
    
    Example config:
        segment_prod:
          type: segment
          access_token: secret://segment/access_token
          workspace_slug: my-workspace
          s3_bucket: segment-logs
          s3_prefix: segment-logs/my-workspace/
    
    Usage:
        >>> conn = Connection.get("segment", spark, env="prod")
        >>> # Read from S3 export
        >>> events_df = conn.read(
        ...     source="s3",
        ...     date="2024-01-15"
        ... )
        >>> # Read from Profiles API
        >>> profiles_df = conn.read(source="profiles")
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.access_token = self.config["access_token"]
        self.workspace_slug = self.config.get("workspace_slug")
        self.s3_bucket = self.config.get("s3_bucket")
        self.s3_prefix = self.config.get("s3_prefix", "")
        self.base_url = "https://api.segmentapis.com"
    
    def _get_headers(self) -> Dict[str, str]:
        """Get Segment API headers."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
    
    def test(self) -> bool:
        """Test Segment API connection."""
        try:
            url = f"{self.base_url}/workspaces/{self.workspace_slug}"
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            self.logger.info(f"✓ Segment API connection successful")
            return True
        except Exception as e:
            self.logger.error(f"✗ Segment API test failed: {e}")
            return False
    
    def read(
        self,
        source: str = "s3",
        date: Optional[str] = None,
        limit: int = 1000,
        **kwargs
    ) -> DataFrame:
        """
        Read data from Segment.
        
        Args:
            source: 's3' for S3 exports, 'profiles' for Profiles API, 'tracking_plans'
            date: For S3 source, date in YYYY-MM-DD format
            limit: For API sources, max records
        
        Returns:
            DataFrame with Segment data
        """
        if source == "s3":
            if not self.s3_bucket:
                raise ValueError("s3_bucket must be configured for S3 source")
            
            if not date:
                from datetime import datetime
                date = datetime.utcnow().strftime("%Y-%m-%d")
            
            # Segment S3 export path: s3://bucket/prefix/YYYY-MM-DD/
            s3_path = f"s3a://{self.s3_bucket}/{self.s3_prefix}{date}/"
            
            self.logger.info(f"Reading Segment S3 export from {s3_path}")
            
            # Segment exports are gzipped JSON lines
            df = self.spark.read.json(s3_path)
            return df
        
        elif source == "profiles":
            # Segment Personas Profiles API
            url = f"{self.base_url}/spaces/{self.workspace_slug}/collections/users/profiles"
            
            all_profiles = []
            cursor = None
            
            while True:
                params = {"limit": limit}
                if cursor:
                    params["cursor"] = cursor
                
                response = requests.get(url, headers=self._get_headers(), params=params)
                response.raise_for_status()
                
                result = response.json()
                profiles = result.get("data", {}).get("profiles", [])
                
                if not profiles:
                    break
                
                all_profiles.extend(profiles)
                
                # Get next cursor
                cursor = result.get("data", {}).get("cursor", {}).get("next")
                if not cursor:
                    break
                
                time.sleep(0.2)  # Rate limiting
            
            self.logger.info(f"Fetched {len(all_profiles)} profiles from Segment")
            
            return self.spark.createDataFrame(all_profiles) if all_profiles else self.spark.createDataFrame([], "id STRING")
        
        elif source == "tracking_plans":
            # Tracking Plans API
            url = f"{self.base_url}/workspaces/{self.workspace_slug}/tracking-plans"
            
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            
            plans = response.json().get("data", {}).get("tracking_plans", [])
            
            return self.spark.createDataFrame(plans) if plans else self.spark.createDataFrame([], "id STRING")
        
        else:
            raise ValueError(f"Unsupported source: {source}. Use 's3', 'profiles', or 'tracking_plans'")
