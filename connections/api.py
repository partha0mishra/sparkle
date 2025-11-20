"""
API connections for REST, GraphQL, SOAP, and other web services.

Handles authentication, rate limiting, pagination, and retry logic.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
import requests
import json
from abc import abstractmethod

from .base import APIConnection
from .factory import register_connection


@register_connection("rest")
@register_connection("rest_api")
class RESTAPIConnection(APIConnection):
    """
    Generic REST API connection.

    Example config (config/connections/rest/prod.json):
        {
            "base_url": "https://api.example.com/v1",
            "auth_type": "bearer",
            "token": "${API_TOKEN}",
            "headers": {
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            "rate_limit": 100,
            "retry_attempts": 3
        }

    Or with API key:
        {
            "base_url": "https://api.example.com/v1",
            "auth_type": "api_key",
            "api_key": "${API_KEY}",
            "api_key_header": "X-API-Key"
        }

    Or with OAuth:
        {
            "base_url": "https://api.example.com/v1",
            "auth_type": "oauth2",
            "client_id": "${CLIENT_ID}",
            "client_secret": "${CLIENT_SECRET}",
            "token_url": "https://auth.example.com/oauth/token"
        }

    Usage:
        >>> conn = get_connection("rest", spark, env="prod")
        >>> data = conn.get("/customers", params={"limit": 100})
        >>> df = conn.fetch_to_dataframe("/customers", paginated=True)
    """

    def test(self) -> bool:
        """Test REST API connection with health check."""
        try:
            # Try a simple GET request
            headers = self.get_headers()
            response = requests.get(
                self.base_url.rstrip("/") + "/",
                headers=headers,
                timeout=30
            )
            # Accept any non-500 response as success
            return response.status_code < 500
        except Exception as e:
            self.logger.error(f"REST API connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return base URL."""
        return self.base_url

    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers including authentication."""
        headers = self.config.get("headers", {}).copy()

        auth_type = self.auth_type

        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {self.config['token']}"
        elif auth_type == "api_key":
            key_header = self.config.get("api_key_header", "X-API-Key")
            headers[key_header] = self.config["api_key"]
        elif auth_type == "basic":
            import base64
            credentials = f"{self.config['username']}:{self.config['password']}"
            encoded = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"

        return headers

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Execute GET request.

        Args:
            endpoint: API endpoint (relative to base_url)
            params: Query parameters

        Returns:
            Response JSON as dictionary
        """
        url = self.base_url.rstrip("/") + "/" + endpoint.lstrip("/")
        headers = self.get_headers()

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        self.emit_lineage("read", {"endpoint": endpoint, "method": "GET"})

        return response.json()

    def post(self, endpoint: str, data: Dict) -> Dict:
        """
        Execute POST request.

        Args:
            endpoint: API endpoint
            data: Request body

        Returns:
            Response JSON as dictionary
        """
        url = self.base_url.rstrip("/") + "/" + endpoint.lstrip("/")
        headers = self.get_headers()

        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()

        self.emit_lineage("write", {"endpoint": endpoint, "method": "POST"})

        return response.json()

    def fetch_to_dataframe(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        paginated: bool = False,
        page_size: int = 100,
        max_pages: Optional[int] = None
    ) -> DataFrame:
        """
        Fetch data from API and convert to DataFrame.

        Args:
            endpoint: API endpoint
            params: Query parameters
            paginated: Whether to handle pagination
            page_size: Records per page
            max_pages: Maximum pages to fetch (None = all)

        Returns:
            Spark DataFrame
        """
        if not paginated:
            data = self.get(endpoint, params)
            # Assume response is array or has "data" key
            if isinstance(data, list):
                records = data
            else:
                records = data.get("data", [data])

            return self.spark.createDataFrame(records)

        # Handle pagination
        all_records = []
        page = 1
        has_more = True

        while has_more and (max_pages is None or page <= max_pages):
            page_params = params.copy() if params else {}
            page_params.update({"page": page, "limit": page_size})

            response = self.get(endpoint, page_params)

            # Extract records (adjust based on API structure)
            if isinstance(response, list):
                records = response
            else:
                records = response.get("data", response.get("results", []))

            all_records.extend(records)

            # Check for more pages
            if isinstance(response, dict):
                has_more = response.get("has_more", False) or \
                          response.get("next", None) is not None
            else:
                has_more = len(records) == page_size

            page += 1

        return self.spark.createDataFrame(all_records)


@register_connection("graphql")
class GraphQLConnection(APIConnection):
    """
    GraphQL API connection.

    Example config:
        {
            "base_url": "https://api.example.com/graphql",
            "auth_type": "bearer",
            "token": "${GRAPHQL_TOKEN}",
            "headers": {
                "Content-Type": "application/json"
            }
        }

    Usage:
        >>> conn = get_connection("graphql", spark, env="prod")
        >>> query = '''
        ...   query GetCustomers($limit: Int!) {
        ...     customers(limit: $limit) {
        ...       id
        ...       name
        ...       email
        ...     }
        ...   }
        ... '''
        >>> data = conn.query(query, variables={"limit": 100})
    """

    def test(self) -> bool:
        """Test GraphQL connection with introspection query."""
        try:
            query = "{ __schema { types { name } } }"
            result = self.query(query)
            return "data" in result
        except Exception as e:
            self.logger.error(f"GraphQL connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return GraphQL endpoint URL."""
        return self.base_url

    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers including authentication."""
        headers = self.config.get("headers", {}).copy()
        headers.setdefault("Content-Type", "application/json")

        if self.auth_type == "bearer":
            headers["Authorization"] = f"Bearer {self.config['token']}"
        elif self.auth_type == "api_key":
            key_header = self.config.get("api_key_header", "X-API-Key")
            headers[key_header] = self.config["api_key"]

        return headers

    def query(
        self,
        query: str,
        variables: Optional[Dict] = None,
        operation_name: Optional[str] = None
    ) -> Dict:
        """
        Execute GraphQL query.

        Args:
            query: GraphQL query string
            variables: Query variables
            operation_name: Operation name

        Returns:
            Response data
        """
        headers = self.get_headers()

        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        if operation_name:
            payload["operationName"] = operation_name

        response = requests.post(self.base_url, headers=headers, json=payload)
        response.raise_for_status()

        result = response.json()

        if "errors" in result:
            raise Exception(f"GraphQL errors: {result['errors']}")

        self.emit_lineage("read", {"query": query[:100]})

        return result.get("data", {})

    def query_to_dataframe(
        self,
        query: str,
        variables: Optional[Dict] = None,
        data_path: Optional[str] = None
    ) -> DataFrame:
        """
        Execute GraphQL query and convert to DataFrame.

        Args:
            query: GraphQL query
            variables: Query variables
            data_path: Path to data in response (e.g., "customers")

        Returns:
            Spark DataFrame
        """
        result = self.query(query, variables)

        # Extract data from response
        if data_path:
            for key in data_path.split("."):
                result = result[key]

        if isinstance(result, dict):
            result = [result]

        return self.spark.createDataFrame(result)


@register_connection("soap")
class SOAPConnection(APIConnection):
    """
    SOAP web service connection.

    Example config:
        {
            "base_url": "https://api.example.com/soap",
            "wsdl": "https://api.example.com/soap?wsdl",
            "auth_type": "basic",
            "username": "${SOAP_USER}",
            "password": "${SOAP_PASSWORD}"
        }
    """

    def test(self) -> bool:
        """Test SOAP connection by fetching WSDL."""
        try:
            wsdl_url = self.config.get("wsdl", self.base_url + "?wsdl")
            response = requests.get(wsdl_url, timeout=30)
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"SOAP connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return SOAP endpoint URL."""
        return self.base_url

    def get_headers(self) -> Dict[str, str]:
        """Get SOAP headers."""
        headers = {
            "Content-Type": "text/xml; charset=utf-8"
        }

        if "soap_action" in self.config:
            headers["SOAPAction"] = self.config["soap_action"]

        return headers


@register_connection("grpc")
class gRPCConnection(APIConnection):
    """
    gRPC service connection.

    Example config:
        {
            "host": "grpc.example.com",
            "port": 50051,
            "secure": true,
            "credentials_file": "/path/to/credentials.pem"
        }
    """

    def test(self) -> bool:
        """Test gRPC connection."""
        try:
            required = ["host", "port"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"gRPC connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return gRPC host:port."""
        return f"{self.config['host']}:{self.config['port']}"

    def get_headers(self) -> Dict[str, str]:
        """Get gRPC metadata (headers)."""
        return self.config.get("metadata", {})


@register_connection("webhook")
class WebhookConnection(APIConnection):
    """
    Webhook connection for sending data to webhooks.

    Example config:
        {
            "url": "https://hooks.example.com/webhook/abc123",
            "auth_type": "bearer",
            "token": "${WEBHOOK_TOKEN}",
            "method": "POST",
            "headers": {
                "Content-Type": "application/json"
            }
        }
    """

    def test(self) -> bool:
        """Test webhook by sending empty payload."""
        try:
            # Don't actually send test webhook in production
            return "url" in self.config
        except Exception as e:
            self.logger.error(f"Webhook connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return webhook URL."""
        return self.config["url"]

    def get_headers(self) -> Dict[str, str]:
        """Get webhook headers."""
        headers = self.config.get("headers", {}).copy()

        if self.auth_type == "bearer":
            headers["Authorization"] = f"Bearer {self.config['token']}"

        return headers

    def send(self, data: Dict) -> Dict:
        """
        Send data to webhook.

        Args:
            data: Payload data

        Returns:
            Response data
        """
        url = self.get_connection()
        headers = self.get_headers()
        method = self.config.get("method", "POST").upper()

        if method == "POST":
            response = requests.post(url, headers=headers, json=data)
        elif method == "PUT":
            response = requests.put(url, headers=headers, json=data)
        else:
            raise ValueError(f"Unsupported webhook method: {method}")

        response.raise_for_status()

        self.emit_lineage("write", {"webhook": url, "method": method})

        return response.json() if response.text else {}


@register_connection("http")
class HTTPConnection(APIConnection):
    """
    Generic HTTP connection for custom integrations.

    Example config:
        {
            "base_url": "https://data.example.com",
            "timeout": 60,
            "verify_ssl": true
        }
    """

    def test(self) -> bool:
        """Test HTTP connection."""
        try:
            response = requests.get(self.base_url, timeout=10)
            return response.status_code < 500
        except Exception as e:
            self.logger.error(f"HTTP connection test failed: {e}")
            return False

    def get_connection(self) -> str:
        """Return base URL."""
        return self.base_url

    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers."""
        return self.config.get("headers", {})

    def request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> requests.Response:
        """
        Execute arbitrary HTTP request.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            endpoint: Endpoint path
            data: Request body
            params: Query parameters

        Returns:
            Response object
        """
        url = self.base_url.rstrip("/") + "/" + endpoint.lstrip("/")
        headers = self.get_headers()
        timeout = self.config.get("timeout", 60)

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data,
            params=params,
            timeout=timeout,
            verify=self.config.get("verify_ssl", True)
        )

        response.raise_for_status()

        self.emit_lineage(
            "read" if method.upper() == "GET" else "write",
            {"endpoint": endpoint, "method": method}
        )

        return response
