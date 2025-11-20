# Extending Sparkle Connections

Guide for adding new connection types to Sparkle.

## Quick Start: Adding a New Connection

### Step 1: Choose the Right Base Class

```python
from sparkle.connections.base import (
    JDBCConnection,          # For JDBC databases
    CloudStorageConnection,   # For cloud storage
    StreamingConnection,      # For streaming systems
    APIConnection,            # For REST/GraphQL/SaaS APIs
    SparkleConnection        # For custom types
)
```

### Step 2: Create Your Connection Class

```python
from sparkle.connections import register_connection
from sparkle.connections.base import APIConnection
from pyspark.sql import DataFrame
from typing import Dict, Any

@register_connection("my_saas_platform")
class MySaaSConnection(APIConnection):
    """
    Connection to My SaaS Platform.

    Example config:
        {
            "base_url": "https://api.myplatform.com/v1",
            "api_key": "secret://prod/myplatform-key",
            "auth_type": "bearer"
        }

    Usage:
        >>> conn = Connection.get("my_saas_platform", spark, env="prod")
        >>> df = conn.read(endpoint="customers", limit=1000)
        >>> conn.write(df, endpoint="customers_export")
    """

    def test(self) -> bool:
        """Test API connectivity."""
        try:
            import requests
            response = requests.get(
                f"{self.base_url}/health",
                headers=self.get_headers(),
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False

    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers with authentication."""
        return {
            "Authorization": f"Bearer {self.config['api_key']}",
            "Content-Type": "application/json"
        }

    def read(
        self,
        endpoint: str,
        params: Dict[str, Any] = None,
        limit: int = 10000,
        **kwargs
    ) -> DataFrame:
        """
        Read data from API endpoint.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            limit: Max records to fetch
            **kwargs: Additional options

        Returns:
            Spark DataFrame
        """
        import requests
        import pandas as pd

        url = f"{self.base_url}/{endpoint}"
        headers = self.get_headers()

        # Fetch data with pagination
        all_records = []
        page = 1

        while len(all_records) < limit:
            response = requests.get(
                url,
                headers=headers,
                params={**(params or {}), "page": page, "page_size": 100}
            )
            response.raise_for_status()

            data = response.json()
            records = data.get("results", [])

            if not records:
                break

            all_records.extend(records)
            page += 1

        # Convert to Spark DataFrame
        pdf = pd.DataFrame(all_records[:limit])
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"endpoint": endpoint, "records": len(all_records)})

        return df

    def write(
        self,
        df: DataFrame,
        endpoint: str,
        mode: str = "append",
        batch_size: int = 100,
        **kwargs
    ) -> None:
        """
        Write DataFrame to API endpoint.

        Args:
            df: Spark DataFrame to write
            endpoint: API endpoint path
            mode: Write mode (append/overwrite)
            batch_size: Records per API call
            **kwargs: Additional options
        """
        import requests

        url = f"{self.base_url}/{endpoint}"
        headers = self.get_headers()

        # Convert to list of dictionaries
        records = [row.asDict() for row in df.collect()]

        # Send in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]

            response = requests.post(
                url,
                headers=headers,
                json={"records": batch}
            )
            response.raise_for_status()

        self.emit_lineage("write", {"endpoint": endpoint, "records": len(records)})
```

### Step 3: Add Configuration

Create `config/connections/my_saas_platform/prod.json`:

```json
{
  "base_url": "https://api.myplatform.com/v1",
  "api_key": "secret://prod-secrets/myplatform-api-key",
  "auth_type": "bearer",
  "rate_limit": 100,
  "max_retries": 3
}
```

### Step 4: Use Your Connection

```python
from sparkle.connections import Connection

conn = Connection.get("my_saas_platform", spark, env="prod")

# Read data
df = conn.read(endpoint="customers", limit=10000)

# Transform
result_df = df.filter(df.status == "active")

# Write back
conn.write(result_df, endpoint="active_customers")
```

---

## Patterns for Common Connection Types

### JDBC Database

```python
@register_connection("my_database")
class MyDatabaseConnection(JDBCConnection):
    def __init__(self, spark, config, env="prod", **kwargs):
        # Set driver if not in config
        if "driver" not in config:
            config["driver"] = "com.mydatabase.jdbc.Driver"
        super().__init__(spark, config, env, **kwargs)
```

That's it! JDBCConnection already has read() and write() implemented.

### Cloud Storage (S3-compatible)

```python
@register_connection("cloudflare_r2")
class CloudflareR2Connection(CloudStorageConnection):
    def __init__(self, spark, config, env="prod", **kwargs):
        super().__init__(spark, config, env, **kwargs)
        self._configure_spark()

    def _configure_spark(self):
        conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        conf.set("fs.s3a.endpoint", self.config["endpoint"])
        conf.set("fs.s3a.access.key", self.config["access_key"])
        conf.set("fs.s3a.secret.key", self.config["secret_key"])
        conf.set("fs.s3a.path.style.access", "true")

    def test(self) -> bool:
        try:
            base_path = self.get_base_path()
            files = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
                .get(self.spark.sparkContext._jsc.hadoopConfiguration()) \
                .listStatus(self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path))
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
```

CloudStorageConnection already has read() and write() with multi-format support.

### Streaming System

```python
@register_connection("nats_jetstream")
class NATSJetStreamConnection(StreamingConnection):
    def get_stream_format(self) -> str:
        return "nats"  # or custom format

    def get_read_stream_options(self) -> Dict[str, str]:
        return {
            "nats.url": self.config["url"],
            "nats.subject": self.config.get("subject", ""),
            "nats.credentials": self.config.get("credentials", "")
        }

    def get_write_stream_options(self) -> Dict[str, str]:
        return self.get_read_stream_options()

    def test(self) -> bool:
        # Test NATS connectivity
        return True
```

StreamingConnection provides read_stream() and write_stream().

### REST API with Pagination

```python
@register_connection("hubspot")
class HubSpotConnection(APIConnection):
    def get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config['api_token']}",
            "Content-Type": "application/json"
        }

    def read(self, object_type: str, properties: list = None, **kwargs) -> DataFrame:
        """Read HubSpot objects (contacts, companies, deals, etc.)."""
        import requests
        import pandas as pd

        url = f"{self.base_url}/crm/v3/objects/{object_type}"
        headers = self.get_headers()

        all_records = []
        after = None

        while True:
            params = {"limit": 100}
            if after:
                params["after"] = after
            if properties:
                params["properties"] = ",".join(properties)

            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()
            all_records.extend(data.get("results", []))

            # Check for more pages
            paging = data.get("paging", {})
            after = paging.get("next", {}).get("after")

            if not after:
                break

        pdf = pd.DataFrame(all_records)
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"object_type": object_type, "records": len(all_records)})

        return df

    def write(self, df: DataFrame, object_type: str, **kwargs) -> None:
        """Write records to HubSpot."""
        import requests

        url = f"{self.base_url}/crm/v3/objects/{object_type}/batch/create"
        headers = self.get_headers()

        records = [row.asDict() for row in df.collect()]

        # HubSpot batch API accepts max 100 records
        for i in range(0, len(records), 100):
            batch = records[i:i + 100]

            response = requests.post(
                url,
                headers=headers,
                json={"inputs": batch}
            )
            response.raise_for_status()

        self.emit_lineage("write", {"object_type": object_type, "records": len(records)})
```

---

## Advanced Patterns

### Using Retry Logic

```python
def read(self, **kwargs) -> DataFrame:
    def fetch_data():
        response = requests.get(...)
        response.raise_for_status()
        return response.json()

    # Retry up to 3 times with exponential backoff
    data = self.retry(
        fetch_data,
        max_attempts=3,
        delay=1,
        backoff=2,
        exceptions=(requests.RequestException,)
    )

    return self.spark.createDataFrame(data)
```

### Using Secrets

```python
def __init__(self, spark, config, env="prod", **kwargs):
    super().__init__(spark, config, env, **kwargs)

    # Config automatically resolves secrets:
    # "api_key": "secret://prod/my-api-key" -> dbutils.secrets.get("prod", "my-api-key")
    # "password": "${DB_PASSWORD}" -> os.environ["DB_PASSWORD"]
    # "endpoint": "conf://my.custom.endpoint" -> spark.conf.get("my.custom.endpoint")

    self.api_key = self.config["api_key"]  # Already resolved!

    # Or explicitly get secrets:
    self.api_key = self.get_secret("prod-secrets", "my-api-key")
    self.endpoint = self.get_conf("my.custom.endpoint", "https://default.com")
```

### Streaming with Checkpointing

```python
def read_stream(self, topic: str, **kwargs) -> DataFrame:
    """Read from stream with automatic checkpointing."""
    options = self.get_read_stream_options()
    options["subscribe"] = topic

    reader = self.spark.readStream.format(self.get_stream_format())

    for key, value in options.items():
        reader = reader.option(key, str(value))

    return reader.load()

# Usage
stream_df = conn.read_stream(topic="events")

query = conn.write_stream(
    stream_df,
    checkpoint_location="/tmp/checkpoints/events",
    output_mode="append",
    trigger={"processingTime": "10 seconds"}
)

query.awaitTermination()
```

---

## Testing Your Connection

```python
def test_my_connection():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("TestMyConnection") \
        .getOrCreate()

    config = {
        "base_url": "https://api.example.com",
        "api_key": "test-key"
    }

    conn = Connection.get("my_saas_platform", spark, config=config)

    # Test connectivity
    assert conn.test(), "Connection test failed"

    # Test read
    df = conn.read(endpoint="test", limit=10)
    assert df.count() > 0, "No data returned"

    print("✓ All tests passed!")

if __name__ == "__main__":
    test_my_connection()
```

---

## Best Practices

### 1. Always Implement test()

```python
def test(self) -> bool:
    """Test connection health."""
    try:
        # Perform lightweight connectivity check
        # e.g., ping endpoint, list tables, check auth
        return True
    except Exception as e:
        self.logger.error(f"Test failed: {e}")
        return False
```

### 2. Handle Pagination

```python
def read(self, **kwargs) -> DataFrame:
    all_records = []
    page = 1

    while True:
        response = self._fetch_page(page)
        records = response.get("data", [])

        if not records:
            break

        all_records.extend(records)
        page += 1

        # Respect rate limits
        time.sleep(0.1)

    return self.spark.createDataFrame(all_records)
```

### 3. Emit Lineage

```python
def read(self, **kwargs) -> DataFrame:
    df = # ... fetch data ...

    self.emit_lineage("read", {
        "source": "my_source",
        "records": df.count(),
        "timestamp": datetime.utcnow().isoformat()
    })

    return df
```

### 4. Use Type Hints

```python
from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame

def read(
    self,
    table: str,
    columns: Optional[List[str]] = None,
    filter: Optional[str] = None,
    **kwargs
) -> DataFrame:
    """Read with type hints for better IDE support."""
    pass
```

### 5. Document with Examples

```python
class MyConnection(SparkleConnection):
    """
    Connection to My Platform.

    Example config:
        {
            "url": "https://api.example.com",
            "token": "secret://prod/api-token"
        }

    Usage:
        >>> conn = Connection.get("my_platform", spark, env="prod")
        >>> df = conn.read(table="customers")
        >>> df.show()
        >>> conn.write(df, table="customers_processed")
    """
```

---

## File Organization

### Option 1: Add to Existing Grouped File

Add your connection to the appropriate file:
- `jdbc_connections.py` - JDBC databases
- `cloud_storage.py` - Cloud storage
- `streaming.py` - Streaming systems
- `api.py` - REST/GraphQL APIs
- `nosql.py` - NoSQL databases

### Option 2: Create Individual File

Create `connections/my_platform.py`:

```python
"""My Platform connection."""

from .base import APIConnection
from .factory import register_connection

@register_connection("my_platform")
class MyPlatformConnection(APIConnection):
    # ... implementation ...
```

Then import in `connections/__init__.py`:

```python
from .my_platform import MyPlatformConnection

__all__ = [
    # ... existing exports ...
    "MyPlatformConnection",
]
```

---

## Common Gotchas

### 1. Secrets Not Resolving

Make sure your config uses the right pattern:
```json
{
  "password": "secret://scope/key",  # ✓ Correct
  "password": "secrets://scope/key", # ✗ Wrong (extra 's')
  "password": "${DB_PASSWORD}"       # ✓ Correct for env vars
}
```

### 2. JDBC Driver Not Found

Add JAR to Spark submit:
```bash
spark-submit --jars /path/to/driver.jar my_job.py
```

Or in notebook:
```python
spark.conf.set("spark.jars", "/path/to/driver.jar")
```

### 3. Streaming Checkpoint Issues

Always use unique checkpoint location per stream:
```python
conn.write_stream(
    df,
    checkpoint_location=f"/checkpoints/{job_name}/{stream_name}",
    # ...
)
```

---

## Examples in the Wild

See existing connections for reference:
- **JDBC**: `connections/jdbc_connections.py` - PostgreSQL, MySQL, etc.
- **Cloud**: `connections/cloud_storage.py` - S3, ADLS, GCS
- **Streaming**: `connections/streaming.py` - Kafka, Kinesis, etc.
- **API**: `connections/api.py` - REST, GraphQL
- **NoSQL**: `connections/nosql.py` - MongoDB, Cassandra, etc.
- **Warehouse**: `connections/data_warehouses.py` - Snowflake, BigQuery, etc.

---

## Contributing

1. Create your connection class
2. Add configuration examples
3. Write tests
4. Update documentation
5. Submit PR with:
   - Connection implementation
   - Config examples
   - Usage examples
   - Test cases

---

## Need Help?

- Check `connections/STATUS.md` for implementation status
- Review existing connections for patterns
- See `connections/README.md` for architecture
- Open an issue for questions

---

**Happy connecting!** 🔌✨
