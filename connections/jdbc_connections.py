"""
JDBC connection implementations for all major relational databases.

Supports: PostgreSQL, MySQL, Oracle, SQL Server, DB2, Teradata,
MariaDB, SAP HANA, Vertica, Greenplum, Netezza, Sybase, SQLite.

All connections are automatically registered with ConnectionFactory.
"""

from typing import Dict, Any
from pyspark.sql import SparkSession

from .base import JDBCConnection
from .factory import register_connection


@register_connection("postgresql")
@register_connection("postgres")
class PostgreSQLConnection(JDBCConnection):
    """
    PostgreSQL connection via JDBC.
    Sub-Group: Relational Databases

    Example config (config/connections/postgresql/prod.json):
        {
            "driver": "org.postgresql.Driver",
            "url": "jdbc:postgresql://prod-db.example.com:5432/analytics",
            "user": "${POSTGRES_USER}",
            "password": "${POSTGRES_PASSWORD}",
            "properties": {
                "fetchsize": "10000",
                "batchsize": "10000",
                "queryTimeout": "300",
                "ssl": "true",
                "sslmode": "require"
            }
        }

    Usage:
        >>> from sparkle.connections import get_connection
        >>> conn = get_connection("postgres", spark, env="prod")
        >>> df = conn.read_table(
        ...     "public.customers",
        ...     partition_column="customer_id",
        ...     lower_bound=0,
        ...     upper_bound=1000000,
        ...     num_partitions=20
        ... )
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "org.postgresql.Driver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("mysql")
class MySQLConnection(JDBCConnection):
    """
    MySQL connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "com.mysql.cj.jdbc.Driver",
            "url": "jdbc:mysql://mysql.example.com:3306/mydb",
            "user": "${MYSQL_USER}",
            "password": "${MYSQL_PASSWORD}",
            "properties": {
                "useSSL": "true",
                "serverTimezone": "UTC",
                "zeroDateTimeBehavior": "convertToNull",
                "rewriteBatchedStatements": "true"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "com.mysql.cj.jdbc.Driver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("mariadb")
class MariaDBConnection(JDBCConnection):
    """
    MariaDB connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "org.mariadb.jdbc.Driver",
            "url": "jdbc:mariadb://mariadb.example.com:3306/mydb",
            "user": "${MARIADB_USER}",
            "password": "${MARIADB_PASSWORD}",
            "properties": {
                "useSsl": "true"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "org.mariadb.jdbc.Driver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("oracle")
class OracleConnection(JDBCConnection):
    """
    Oracle Database connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "oracle.jdbc.OracleDriver",
            "url": "jdbc:oracle:thin:@//oracle.example.com:1521/PROD",
            "user": "${ORACLE_USER}",
            "password": "${ORACLE_PASSWORD}",
            "properties": {
                "fetchsize": "10000",
                "oracle.jdbc.timezoneAsRegion": "false",
                "v$session.program": "SparkleETL"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "oracle.jdbc.OracleDriver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("sqlserver")
@register_connection("mssql")
class SQLServerConnection(JDBCConnection):
    """
    Microsoft SQL Server connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "url": "jdbc:sqlserver://sqlserver.example.com:1433;databaseName=Analytics",
            "user": "${SQLSERVER_USER}",
            "password": "${SQLSERVER_PASSWORD}",
            "properties": {
                "encrypt": "true",
                "trustServerCertificate": "false",
                "loginTimeout": "30"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("db2")
class DB2Connection(JDBCConnection):
    """
    IBM DB2 connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "com.ibm.db2.jcc.DB2Driver",
            "url": "jdbc:db2://db2.example.com:50000/PROD",
            "user": "${DB2_USER}",
            "password": "${DB2_PASSWORD}",
            "properties": {
                "progressiveStreaming": "2",
                "progresssiveLocators": "2"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "com.ibm.db2.jcc.DB2Driver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("teradata")
class TeradataConnection(JDBCConnection):
    """
    Teradata connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "com.teradata.jdbc.TeraDriver",
            "url": "jdbc:teradata://teradata.example.com/DATABASE=prod",
            "user": "${TERADATA_USER}",
            "password": "${TERADATA_PASSWORD}",
            "properties": {
                "TYPE": "FASTEXPORT",
                "SESSIONS": "10",
                "TMODE": "ANSI"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "com.teradata.jdbc.TeraDriver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("saphana")
@register_connection("hana")
class SAPHANAConnection(JDBCConnection):
    """
    SAP HANA connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "com.sap.db.jdbc.Driver",
            "url": "jdbc:sap://hana.example.com:30015/",
            "user": "${HANA_USER}",
            "password": "${HANA_PASSWORD}",
            "properties": {
                "reconnect": "true",
                "currentschema": "PROD"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "com.sap.db.jdbc.Driver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("vertica")
class VerticaConnection(JDBCConnection):
    """
    Vertica connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "com.vertica.jdbc.Driver",
            "url": "jdbc:vertica://vertica.example.com:5433/analytics",
            "user": "${VERTICA_USER}",
            "password": "${VERTICA_PASSWORD}",
            "properties": {
                "ConnectionLoadBalance": "true",
                "LabelName": "SparkleETL"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "com.vertica.jdbc.Driver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("greenplum")
class GreenplumConnection(JDBCConnection):
    """
    Greenplum connection via JDBC (uses PostgreSQL driver).
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "org.postgresql.Driver",
            "url": "jdbc:postgresql://greenplum.example.com:5432/prod",
            "user": "${GREENPLUM_USER}",
            "password": "${GREENPLUM_PASSWORD}",
            "properties": {
                "fetchsize": "10000"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "org.postgresql.Driver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("netezza")
class NetezzaConnection(JDBCConnection):
    """
    IBM Netezza connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "org.netezza.Driver",
            "url": "jdbc:netezza://netezza.example.com:5480/PROD",
            "user": "${NETEZZA_USER}",
            "password": "${NETEZZA_PASSWORD}"
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "org.netezza.Driver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("sybase")
class SybaseConnection(JDBCConnection):
    """
    Sybase ASE connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "com.sybase.jdbc4.jdbc.SybDriver",
            "url": "jdbc:sybase:Tds:sybase.example.com:5000/prod",
            "user": "${SYBASE_USER}",
            "password": "${SYBASE_PASSWORD}"
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "com.sybase.jdbc4.jdbc.SybDriver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("sqlite")
class SQLiteConnection(JDBCConnection):
    """
    SQLite connection via JDBC (for local testing).
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "org.sqlite.JDBC",
            "url": "jdbc:sqlite:/tmp/test.db"
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "org.sqlite.JDBC"
        super().__init__(spark, config, env, **kwargs)

    def test(self) -> bool:
        """Test SQLite connection."""
        try:
            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config["url"]) \
                .option("driver", self.config["driver"]) \
                .option("query", "SELECT 1 AS test") \
                .load()
            return test_df.count() == 1
        except Exception as e:
            self.logger.error(f"SQLite connection test failed: {e}")
            return False


@register_connection("presto")
class PrestoConnection(JDBCConnection):
    """
    Presto connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "com.facebook.presto.jdbc.PrestoDriver",
            "url": "jdbc:presto://presto.example.com:8080/hive/default",
            "user": "${PRESTO_USER}",
            "properties": {
                "SSL": "true"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "com.facebook.presto.jdbc.PrestoDriver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("trino")
class TrinoConnection(JDBCConnection):
    """
    Trino (formerly Presto SQL) connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "io.trino.jdbc.TrinoDriver",
            "url": "jdbc:trino://trino.example.com:8080/hive/default",
            "user": "${TRINO_USER}",
            "properties": {
                "SSL": "true"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "io.trino.jdbc.TrinoDriver"
        super().__init__(spark, config, env, **kwargs)


@register_connection("clickhouse")
class ClickHouseConnection(JDBCConnection):
    """
    ClickHouse connection via JDBC.
    Sub-Group: Relational Databases

    Example config:
        {
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "url": "jdbc:clickhouse://clickhouse.example.com:8123/default",
            "user": "${CLICKHOUSE_USER}",
            "password": "${CLICKHOUSE_PASSWORD}",
            "properties": {
                "socket_timeout": "300000",
                "max_execution_time": "300"
            }
        }
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], env: str = "dev", **kwargs):
        if "driver" not in config:
            config["driver"] = "com.clickhouse.jdbc.ClickHouseDriver"
        super().__init__(spark, config, env, **kwargs)
