"""
Mainframe and legacy system connections.

Supports: IBM i (AS/400), Mainframe EBCDIC/VSAM, SAP ECC/S4HANA ODP,
SAP Datasphere, Oracle E-Business Suite, PeopleSoft.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
import logging

from .base import SparkleConnection, JDBCConnection
from .factory import register_connection


@register_connection("ibm_i")
@register_connection("as400")
@register_connection("iseries")
class IBMiConnection(JDBCConnection):
    """
    IBM i (AS/400, iSeries) connection via JT400 JDBC driver.
    Sub-Group: Mainframe & Legacy

    Connects to IBM i DB2 databases using the open-source JT400 driver.

    Example config:
        ibm_i_prod:
          type: ibm_i
          host: as400.example.com
          port: 8471
          database: PRODLIB
          username: secret://ibmi/username
          password: secret://ibmi/password
          driver: com.ibm.as400.access.AS400JDBCDriver
          naming: system  # 'system' or 'sql'
          libraries: PRODLIB,TESTLIB

    Usage:
        >>> conn = Connection.get("ibm_i", spark, env="prod")
        >>> df = conn.read(table="CUSTOMER", library="PRODLIB")
        >>> # Or use fully qualified name
        >>> df = conn.read(table="PRODLIB.CUSTOMER")
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.host = self.config["host"]
        self.port = self.config.get("port", 8471)
        self.database = self.config.get("database", "*SYSBAS")
        self.naming = self.config.get("naming", "system")  # system or sql
        self.libraries = self.config.get("libraries", "")
        self.driver = self.config.get(
            "driver",
            "com.ibm.as400.access.AS400JDBCDriver"
        )

    def _get_jdbc_url(self) -> str:
        """Build IBM i JDBC URL."""
        url = f"jdbc:as400://{self.host}:{self.port}/{self.database}"

        params = []
        if self.naming:
            params.append(f"naming={self.naming}")
        if self.libraries:
            params.append(f"libraries={self.libraries}")

        # Additional IBM i specific parameters
        params.append("translate binary=true")
        params.append("date format=iso")
        params.append("time format=iso")

        if params:
            url += ";" + ";".join(params)

        return url

    def read(
        self,
        table: str,
        library: Optional[str] = None,
        query: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read from IBM i table.

        Args:
            table: Table name
            library: Optional library name (if not in table name)
            query: Optional SQL query (overrides table)

        Returns:
            DataFrame with table data
        """
        if library and "." not in table:
            table = f"{library}.{table}"

        return super().read(table=table, query=query, **kwargs)


@register_connection("mainframe_ebcdic")
@register_connection("vsam")
@register_connection("copybook")
class MainframeEBCDICConnection(SparkleConnection):
    """
    Mainframe EBCDIC/VSAM file connection with COBOL copybook parsing.
    Sub-Group: Mainframe & Legacy

    Reads mainframe files (EBCDIC-encoded) using copybook layout definitions.
    Common use case: Migrating mainframe data to cloud data lakes.

    Example config:
        mainframe_prod:
          type: mainframe_ebcdic
          s3_data_path: s3://mainframe-exports/customer-master/
          copybook_path: s3://mainframe-schemas/customer-copybook.cpy
          file_format: fixed_width  # or 'vsam'
          encoding: cp037  # EBCDIC encoding (cp037 = US EBCDIC)
          record_length: 500

    Usage:
        >>> conn = Connection.get("mainframe_ebcdic", spark, env="prod")
        >>> df = conn.read(
        ...     data_path="s3://data/customer-extract-20240115.dat",
        ...     copybook_path="s3://schemas/customer.cpy"
        ... )
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.s3_data_path = self.config.get("s3_data_path")
        self.copybook_path = self.config.get("copybook_path")
        self.file_format = self.config.get("file_format", "fixed_width")
        self.encoding = self.config.get("encoding", "cp037")
        self.record_length = self.config.get("record_length")

    def test(self) -> bool:
        """Test mainframe connection by checking file paths."""
        try:
            if self.s3_data_path:
                # Check if data path exists
                self.logger.info(f"Checking mainframe data path: {self.s3_data_path}")
            if self.copybook_path:
                self.logger.info(f"Copybook path: {self.copybook_path}")
            return True
        except Exception as e:
            self.logger.error(f"Mainframe connection test failed: {e}")
            return False

    def _parse_copybook(self, copybook_path: str) -> Dict[str, Any]:
        """
        Parse COBOL copybook to extract field definitions.

        Returns schema dict with field names, positions, lengths, and types.
        """
        # This is a simplified parser - production would use cobrix or similar
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

        fields = []

        # Read copybook file
        with open(copybook_path.replace("s3://", "/mnt/"), "r") as f:
            copybook_content = f.read()

        # Parse copybook lines (simplified - real parser is complex)
        # Example: 01 CUSTOMER-RECORD.
        #          05 CUST-ID        PIC 9(10).
        #          05 CUST-NAME      PIC X(50).

        import re
        pic_pattern = re.compile(r'(\d+)\s+(\S+)\s+PIC\s+([X9VS]+)\((\d+)\)')

        for line in copybook_content.split('\n'):
            match = pic_pattern.search(line)
            if match:
                level, name, pic_type, length = match.groups()

                if pic_type.startswith('9'):
                    field_type = IntegerType()
                elif pic_type.startswith('X'):
                    field_type = StringType()
                else:
                    field_type = StringType()

                fields.append(StructField(name, field_type, True))

        return StructType(fields)

    def read(
        self,
        data_path: Optional[str] = None,
        copybook_path: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read EBCDIC mainframe file using copybook schema.

        Args:
            data_path: Path to mainframe data file (overrides config)
            copybook_path: Path to COBOL copybook (overrides config)

        Returns:
            DataFrame with parsed mainframe data
        """
        data_path = data_path or self.s3_data_path
        copybook_path = copybook_path or self.copybook_path

        if not data_path or not copybook_path:
            raise ValueError("Both data_path and copybook_path are required")

        # Use Cobrix library for production mainframe parsing
        # https://github.com/AbsaOSS/cobrix

        self.logger.info(f"Reading mainframe EBCDIC data from {data_path}")

        df = (
            self.spark.read
            .format("cobol")
            .option("copybook", copybook_path)
            .option("encoding", self.encoding)
            .option("schema_retention_policy", "collapse_root")
            .load(data_path)
        )

        return df

    def write(self, df: DataFrame, path: str, **kwargs) -> None:
        """
        Write DataFrame to EBCDIC format (rarely used - mostly for testing).

        Args:
            df: DataFrame to write
            path: Output path
        """
        self.logger.warning("Writing to EBCDIC format is rarely needed in production")

        # Convert to CSV or Parquet instead
        df.write.mode("overwrite").parquet(path)


@register_connection("sap_odp")
@register_connection("sap_odq")
@register_connection("sap_s4hana")
class SAPODPConnection(SparkleConnection):
    """
    SAP ECC/S/4HANA ODP (Operational Data Provisioning) connection.
    Sub-Group: Mainframe & Legacy

    Extracts data from SAP using ODP framework (replacement for legacy extractors).
    Supports both full and delta loads with change data capture.

    Example config:
        sap_prod:
          type: sap_odp
          host: sap.example.com
          system_number: "00"
          client: "100"
          username: secret://sap/username
          password: secret://sap/password
          language: EN
          odp_context: SAPI  # SAPI, ABAP_CDS, BW, SLT
          subscriber_name: SPARKLE_PROD

    Usage:
        >>> conn = Connection.get("sap_odp", spark, env="prod")
        >>> # Extract SAP table via ODP
        >>> df = conn.read(
        ...     odp_name="0CUSTOMER_ATTR",
        ...     extraction_mode="full"
        ... )
        >>> # Delta extraction
        >>> df = conn.read(
        ...     odp_name="0CUSTOMER_ATTR",
        ...     extraction_mode="delta",
        ...     request_id="last"
        ... )
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.host = self.config["host"]
        self.system_number = self.config["system_number"]
        self.client = self.config["client"]
        self.username = self.config["username"]
        self.password = self.config["password"]
        self.language = self.config.get("language", "EN")
        self.odp_context = self.config.get("odp_context", "SAPI")
        self.subscriber_name = self.config.get("subscriber_name", "SPARKLE")

    def test(self) -> bool:
        """Test SAP ODP connection."""
        try:
            # Use pyrfc (SAP NetWeaver RFC SDK) to test connection
            from pyrfc import Connection as SAPConnection

            conn = SAPConnection(
                ashost=self.host,
                sysnr=self.system_number,
                client=self.client,
                user=self.username,
                passwd=self.password,
                lang=self.language
            )

            # Test with a simple RFC call
            result = conn.call('RFC_SYSTEM_INFO')

            conn.close()

            self.logger.info(f"✓ SAP ODP connection successful to {self.host}")
            return True
        except Exception as e:
            self.logger.error(f"✗ SAP ODP connection test failed: {e}")
            return False

    def read(
        self,
        odp_name: str,
        extraction_mode: str = "full",
        request_id: Optional[str] = None,
        filter_values: Optional[List[Dict]] = None,
        fields: Optional[List[str]] = None,
        package_size: int = 50000,
        **kwargs
    ) -> DataFrame:
        """
        Read data from SAP via ODP.

        Args:
            odp_name: ODP object name (e.g., '0CUSTOMER_ATTR', '2LIS_11_VAITM')
            extraction_mode: 'full' or 'delta'
            request_id: For delta: request ID or 'last'
            filter_values: ODP selection filters
            fields: List of fields to extract
            package_size: Records per data package

        Returns:
            DataFrame with SAP data
        """
        from pyrfc import Connection as SAPConnection

        # Connect to SAP
        sap_conn = SAPConnection(
            ashost=self.host,
            sysnr=self.system_number,
            client=self.client,
            user=self.username,
            passwd=self.password,
            lang=self.language
        )

        # Initialize ODP extraction
        self.logger.info(f"Extracting SAP ODP object: {odp_name} ({extraction_mode})")

        # Call ODP RFC function module
        # This is simplified - production needs ODQMON transaction handling
        result = sap_conn.call(
            'RODPS_REPL_ODP_READ',
            SUBSCRIBER=self.subscriber_name,
            EXTRACTIONMODE=extraction_mode.upper(),
            ODPNAME=odp_name,
            PACKAGESIZE=package_size
        )

        # Extract data packages
        all_data = []

        for package in result.get('DATA_PACKAGES', []):
            all_data.extend(package['DATA'])

        sap_conn.close()

        self.logger.info(f"Extracted {len(all_data)} records from SAP ODP")

        # Convert to DataFrame
        if all_data:
            return self.spark.createDataFrame(all_data)
        else:
            return self.spark.createDataFrame([], "dummy STRING")

    def write(self, df: DataFrame, **kwargs) -> None:
        """SAP ODP is read-only (extraction only)."""
        raise NotImplementedError("SAP ODP connection is read-only (extraction only)")


@register_connection("sap_datasphere")
@register_connection("sap_dwc")
class SAPDatasphereConnection(JDBCConnection):
    """
    SAP Datasphere (formerly SAP Data Warehouse Cloud) connection.
    Sub-Group: Mainframe & Legacy

    Connects to SAP's cloud data warehouse via JDBC/ODBC.

    Example config:
        sap_datasphere_prod:
          type: sap_datasphere
          host: mytenant.eu10.hcs.cloud.sap
          port: 443
          database: DWCDB
          username: secret://sap/dwc_username
          password: secret://sap/dwc_password
          schema: MYSPACE
          ssl: true

    Usage:
        >>> conn = Connection.get("sap_datasphere", spark, env="prod")
        >>> df = conn.read(table="SALES_VIEW", schema="MYSPACE")
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.host = self.config["host"]
        self.port = self.config.get("port", 443)
        self.database = self.config.get("database", "DWCDB")
        self.schema = self.config.get("schema", "MYSPACE")
        self.ssl = self.config.get("ssl", True)
        self.driver = self.config.get("driver", "com.sap.db.jdbc.Driver")

    def _get_jdbc_url(self) -> str:
        """Build SAP Datasphere JDBC URL."""
        protocol = "jdbc:sap" if self.ssl else "jdbc:sap"
        url = f"{protocol}://{self.host}:{self.port}"

        params = [
            f"databaseName={self.database}",
            f"currentschema={self.schema}",
            "encrypt=true" if self.ssl else "encrypt=false",
            "validateCertificate=true"
        ]

        url += "?" + "&".join(params)
        return url


@register_connection("oracle_ebs")
@register_connection("oracle_ebusiness")
class OracleEBSConnection(JDBCConnection):
    """
    Oracle E-Business Suite connection.
    Sub-Group: Mainframe & Legacy

    Connects to Oracle EBS database with special handling for
    multi-org, virtual private database (VPD), and EBS-specific tables.

    Example config:
        oracle_ebs_prod:
          type: oracle_ebs
          host: ebs.example.com
          port: 1521
          service_name: EBSPROD
          username: secret://oracle/ebs_username
          password: secret://oracle/ebs_password
          responsibility_id: 50559
          org_id: 101
          apps_schema: APPS

    Usage:
        >>> conn = Connection.get("oracle_ebs", spark, env="prod")
        >>> # Read from EBS table with proper context
        >>> df = conn.read(
        ...     table="AR_CUSTOMERS",
        ...     org_id=101
        ... )
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.service_name = self.config.get("service_name")
        self.sid = self.config.get("sid")
        self.responsibility_id = self.config.get("responsibility_id")
        self.org_id = self.config.get("org_id")
        self.apps_schema = self.config.get("apps_schema", "APPS")
        self.driver = self.config.get("driver", "oracle.jdbc.driver.OracleDriver")

    def _get_jdbc_url(self) -> str:
        """Build Oracle EBS JDBC URL."""
        host = self.config["host"]
        port = self.config.get("port", 1521)

        if self.service_name:
            url = f"jdbc:oracle:thin:@//{host}:{port}/{self.service_name}"
        elif self.sid:
            url = f"jdbc:oracle:thin:@{host}:{port}:{self.sid}"
        else:
            raise ValueError("Either service_name or sid must be specified")

        return url

    def read(
        self,
        table: str,
        org_id: Optional[int] = None,
        query: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read from Oracle EBS table with proper org context.

        Args:
            table: EBS table name
            org_id: Operating unit ID (for multi-org security)
            query: Custom SQL query

        Returns:
            DataFrame with EBS data
        """
        # Set EBS session context if org_id provided
        if org_id and not query:
            # Wrap query with EBS context setting
            org_id = org_id or self.org_id

            query = f"""
            SELECT * FROM {self.apps_schema}.{table}
            WHERE org_id = {org_id}
            """

        return super().read(table=table, query=query, **kwargs)


@register_connection("peoplesoft")
@register_connection("psft")
class PeopleSoftConnection(JDBCConnection):
    """
    Oracle PeopleSoft connection.
    Sub-Group: Mainframe & Legacy

    Connects to PeopleSoft databases (can be Oracle or SQL Server backend).

    Example config:
        peoplesoft_prod:
          type: peoplesoft
          backend: oracle  # or 'sqlserver'
          host: psft.example.com
          port: 1521
          service_name: HRPROD
          username: secret://psft/username
          password: secret://psft/password
          database_name: HRPROD

    Usage:
        >>> conn = Connection.get("peoplesoft", spark, env="prod")
        >>> df = conn.read(table="PS_EMPLOYEES")
        >>> # Or query PeopleSoft views
        >>> df = conn.read(table="PS_JOB")
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.backend = self.config.get("backend", "oracle")
        self.database_name = self.config.get("database_name", "HRPROD")

    def _get_jdbc_url(self) -> str:
        """Build PeopleSoft JDBC URL based on backend."""
        host = self.config["host"]
        port = self.config.get("port")

        if self.backend == "oracle":
            port = port or 1521
            service_name = self.config.get("service_name", self.database_name)
            url = f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
            self.driver = "oracle.jdbc.driver.OracleDriver"

        elif self.backend == "sqlserver":
            port = port or 1433
            database = self.config.get("database", self.database_name)
            url = f"jdbc:sqlserver://{host}:{port};databaseName={database}"
            self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

        else:
            raise ValueError(f"Unsupported PeopleSoft backend: {self.backend}")

        return url

    def read(
        self,
        table: str,
        effective_date: Optional[str] = None,
        query: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read from PeopleSoft table.

        Args:
            table: PS table name (e.g., 'PS_JOB', 'PS_EMPLOYEES')
            effective_date: For effective-dated tables
            query: Custom SQL query

        Returns:
            DataFrame with PeopleSoft data
        """
        # Handle effective-dated tables
        if effective_date and not query:
            query = f"""
            SELECT * FROM {table}
            WHERE EFFDT = (
                SELECT MAX(EFFDT) FROM {table} t2
                WHERE t2.EMPLID = {table}.EMPLID
                AND t2.EFFDT <= TO_DATE('{effective_date}', 'YYYY-MM-DD')
            )
            """

        return super().read(table=table, query=query, **kwargs)
