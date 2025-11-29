"""
Industry-specific protocol handlers.

Healthcare: HL7, FHIR, X12 EDI
Financial: FIX, SWIFT
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
import logging
import requests
import json

from .base import SparkleConnection, APIConnection
from .factory import register_connection


@register_connection("hl7_fhir")
@register_connection("fhir")
class FHIRConnection(APIConnection):
    """
    HL7 FHIR (Fast Healthcare Interoperability Resources) connection.
    Sub-Group: Industry Protocols

    Reads healthcare data from FHIR R4/R5 compliant servers.
    Supports Patient, Observation, Encounter, MedicationRequest, etc.

    Example config (config/connections/fhir/prod.json):
        {
            "base_url": "https://fhir.example.com/api/v1",
            "auth_type": "oauth2",
            "client_id": "${FHIR_CLIENT_ID}",
            "client_secret": "${FHIR_CLIENT_SECRET}",
            "token_url": "https://auth.example.com/oauth/token",
            "fhir_version": "R4"
        }

    Example config (YAML):
        fhir_prod:
          type: fhir
          base_url: https://fhir.example.com/api/v1
          auth_type: oauth2  # or 'basic', 'bearer'
          client_id: secret://fhir/client_id
          client_secret: secret://fhir/client_secret
          token_url: https://auth.example.com/oauth/token
          fhir_version: R4

    Usage:
        >>> conn = Connection.get("fhir", spark, env="prod")
        >>> # Read all patients
        >>> patients_df = conn.read(resource_type="Patient", count=100)
        >>> # Search with parameters
        >>> observations_df = conn.read(
        ...     resource_type="Observation",
        ...     search_params={"patient": "12345", "code": "8867-4"}
        ... )
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.base_url = self.config["base_url"].rstrip("/")
        self.auth_type = self.config.get("auth_type", "bearer")
        self.fhir_version = self.config.get("fhir_version", "R4")
        self._access_token = None

    def _get_access_token(self) -> str:
        """Get OAuth2 access token for FHIR server."""
        if self._access_token:
            return self._access_token

        if self.auth_type == "oauth2":
            token_url = self.config["token_url"]
            client_id = self.config["client_id"]
            client_secret = self.config["client_secret"]
            scope = self.config.get("scope", "system/*.read")

            response = requests.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": scope
                }
            )
            response.raise_for_status()

            self._access_token = response.json()["access_token"]
            return self._access_token

        elif self.auth_type == "bearer":
            return self.config["bearer_token"]

        return None

    def _get_headers(self) -> Dict[str, str]:
        """Get FHIR API headers."""
        headers = {
            "Accept": "application/fhir+json",
            "Content-Type": "application/fhir+json"
        }

        if self.auth_type in ("oauth2", "bearer"):
            token = self._get_access_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"

        return headers

    def test(self) -> bool:
        """Test FHIR server connection with metadata endpoint."""
        try:
            url = f"{self.base_url}/metadata"
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()

            metadata = response.json()
            fhir_version = metadata.get("fhirVersion", "unknown")

            self.logger.info(f"✓ FHIR server connection successful (version: {fhir_version})")
            return True
        except Exception as e:
            self.logger.error(f"✗ FHIR server connection test failed: {e}")
            return False

    def read(
        self,
        resource_type: str,
        search_params: Optional[Dict[str, str]] = None,
        count: int = 100,
        include: Optional[List[str]] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read FHIR resources.

        Args:
            resource_type: FHIR resource (Patient, Observation, Encounter, etc.)
            search_params: FHIR search parameters
            count: Max resources per page (_count parameter)
            include: Resources to include (_include parameter)

        Returns:
            DataFrame with FHIR resources (flattened JSON)
        """
        url = f"{self.base_url}/{resource_type}"
        headers = self._get_headers()

        params = {"_count": count}
        if search_params:
            params.update(search_params)
        if include:
            params["_include"] = ",".join(include)

        all_resources = []

        while url:
            self.logger.info(f"Fetching FHIR {resource_type} from {url}")

            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            bundle = response.json()

            # Extract resources from FHIR Bundle
            entries = bundle.get("entry", [])
            for entry in entries:
                resource = entry.get("resource", {})
                all_resources.append(resource)

            # Get next page link
            links = bundle.get("link", [])
            next_link = next((link["url"] for link in links if link["relation"] == "next"), None)

            url = next_link
            params = {}  # Next URL contains all params

        self.logger.info(f"Fetched {len(all_resources)} {resource_type} resources from FHIR")

        if all_resources:
            return self.spark.createDataFrame(all_resources)
        else:
            return self.spark.createDataFrame([], "id STRING, resourceType STRING")

    def write(self, df: DataFrame, resource_type: str, **kwargs) -> None:
        """
        Write FHIR resources (creates or updates).

        Args:
            df: DataFrame with FHIR resources (JSON structure)
            resource_type: FHIR resource type
        """
        headers = self._get_headers()
        url = f"{self.base_url}/{resource_type}"

        resources = df.collect()

        for row in resources:
            resource = row.asDict()

            # POST (create) or PUT (update)
            if "id" in resource and resource["id"]:
                # Update existing resource
                resource_id = resource["id"]
                response = requests.put(
                    f"{url}/{resource_id}",
                    headers=headers,
                    json=resource
                )
            else:
                # Create new resource
                response = requests.post(url, headers=headers, json=resource)

            response.raise_for_status()

        self.logger.info(f"Wrote {len(resources)} {resource_type} resources to FHIR")


@register_connection("x12_edi")
@register_connection("edi_837")
@register_connection("edi_835")
class X12EDIConnection(SparkleConnection):
    """
    X12 EDI (Electronic Data Interchange) connection for healthcare claims.
    Sub-Group: Industry Protocols

    Parses X12 837 (claims), 835 (remittance), 270/271 (eligibility) files.

    Example config (config/connections/x12_edi/prod.json):
        {
            "input_path": "s3://edi-inbox/837/",
            "output_path": "s3://edi-processed/",
            "transaction_set": "837",
            "segment_terminator": "~",
            "element_separator": "*",
            "sub_element_separator": ":"
        }

    Example config (YAML):
        x12_edi_prod:
          type: x12_edi
          input_path: s3://edi-inbox/837/
          output_path: s3://edi-processed/
          transaction_set: 837  # 837, 835, 270, 271, etc.
          segment_terminator: "~"
          element_separator: "*"
          sub_element_separator: ":"

    Usage:
        >>> conn = Connection.get("x12_edi", spark, env="prod")
        >>> # Read 837 claims
        >>> claims_df = conn.read(
        ...     path="s3://edi/837/claims_20240115.edi",
        ...     transaction_set="837"
        ... )
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.input_path = self.config.get("input_path")
        self.output_path = self.config.get("output_path")
        self.transaction_set = self.config.get("transaction_set", "837")
        self.segment_terminator = self.config.get("segment_terminator", "~")
        self.element_separator = self.config.get("element_separator", "*")
        self.sub_element_separator = self.config.get("sub_element_separator", ":")

    def test(self) -> bool:
        """Test X12 EDI connection."""
        try:
            self.logger.info(f"X12 EDI configuration for transaction set {self.transaction_set}")
            return True
        except Exception as e:
            self.logger.error(f"X12 EDI test failed: {e}")
            return False

    def _parse_x12_file(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Parse X12 EDI file into structured records.

        This is a simplified parser - production would use tigershark or pyx12.
        """
        import re

        # Read EDI file
        with open(file_path.replace("s3://", "/mnt/"), "r") as f:
            edi_content = f.read()

        # Split into segments
        segments = edi_content.split(self.segment_terminator)

        records = []
        current_claim = {}

        for segment in segments:
            if not segment.strip():
                continue

            elements = segment.split(self.element_separator)
            segment_id = elements[0]

            # Parse key segments for 837 claims
            if segment_id == "CLM":
                # Claim segment
                current_claim = {
                    "claim_id": elements[1],
                    "claim_amount": elements[2],
                    "claim_place_of_service": elements[5] if len(elements) > 5 else None
                }

            elif segment_id == "NM1":
                # Name segment
                entity_type = elements[1]
                name = f"{elements[3]} {elements[4]}" if len(elements) > 4 else elements[3]

                if entity_type == "IL":  # Insured
                    current_claim["patient_name"] = name
                elif entity_type == "82":  # Rendering provider
                    current_claim["provider_name"] = name

            elif segment_id == "SE":
                # Transaction set trailer - end of claim
                if current_claim:
                    records.append(current_claim.copy())
                    current_claim = {}

        return records

    def read(
        self,
        path: Optional[str] = None,
        transaction_set: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read and parse X12 EDI files.

        Args:
            path: Path to EDI file(s)
            transaction_set: X12 transaction set (837, 835, etc.)

        Returns:
            DataFrame with parsed EDI data
        """
        path = path or self.input_path
        transaction_set = transaction_set or self.transaction_set

        self.logger.info(f"Parsing X12 {transaction_set} EDI from {path}")

        # Use custom EDI parser or library like pyx12
        # For production, use: https://github.com/azoner/pyx12

        # Simplified: read as text and parse
        edi_rdd = self.spark.sparkContext.textFile(path)

        # Parse EDI content
        # This is a placeholder - production needs proper EDI parser
        records = self._parse_x12_file(path)

        if records:
            return self.spark.createDataFrame(records)
        else:
            return self.spark.createDataFrame([], "claim_id STRING")

    def write(self, df: DataFrame, path: str, **kwargs) -> None:
        """
        Write DataFrame as X12 EDI (rarely used - mostly for testing).

        Production systems typically generate EDI via specialized tools.
        """
        self.logger.warning("Writing X12 EDI format requires specialized encoding")

        # Convert to JSON or CSV instead
        df.write.mode("overwrite").json(path)


@register_connection("fix_protocol")
@register_connection("fix")
class FIXProtocolConnection(SparkleConnection):
    """
    FIX (Financial Information eXchange) protocol connection.
    Sub-Group: Industry Protocols

    Parses FIX protocol messages (4.2, 4.4, 5.0) used in trading systems.

    Example config (config/connections/fix_protocol/prod.json):
        {
            "log_path": "s3://trading-logs/fix/",
            "fix_version": "FIX.4.4",
            "sender_comp_id": "BROKER1",
            "target_comp_id": "EXCHANGE1"
        }

    Example config (YAML):
        fix_prod:
          type: fix_protocol
          log_path: s3://trading-logs/fix/
          fix_version: FIX.4.4
          sender_comp_id: BROKER1
          target_comp_id: EXCHANGE1

    Usage:
        >>> conn = Connection.get("fix_protocol", spark, env="prod")
        >>> # Parse FIX logs
        >>> fix_df = conn.read(
        ...     path="s3://logs/fix_20240115.log",
        ...     message_types=["D", "8", "9"]  # NewOrder, Execution, CancelReject
        ... )
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.log_path = self.config.get("log_path")
        self.fix_version = self.config.get("fix_version", "FIX.4.4")
        self.sender_comp_id = self.config.get("sender_comp_id")
        self.target_comp_id = self.config.get("target_comp_id")

    def test(self) -> bool:
        """Test FIX connection."""
        try:
            self.logger.info(f"FIX Protocol configuration ({self.fix_version})")
            return True
        except Exception as e:
            self.logger.error(f"FIX connection test failed: {e}")
            return False

    def _parse_fix_message(self, message: str) -> Dict[str, Any]:
        """
        Parse FIX message into dictionary.

        FIX messages use format: tag=value|tag=value|tag=value
        SOH character (ASCII 1) separates fields.
        """
        SOH = chr(1)  # Start of Header

        # Split on SOH
        fields = message.split(SOH)

        parsed = {}

        # Common FIX tags
        fix_tags = {
            "8": "BeginString",
            "9": "BodyLength",
            "35": "MsgType",
            "49": "SenderCompID",
            "56": "TargetCompID",
            "34": "MsgSeqNum",
            "52": "SendingTime",
            "11": "ClOrdID",
            "55": "Symbol",
            "54": "Side",
            "38": "OrderQty",
            "40": "OrdType",
            "44": "Price",
            "32": "LastQty",
            "31": "LastPx",
            "39": "OrdStatus",
            "150": "ExecType"
        }

        for field in fields:
            if "=" in field:
                tag, value = field.split("=", 1)
                field_name = fix_tags.get(tag, f"Tag{tag}")
                parsed[field_name] = value

        return parsed

    def read(
        self,
        path: Optional[str] = None,
        message_types: Optional[List[str]] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read and parse FIX protocol logs.

        Args:
            path: Path to FIX log files
            message_types: Filter by message type (35 tag): D, 8, 9, etc.

        Returns:
            DataFrame with parsed FIX messages
        """
        path = path or self.log_path

        self.logger.info(f"Parsing FIX protocol logs from {path}")

        # Read FIX log files
        fix_rdd = self.spark.sparkContext.textFile(path)

        # Parse each FIX message
        parsed_rdd = fix_rdd.map(self._parse_fix_message)

        # Filter by message type if specified
        if message_types:
            parsed_rdd = parsed_rdd.filter(
                lambda msg: msg.get("MsgType") in message_types
            )

        df = self.spark.createDataFrame(parsed_rdd.collect()) if parsed_rdd.count() > 0 else self.spark.createDataFrame([], "MsgType STRING")

        return df

    def write(self, df: DataFrame, **kwargs) -> None:
        """FIX protocol write not typically used in analytics."""
        raise NotImplementedError("FIX protocol connection is read-only (log parsing)")


@register_connection("swift")
@register_connection("swift_mt")
@register_connection("swift_mx")
class SWIFTConnection(SparkleConnection):
    """
    SWIFT (Society for Worldwide Interbank Financial Telecommunication) connection.
    Sub-Group: Industry Protocols

    Parses SWIFT MT (Message Type) and MX (ISO 20022 XML) messages.

    Example config (config/connections/swift/prod.json):
        {
            "message_format": "MT",
            "input_path": "s3://swift-messages/mt103/",
            "message_types": ["MT103", "MT202", "MT940"]
        }

    Example config (YAML):
        swift_prod:
          type: swift
          message_format: MT  # MT or MX
          input_path: s3://swift-messages/mt103/
          message_types: [MT103, MT202, MT940]

    Usage:
        >>> conn = Connection.get("swift", spark, env="prod")
        >>> # Parse SWIFT MT103 (customer transfer)
        >>> payments_df = conn.read(
        ...     path="s3://swift/mt103/",
        ...     message_types=["MT103"]
        ... )
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config)
        self.message_format = self.config.get("message_format", "MT")
        self.input_path = self.config.get("input_path")
        self.message_types = self.config.get("message_types", [])

    def test(self) -> bool:
        """Test SWIFT connection."""
        try:
            self.logger.info(f"SWIFT {self.message_format} message configuration")
            return True
        except Exception as e:
            self.logger.error(f"SWIFT connection test failed: {e}")
            return False

    def _parse_swift_mt(self, message: str) -> Dict[str, Any]:
        """
        Parse SWIFT MT message.

        MT messages have structure:
        {1:F01...}  - Basic Header
        {2:...}     - Application Header
        {3:...}     - User Header (optional)
        {4:...}     - Text Block
        {5:...}     - Trailer (optional)
        """
        import re

        parsed = {}

        # Extract message type from block 2
        mt_type_match = re.search(r'\{2:.*?(MT\d{3})', message)
        if mt_type_match:
            parsed["message_type"] = mt_type_match.group(1)

        # Extract text block (block 4)
        text_block_match = re.search(r'\{4:(.*?)\}', message, re.DOTALL)
        if text_block_match:
            text_block = text_block_match.group(1)

            # Parse fields (format: :20:REFERENCE)
            field_pattern = re.compile(r':(\d{2}[A-Z]?):(.*?)(?=:\d{2}|$)', re.DOTALL)

            for match in field_pattern.finditer(text_block):
                tag = match.group(1)
                value = match.group(2).strip()

                # Common SWIFT MT tags
                field_names = {
                    "20": "TransactionReference",
                    "32A": "ValueDateCurrencyAmount",
                    "50K": "OrderingCustomer",
                    "59": "BeneficiaryCustomer",
                    "71A": "ChargesBearer"
                }

                field_name = field_names.get(tag, f"Field{tag}")
                parsed[field_name] = value

        return parsed

    def _parse_swift_mx(self, message: str) -> Dict[str, Any]:
        """
        Parse SWIFT MX (ISO 20022 XML) message.
        """
        import xml.etree.ElementTree as ET

        root = ET.fromstring(message)

        parsed = {
            "message_type": root.tag.split("}")[-1] if "}" in root.tag else root.tag
        }

        # Extract key fields (simplified - full parsing is complex)
        # MX messages are XML-based
        for child in root.iter():
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child.text and child.text.strip():
                parsed[tag] = child.text.strip()

        return parsed

    def read(
        self,
        path: Optional[str] = None,
        message_types: Optional[List[str]] = None,
        **kwargs
    ) -> DataFrame:
        """
        Read and parse SWIFT messages.

        Args:
            path: Path to SWIFT message files
            message_types: Filter by message type (MT103, MT202, etc.)

        Returns:
            DataFrame with parsed SWIFT messages
        """
        path = path or self.input_path
        message_types = message_types or self.message_types

        self.logger.info(f"Parsing SWIFT {self.message_format} messages from {path}")

        # Read SWIFT message files
        messages_rdd = self.spark.sparkContext.wholeTextFiles(path)

        # Parse messages based on format
        if self.message_format == "MT":
            parsed_rdd = messages_rdd.map(lambda x: self._parse_swift_mt(x[1]))
        elif self.message_format == "MX":
            parsed_rdd = messages_rdd.map(lambda x: self._parse_swift_mx(x[1]))
        else:
            raise ValueError(f"Unsupported SWIFT format: {self.message_format}")

        # Filter by message type
        if message_types:
            parsed_rdd = parsed_rdd.filter(
                lambda msg: msg.get("message_type") in message_types
            )

        df = self.spark.createDataFrame(parsed_rdd.collect()) if parsed_rdd.count() > 0 else self.spark.createDataFrame([], "message_type STRING")

        return df

    def write(self, df: DataFrame, **kwargs) -> None:
        """SWIFT message write not typically used in analytics."""
        raise NotImplementedError("SWIFT connection is read-only (message parsing)")


@register_connection("aws_healthlake")
@register_connection("healthlake")
class AWSHealthLakeConnection(FHIRConnection):
    """
    AWS HealthLake connection (FHIR-based healthcare data store).
    Sub-Group: Industry Protocols

    AWS HealthLake is a FHIR R4 compliant service for healthcare data.
    Extends FHIRConnection with AWS-specific authentication.

    Example config (config/connections/healthlake/prod.json):
        {
            "datastore_endpoint": "https://healthlake.us-east-1.amazonaws.com/datastore/abc123",
            "region": "us-east-1",
            "aws_access_key_id": "${AWS_ACCESS_KEY_ID}",
            "aws_secret_access_key": "${AWS_SECRET_ACCESS_KEY}"
        }

    Example config (YAML):
        healthlake_prod:
          type: aws_healthlake
          datastore_endpoint: https://healthlake.us-east-1.amazonaws.com/datastore/abc123
          region: us-east-1
          aws_access_key_id: secret://aws/access_key_id
          aws_secret_access_key: secret://aws/secret_access_key

    Usage:
        >>> conn = Connection.get("aws_healthlake", spark, env="prod")
        >>> patients_df = conn.read(resource_type="Patient")
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        # Set up base_url for FHIRConnection
        config["base_url"] = config["datastore_endpoint"]
        config["auth_type"] = "aws"
        config["fhir_version"] = "R4"

        super().__init__(spark, config)

        self.region = self.config["region"]
        self.aws_access_key_id = self.config.get("aws_access_key_id")
        self.aws_secret_access_key = self.config.get("aws_secret_access_key")

    def _get_headers(self) -> Dict[str, str]:
        """Get AWS HealthLake headers with SigV4 authentication."""
        import boto3
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        headers = {
            "Accept": "application/fhir+json",
            "Content-Type": "application/fhir+json"
        }

        # Use AWS SigV4 signing
        if self.aws_access_key_id and self.aws_secret_access_key:
            # Create boto3 session
            session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region
            )

            credentials = session.get_credentials()

            # Sign request with SigV4
            request = AWSRequest(
                method="GET",
                url=self.base_url,
                headers=headers
            )

            SigV4Auth(credentials, "healthlake", self.region).add_auth(request)

            headers.update(dict(request.headers))

        return headers

    def test(self) -> bool:
        """Test AWS HealthLake connection."""
        try:
            url = f"{self.base_url}/metadata"
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()

            self.logger.info(f"✓ AWS HealthLake connection successful")
            return True
        except Exception as e:
            self.logger.error(f"✗ AWS HealthLake connection test failed: {e}")
            return False
