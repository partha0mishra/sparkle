"""
Specialized connections for SFTP, FTP, SMTP, LDAP, Salesforce, SAP, etc.

These are less common but critical for enterprise integrations.
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame

from .base import SparkleConnection
from .factory import register_connection


@register_connection("sftp")
class SFTPConnection(SparkleConnection):
    """
    SFTP connection for secure file transfer.
    Sub-Group: Specialized

    Example config (config/connections/sftp/prod.json):
        {
            "host": "sftp.example.com",
            "port": 22,
            "username": "${SFTP_USER}",
            "password": "${SFTP_PASSWORD}",
            "remote_dir": "/data/incoming"
        }

    Or with key-based auth:
        {
            "host": "sftp.example.com",
            "port": 22,
            "username": "${SFTP_USER}",
            "private_key_path": "/path/to/private_key",
            "remote_dir": "/data/incoming"
        }
    """

    def test(self) -> bool:
        """Test SFTP connection."""
        try:
            import paramiko

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs = {
                "hostname": self.config["host"],
                "port": self.config.get("port", 22),
                "username": self.config["username"]
            }

            if "password" in self.config:
                connect_kwargs["password"] = self.config["password"]
            elif "private_key_path" in self.config:
                connect_kwargs["key_filename"] = self.config["private_key_path"]

            ssh.connect(**connect_kwargs, timeout=10)
            ssh.close()
            return True
        except Exception as e:
            self.logger.error(f"SFTP connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get SFTP configuration."""
        return {
            "host": self.config["host"],
            "port": str(self.config.get("port", 22)),
            "username": self.config["username"],
            "remote_dir": self.config.get("remote_dir", "/")
        }

    def list_files(self, remote_dir: Optional[str] = None) -> list:
        """List files in remote directory."""
        import paramiko

        remote_dir = remote_dir or self.config.get("remote_dir", "/")

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs = {
            "hostname": self.config["host"],
            "port": self.config.get("port", 22),
            "username": self.config["username"]
        }

        if "password" in self.config:
            connect_kwargs["password"] = self.config["password"]
        elif "private_key_path" in self.config:
            connect_kwargs["key_filename"] = self.config["private_key_path"]

        ssh.connect(**connect_kwargs)
        sftp = ssh.open_sftp()

        files = sftp.listdir(remote_dir)

        sftp.close()
        ssh.close()

        return files

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Download file from SFTP."""
        import paramiko

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs = {
            "hostname": self.config["host"],
            "port": self.config.get("port", 22),
            "username": self.config["username"]
        }

        if "password" in self.config:
            connect_kwargs["password"] = self.config["password"]
        elif "private_key_path" in self.config:
            connect_kwargs["key_filename"] = self.config["private_key_path"]

        ssh.connect(**connect_kwargs)
        sftp = ssh.open_sftp()

        sftp.get(remote_path, local_path)

        sftp.close()
        ssh.close()

        self.emit_lineage("read", {"remote_path": remote_path, "local_path": local_path})


@register_connection("ftp")
class FTPConnection(SparkleConnection):
    """
    FTP connection for file transfer.
    Sub-Group: Specialized

    Example config:
        {
            "host": "ftp.example.com",
            "port": 21,
            "username": "${FTP_USER}",
            "password": "${FTP_PASSWORD}",
            "remote_dir": "/data"
        }
    """

    def test(self) -> bool:
        """Test FTP connection."""
        try:
            from ftplib import FTP

            ftp = FTP()
            ftp.connect(self.config["host"], self.config.get("port", 21), timeout=10)
            ftp.login(self.config["username"], self.config.get("password", ""))
            ftp.quit()
            return True
        except Exception as e:
            self.logger.error(f"FTP connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get FTP configuration."""
        return {
            "host": self.config["host"],
            "port": str(self.config.get("port", 21)),
            "username": self.config["username"]
        }


@register_connection("smtp")
@register_connection("email")
class SMTPConnection(SparkleConnection):
    """
    SMTP connection for sending emails.
    Sub-Group: Specialized

    Example config:
        {
            "host": "smtp.gmail.com",
            "port": 587,
            "username": "${EMAIL_USER}",
            "password": "${EMAIL_PASSWORD}",
            "from_address": "sparkle@example.com",
            "use_tls": true
        }
    """

    def test(self) -> bool:
        """Test SMTP connection."""
        try:
            import smtplib

            port = self.config.get("port", 587)
            use_tls = self.config.get("use_tls", True)

            if use_tls:
                server = smtplib.SMTP(self.config["host"], port, timeout=10)
                server.starttls()
            else:
                server = smtplib.SMTP_SSL(self.config["host"], port, timeout=10)

            if "username" in self.config:
                server.login(self.config["username"], self.config["password"])

            server.quit()
            return True
        except Exception as e:
            self.logger.error(f"SMTP connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get SMTP configuration."""
        return {
            "host": self.config["host"],
            "port": str(self.config.get("port", 587)),
            "from_address": self.config.get("from_address", "")
        }

    def send_email(
        self,
        to_addresses: list,
        subject: str,
        body: str,
        html: bool = False
    ) -> None:
        """
        Send email via SMTP.

        Args:
            to_addresses: List of recipient email addresses
            subject: Email subject
            body: Email body
            html: Whether body is HTML
        """
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        msg = MIMEMultipart()
        msg["From"] = self.config.get("from_address", "")
        msg["To"] = ", ".join(to_addresses)
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "html" if html else "plain"))

        port = self.config.get("port", 587)
        use_tls = self.config.get("use_tls", True)

        if use_tls:
            server = smtplib.SMTP(self.config["host"], port)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(self.config["host"], port)

        if "username" in self.config:
            server.login(self.config["username"], self.config["password"])

        server.send_message(msg)
        server.quit()

        self.emit_lineage("write", {"to": to_addresses, "subject": subject})


@register_connection("ldap")
class LDAPConnection(SparkleConnection):
    """
    LDAP connection for directory services.
    Sub-Group: Specialized

    Example config:
        {
            "server": "ldap://ldap.example.com:389",
            "bind_dn": "cn=admin,dc=example,dc=com",
            "bind_password": "${LDAP_PASSWORD}",
            "base_dn": "dc=example,dc=com"
        }
    """

    def test(self) -> bool:
        """Test LDAP connection."""
        try:
            import ldap3

            server = ldap3.Server(self.config["server"])
            conn = ldap3.Connection(
                server,
                user=self.config["bind_dn"],
                password=self.config["bind_password"],
                auto_bind=True
            )
            conn.unbind()
            return True
        except Exception as e:
            self.logger.error(f"LDAP connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get LDAP configuration."""
        return {
            "server": self.config["server"],
            "base_dn": self.config.get("base_dn", "")
        }


@register_connection("salesforce")
class SalesforceConnection(SparkleConnection):
    """
    Salesforce connection via Bulk API.
    Sub-Group: Specialized

    Example config:
        {
            "username": "${SALESFORCE_USER}",
            "password": "${SALESFORCE_PASSWORD}",
            "security_token": "${SALESFORCE_TOKEN}",
            "domain": "login",
            "api_version": "v57.0"
        }

    Or with OAuth:
        {
            "instance_url": "https://mycompany.my.salesforce.com",
            "access_token": "${SALESFORCE_ACCESS_TOKEN}",
            "api_version": "v57.0"
        }
    """

    def test(self) -> bool:
        """Test Salesforce connection."""
        try:
            from simple_salesforce import Salesforce

            if "access_token" in self.config:
                sf = Salesforce(
                    instance_url=self.config["instance_url"],
                    session_id=self.config["access_token"]
                )
            else:
                sf = Salesforce(
                    username=self.config["username"],
                    password=self.config["password"],
                    security_token=self.config["security_token"],
                    domain=self.config.get("domain", "login")
                )

            # Test by querying metadata
            sf.describe()
            return True
        except Exception as e:
            self.logger.error(f"Salesforce connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Salesforce configuration."""
        return {
            "username": self.config.get("username", ""),
            "domain": self.config.get("domain", "login"),
            "api_version": self.config.get("api_version", "v57.0")
        }

    def query_to_dataframe(self, soql: str) -> DataFrame:
        """
        Execute SOQL query and return DataFrame.

        Args:
            soql: Salesforce SOQL query

        Returns:
            Spark DataFrame
        """
        from simple_salesforce import Salesforce
        import pandas as pd

        if "access_token" in self.config:
            sf = Salesforce(
                instance_url=self.config["instance_url"],
                session_id=self.config["access_token"]
            )
        else:
            sf = Salesforce(
                username=self.config["username"],
                password=self.config["password"],
                security_token=self.config["security_token"],
                domain=self.config.get("domain", "login")
            )

        result = sf.query_all(soql)
        records = result["records"]

        # Remove attributes field
        for record in records:
            record.pop("attributes", None)

        pdf = pd.DataFrame(records)
        df = self.spark.createDataFrame(pdf)

        self.emit_lineage("read", {"query": soql})

        return df


@register_connection("sap")
class SAPConnection(SparkleConnection):
    """
    SAP ERP connection (generic adapter).
    Sub-Group: Specialized

    Example config:
        {
            "host": "sap.example.com",
            "system_number": "00",
            "client": "100",
            "username": "${SAP_USER}",
            "password": "${SAP_PASSWORD}",
            "language": "EN"
        }
    """

    def test(self) -> bool:
        """Test SAP connection."""
        try:
            # Would need pyrfc or similar library
            required = ["host", "system_number", "client", "username", "password"]
            return all(k in self.config for k in required)
        except Exception as e:
            self.logger.error(f"SAP connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get SAP configuration."""
        return {
            "host": self.config["host"],
            "system_number": self.config["system_number"],
            "client": self.config["client"]
        }


@register_connection("jira")
class JiraConnection(SparkleConnection):
    """
    Jira connection via REST API.
    Sub-Group: Specialized

    Example config:
        {
            "server": "https://company.atlassian.net",
            "username": "${JIRA_USER}",
            "api_token": "${JIRA_API_TOKEN}"
        }
    """

    def test(self) -> bool:
        """Test Jira connection."""
        try:
            import requests
            from requests.auth import HTTPBasicAuth

            response = requests.get(
                f"{self.config['server']}/rest/api/3/myself",
                auth=HTTPBasicAuth(self.config["username"], self.config["api_token"]),
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Jira connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Jira configuration."""
        return {
            "server": self.config["server"],
            "username": self.config["username"]
        }


@register_connection("slack")
class SlackConnection(SparkleConnection):
    """
    Slack connection for notifications.
    Sub-Group: Specialized

    Example config:
        {
            "webhook_url": "${SLACK_WEBHOOK_URL}",
            "channel": "#data-alerts",
            "username": "Sparkle Bot"
        }

    Or with Bot token:
        {
            "bot_token": "${SLACK_BOT_TOKEN}",
            "channel": "#data-alerts"
        }
    """

    def test(self) -> bool:
        """Test Slack connection."""
        try:
            if "webhook_url" in self.config:
                return "hooks.slack.com" in self.config["webhook_url"]
            elif "bot_token" in self.config:
                return self.config["bot_token"].startswith("xoxb-")
            return False
        except Exception as e:
            self.logger.error(f"Slack connection test failed: {e}")
            return False

    def get_connection(self) -> Dict[str, str]:
        """Get Slack configuration."""
        return {
            "channel": self.config.get("channel", ""),
            "username": self.config.get("username", "Sparkle")
        }

    def send_message(self, message: str, channel: Optional[str] = None) -> None:
        """
        Send message to Slack.

        Args:
            message: Message text
            channel: Optional channel override
        """
        import requests

        channel = channel or self.config.get("channel", "")

        if "webhook_url" in self.config:
            # Use webhook
            payload = {
                "text": message,
                "channel": channel,
                "username": self.config.get("username", "Sparkle")
            }
            response = requests.post(self.config["webhook_url"], json=payload)
            response.raise_for_status()
        elif "bot_token" in self.config:
            # Use Web API
            headers = {"Authorization": f"Bearer {self.config['bot_token']}"}
            payload = {
                "channel": channel,
                "text": message
            }
            response = requests.post(
                "https://slack.com/api/chat.postMessage",
                headers=headers,
                json=payload
            )
            response.raise_for_status()

        self.emit_lineage("write", {"channel": channel, "message": message[:50]})
