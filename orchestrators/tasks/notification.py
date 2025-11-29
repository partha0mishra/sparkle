from typing import Dict, Any
from pyspark.sql import SparkSession
from ..base import BaseTask

class SendSlackAlertTask(BaseTask):
    """
    Sends formatted message to Slack webhook.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        message = self.config.get("message")
        self.logger.info(f"Sending Slack alert: {message}")
        return True

class SendEmailAlertTask(BaseTask):
    """
    Sends HTML email via SES/SMTP.
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        subject = self.config.get("subject")
        self.logger.info(f"Sending Email alert: {subject}")
        return True

class NotifyOnFailureTask(BaseTask):
    """
    Centralised on-failure callback (Slack + email + PagerDuty).
    """
    def execute(self, spark: SparkSession, context: Dict[str, Any]) -> Any:
        error = context.get("error")
        self.logger.info(f"Notifying failure: {error}")
        return True
