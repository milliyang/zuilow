"""
Notifier: email, webhook, DingTalk notifications on job success/failure/signal.

Classes:
    NotificationType   Enum: EMAIL, WEBHOOK, DINGTALK, WECHAT
    NotificationConfig Config dataclass
    Notifier           Send notifications

NotificationConfig fields:
    enabled, types (list), email_smtp_host, email_smtp_port, email_from, email_password,
    email_to, webhook_url, webhook_headers, dingtalk_webhook, dingtalk_secret,
    notify_on_success, notify_on_failure, notify_on_signal

Notifier methods:
    .notify(event_type: str, job_name: str, message: str, data: Optional[dict] = None) -> None
        event_type: "success" | "failure" | "signal"

Functions:
    get_notifier() -> Optional[Notifier]
    set_notifier(notifier: Optional[Notifier]) -> None

"""

from __future__ import annotations

import logging
import requests
from typing import Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

from zuilow.components.control import ctrl

logger = logging.getLogger(__name__)


class NotificationType(Enum):
    """Notification type."""
    EMAIL = "email"
    WEBHOOK = "webhook"
    DINGTALK = "dingtalk"
    WECHAT = "wechat"


@dataclass
class NotificationConfig:
    """Notification config."""
    enabled: bool = False
    types: list[str] = None
    email_smtp_host: str = ""
    email_smtp_port: int = 465
    email_from: str = ""
    email_password: str = ""
    email_to: list[str] = None
    webhook_url: str = ""
    webhook_headers: dict = None
    dingtalk_webhook: str = ""
    dingtalk_secret: str = ""
    notify_on_success: bool = False
    notify_on_failure: bool = True
    notify_on_signal: bool = True


class Notifier:
    """Notifier."""

    def __init__(self, config: NotificationConfig):
        self.config = config

    def notify(
        self,
        event_type: str,
        job_name: str,
        message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Send notification."""
        if not self.config.enabled:
            return
        if event_type == "success" and not self.config.notify_on_success:
            return
        if event_type == "failure" and not self.config.notify_on_failure:
            return
        if event_type == "signal" and not self.config.notify_on_signal:
            return
        if self.config.types:
            for notify_type in self.config.types:
                try:
                    if notify_type == "email":
                        self._send_email(job_name, message, details)
                    elif notify_type == "webhook":
                        self._send_webhook(event_type, job_name, message, details)
                    elif notify_type == "dingtalk":
                        self._send_dingtalk(job_name, message, details)
                except Exception as e:
                    logger.error(f"Send {notify_type} failed: {e}")

    def _send_email(self, job_name: str, message: str, details: Optional[Dict] = None):
        """Send email."""
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            msg = MIMEMultipart()
            msg['From'] = self.config.email_from
            msg['To'] = ', '.join(self.config.email_to or [])
            msg['Subject'] = f'[ZuiLow Scheduler] {job_name}'
            body = f"{message}\n\n"
            if details:
                body += "Details:\n"
                for key, value in details.items():
                    body += f"  {key}: {value}\n"
            msg.attach(MIMEText(body, 'plain'))
            with smtplib.SMTP_SSL(
                self.config.email_smtp_host,
                self.config.email_smtp_port
            ) as server:
                server.login(self.config.email_from, self.config.email_password)
                server.send_message(msg)
            logger.info(f"Email sent: {job_name}")
        except ImportError:
            logger.error("Email requires smtplib and email")
        except Exception as e:
            logger.error(f"Send email failed: {e}")
    
    def _send_webhook(
        self,
        event_type: str,
        job_name: str,
        message: str,
        details: Optional[Dict] = None
    ):
        """Send Webhook notification."""
        try:
            payload = {
                "event": event_type,
                "job_name": job_name,
                "message": message,
                "details": details or {},
                "timestamp": ctrl.get_current_time_iso()
            }
            
            headers = self.config.webhook_headers or {
                "Content-Type": "application/json"
            }
            
            response = requests.post(
                self.config.webhook_url,
                json=payload,
                headers=headers,
                timeout=5
            )
            
            if response.status_code == 200:
                logger.info(f"Webhook sent: {job_name}")
            else:
                logger.warning(f"Webhook response: {response.status_code}")
        except Exception as e:
            logger.error(f"Send Webhook failed: {e}")

    def _send_dingtalk(self, job_name: str, message: str, details: Optional[Dict] = None):
        """Send DingTalk notification."""
        try:
            import time
            import hmac
            import hashlib
            import base64
            from urllib.parse import quote_plus
            timestamp = str(round(time.time() * 1000))
            secret = self.config.dingtalk_secret
            
            secret_enc = secret.encode('utf-8')
            string_to_sign = f'{timestamp}\n{secret}'
            string_to_sign_enc = string_to_sign.encode('utf-8')
            
            hmac_code = hmac.new(
                secret_enc,
                string_to_sign_enc,
                digestmod=hashlib.sha256
            ).digest()
            
            sign = quote_plus(base64.b64encode(hmac_code))
            content = f"**{job_name}**\n\n{message}"
            if details:
                content += "\n\nDetails:"
                for key, value in details.items():
                    content += f"\n- {key}: {value}"
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "title": job_name,
                    "text": content
                }
            }
            
            url = f"{self.config.dingtalk_webhook}&timestamp={timestamp}&sign={sign}"
            
            response = requests.post(url, json=payload, timeout=5)
            
            if response.status_code == 200:
                logger.info(f"DingTalk sent: {job_name}")
            else:
                logger.warning(f"DingTalk response: {response.status_code}")
        except Exception as e:
            logger.error(f"Send DingTalk failed: {e}")


_notifier: Optional[Notifier] = None


def get_notifier() -> Optional[Notifier]:
    """Get global notifier."""
    return _notifier


def set_notifier(notifier: Notifier):
    """Set global notifier."""
    global _notifier
    _notifier = notifier
