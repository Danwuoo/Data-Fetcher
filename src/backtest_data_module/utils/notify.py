from __future__ import annotations

from abc import ABC, abstractmethod
import httpx
import smtplib


class Notifier(ABC):
    """通知介面，所有通知方式皆應實作 send 方法。"""

    @abstractmethod
    def send(self, message: str) -> None:
        """傳送訊息。"""
        raise NotImplementedError


class SlackNotifier(Notifier):
    """透過 Slack webhook 傳送訊息。"""

    def __init__(self, webhook_url: str | None) -> None:
        self.webhook_url = webhook_url

    def send(self, message: str) -> None:
        if not self.webhook_url:
            return
        try:
            httpx.post(self.webhook_url, json={"text": message})
        except Exception:
            pass


class PagerDutyNotifier(Notifier):
    """透過 PagerDuty 事件 API 傳送警報。"""

    def __init__(self, routing_key: str | None) -> None:
        self.routing_key = routing_key
        self.url = "https://events.pagerduty.com/v2/enqueue"

    def send(self, message: str) -> None:
        if not self.routing_key:
            return
        payload = {
            "routing_key": self.routing_key,
            "event_action": "trigger",
            "payload": {
                "summary": message,
                "source": "data-fetcher",
                "severity": "error",
            },
        }
        try:
            httpx.post(self.url, json=payload)
        except Exception:
            pass


class EmailNotifier(Notifier):
    """以 SMTP 發送電子郵件。"""

    def __init__(
        self, from_addr: str, to_addr: str, smtp_server: str = "localhost"
    ) -> None:
        self.from_addr = from_addr
        self.to_addr = to_addr
        self.smtp_server = smtp_server

    def send(self, message: str) -> None:
        try:
            with smtplib.SMTP(self.smtp_server) as smtp:
                smtp.sendmail(self.from_addr, [self.to_addr], message)
        except Exception:
            pass
