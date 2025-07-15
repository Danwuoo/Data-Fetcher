import smtplib
import httpx
from utils.notify import SlackNotifier, PagerDutyNotifier, EmailNotifier


def test_slack_notifier(monkeypatch):
    messages = []

    def fake_post(url, json):
        messages.append((url, json))

    monkeypatch.setattr(httpx, "post", fake_post)
    notifier = SlackNotifier("http://hook")
    notifier.send("hello")
    assert messages and messages[0][1]["text"] == "hello"


def test_pagerduty_notifier(monkeypatch):
    posts = []

    def fake_post(url, json):
        posts.append((url, json))

    monkeypatch.setattr(httpx, "post", fake_post)
    notifier = PagerDutyNotifier("RK")
    notifier.send("alert")
    assert posts
    assert posts[0][1]["payload"]["summary"] == "alert"


def test_email_notifier(monkeypatch):
    sent = {}

    class FakeSMTP:
        def __init__(self, server):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

        def sendmail(self, from_addr, to_addrs, msg):
            sent["from"] = from_addr
            sent["to"] = to_addrs
            sent["msg"] = msg

    monkeypatch.setattr(smtplib, "SMTP", FakeSMTP)
    notifier = EmailNotifier("a@b.com", "c@d.com")
    notifier.send("msg")
    assert sent["to"] == ["c@d.com"]
    assert sent["msg"] == "msg"
