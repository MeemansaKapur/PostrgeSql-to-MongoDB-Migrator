"""Alerts fire on STATE CHANGE only (OK -> FIRING, FIRING -> RESOLVED), with a cooldown
for re-notification, so a persistent problem doesn't page every 30 s.

Delivery: ERROR log line always; POST to ALERT_WEBHOOK_URL (Slack/Discord) if set."""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.request

log = logging.getLogger("migrator.alerts")


class Alerts:
    def __init__(self, cooldown_s: float = 300, webhook: str | None = None):
        self.cooldown_s = cooldown_s
        self.webhook = webhook or os.environ.get("ALERT_WEBHOOK_URL")
        self.state: dict[str, bool] = {}
        self.last_sent: dict[str, float] = {}
        self.streak: dict[str, int] = {}

    def check(self, name: str, firing: bool, detail: str = "", for_checks: int = 1) -> bool:
        """for_checks: condition must hold this many consecutive checks before firing."""
        self.streak[name] = self.streak.get(name, 0) + 1 if firing else 0
        firing = self.streak[name] >= for_checks
        was = self.state.get(name, False)
        now = time.time()
        if firing and (not was or now - self.last_sent.get(name, 0) > self.cooldown_s):
            self._notify(f"FIRING {name}: {detail}", logging.ERROR)
            self.last_sent[name] = now
        elif was and not firing:
            self._notify(f"RESOLVED {name}", logging.INFO)
        self.state[name] = firing
        return firing

    @property
    def firing(self) -> list[str]:
        return [k for k, v in self.state.items() if v]

    def _notify(self, text: str, level: int) -> None:
        log.log(level, "ALERT %s", text)
        if self.webhook:
            try:
                req = urllib.request.Request(
                    self.webhook, data=json.dumps({"text": f"[migrator] {text}", "content": f"[migrator] {text}"}).encode(),
                    headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=5).close()
            except Exception as e:   # alerting must never crash the verifier
                log.warning("webhook failed: %s", e)
