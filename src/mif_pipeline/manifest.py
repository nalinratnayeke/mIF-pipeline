from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Manifest:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.data: dict[str, Any] = {
            "created_at": utc_now(),
            "updated_at": utc_now(),
            "steps": {},
            "resolved": {},
            "params": {},
        }
        if self.path.exists():
            self.data = json.loads(self.path.read_text(encoding="utf-8"))

    def set_context(self, resolved: dict[str, Any], params: dict[str, Any]) -> None:
        self.data["resolved"] = resolved
        self.data["params"] = params
        self.data["updated_at"] = utc_now()
        self.save()

    def record(self, step: str, status: str, command: str | None = None, detail: dict[str, Any] | None = None) -> None:
        payload: dict[str, Any] = {
            "status": status,
            "timestamp": utc_now(),
        }
        if command:
            payload["command"] = command
        if detail:
            payload["detail"] = detail
        self.data.setdefault("steps", {})[step] = payload
        self.data["updated_at"] = utc_now()
        self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2), encoding="utf-8")
