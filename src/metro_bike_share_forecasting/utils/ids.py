from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4


def generate_run_id(prefix: str = "run") -> str:
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{stamp}_{uuid4().hex[:8]}"

