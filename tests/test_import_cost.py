"""Optional backends must not be imported by ``import purser``."""

from __future__ import annotations

import subprocess
import sys

_SCRIPT = """
import sys
import purser
loaded = sorted(
    name for name in sys.modules
    if name == "redis" or name.startswith("redis.") or name.startswith("cryptography")
)
print(",".join(loaded))
"""


def test_import_purser_skips_redis_and_cryptography() -> None:
    result = subprocess.run(
        [sys.executable, "-c", _SCRIPT],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == ""
