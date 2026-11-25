"""Measure ``import purser`` wall time in a fresh interpreter.

Run with the project virtualenv::

    python benchmarks/import_time.py

Redis and cryptography are imported lazily, so only the core path is paid for
at import time.
"""

from __future__ import annotations

import subprocess
import sys

_SCRIPT = """
import sys, time
start = time.perf_counter()
import purser
elapsed = time.perf_counter() - start
lazy = [
    name for name in sys.modules
    if name == "redis" or name.startswith("redis.")
    or name.startswith("cryptography")
]
print(f"{elapsed * 1000:.0f} ms, lazy backends loaded: {lazy or 'none'}")
"""

if __name__ == "__main__":
    subprocess.run([sys.executable, "-c", _SCRIPT], check=True)
