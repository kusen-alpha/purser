"""Auto-start coverage inside spawn()ed scheduler/worker processes.

Only active when ``COVERAGE_PROCESS_START`` is set (see tests/conftest.py),
so normal runs pay nothing.
"""

import coverage

coverage.process_startup()
