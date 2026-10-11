"""``DATORIUM_TEST_NO_SKIPS=1``: a skipped test fails instead.

Store-backed tests skip when their store is not configured, which is right on a
laptop and wrong in a CI job that starts every store: there a skip means a test
silently stopped running. Imported by ``conftest.py``.
"""

import os
from collections.abc import Generator
from typing import Any

import pytest

ENV = "DATORIUM_TEST_NO_SKIPS"


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, Any]:
    outcome = yield
    report: pytest.TestReport = outcome.get_result()
    if os.environ.get(ENV) == "1" and report.skipped and not hasattr(report, "wasxfail"):
        reason = report.longrepr[2] if isinstance(report.longrepr, tuple) else str(report.longrepr)
        report.outcome = "failed"
        report.longrepr = f"skipped while {ENV}=1: {reason}"
