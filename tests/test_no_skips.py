"""DATORIUM_TEST_NO_SKIPS=1 turns every skip into a failure, for CI jobs where all stores run."""

import pytest

pytest_plugins = ["pytester"]

SKIPPING = """
import pytest

def test_passes():
    pass

def test_skips():
    pytest.skip("store not running")
"""


@pytest.fixture
def project(pytester: pytest.Pytester) -> pytest.Pytester:
    pytester.makeconftest("from no_skips import pytest_runtest_makereport  # noqa: F401\n")
    pytester.makepyfile(test_sample=SKIPPING)
    return pytester


def test_skips_are_skips_by_default(
    project: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DATORIUM_TEST_NO_SKIPS", raising=False)
    project.runpytest("-p", "no:cacheprovider").assert_outcomes(passed=1, skipped=1)


def test_no_skips_turns_a_skip_into_a_failure(
    project: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATORIUM_TEST_NO_SKIPS", "1")
    result = project.runpytest("-p", "no:cacheprovider")
    result.assert_outcomes(passed=1, failed=1)
    result.stdout.fnmatch_lines(["*store not running*"])
