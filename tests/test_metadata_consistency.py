"""Version numbers must agree across packaging, docs and security policy."""

from __future__ import annotations

import re
from pathlib import Path

import purser

ROOT = Path(__file__).resolve().parents[1]
# Archived design notes are excluded from the published site (mkdocs exclude_docs)
# and legitimately mention third-party versions.
UNPUBLISHED = {
    "docs/design/design-doc.md",
    "docs/design/feasibility.md",
    "docs/design/bundle-v2.md",
}
# The semver policy page deliberately names hypothetical future versions
# (patch/minor/major examples), so it is exempt from the stale-literal scan.
ILLUSTRATIVE = {"docs/reference/stability.md"}


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _version() -> str:
    match = re.search(r'^version = "([^"]+)"', _read("pyproject.toml"), re.M)
    assert match is not None, "pyproject.toml has no static version"
    return match.group(1)


def test_pyproject_matches_package_version():
    assert _version() == purser.__version__


def test_changelog_top_release_matches():
    match = re.search(
        r"^## \[(\d+\.\d+\.\d+)\] - \d{4}-\d{2}-\d{2}",
        _read("CHANGELOG.md"),
        re.M,
    )
    assert match is not None, "CHANGELOG has no dated release heading"
    assert match.group(1) == _version()


def test_docs_version_markers():
    version = _version()
    badge = re.search(r"version-(\d+\.\d+\.\d+)", _read("docs/index.md"))
    assert badge is not None, "docs/index.md has no version badge"
    assert badge.group(1) == version
    for rel in (
        "docs/index.md",
        "docs/reference/index.md",
        "docs/reference/python-api/index.md",
    ):
        assert f"`{version}`" in _read(rel), rel


def test_security_support_table_matches():
    major, minor, _ = _version().split(".")
    assert f"| {major}.{minor}.x | 是 |" in _read("SECURITY.md")


def test_contributing_tag_example_matches():
    assert f"v{_version()}" in _read("CONTRIBUTING.md")


def test_no_stale_versions_in_docs():
    version = _version()
    pattern = re.compile(r"(?<![\d.])\d+\.\d+\.\d+(?![\d.])")
    stale: list[str] = []
    for path in sorted((ROOT / "docs").rglob("*.md")):
        rel = path.relative_to(ROOT).as_posix()
        if rel in UNPUBLISHED or rel in ILLUSTRATIVE:
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for found in pattern.findall(line):
                if found != version:
                    stale.append(f"{path.relative_to(ROOT)}:{lineno}: {found}")
    assert not stale, "stale version literals: " + "; ".join(stale)
