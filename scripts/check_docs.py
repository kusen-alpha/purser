#!/usr/bin/env python3
"""Check docs links, anchors, nav coverage and catalog drift.

Runs with the standard library only so it can execute in the lint job before
the docs extra is installed.  MkDocs itself still performs the final build.
"""

from __future__ import annotations

import argparse
import ast
import fnmatch
import posixpath
import re
import sys
import unicodedata
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

LINK_RE = re.compile(r"!?\[[^\]]*\]\(\s*<?([^)\s>]+)>?(?:\s+[\"'][^\"']*[\"'])?\s*\)")
REF_RE = re.compile(r"^\s*\[[^\]]+\]:\s*<?(\S+?)>?\s*$", re.MULTILINE)
FENCE_RE = re.compile(r"^\s*(`{3,}|~{3,})")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
ATTR_RE = re.compile(r"\s*\{([^}]*)\}\s*$")
HTML_TAG_RE = re.compile(r"<[^>]+>")
SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")

EXCLUDE_BLOCK_RE = re.compile(r"^exclude_docs:\s*\|?\s*$")


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def slugify(text: str) -> str:
    # Mirrors markdown.extensions.toc.slugify(..., unicode=True): keep Unicode
    # word characters, drop punctuation, lowercase, collapse whitespace/hyphens.
    value = unicodedata.normalize("NFKC", text)
    value = re.sub(r"[^\w\s-]", "", value).strip().lower()
    return re.sub(r"[-\s]+", "-", value)


def heading_text(raw: str) -> str:
    text = re.sub(r"`([^`]*)`", r"\1", raw)
    text = re.sub(r"!\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = HTML_TAG_RE.sub("", text)
    text = re.sub(r"[*_~]", "", text)
    text = text.replace("\\", "")
    return text.strip()


def strip_code(text: str) -> str:
    out: list[str] = []
    fence: str | None = None
    for line in text.splitlines():
        match = FENCE_RE.match(line)
        if match:
            marker = match.group(1)[0]
            if fence is None:
                fence = marker
            elif marker == fence:
                fence = None
            continue
        if fence is not None:
            continue
        out.append(re.sub(r"`[^`]*`", "", line))
    return "\n".join(out)


def anchors_for(text: str) -> set[str]:
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    for line in strip_code(text).splitlines():
        match = HEADING_RE.match(line)
        if not match:
            continue
        raw = match.group(2)
        explicit = None
        attr = ATTR_RE.search(raw)
        if attr:
            id_match = re.search(r"#([\w.-]+)", attr.group(1))
            if id_match:
                explicit = id_match.group(1)
            raw = raw[: attr.start()]
        base = explicit or slugify(heading_text(raw))
        if not base:
            continue
        seen = counts.get(base, 0)
        anchors.add(base if seen == 0 else f"{base}_{seen}")
        counts[base] = seen + 1
    return anchors


def parse_nav(text: str) -> tuple[list[str], list[str]]:
    lines = text.splitlines()
    nav: list[str] = []
    excludes: list[str] = []
    in_nav = False
    in_exclude = False
    for line in lines:
        stripped = line.strip()
        if not line[:1].isspace() and not stripped.startswith("-"):
            in_nav = stripped == "nav:"
            in_exclude = bool(EXCLUDE_BLOCK_RE.match(stripped))
            continue
        if in_exclude:
            if stripped and not stripped.startswith("#"):
                excludes.append(stripped)
            continue
        if in_nav:
            value = stripped.lstrip("-").strip()
            if not value or value.startswith("#"):
                continue
            if ":" in value:
                value = value.rsplit(":", 1)[1].strip()
            value = value.strip("\"'")
            if value.endswith(".md") and not any(
                char.isspace() for char in value
            ):
                nav.append(value)
    return nav, excludes


def is_excluded(rel: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(rel, pattern) for pattern in patterns)


def code_events() -> set[str]:
    events: set[str] = set()
    names_used: set[str] = set()
    assignments: list[tuple[str, ast.expr]] = []
    for path in (ROOT / "purser").rglob("*.py"):
        tree = ast.parse(read(path), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if (
                    isinstance(func, ast.Attribute)
                    and func.attr == "publish"
                    and node.args
                ):
                    arg = node.args[0]
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        events.add(arg.value)
                    elif isinstance(arg, ast.Name):
                        names_used.add(arg.id)
            elif isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name):
                    assignments.append((target.id, node.value))
    for name, value in assignments:
        if name not in names_used:
            continue
        for child in ast.walk(value):
            if isinstance(child, ast.Constant) and isinstance(child.value, str):
                events.add(child.value)
    return events


def _metric_template(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            else:
                parts.append("{}")
        return "".join(parts)
    return None


def code_metrics() -> set[str]:
    metrics: set[str] = set()
    for path in (ROOT / "purser").rglob("*.py"):
        tree = ast.parse(read(path), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (isinstance(func, ast.Attribute) and func.attr == "inc"):
                continue
            if not node.args:
                continue
            template = _metric_template(node.args[0])
            if template and "." in template:
                metrics.add(template.rsplit(".", 1)[-1])
    return metrics


def code_errors() -> set[str]:
    codes: set[str] = set()
    for path in (ROOT / "purser").rglob("*.py"):
        tree = ast.parse(read(path), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            for stmt in node.body:
                target = value = None
                if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
                    target, value = stmt.targets[0], stmt.value
                elif isinstance(stmt, ast.AnnAssign):
                    target, value = stmt.target, stmt.value
                if (
                    isinstance(target, ast.Name)
                    and target.id == "code"
                    and isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                ):
                    codes.add(value.value)
    return codes


def doc_catalog(path: Path, key: str) -> set[str]:
    text = read(path)
    match = re.search(
        rf"<!-- catalog:{key}:start -->(.*?)<!-- catalog:{key}:end -->",
        text,
        re.DOTALL,
    )
    if not match:
        raise ValueError(f"{path.relative_to(ROOT)}: missing catalog:{key} markers")
    names: set[str] = set()
    for line in match.group(1).splitlines():
        cells = line.split("|")
        if len(cells) < 2:
            continue
        cell = cells[1]
        code = re.search(r"`([^`]+)`", cell)
        if code:
            name = code.group(1).strip()
            if re.fullmatch(r"[a-z][a-z0-9_]*", name) and name != "code":
                names.add(name)
    return names


def check_links(
    docs_files: dict[str, Path],
    all_assets: set[str],
    anchors: dict[str, set[str]],
    excludes: list[str],
) -> list[str]:
    errors: list[str] = []
    for rel, path in sorted(docs_files.items()):
        if is_excluded(rel, excludes):
            continue
        text = strip_code(read(path))
        targets = LINK_RE.findall(text) + REF_RE.findall(text)
        for raw in targets:
            target = raw.strip()
            if not target or SCHEME_RE.match(target) or target.startswith("//"):
                continue
            target = target.split("?", 1)[0]
            path_part, has_fragment, fragment = target.partition("#")
            path_part = unquote(path_part)
            fragment = unquote(fragment)
            if not path_part:
                target_rel = rel
            else:
                target_rel = posixpath.normpath(
                    posixpath.join(posixpath.dirname(rel), path_part)
                )
                if target_rel.startswith("../"):
                    errors.append(f"{rel}: link {target!r} points outside docs/")
                    continue
                if not target_rel.endswith(".md"):
                    if f"{target_rel}/index.md" in docs_files:
                        target_rel = f"{target_rel}/index.md"
                    elif target_rel in all_assets:
                        if has_fragment:
                            errors.append(
                                f"{rel}: fragment on non-markdown target {target!r}"
                            )
                        continue
                    elif not Path(target_rel).suffix:
                        errors.append(f"{rel}: link {target!r} has no matching file")
                    else:
                        errors.append(f"{rel}: missing asset {target!r}")
                    continue
                if target_rel not in docs_files:
                    errors.append(f"{rel}: missing page {target!r}")
                    continue
            if has_fragment and fragment:
                if fragment not in anchors.get(target_rel, set()):
                    errors.append(
                        f"{rel}: missing anchor #{fragment} in {target_rel}"
                    )
    return errors


def check_fences(docs_files: dict[str, Path], excludes: list[str]) -> list[str]:
    errors: list[str] = []
    for rel, path in sorted(docs_files.items()):
        if is_excluded(rel, excludes):
            continue
        open_marker: str | None = None
        for lineno, line in enumerate(read(path).splitlines(), 1):
            match = FENCE_RE.match(line)
            if not match:
                continue
            marker = match.group(1)
            if open_marker is None:
                open_marker = marker
            elif marker[0] == open_marker[0] and len(marker) >= len(open_marker):
                open_marker = None
        if open_marker is not None:
            errors.append(f"{rel}: unclosed code fence ({open_marker})")
    return errors


def compare(label: str, code: set[str], documented: set[str]) -> list[str]:
    errors: list[str] = []
    for name in sorted(code - documented):
        errors.append(f"catalog {label}: {name!r} exists in code but not in docs")
    for name in sorted(documented - code):
        errors.append(f"catalog {label}: {name!r} documented but not in code")
    return errors


ADMIN_REF_RE = re.compile(r"pool\.admin\.([a-z_]+)")


def code_error_details() -> dict[str, tuple[str, bool]]:
    details: dict[str, tuple[str, bool]] = {}
    for path in (ROOT / "purser").rglob("*.py"):
        tree = ast.parse(read(path), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            code: str | None = None
            retryable = False
            for stmt in node.body:
                if not isinstance(stmt, ast.Assign) or len(stmt.targets) != 1:
                    continue
                target = stmt.targets[0]
                if not isinstance(target, ast.Name):
                    continue
                if (
                    target.id == "code"
                    and isinstance(stmt.value, ast.Constant)
                    and isinstance(stmt.value.value, str)
                ):
                    code = stmt.value.value
                elif target.id == "retryable" and isinstance(stmt.value, ast.Constant):
                    retryable = bool(stmt.value.value)
            if code:
                details[code] = (node.name, retryable)
    return details


def check_error_table(path: Path) -> list[str]:
    errors: list[str] = []
    text = read(path)
    match = re.search(
        r"<!-- catalog:errors:start -->(.*?)<!-- catalog:errors:end -->",
        text,
        re.DOTALL,
    )
    if not match:
        return [f"{path.relative_to(ROOT)}: missing catalog:errors markers"]
    details = code_error_details()
    for line in match.group(1).splitlines():
        row = re.match(
            r"^\| `([a-z_]+)` \| `([A-Za-z]+)` \| ([是否]) \|", line
        )
        if not row:
            continue
        code, name, retryable = row.groups()
        if code not in details:
            continue
        real_name, real_retryable = details[code]
        if name != real_name:
            errors.append(
                f"{path.relative_to(ROOT)}: {code} maps to {name}, "
                f"code uses {real_name}"
            )
        expected = "是" if real_retryable else "否"
        if retryable != expected:
            errors.append(
                f"{path.relative_to(ROOT)}: {code} retryable is {retryable}, "
                f"code says {expected}"
            )
    return errors


def code_state_count() -> int:
    tree = ast.parse(read(ROOT / "purser/core/state.py"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "ResourceState":
            return sum(
                1
                for stmt in node.body
                if isinstance(stmt, ast.Assign)
                and len(stmt.targets) == 1
                and isinstance(stmt.targets[0], ast.Name)
            )
    return 0


def check_counts(root: Path) -> list[str]:
    errors: list[str] = []
    states = code_state_count()
    state_page = read(root / "docs/reference/state-machine.md")
    if f"{states} 个状态" not in state_page:
        errors.append(
            f"docs/reference/state-machine.md: code has {states} states, "
            "the page does not say so"
        )
    availability = read(root / "docs/guides/availability.md")
    rows = len(re.findall(r"^\| \d+ \|", availability, re.MULTILINE))
    guide_index = read(root / "docs/guides/index.md")
    match = re.search(r"过滤链的 (\d+) 个检查", guide_index)
    if not match:
        errors.append("docs/guides/index.md: missing '过滤链的 N 个检查'")
    elif int(match.group(1)) != rows or rows != 13:
        errors.append(
            f"filter checks: availability has {rows} rows, "
            f"guides/index says {match.group(1)}, expected 13"
        )
    return errors


def check_admin_refs(
    docs_files: dict[str, Path], excludes: list[str]
) -> list[str]:
    tree = ast.parse(read(ROOT / "purser/pool/admin.py"))
    methods = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    errors: list[str] = []
    for rel, path in sorted(docs_files.items()):
        if is_excluded(rel, excludes):
            continue
        for lineno, line in enumerate(read(path).splitlines(), 1):
            for match in ADMIN_REF_RE.finditer(line):
                name = match.group(1)
                if name not in methods:
                    errors.append(
                        f"{rel}:{lineno}: pool.admin.{name} does not exist"
                    )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    root = args.root.resolve()
    docs_dir = root / "docs"

    mkdocs_text = read(root / "mkdocs.yml")
    nav, excludes = parse_nav(mkdocs_text)

    docs_files = {
        path.relative_to(docs_dir).as_posix(): path
        for path in docs_dir.rglob("*.md")
    }
    all_assets = {
        path.relative_to(docs_dir).as_posix()
        for path in docs_dir.rglob("*")
        if path.is_file()
    }

    errors: list[str] = []

    seen_nav: set[str] = set()
    for rel in nav:
        if rel in seen_nav:
            errors.append(f"mkdocs.yml: duplicate nav entry {rel}")
        seen_nav.add(rel)
        if rel not in docs_files:
            errors.append(f"mkdocs.yml: nav target {rel} does not exist")

    for rel in sorted(docs_files):
        if rel in seen_nav or is_excluded(rel, excludes):
            continue
        errors.append(f"{rel}: document is not listed in mkdocs.yml nav")

    anchors = {rel: anchors_for(read(path)) for rel, path in docs_files.items()}
    errors.extend(check_links(docs_files, all_assets, anchors, excludes))
    errors.extend(check_fences(docs_files, excludes))

    try:
        errors.extend(
            compare(
                "events",
                code_events(),
                doc_catalog(root / "docs/reference/events.md", "events"),
            )
        )
        errors.extend(
            compare(
                "metrics",
                code_metrics(),
                doc_catalog(root / "docs/reference/metrics.md", "metrics"),
            )
        )
        errors.extend(
            compare(
                "errors",
                code_errors(),
                doc_catalog(root / "docs/reference/error-codes.md", "errors"),
            )
        )
    except ValueError as exc:
        errors.append(str(exc))

    errors.extend(check_error_table(root / "docs/reference/error-codes.md"))
    errors.extend(check_counts(root))
    errors.extend(check_admin_refs(docs_files, excludes))

    if errors:
        print(f"docs check failed with {len(errors)} problem(s):", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print(
        f"docs check passed: {len(docs_files)} pages, {len(nav)} nav entries, "
        f"{sum(len(v) for v in anchors.values())} anchors"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())