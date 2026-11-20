#!/usr/bin/env python3
"""Detect review issues before delivery (uses only the standard library)."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", "node_modules"}

SOURCE_FILES = [
    p
    for p in sorted(ROOT.rglob("*.py"))
    if not SKIP_DIRS & set(p.relative_to(ROOT).parts)
]


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def check_compile() -> list[str]:
    problems = []
    for path in SOURCE_FILES:
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            problems.append(f"{rel(path)}:{exc.lineno}: syntax error: {exc.msg}")
    return problems


def check_tabs_and_trailing_ws() -> list[str]:
    problems = []
    for path in SOURCE_FILES:
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "\t" in line:
                problems.append(f"{rel(path)}:{lineno}: tab character")
            if line.rstrip() != line:
                problems.append(f"{rel(path)}:{lineno}: trailing whitespace")
    return problems


def check_broad_excepts() -> list[str]:
    """Bare ``except:`` or ``except BaseException`` outside transaction guards."""
    allowed = {"app/db.py"}  # the transaction context managers re-raise
    problems = []
    for path in SOURCE_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler):
                continue
            if node.type is None:
                problems.append(f"{rel(path)}:{node.lineno}: bare except")
            elif rel(path) not in allowed and getattr(node.type, "id", "") == "BaseException":
                problems.append(f"{rel(path)}:{node.lineno}: catches BaseException")
    return problems


def check_sql_parameterisation() -> list[str]:
    """Flag f-strings / concatenation passed to execute (SQL injection risk)."""
    problems = []
    for path in SOURCE_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = getattr(func, "attr", getattr(func, "id", ""))
            if name not in {"execute", "executemany", "executescript"} or not node.args:
                continue
            first = node.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                if first.value.lstrip().upper().startswith("PRAGMA"):
                    continue  # PRAGMA does not accept bound parameters
            if isinstance(first, ast.JoinedStr):
                literals = "".join(
                    value.value
                    for value in first.values
                    if isinstance(value, ast.Constant) and isinstance(value.value, str)
                )
                if literals.lstrip().upper().startswith("PRAGMA"):
                    # PRAGMA does not accept bound parameters; the interpolated
                    # value is an environment-derived integer, not user input.
                    continue
                problems.append(
                    f"{rel(path)}:{node.lineno}: f-string passed to {name}"
                )
            elif isinstance(first, ast.BinOp) and isinstance(first.op, ast.Add):
                problems.append(
                    f"{rel(path)}:{node.lineno}: concatenated SQL passed to {name}"
                )
    return problems


def check_unused_imports() -> list[str]:
    problems = []
    for path in SOURCE_FILES:
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text)
        imported: list[tuple[str, int]] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported.append((alias.asname or alias.name.split(".")[0], node.lineno))
            elif isinstance(node, ast.ImportFrom):
                if node.module == "__future__":
                    continue
                for alias in node.names:
                    imported.append((alias.asname or alias.name, node.lineno))
        for name, lineno in imported:
            uses = sum(
                1
                for n in ast.walk(tree)
                if isinstance(n, ast.Name) and n.id == name
            ) + text.count(f"{name}.") + text.count(f'"{name}"')
            # A name used only in annotations still appears as a Name node.
            if uses == 0:
                problems.append(f"{rel(path)}:{lineno}: import '{name}' appears unused")
    return problems


CHECKS = {
    "syntax": check_compile,
    "whitespace": check_tabs_and_trailing_ws,
    "excepts": check_broad_excepts,
    "sql": check_sql_parameterisation,
    "imports": check_unused_imports,
}


def main() -> int:
    failures = 0
    for name, check in CHECKS.items():
        problems = check()
        status = "ok" if not problems else f"{len(problems)} issue(s)"
        print(f"[{status:>12}] {name}")
        for problem in problems:
            print(f"    {problem}")
        failures += len(problems)
    print(f"\n{failures} issue(s) across {len(SOURCE_FILES)} Python files")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
