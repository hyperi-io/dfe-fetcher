# Project:   dfe-fetcher
# File:      scripts/generics_gates.py
# Purpose:   The generics acceptance gates: every provider-shaped mechanism lives at one framework site
# Language:  Python
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED

"""Run the generics acceptance gates over `crates/`.

Each gate is a ripgrep pattern that may hit only the framework site that owns
the mechanism (a pager, a signer, a lookback), plus tests, fixtures and the
shipped profiles that name provider fields as data. A hit anywhere else is a
provider mechanism written twice. The dependency gates check that the core
crate pulls no sibling crate and that no shape crate depends on the app, and
the budget gate counts the Rust lines of every hook against its cap.

Usage: `scripts/generics-gates.sh` from anywhere inside the repo; exit 0 when
every gate holds, 1 when one does not, 2 when a tool the gates need is missing.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Paths that may hit any gate: test code, fixtures, the PGO mock server, the
# profile contract tests and the shipped profiles, which name provider fields
# as data rather than mechanism.
ALWAYS_ALLOWED = (
    "crates/fetcher/tests/",
    "crates/rest/tests/",
    "crates/fetcher/src/bin/pgo_driver.rs",
    "crates/fetcher/src/profiles/mod.rs",
    "crates/fetcher/profiles/",
)


@dataclass(frozen=True, slots=True)
class Gate:
    """One ripgrep gate: what to search for, where, and which sites may hit."""

    name: str
    pattern: str
    paths: tuple[str, ...] = ("crates/",)
    allowed: tuple[str, ...] = ()
    glob: str | None = None
    expect: str = "only the sites named"


GATES = (
    Gate(
        "link-header paging",
        r'rel="next"|rel=next',
        allowed=("crates/rest/src/page.rs", "crates/rest/src/profile/mod.rs"),
    ),
    Gate(
        "token exchange",
        r"grant_type|access_token|expires_in",
        allowed=("crates/rest/src/auth.rs", "crates/rest/src/profile/mod.rs"),
        glob="!*.yaml",
    ),
    Gate("jwt signing", r"jsonwebtoken::|EncodingKey", allowed=("crates/rest/src/auth.rs",)),
    Gate(
        # `reqsign::` rather than `reqsign`, so naming the crate in a comment is
        # not a second signer. The jwt gate reads `jsonwebtoken::` for the same
        # reason.
        "sigv4 signing",
        r"reqsign::|sign_static",
        allowed=("crates/rest/src/auth.rs",),
        glob="*.rs",
    ),
    Gate(
        "percent encoders",
        r"fn percent_encode|fn urlencoded|fn urlencoding_encode|fn encode_query_value",
        expect="nothing",
    ),
    Gate(
        "pagination token names",
        r"nextToken|nextPageToken|pageToken|continuationToken|next_cursor|NextPageUri|nextLink|nextRecordsUrl|next_offset|total_pages|next_key",
        allowed=("crates/rest/src/page.rs", "crates/rest/src/profile/mod.rs"),
        glob="*.rs",
        expect="only the pager and its grammar; the names live in profiles/*.yaml",
    ),
    Gate(
        "page caps",
        r"MAX_PAGES|max_pages",
        allowed=(
            "crates/rest/",
            "crates/db/",
            "crates/core/src/error.rs",
            "crates/core/src/metric_names.rs",
            "crates/fetcher/src/metrics/mod.rs",
        ),
        glob="*.rs",
        expect="the REST pager's ceiling, the DB tail's per-tick cap, and the shared error and metric names",
    ),
    Gate(
        "window arithmetic",
        r"DEFAULT_LOOKBACK_HOURS|fn window\(",
        allowed=("crates/fetcher/src/driver.rs", "crates/core/src/batch.rs"),
    ),
    Gate(
        "rate limiting",
        r'is_rate_limited|RATE_LIMIT_BACKOFF|contains\("429"\)',
        allowed=("crates/rest/src/request.rs",),
        expect="nothing outside request.rs",
    ),
    Gate(
        "array extraction in shapes and hooks",
        r"\.as_array\(\)",
        paths=("crates/rest/src/shape", "crates/rest/src/hooks"),
        expect="nothing",
    ),
    Gate(
        "per-record serialisation in shapes and hooks",
        r"serde_json::to_vec\(",
        paths=("crates/rest/src/shape", "crates/rest/src/hooks"),
        allowed=("crates/rest/src/hooks/",),
        expect="only the named RowBuilder hooks",
    ),
    Gate("Source impls", r"impl Source for", expect="nothing: the trait is gone"),
    Gate(
        "credential resolution",
        r"credential::resolve\(|secrets::resolve\(",
        allowed=(
            "crates/rest/src/auth.rs",
            "crates/db/src/",
            "crates/fetcher/src/config/resolve.rs",
        ),
    ),
    Gate("macros in main", r"macro_rules!", paths=("crates/fetcher/src/main.rs",), expect="nothing"),
    Gate("continue-on-failure logging", r"fetch failed, continuing", expect="nothing"),
    Gate("token manager", r"TokenManager", expect="nothing"),
    Gate(
        "connection expansion",
        r"fn resolved\(",
        paths=("crates/fetcher/src/config/",),
        allowed=("crates/fetcher/src/config/mod.rs",),
        expect="at most one, the generic",
    ),
    Gate(
        "HTTP outside the REST crate",
        r"reqwest::|hyper::",
        paths=("crates/core", "crates/fetcher/src"),
        expect="nothing",
    ),
)

# Rust lines (before the test module, blank and comment lines excluded) each
# hook or shape may carry; the two signers are accepted over budget.
BUDGETS = {
    "crates/rest/src/hooks/cloudwatch_metrics.rs": 300,
    "crates/rest/src/hooks/s3_list.rs": 250,
    "crates/rest/src/shape/queue.rs": 150,
    "crates/rest/src/hooks/columnar_table.rs": 100,
    "crates/rest/src/hooks/go_module_aggregate.rs": 100,
    "crates/rest/src/hooks/pubsub_message.rs": 100,
    "crates/rest/src/hooks/wrap_non_object.rs": 100,
}


@dataclass(slots=True)
class Report:
    """What the run found."""

    failures: list[str] = field(default_factory=list)

    def fail(self, message: str) -> None:
        """Record one failed gate."""
        self.failures.append(message)
        print(f"  FAIL {message}")


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a tool from the repo root, decoding as UTF-8 whatever the locale."""
    return subprocess.run(
        cmd,
        cwd=REPO,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def rg_hits(gate: Gate) -> list[str]:
    """Every `path:line:text` the gate's pattern hits under its paths."""
    cmd = ["rg", "-n", "--no-heading", "--color", "never", gate.pattern]
    if gate.glob:
        cmd += ["--glob", gate.glob]
    cmd += list(gate.paths)
    done = run(cmd)
    if done.returncode not in (0, 1):
        raise RuntimeError(f"rg failed for `{gate.name}`: {done.stderr.strip()}")
    return [line for line in done.stdout.splitlines() if line]


def test_module_starts(path: str, cache: dict[str, int | None]) -> int | None:
    """The line the file's `#[cfg(test)]` module starts on, if it has one."""
    if path not in cache:
        cache[path] = None
        if path.endswith(".rs"):
            lines = (REPO / path).read_text(encoding="utf-8").splitlines()
            for number, line in enumerate(lines, start=1):
                if line.strip() == "#[cfg(test)]":
                    cache[path] = number
                    break
    return cache[path]


def allowed(gate: Gate, hit: str, cache: dict[str, int | None]) -> bool:
    """Whether the hit is at a site the gate permits, or inside a test module."""
    path, line, _ = hit.split(":", 2)
    if path.startswith(ALWAYS_ALLOWED) or path.startswith(gate.allowed):
        return True
    start = test_module_starts(path, cache)
    return start is not None and int(line) > start


def check_rg(report: Report) -> None:
    """Run every ripgrep gate, printing each hit and failing on a forbidden one."""
    cache: dict[str, int | None] = {}
    for gate in GATES:
        hits = rg_hits(gate)
        print(f"gate: {gate.name} ({gate.expect})")
        forbidden = [hit for hit in hits if not allowed(gate, hit, cache)]
        for hit in hits:
            mark = "  " if hit not in forbidden else "!!"
            print(f"  {mark} {hit}")
        if not hits:
            print("     nothing")
        if forbidden:
            report.fail(f"{gate.name}: {len(forbidden)} hit(s) outside the framework site")


def check_dependencies(report: Report) -> None:
    """The core crate pulls no sibling and no shape crate depends on the app."""
    print("gate: core dependencies (no workspace crate under dfe-fetcher-core)")
    tree = run(["cargo", "tree", "-p", "dfe-fetcher-core", "-e", "normal", "--prefix", "none"])
    if tree.returncode != 0:
        report.fail(f"cargo tree failed: {tree.stderr.strip()}")
        return
    siblings = sorted(
        {
            line.split()[0]
            for line in tree.stdout.splitlines()
            if line.startswith("dfe-fetcher") and not line.startswith("dfe-fetcher-core")
        }
    )
    print(f"     {len(tree.stdout.splitlines())} packages, workspace crates: {siblings or 'none'}")
    if siblings:
        report.fail(f"core depends on {siblings}")

    print("gate: the app is depended on by nothing (cargo tree -i dfe-fetcher -p dfe-fetcher-rest)")
    inverse = run(["cargo", "tree", "-i", "dfe-fetcher", "-p", "dfe-fetcher-rest"])
    dependents = [line for line in inverse.stdout.splitlines() if line.strip()]
    print(f"     {dependents or 'empty'}")
    if dependents:
        report.fail("a shape crate depends on the app")


def rust_lines(path: Path) -> int:
    """Non-blank, non-comment Rust lines before the file's test module."""
    count = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("#[cfg(test)]"):
            break
        if stripped and not stripped.startswith("//"):
            count += 1
    return count


def check_budgets(report: Report) -> None:
    """Every hook and shape under its line budget; the signers are reported."""
    print("gate: hook budgets (Rust lines before #[cfg(test)])")
    for rel, cap in BUDGETS.items():
        lines = rust_lines(REPO / rel)
        state = "ok" if lines <= cap else "OVER"
        print(f"     {lines:4} / {cap:3} {state:4} {rel}")
        if lines > cap:
            report.fail(f"{rel} is {lines} lines against a budget of {cap}")
    auth = rust_lines(REPO / "crates/rest/src/auth.rs")
    print(f"     {auth:4} / --- info crates/rest/src/auth.rs (signers, accepted over budget)")
    if tokei := shutil.which("tokei"):
        done = run([tokei, "crates/rest/src/hooks", "crates/rest/src/auth.rs"])
        print(done.stdout)


def main() -> int:
    """Run every gate; 0 when all hold, 1 when one fails, 2 without ripgrep."""
    if shutil.which("rg") is None:
        print("ripgrep (rg) is required: the gates cannot run, so they have not passed")
        return 2
    report = Report()
    try:
        check_rg(report)
    except RuntimeError as e:
        print(str(e))
        return 2
    check_dependencies(report)
    check_budgets(report)
    if report.failures:
        print(f"\n{len(report.failures)} gate(s) failed:")
        for failure in report.failures:
            print(f"  - {failure}")
        return 1
    print("\nevery gate holds")
    return 0


if __name__ == "__main__":
    sys.exit(main())
