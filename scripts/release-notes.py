#!/usr/bin/env python3
"""Writes the release notes of a version from the commits since the release before, to stdout.

    scripts/release-notes.py <version> [--since <tag>] [--sums <SHA256SUMS>] [--summary <text>]

A commit that changes only what a user never gets (tests, scripts, CI, the fuzzers, lock files)
is left out, as is the version commit; the rest are grouped by what they changed: the kernel,
the SDKs and kits, the documentation, dependency updates. The summary (the tag's message, when
it says more than "kernable <version>") goes first; install lines, the protocol's state, the
SHA-256 sums and the link to every commit go last.
"""

import argparse
import fnmatch
import re
import subprocess
import sys

REPO = "kernable/kernable"

# What a user never gets: a commit of only these is left out.
INTERNAL = [
    "*/tests/*", "tests/*", "*.test.ts", "scripts/*", ".github/*", "Makefile", "fuzz/*",
    "bench/*", "RELEASING.md", "Dockerfile", "*.lock", "*package-lock.json", "design/*",
]
# Never decides where a commit goes (it comes along with changes elsewhere).
NEUTRAL = ["README.md", "CONTRIBUTING.md", "LICENSE", "*/README.md", ".gitignore", "rust-toolchain.toml"]
SHIPPED = ["scripts/install.sh"]  # under scripts/, but every user runs it
AREAS = [
    ("Kernel", ["crates/*", "proto/*", "scripts/install.sh"]),
    ("SDKs and kits", ["sdks/*", "kits/*"]),
    ("Documentation", ["docs/*"]),
]
BOTS = ("dependabot[bot]",)


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def matches(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(path, p) for p in patterns)


def area(files: list[str], bot: bool) -> str | None:
    """Where a commit goes in the notes; None leaves it out."""
    shipped = [f for f in files if matches(f, SHIPPED) or not matches(f, INTERNAL + NEUTRAL)]
    if bot:
        # A dependency update of the CI only (GitHub's actions) is not the user's.
        return "Dependencies" if [f for f in files if not f.startswith(".github/")] else None
    for name, patterns in AREAS:
        if any(matches(f, patterns) for f in shipped):
            return name
    return "Other" if shipped else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("version")
    ap.add_argument("--since", help="the release before (default: the newest tag before HEAD)")
    ap.add_argument("--sums", help="SHA256SUMS of the archives")
    ap.add_argument("--summary", default="", help="a paragraph to start with")
    a = ap.parse_args()
    tag = f"v{a.version}"
    since = a.since or git("describe", "--tags", "--abbrev=0", "--exclude", tag, "HEAD").strip()

    groups: dict[str, list[str]] = {}
    log = git("log", "--no-merges", "--reverse", "--format=%H%x1f%an%x1f%s", f"{since}..HEAD")
    for line in log.splitlines():
        sha, author, subject = line.split("\x1f")
        # The version commit: "0.2.1", or "0.2.1; ..." as before release.sh.
        if re.match(rf"^{re.escape(a.version)}(\b|$)", subject):
            continue
        files = git("show", "--name-only", "--format=", sha).split()
        where = area(files, author in BOTS)
        if where:
            groups.setdefault(where, []).append(f"- {subject} ({sha[:7]})")

    out: list[str] = [f"**kernable {a.version}.**" + (f" {a.summary.strip()}" if a.summary.strip() else "")]
    if not groups:
        out += ["", "No change a user gets; a release for the release's sake."]
    for name in [n for n, _ in AREAS] + ["Other", "Dependencies"]:
        if name in groups:
            out += ["", f"### {name}", "", *groups[name]]
    proto = git("diff", "--name-only", since, "HEAD", "--", "proto").strip()
    out += [
        "",
        "### Upgrading",
        "",
        "The protocol changed: kernel and SDKs of this release go together." if proto
        else "The protocol is unchanged: SDKs and kernels of the release before work with this one.",
        "Before 1.0 the stored formats stay version 1: nothing to migrate.",
        "",
        "Install: `curl -fsSL https://kernable.com/install.sh | sh` · SDKs: `npm install @kernable/sdk`, "
        "`pip install kernable-sdk`, `cargo add kernable-sdk`",
    ]
    if a.sums:
        with open(a.sums) as f:
            out += ["", "SHA-256:", "", "```", f.read().rstrip(), "```"]
    out += ["", f"**Full Changelog**: https://github.com/{REPO}/compare/{since}...{tag}"]
    sys.stdout.write("\n".join(out) + "\n")


if __name__ == "__main__":
    main()
