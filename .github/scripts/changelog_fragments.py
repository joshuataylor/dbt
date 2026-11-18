#!/usr/bin/env python3
"""Checks and release-time stamping for the changie fragments in .changes/unreleased.

    changelog_fragments.py lint [FILE ...]     validate fragments against .changie.yaml
    changelog_fragments.py stamp-commits       record the commit that added each fragment

Run from the repository root. YAML is read through `yq` (mikefarah/yq, preinstalled
on GitHub-hosted runners), so there is nothing to pip install.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

CONFIG = Path(".changie.yaml")
TOP_LEVEL_KEYS = {"kind", "body", "time", "custom"}
# Written by stamp-commits, never prompted for, so it is not in the config.
STAMPED_KEYS = {"commit"}
HANDLE_RE = re.compile(r"^[A-Za-z0-9](?:-?[A-Za-z0-9])*(?:\[bot\])?$")
TIME_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})")


def read_yaml(path: Path) -> dict:
    out = subprocess.run(["yq", "-o=json", ".", str(path)], check=True, capture_output=True, text=True).stdout
    data = json.loads(out)
    if not isinstance(data, dict):
        raise ValueError("not a YAML mapping")
    return data


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def unreleased_dir(config: dict) -> Path:
    return Path(config["changesDir"]) / config["unreleasedDir"]


def lint_fragment(path: Path, config: dict) -> tuple[list[str], list[str]]:
    """Return (errors, warnings) for one fragment."""
    errors: list[str] = []
    warnings: list[str] = []
    try:
        frag = read_yaml(path)
    except (subprocess.CalledProcessError, ValueError) as exc:
        return [f"does not parse as a YAML mapping ({exc})"], warnings

    for key in sorted(set(frag) - TOP_LEVEL_KEYS):
        errors.append(f"unknown key `{key}`")
    for key in ("kind", "body", "time"):
        if not frag.get(key):
            errors.append(f"missing `{key}`")
    if errors:
        return errors, warnings

    kinds = {k["label"]: k for k in config.get("kinds", [])}
    kind = frag["kind"]
    if kind not in kinds:
        errors.append(f"kind `{kind}` is not one of: {', '.join(kinds)}")

    body = str(frag["body"]).strip()
    limits = config.get("body", {})
    if len(body) < limits.get("minLength", 0):
        errors.append(f"body is {len(body)} characters, the minimum is {limits['minLength']}: say what changed")
    if len(body) > limits.get("maxLength", sys.maxsize):
        errors.append(f"body is {len(body)} characters, the maximum is {limits['maxLength']}")

    # `changie new` derives the file name from the kind and time, so a mismatch
    # means the fragment was written or copied by hand.
    match = TIME_RE.match(str(frag["time"]))
    if not match:
        errors.append(f"time `{frag['time']}` is not an RFC 3339 timestamp")
    else:
        y, mo, d, h, mi, s = match.groups()
        expected = f"{kind}-{y}{mo}{d}-{h}{mi}{s}.yaml"
        if path.name != expected:
            errors.append(f"file name does not match kind and time (expected `{expected}`): create entries with `changie new`")

    custom = frag.get("custom") or {}
    choices = {c["key"]: c for c in config.get("custom", [])}
    choices.update({c["key"]: c for c in kinds.get(kind, {}).get("additionalChoices", [])})
    post = {p["key"]: p["value"] for p in config.get("post", [])}
    for key in sorted(set(custom) - set(choices) - set(post) - STAMPED_KEYS):
        errors.append(f"unknown custom key `{key}` (keys are case-sensitive)")

    for key, choice in choices.items():
        value = str(custom.get(key) or "").strip()
        if not value:
            if not choice.get("optional"):
                errors.append(f"custom `{key}` is required")
            continue
        if len(value) < choice.get("minLength", 0):
            errors.append(f"custom `{key}` is shorter than {choice['minLength']} characters")
        if choice["type"] == "enum" and value not in choice.get("enumOptions", []):
            errors.append(f"custom `{key}` is `{value}`, not one of: {', '.join(choice['enumOptions'])}")

    for key in ("issue", "pr"):
        for token in str(custom.get(key) or "").split():
            if not token.isdigit() or int(token) == 0:
                errors.append(f"custom `{key}` has `{token}`, which is not an issue or pull request number")
    for handle in str(custom.get("author") or "").split():
        if not HANDLE_RE.match(handle):
            errors.append(f"custom `author` has `{handle}`, which is not a GitHub handle")

    for key, value in post.items():
        if key in custom and custom[key] != value:
            warnings.append(f"custom `{key}` is `{custom[key]}`, but `changie new` writes `{value}`")
    return errors, warnings


def cmd_lint(args: argparse.Namespace) -> int:
    config = read_yaml(CONFIG)
    paths = [Path(f) for f in args.files] or sorted(unreleased_dir(config).glob("*.yaml"))
    failed = 0
    for path in paths:
        if not path.exists():
            continue  # deleted in the pull request
        errors, warnings = lint_fragment(path, config)
        for message in warnings:
            print(f"{path}: WARNING: {message}")
        for message in errors:
            print(f"{path}: ERROR: {message}")
        failed += bool(errors)
    print(f"{len(paths)} fragment(s) checked, {failed} with errors")
    return 1 if failed else 0


def adding_commit(path: Path, changes_dir: str) -> str | None:
    """The commit that added the fragment, when that commit also carried the change.

    The export squashes each change into one commit, so a fragment added beside
    code identifies the change. A commit that touched only the changes directory
    (a backfilled or bot-written entry) says nothing about the code, so it is
    left out.
    """
    shas = git("log", "--diff-filter=A", "--format=%H", "--", str(path)).split()
    if not shas:
        return None
    sha = shas[-1]
    touched = git("show", "--name-only", "--format=", sha).splitlines()
    if all(name.startswith(f"{changes_dir}/") for name in touched if name):
        return None
    return sha


def cmd_stamp_commits(args: argparse.Namespace) -> int:
    config = read_yaml(CONFIG)
    if git("rev-parse", "--is-shallow-repository").strip() == "true":
        print("ERROR: shallow clone, so the adding commits are not reachable (use fetch-depth: 0)")
        return 1
    stamped = skipped = 0
    for path in sorted(unreleased_dir(config).glob("*.yaml")):
        frag = read_yaml(path)
        if (frag.get("custom") or {}).get("commit"):
            continue
        sha = adding_commit(path, config["changesDir"])
        if sha is None:
            skipped += 1
            continue
        if not args.dry_run:
            # -I4 keeps the four-space indent changie writes.
            subprocess.run(["yq", "-i", "-I4", f'.custom.commit = "{sha}"', str(path)], check=True)
        stamped += 1
        print(f"{path}: {sha[:9]}")
    print(f"{stamped} fragment(s) stamped, {skipped} without a change commit")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    lint = sub.add_parser("lint", help="validate fragments against .changie.yaml")
    lint.add_argument("files", nargs="*", help="fragments to check (default: every unreleased fragment)")
    lint.set_defaults(func=cmd_lint)
    stamp = sub.add_parser("stamp-commits", help="record the commit that added each unreleased fragment")
    stamp.add_argument("--dry-run", action="store_true", help="print what would be written")
    stamp.set_defaults(func=cmd_stamp_commits)
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
