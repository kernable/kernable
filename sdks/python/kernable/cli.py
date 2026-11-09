"""kernable-sdk: the SDK's own tool. `kernable-sdk add kit [dir]` (or `python -m kernable add kit
[dir]`) copies the kit of this SDK's version into your project: tenancy, accounts,
identities, groups, invitations and authorization as source that is yours to change.

The kit is a Python package: `dir` (default `kit`) becomes it, with its tests, the shared fixtures
and an example server inside, so `from kit import kit_domains` works from the directory above."""

from __future__ import annotations

import re
import shutil
import sys
from importlib.metadata import PackageNotFoundError, version as installed_version
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _version() -> str:
    try:
        return installed_version("kernable-sdk")
    except PackageNotFoundError:
        return "0.0.0"


def kit_source() -> dict[str, Path] | None:
    """The kit's files: packed into the SDK (`kernable/_kit`), or in the repository when run from a checkout."""
    packed = HERE / "_kit"
    if (packed / "kit").is_dir():
        return {"kit": packed / "kit", "extras": packed, "fixtures": packed / "fixtures", "license": packed / "LICENSE"}
    repo = HERE.parents[2] / "kits"
    if (repo / "python" / "kit").is_dir():
        return {"kit": repo / "python" / "kit", "extras": repo / "python", "fixtures": repo / "fixtures", "license": repo / "LICENSE"}
    return None


#: Besides the package itself: the example, the contract's runners, the dependencies, the README.
EXTRAS = ["example", "tests/__init__.py", "tests/test_fixtures.py", "tests/test_authorization.py", "tests/test_api.py",
          "requirements.txt", "README.md"]
_IMPORT = re.compile(r"^(\s*)(from|import) kit(\.|\s|$)", re.M)


def _write_py(source: Path, target: Path, package: str) -> None:
    text = source.read_text()
    text = _IMPORT.sub(lambda m: f"{m.group(1)}{m.group(2)} {package}{m.group(3)}", text)
    head = f"# From the kernable kit {_version()} (MIT, see LICENSE): yours to change.\n"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text if text.startswith("# From the kernable kit") else head + text)


def _copy(source: Path, target: Path, package: str, written: list[Path]) -> None:
    if source.is_dir():
        for child in sorted(source.iterdir()):
            if child.name != "__pycache__":
                _copy(child, target / child.name, package, written)
        return
    if source.suffix == ".py":
        _write_py(source, target, package)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    written.append(target)


def add_kit(directory: str, force: bool) -> int:
    source = kit_source()
    if source is None:
        print("kernable-sdk: this SDK carries no kit", file=sys.stderr)
        return 1
    target = Path(directory)
    package = target.resolve().name
    if not package.isidentifier():
        print(f"kernable-sdk: {target} is a Python package once copied: name it like one (kit, auth_kit)", file=sys.stderr)
        return 1
    if target.exists() and any(target.iterdir()) and not force:
        print(f"kernable-sdk: {target} is not empty (the kit is copied, not merged); choose another directory, or --force", file=sys.stderr)
        return 1
    written: list[Path] = []
    _copy(source["kit"], target, package, written)
    for extra in EXTRAS:
        if (source["extras"] / extra).exists():
            _copy(source["extras"] / extra, target / extra, package, written)
    _copy(source["fixtures"], target / "fixtures", package, written)
    _copy(source["license"], target / "LICENSE", package, written)
    print(f"The kernable kit {_version()} is in {target} ({len(written)} files), yours to change.\n"
          f"Add its dependencies:  pip install -r {target / 'requirements.txt'}\n"
          f"Its contract:          python -m unittest discover -s {target / 'tests'} -t {target.parent}\n"
          f"Then see {target / 'README.md'}.")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    force = "--force" in args
    rest = [a for a in args if a != "--force"]
    if rest[:2] == ["add", "kit"]:
        return add_kit(rest[2] if len(rest) > 2 else "kit", force)
    if rest[:1] == ["--version"]:
        print(_version())
        return 0
    print("usage: kernable-sdk add kit [dir] [--force]   copies the kit into dir (default: kit)", file=sys.stderr)
    return 0 if not rest or rest[0] == "--help" else 2


if __name__ == "__main__":
    sys.exit(main())
