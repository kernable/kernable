"""Packs the Python kit into the SDK (`kernable/_kit`) before building a release, so
`kernable-sdk add kit` copies the kit of exactly this version:

    python pack_kit.py && python -m build && rm -rf kernable/_kit
"""

import shutil
from pathlib import Path

here = Path(__file__).resolve().parent
kits = here.parent.parent / "kits"
target = here / "kernable" / "_kit"
shutil.rmtree(target, ignore_errors=True)
ignore = shutil.ignore_patterns("__pycache__", ".venv", "e2e_*", "authenticator.py", "oidc_provider.py")
for part in ("kit", "example", "tests"):
    shutil.copytree(kits / "python" / part, target / part, ignore=ignore)
for file in ("README.md", "requirements.txt"):
    shutil.copyfile(kits / "python" / file, target / file)
shutil.copytree(kits / "fixtures", target / "fixtures")
print(f"packed the kit into {target}")
