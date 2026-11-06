"""`python -m kernable add kit [dir]`: the same as `kernable-sdk add kit [dir]`."""

import sys

from .cli import main

sys.exit(main())
