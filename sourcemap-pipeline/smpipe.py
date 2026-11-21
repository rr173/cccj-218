#!/usr/bin/env python3
"""Project-root entry point that works under ``python -I`` (isolated mode).

Running the interpreter with -I removes the current directory and the
script directory from ``sys.path``; this wrapper inserts the package location
explicitly and then hands control to the real CLI.
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from smpipe.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
