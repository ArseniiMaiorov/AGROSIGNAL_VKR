#!/usr/bin/env python3
from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BACKEND_SCRIPTS = ROOT / "backend" / "scripts"

if str(BACKEND_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(BACKEND_SCRIPTS))

runpy.run_path(str(BACKEND_SCRIPTS / "stage4_cli.py"), run_name="__main__")
