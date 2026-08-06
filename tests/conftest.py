"""Make the relay importable when the suites are collected by pytest.

Each suite also inserts this path itself, so that running one directly
(`python tests/test_session_unit.py`) works with no reliance on pytest. This file covers the
other direction: pytest sets sys.path from its rootdir, not from the file being collected.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
