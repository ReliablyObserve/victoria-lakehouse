"""pytest configuration: the harness modules are plain scripts next to the tests' parent directory."""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
AB = os.path.dirname(HERE)
ROOT = os.path.abspath(os.path.join(AB, "..", "..", ".."))
for p in (ROOT, AB):
    if p not in sys.path:
        sys.path.insert(0, p)
