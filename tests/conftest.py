"""
Pytest configuration for NMDC Mass Spec Automation tests.

This file ensures the project root is on sys.path so tests can import nmdc_dp_utils.
"""

import sys
from pathlib import Path

# Add project root to sys.path so nmdc_dp_utils can be imported
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
