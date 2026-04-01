"""Allow running rosetta as: python -m rosetta"""

import sys

# Use new CLI module
from .cli.main import main

sys.exit(main())
