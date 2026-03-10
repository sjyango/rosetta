"""Allow running rosetta as: python -m rosetta"""

import sys

from .cli import main

sys.exit(main())
