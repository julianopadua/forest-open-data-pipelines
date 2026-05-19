"""python -m forest_pipelines.social.research_trends — research-trends deck."""

from __future__ import annotations

import sys

from forest_pipelines.social.research_trends.pipeline import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
