from __future__ import annotations

import sys

from forest_pipelines.social.bdqueimadas_daily.pipeline import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
