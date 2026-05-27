from __future__ import annotations

import sys

from forest_pipelines.social.anp_producao.pipeline import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
