"""python -m forest_pipelines.social — gera assets BDQueimadas para social-post-templates."""

from __future__ import annotations

import sys

from forest_pipelines.social.bdqueimadas_monthly_chart import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
