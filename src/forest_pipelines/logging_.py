# src/forest_pipelines/logging_.py
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path


def get_logger(logs_dir: Path, dataset_id: str) -> logging.Logger:
    logger = logging.getLogger(dataset_id)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    logs_dir.mkdir(parents=True, exist_ok=True)
    date = datetime.utcnow().strftime("%Y-%m-%d")
    log_file = logs_dir / f"{dataset_id}_{date}.log"

    fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(name)s - %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger
