# src/forest_pipelines/logging_.py
from __future__ import annotations
import logging
from datetime import datetime
from pathlib import Path

def get_logger(logs_dir: Path, dataset_id: str) -> logging.Logger:
    """
    Creates an organized log structure: 
    logs/eia/petroleum_monthly/2026/01/2026-01-17.log
    """
    logger = logging.getLogger(dataset_id)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    # Organize by provider/dataset/year/month
    now = datetime.utcnow()
    # e.g., dataset_id = "eia/petroleum_monthly"
    specific_log_dir = logs_dir / dataset_id / now.strftime("%Y") / now.strftime("%m")
    specific_log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = specific_log_dir / f"{now.strftime('%Y-%m-%d')}.log"

    fmt = logging.Formatter("%(asctime)sZ [%(levelname)s] %(name)s: %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger