# src/forest_pipelines/registry/__init__.py
from forest_pipelines.registry.datasets import get_dataset_runner

__all__ = ["get_dataset_runner"]
