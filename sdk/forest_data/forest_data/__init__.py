"""forest-data: public Python client for the Instituto Forest open-data API."""
from .client import Client
from .models import (
    DatasetManifest,
    DatasetSummary,
    OpenDataItem,
    ReportSummary,
)

__version__ = "0.1.0"
__all__ = [
    "Client",
    "DatasetManifest",
    "DatasetSummary",
    "OpenDataItem",
    "ReportSummary",
    "__version__",
]
