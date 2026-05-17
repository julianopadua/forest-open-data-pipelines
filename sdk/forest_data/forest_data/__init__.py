"""forest-data: public Python client for the Instituto Forest open-data API."""
from .client import Client, ForestDataError, NotFoundError, UpstreamError
from .models import (
    DatasetManifest,
    DatasetSummary,
    OpenDataItem,
    ProfileWarning,
    ReportSummary,
)

__version__ = "0.1.0a0"
__all__ = [
    "Client",
    "DatasetManifest",
    "DatasetSummary",
    "ForestDataError",
    "NotFoundError",
    "OpenDataItem",
    "ProfileWarning",
    "ReportSummary",
    "UpstreamError",
    "__version__",
]
