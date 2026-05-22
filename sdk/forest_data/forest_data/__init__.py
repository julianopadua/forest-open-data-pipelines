"""forest-data: public Python client for the Instituto Forest open-data API."""
from .client import (
    Client,
    ForestDataError,
    NotFoundError,
    UnsafeFilenameError,
    UpstreamError,
)
from .models import (
    DatasetManifest,
    DatasetSummary,
    OpenDataItem,
    ProfileWarning,
    ReportSummary,
    ReportSummaryCoverage,
)

__version__ = "0.1.0a1"
__all__ = [
    "Client",
    "DatasetManifest",
    "DatasetSummary",
    "ForestDataError",
    "NotFoundError",
    "OpenDataItem",
    "ProfileWarning",
    "ReportSummary",
    "ReportSummaryCoverage",
    "UnsafeFilenameError",
    "UpstreamError",
    "__version__",
]
