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
)

__version__ = "0.1.0a2"
__all__ = [
    "Client",
    "DatasetManifest",
    "DatasetSummary",
    "ForestDataError",
    "NotFoundError",
    "OpenDataItem",
    "ProfileWarning",
    "UnsafeFilenameError",
    "UpstreamError",
    "__version__",
]
