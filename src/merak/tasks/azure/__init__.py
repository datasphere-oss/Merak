"""
This module contains a collection of tasks for interacting with Azure resources.
"""

try:
    from merak.tasks.azure.blobstorage import BlobStorageDownload, BlobStorageUpload
    from merak.tasks.azure.cosmosdb import (
        CosmosDBCreateItem,
        CosmosDBReadItems,
        CosmosDBQueryItems,
    )
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.azure` requires merak to be installed with the "azure" extra.'
    ) from err

__all__ = [
    "BlobStorageDownload",
    "BlobStorageUpload",
    "CosmosDBCreateItem",
    "CosmosDBQueryItems",
    "CosmosDBReadItems",
]
