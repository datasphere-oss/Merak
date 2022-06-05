"""
This module contains a collection of tasks for interacting with Azure Machine Learning Service resources.
"""

try:
    from merak.tasks.azureml.dataset import (
        DatasetCreateFromDelimitedFiles,
        DatasetCreateFromParquetFiles,
        DatasetCreateFromFiles,
    )

    from merak.tasks.azureml.datastore import (
        DatastoreRegisterBlobContainer,
        DatastoreList,
        DatastoreGet,
        DatastoreUpload,
    )

except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.azureml` requires merak to be installed with the "azureml" extra.'
    ) from err

__all__ = [
    "DatasetCreateFromDelimitedFiles",
    "DatasetCreateFromFiles",
    "DatasetCreateFromParquetFiles",
    "DatastoreGet",
    "DatastoreList",
    "DatastoreRegisterBlobContainer",
    "DatastoreUpload",
]
