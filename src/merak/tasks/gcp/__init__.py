"""
Tasks that interface with various components of Google Cloud Platform.

Note that these tasks allow for a wide range of custom usage patterns, such as:

- Initialize a task with all settings for one time use
- Initialize a "template" task with default settings and override as needed
- Create a custom Task that inherits from a merak Task and utilizes the merak boilerplate

All GCP related tasks can be authenticated using the `GCP_CREDENTIALS` merak Secret.  See [Third Party Authentication](../../../orchestration/recipes/third_party_auth.html) for more information.
"""
try:
    from merak.tasks.gcp.storage import GCSDownload, GCSUpload, GCSCopy, GCSBlobExists
    from merak.tasks.gcp.secretmanager import GCPSecret
    from merak.tasks.gcp.bigquery import (
        BigQueryTask,
        BigQueryLoadGoogleCloudStorage,
        BigQueryLoadFile,
        BigQueryStreamingInsert,
        CreateBigQueryTable,
    )
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.gcp` requires merak to be installed with the "gcp" extra.'
    ) from err

__all__ = [
    "BigQueryLoadFile",
    "BigQueryLoadGoogleCloudStorage",
    "BigQueryStreamingInsert",
    "BigQueryTask",
    "CreateBigQueryTable",
    "GCPSecret",
    "GCSBlobExists",
    "GCSCopy",
    "GCSDownload",
    "GCSUpload",
]
