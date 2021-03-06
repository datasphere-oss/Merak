"""
This module contains a collection of tasks for interacting with Databricks resources.
"""

try:
    from merak.tasks.databricks.databricks_submitjob import (
        DatabricksSubmitRun,
        DatabricksRunNow,
        DatabricksSubmitMultitaskRun,
    )
    from merak.tasks.databricks.databricks_get_job_id import DatabricksGetJobID
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.databricks` requires merak to be installed with the "databricks" extra.'
    ) from err

__all__ = [
    "DatabricksRunNow",
    "DatabricksSubmitRun",
    "DatabricksSubmitMultitaskRun",
    "DatabricksGetJobID",
]
