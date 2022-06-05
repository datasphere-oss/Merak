"""
This module contains a collection of tasks for interacting with Dremio Query Engine via
the pyarrow library.
"""
try:
    from merak.tasks.dremio.dremio import DremioFetch
except ImportError as import_error:
    raise ImportError(
        'Using `merak.tasks.dremio` requires merak to be installed with the "dremio" extra.'
    ) from import_error

__all__ = ["DremioFetch"]
