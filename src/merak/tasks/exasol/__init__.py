"""
This module contains a collection of tasks for interacting with Exasol databases via
the pyexasol library.
"""
try:
    from merak.tasks.exasol.exasol import (
        ExasolFetch,
        ExasolExecute,
        ExasolImportFromIterable,
        ExasolExportToFile,
    )
except ImportError as exc:
    raise ImportError(
        'Using `merak.tasks.exasol` requires merak to be installed with the "exasol" extra.'
    ) from exc

__all__ = [
    "ExasolExecute",
    "ExasolExportToFile",
    "ExasolFetch",
    "ExasolImportFromIterable",
]
