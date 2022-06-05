"""
This module contains a collection of tasks for interacting with Postgres databases via
the psycopg2 library.
"""

try:
    from merak.tasks.postgres.postgres import (
        PostgresExecute,
        PostgresExecuteMany,
        PostgresFetch,
    )
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.postgres` requires merak to be installed with the "postgres" extra.'
    ) from err

__all__ = ["PostgresExecute", "PostgresExecuteMany", "PostgresFetch"]
