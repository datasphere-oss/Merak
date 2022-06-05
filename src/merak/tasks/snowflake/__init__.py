"""
This module contains a collection of tasks for interacting with snowflake databases via
the snowflake-connector-python library.
"""

try:
    from merak.tasks.snowflake.snowflake import (
        SnowflakeQuery,
        SnowflakeQueriesFromFile,
    )
except ImportError:
    raise ImportError(
        'Using `merak.tasks.snowflake` requires merak to be installed with the "snowflake" extra.'
    ) from err

__all__ = ["SnowflakeQueriesFromFile", "SnowflakeQuery"]
