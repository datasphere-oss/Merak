"""
This module contains a collection of tasks for interacting with MySQL databases via
the pymysql library.
"""
try:
    from merak.tasks.mysql.mysql import MySQLExecute, MySQLFetch
except ImportError as import_error:
    raise ImportError(
        'Using `merak.tasks.mysql` requires merak to be installed with the "mysql" extra.'
    ) from import_error

__all__ = ["MySQLExecute", "MySQLFetch"]
