"""
A collection of tasks for interacting with Google Sheets.
"""
try:
    from merak.tasks.gsheets.gsheets import (
        WriteGsheetRow,
        ReadGsheetRow,
    )
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.gsheets` requires merak to be installed with the "gsheets" extra.'
    ) from err

__all__ = ["ReadGsheetRow", "WriteGsheetRow"]
