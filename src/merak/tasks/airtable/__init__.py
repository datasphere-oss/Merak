"""
A collection of tasks for interacting with Airtable.
"""
try:
    from merak.tasks.airtable.airtable import WriteAirtableRow, ReadAirtableRow
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.airtable` requires merak to be installed with the "airtable" extra.'
    ) from err

__all__ = ["ReadAirtableRow", "WriteAirtableRow"]
