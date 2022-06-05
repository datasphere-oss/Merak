"""
Tasks for interacting with monday.com

Note, to authenticate with the Monday API add a merak Secret
called '"MONDAY_API_TOKEN"' that stores your Monday API Token.
"""

from merak.tasks.monday.monday import CreateItem

__all__ = ["CreateItem"]
