"""
A collection of tasks for running Jupyter notebooks.
"""
try:
    from merak.tasks.jupyter.jupyter import ExecuteNotebook
except ImportError as import_error:
    raise ImportError(
        'Using `merak.tasks.jupyter` requires merak to be installed with the "jupyter" extra.'
    ) from import_error

__all__ = ["ExecuteNotebook"]
