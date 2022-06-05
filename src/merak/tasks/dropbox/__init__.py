"""
Tasks that interface with Dropbox.
"""
try:
    from merak.tasks.dropbox.dropbox import DropboxDownload
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.dropbox` requires merak to be installed with the "dropbox" extra.'
    ) from err

__all__ = ["DropboxDownload"]
