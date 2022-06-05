"""
Tasks for interacting with any SFTP server.
"""
try:
    from merak.tasks.sftp.sftp import SftpDownload, SftpDownload
except ImportError as err:
    raise ImportError(
        'merak.tasks.sftp` requires merak to be installed with the "sftp" extra.'
    ) from err
