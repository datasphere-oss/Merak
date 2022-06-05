"""
Tasks for interacting with SendGrid.
"""
try:
    from merak.tasks.sendgrid.sendgrid import SendEmail
except ImportError as exc:
    raise ImportError(
        'Using `merak.tasks.sendgrid` requires merak to be installed with the "sendgrid" extra.'
    ) from exc

__all__ = ["SendEmail"]
