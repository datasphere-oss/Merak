"""
Tasks for interacting with RSS feeds.
"""
try:
    from merak.tasks.rss.feed import ParseRSSFeed
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.rss` requires merak to be installed with the "rss" extra.'
    ) from err

__all__ = ["ParseRSSFeed"]
