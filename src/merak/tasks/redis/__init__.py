"""
This module contains a collection of tasks for interacting with Redis via
the redis-py library.
"""

try:
    from merak.tasks.redis.redis_tasks import RedisSet, RedisGet, RedisExecute
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.redis` requires merak to be installed with the "redis" extra.'
    ) from err

__all__ = ["RedisExecute", "RedisGet", "RedisSet"]
