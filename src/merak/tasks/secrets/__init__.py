"""
Secret Tasks are a special kind of merak Task used to represent the retrieval of sensitive data.

The base implementation uses merak Cloud secrets, but users are encouraged to subclass the `Secret` task
class for interacting with other secret providers. Secrets always use a special kind of result handler that
prevents the persistence of sensitive information.
"""
from .base import SecretBase, merakSecret
from .env_var import EnvVarSecret

__all__ = ["EnvVarSecret", "merakSecret", "SecretBase"]
