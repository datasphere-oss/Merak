"""
The merak Storage interface encapsulates logic for storing flows. Each
storage unit is able to store _multiple_ flows (with the constraint of name
uniqueness within a given unit).
"""

from warnings import warn

import merak
from merak import config
from merak.storage.base import Storage
from merak.storage.azure import Azure
from merak.storage.bitbucket import Bitbucket
from merak.storage.codecommit import CodeCommit
from merak.storage.docker import Docker
from merak.storage.gcs import GCS
from merak.storage.github import GitHub
from merak.storage.gitlab import GitLab
from merak.storage.local import Local
from merak.storage.module import Module
from merak.storage.s3 import S3
from merak.storage.webhook import Webhook
from merak.storage.git import Git


def get_default_storage_class() -> type:
    """
    Returns the `Storage` class specified in
    `merak.config.flows.defaults.storage.default_class`. If the value is a string, it will
    attempt to load the already-imported object. Otherwise, the value is returned.

    Defaults to `Local` if the string config value can not be loaded
    """
    config_value = config.flows.defaults.storage.default_class
    if isinstance(config_value, str):
        try:
            return merak.utilities.serialization.from_qualified_name(config_value)
        except ValueError:
            warn(
                f"Could not import {config_value}; using merak.storage.Local instead."
            )
            return Local
    else:
        return config_value


__all__ = [
    "Azure",
    "Bitbucket",
    "CodeCommit",
    "Docker",
    "GCS",
    "Git",
    "GitHub",
    "GitLab",
    "Local",
    "Module",
    "S3",
    "Storage",
    "Webhook",
]
