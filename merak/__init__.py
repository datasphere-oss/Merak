import merak.utilities
from merak.configuration import config

from merak.utilities.context import context
from merak.utilities.plugins import API as api, PLUGINS as plugins, MODELS as models

from merak.client import Client
import merak.schedules
import merak.triggers
import merak.storage
import merak.executors

from merak.core import Task, Flow, Parameter
import merak.engine
import merak.tasks
from merak.tasks.control_flow import case
from merak.tasks.core.resource_manager import resource_manager

from merak.utilities.tasks import task, tags, apply_map
from merak.utilities.edges import mapped, unmapped, flatten

import merak.serialization
import merak.agent
import merak.backend
import merak.artifacts

from ._version import get_versions as _get_versions

__version__ = _get_versions()["version"]  # type: ignore
del _get_versions

try:
    import signal as _signal
    from ._siginfo import sig_handler as _sig_handler

    _signal.signal(29, _sig_handler)
except:
    pass

__all__ = [
    "Client",
    "Flow",
    "Parameter",
    "Task",
    "api",
    "apply_map",
    "case",
    "config",
    "context",
    "flatten",
    "mapped",
    "models",
    "plugins",
    "resource_manager",
    "tags",
    "task",
    "unmapped",
]
