# only tasks that don't require `extras` should be automatically imported here;
# others must be explicitly imported so they can raise helpful errors if appropriate

from merak.core.task import Task
import merak.tasks.core
import merak.tasks.control_flow
import merak.tasks.files
import merak.tasks.database
import merak.tasks.docker
import merak.tasks.github
import merak.tasks.notifications
import merak.tasks.secrets
import merak.tasks.shell

__all__ = ["Task"]
