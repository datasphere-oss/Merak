from warnings import warn
from merak import config
import merak.executors
import merak.engine.state
import merak.engine.signals
import merak.engine.result
from merak.engine.flow_runner import FlowRunner
from merak.engine.task_runner import TaskRunner
import merak.engine.cloud


def get_default_executor_class() -> type:
    """
    Returns the `Executor` class specified in
    `merak.config.engine.executor.default_class`. If the value is a string, it will
    attempt to load the already-imported object. Otherwise, the value is returned.

    Defaults to `LocalExecutor` if the string config value can not be loaded
    """
    config_value = config.engine.executor.default_class

    if isinstance(config_value, str):
        try:
            return merak.utilities.serialization.from_qualified_name(config_value)
        except ValueError:
            warn(
                "Could not import {}; using "
                "merak.executors.LocalExecutor instead.".format(config_value)
            )
            return merak.executors.LocalExecutor
    else:
        return config_value


def get_default_flow_runner_class() -> type:
    """
    Returns the `FlowRunner` class specified in
    `merak.config.engine.flow_runner.default_class` If the value is a string, it will
    attempt to load the already-imported object. Otherwise, the value is returned.

    Defaults to `FlowRunner` if the string config value can not be loaded
    """
    config_value = config.engine.flow_runner.default_class

    if isinstance(config_value, str):
        try:
            return merak.utilities.serialization.from_qualified_name(config_value)
        except ValueError:
            warn(
                "Could not import {}; using "
                "merak.engine.flow_runner.FlowRunner instead.".format(config_value)
            )
            return merak.engine.flow_runner.FlowRunner
    else:
        return config_value


def get_default_task_runner_class() -> type:
    """
    Returns the `TaskRunner` class specified in `merak.config.engine.task_runner.default_class` If the
    value is a string, it will attempt to load the already-imported object. Otherwise, the
    value is returned.

    Defaults to `TaskRunner` if the string config value can not be loaded
    """
    config_value = config.engine.task_runner.default_class

    if isinstance(config_value, str):
        try:
            return merak.utilities.serialization.from_qualified_name(config_value)
        except ValueError:
            warn(
                "Could not import {}; using "
                "merak.engine.task_runner.TaskRunner instead.".format(config_value)
            )
            return merak.engine.task_runner.TaskRunner
    else:
        return config_value


__all__ = ["FlowRunner", "TaskRunner"]
