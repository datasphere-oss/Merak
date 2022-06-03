import warnings
from typing import Any

from merak.executors import LocalDaskExecutor as _LocalDaskExecutor
from merak.executors import DaskExecutor as _DaskExecutor


class DaskExecutor(_DaskExecutor):
    def __new__(cls, *args: Any, **kwargs: Any) -> "DaskExecutor":
        warnings.warn(
            "merak.engine.executors.DaskExecutor has been moved to "
            "`merak.executors.DaskExecutor`, please update your imports",
            stacklevel=2,
        )
        return super().__new__(cls)


class LocalDaskExecutor(_LocalDaskExecutor):
    def __new__(cls, *args: Any, **kwargs: Any) -> "LocalDaskExecutor":
        warnings.warn(
            "merak.engine.executors.LocalDaskExecutor has been moved to "
            "`merak.executors.LocalDaskExecutor`, please update your imports",
            stacklevel=2,
        )
        return super().__new__(cls)
