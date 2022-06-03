import warnings
from typing import Any

from merak.executors import LocalExecutor as _LocalExecutor


class LocalExecutor(_LocalExecutor):
    def __new__(cls, *args: Any, **kwargs: Any) -> "LocalExecutor":
        warnings.warn(
            "merak.engine.executors.LocalExecutor has been moved to "
            "`merak.executors.LocalExecutor`, please update your imports",
            stacklevel=2,
        )
        return super().__new__(cls)
