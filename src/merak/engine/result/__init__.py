"""
The Result classes in this `merak.engine.result` package are results used internally by the merak pipeline to track and store results without persistence.

If you are looking for the API docs for the result subclasses you can use to enable task return value checkpointing or to store task data, see the API docs for the [Result Subclasses in `merak.engine.results`](results.html).
"""
import merak
from merak.engine.result.base import Result, NoResult, NoResultType

__all__ = ["NoResult", "NoResultType", "Result"]
