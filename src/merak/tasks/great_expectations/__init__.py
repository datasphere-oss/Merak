"""
A collection of tasks for interacting with Great Expectations deployments and APIs.

Note that all tasks currently require being executed in an environment where the great expectations configuration directory can be found; 
learn more about how to initialize a great expectation deployment [on their Getting Started docs](https://docs.greatexpectations.io/en/latest/intro.html#how-do-i-get-started).
"""
try:
    from merak.tasks.great_expectations.checkpoints import (
        RunGreatExpectationsValidation,
    )
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.great_expectations` requires merak to be installed with the "ge" extra.'
    ) from err

__all__ = ["RunGreatExpectationsValidation"]
