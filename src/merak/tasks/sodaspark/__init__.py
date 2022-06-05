"""
This module contains a collection of tasks to run Data Quality tests using soda-spark library
"""

try:
    from merak.tasks.sodaspark.sodaspark_tasks import SodaSparkScan
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.sodaspark` requires merak to be installed with the "sodaspark" extra.'
    ) from err

__all__ = ["SodaSparkScan"]
