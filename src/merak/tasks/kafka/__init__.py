"""
This module contains a collection of tasks to produce and consume Kafka events
"""

try:
    from merak.tasks.kafka.kafka import KafkaBatchConsume, KafkaBatchProduce
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.kafka` requires merak to be installed with the "kafka" extra.'
    ) from err

__all__ = ["KafkaBatchConsume", "KafkaBatchProduce"]
