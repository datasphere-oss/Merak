"""
Tasks for interacting with Prometheus. The main task are using pushgateway 
"""
try:
    from merak.tasks.prometheus.pushgateway import (
        PushGaugeToGateway,
        PushAddGaugeToGateway,
    )
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.prometheus` requires merak to be installed with the "prometheus" extra.'
    ) from err

__all__ = ["PushAddGaugeToGateway", "PushGaugeToGateway"]
