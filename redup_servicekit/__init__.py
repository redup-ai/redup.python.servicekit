"""Toolkit for Python microservices: config, logging, Prometheus metrics, gRPC.

The :mod:`redup_servicekit` package contains:

- :mod:`redup_servicekit.config` — YAML config singleton
- :mod:`redup_servicekit.logging` — JSON console logging
- :mod:`redup_servicekit.health` — gRPC health checks
- :mod:`redup_servicekit.metrics` — Prometheus registry and constants
- :mod:`redup_servicekit.monitoring` — MonitorServer, MetricServer, task stats
- :mod:`redup_servicekit.grpc` — async client and server decorators
"""

__docformat__ = 'restructuredtext'

import sys

if sys.version_info >= (3, 8):
    from importlib.metadata import version
else:
    from importlib_metadata import version

try:
    __version__ = version("redup-servicekit")
except Exception:
    __version__ = "0.0.0"


__all__ = ['__version__']