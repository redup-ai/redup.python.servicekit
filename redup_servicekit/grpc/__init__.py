"""gRPC utilities: async client and server decorators for metrics and health.

The :mod:`redup_servicekit.grpc` package contains:

- :class:`redup_servicekit.grpc.client.BasicAsyncClient` — async gRPC client
- :func:`redup_servicekit.grpc.decorators.grpc_init_wrapper` — init wrapper for servicer
- :func:`redup_servicekit.grpc.decorators.aio_grpc_method_wrapper` — method wrapper for metrics
"""

__docformat__ = 'restructuredtext'

from .client import BasicAsyncClient
from .decorators import aio_grpc_method_wrapper, grpc_init_wrapper

__all__ = ["BasicAsyncClient", "aio_grpc_method_wrapper", "grpc_init_wrapper"]
