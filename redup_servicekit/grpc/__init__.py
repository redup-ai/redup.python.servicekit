"""gRPC utilities: async client, server bootstrap and decorators for metrics and health.

The :mod:`redup_servicekit.grpc` package contains:

- :class:`redup_servicekit.grpc.client.BasicAsyncClient` — async gRPC client
- :func:`redup_servicekit.grpc.server.run_grpc_application` — service entrypoint
- :func:`redup_servicekit.grpc.decorators.grpc_init_wrapper` — init wrapper for servicer
- :func:`redup_servicekit.grpc.decorators.aio_grpc_method_wrapper` — method wrapper for metrics
"""
from .client import BasicAsyncClient
from .decorators import aio_grpc_method_wrapper, grpc_init_wrapper
from .server import run_grpc_application

__all__ = [
    "BasicAsyncClient",
    "aio_grpc_method_wrapper",
    "grpc_init_wrapper",
    "run_grpc_application",
]