"""gRPC health checking protocol integration.

The :mod:`redup_servicekit.health` module provides:

- :data:`redup_servicekit.health.HEALTH_DESCRIPTOR` — health service descriptor
- :func:`redup_servicekit.health._configure_health_server` — add health servicer to server
"""
import threading
from concurrent import futures

from grpc_health.v1 import health as health_servicer_module
from grpc_health.v1 import health_pb2, health_pb2_grpc

HEALTH_DESCRIPTOR = health_pb2.DESCRIPTOR


def _toggle_health(health_servicer, service):
    health_servicer.set(service, health_pb2.HealthCheckResponse.SERVING)


def _configure_health_server(server, servicer):
    r"""Add the gRPC health servicer to the server and mark the service as SERVING.

    Starts a daemon thread that sets the given service name to SERVING
    so health probes succeed. Call after creating the gRPC server and
    before starting it.

    :param server: The gRPC server (e.g. from ``grpc.aio.server()``).
    :param servicer: Service name string (e.g. ``"my.package.ServiceName"``).
    :type servicer: str

    Example:

    >>> import grpc
    >>> from redup_servicekit.health import _configure_health_server
    >>> server = grpc.aio.server()
    >>> _configure_health_server(server, "my.service.Name")
    >>> # Health check will report SERVING for "my.service.Name"
    """
    health_servicer = health_servicer_module.HealthServicer(
        experimental_non_blocking=True,
        experimental_thread_pool=futures.ThreadPoolExecutor(max_workers=10),
    )
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    threading.Thread(
        target=_toggle_health, args=(health_servicer, servicer), daemon=True
    ).start()
