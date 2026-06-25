"""gRPC server bootstrap for redup services.

The :mod:`redup_servicekit.grpc.server` module contains:

- :func:`redup_servicekit.grpc.server.run_grpc_application`
"""
import asyncio
import logging
import signal
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

import grpc
from grpc_reflection.v1alpha import reflection

from redup_servicekit.config import ConfigSingleton
from redup_servicekit.health import HEALTH_DESCRIPTOR, _configure_health_server
from redup_servicekit.logging import init_console_log
from redup_servicekit.monitoring import MonitorServer

COMPRESSION = {
    "gzip": grpc.Compression.Gzip,
    "deflate": grpc.Compression.Deflate,
    "no_compression": grpc.Compression.NoCompression,
}

AddServicer = Callable[[Any, grpc.aio.Server], None]
BuildServicers = Callable[[Dict[str, Any]], Sequence[Tuple[AddServicer, Any]]]


async def run_grpc_application(
    path_to_cfg: Optional[str],
    *,
    build_servicers: BuildServicers,
    service_names: Sequence[str],
) -> None:
    r"""Load config, start metrics and run gRPC server until shutdown.

    :param path_to_cfg: Path to YAML config file.
    :type path_to_cfg: str or None
    :param build_servicers: Callback that receives loaded config and returns
        ``(add_*_to_server, servicer)`` pairs with worker already wired in.
    :type build_servicers: callable
    :param service_names: Full gRPC service names for reflection and health.
    :type service_names: sequence of str

    Example:

    >>> def build_servicers(config):
    ...     worker = Example(config["Example"])
    ...     return [(add_ExampleServicer_to_server, ExampleServer(worker))]
    >>> asyncio.run(run_grpc_application(
    ...     sys.argv[1],
    ...     build_servicers=build_servicers,
    ...     service_names=[DESCRIPTOR.services_by_name["Example"].full_name],
    ... ))
    """
    ConfigSingleton.load(path_to_cfg)
    ConfigSingleton.inject_os_envs()

    config_obj = ConfigSingleton.get()

    init_console_log(config_obj["service"]["console_log_level"])
    logging.info("Starting gRPC service")

    servicers = build_servicers(config_obj)

    MonitorServer().run(
        config_obj.get("MonitorServer", {}),
        max_workers=int(config_obj["service"]["max_workers"]),
        hpa_max_workers=int(config_obj["service"]["hpa_max_workers"]),
    )

    maximum_concurrent_rpcs = int(config_obj["service"].get("grpc_queue_size", 0))
    if maximum_concurrent_rpcs <= 0:
        maximum_concurrent_rpcs = None

    server_kwargs = {
        **(config_obj["service"].get("grpc_msg_opts") or {}),
        "maximum_concurrent_rpcs": maximum_concurrent_rpcs,
    }
    if compression := config_obj["service"].get("grpc_compression"):
        server_kwargs["compression"] = compression
    if isinstance(server_kwargs.get("compression"), str):
        server_kwargs["compression"] = COMPRESSION.get(
            server_kwargs["compression"], grpc.Compression.NoCompression
        )

    server = grpc.aio.server(**server_kwargs)

    for add_fn, servicer in servicers:
        add_fn(servicer, server)

    if service_names:
        reflection.enable_server_reflection(
            tuple(service_names)
            + (
                HEALTH_DESCRIPTOR.services_by_name["Health"].full_name,
                reflection.SERVICE_NAME,
            ),
            server,
        )
    for service_name in service_names:
        _configure_health_server(server, service_name)

    server.add_insecure_port(config_obj["service"]["port"])

    logging.info(
        "Server running on port {}".format(config_obj["service"]["port"])
    )
    await server.start()

    async def server_graceful_shutdown():
        logging.info("Starting graceful shutdown...")
        await server.stop(
            int(config_obj["service"].get("grpc_stop_grace_seconds", 5))
        )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(
        signal.SIGINT, lambda: asyncio.create_task(server_graceful_shutdown())
    )
    loop.add_signal_handler(
        signal.SIGTERM, lambda: asyncio.create_task(server_graceful_shutdown())
    )

    await server.wait_for_termination()
