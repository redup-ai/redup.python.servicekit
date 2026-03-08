********************
Example: gRPC server
********************

Minimal gRPC service with config, logging, metrics and health, following the pattern of redup.python.service.example: servicer + worker, servicekit decorators, config-driven server options.

Servicer and worker
===================

.. code-block:: python

    from redup_proto_textprocessor.redup.textprocessor.v1.textprocessor_pb2 import (
        DESCRIPTOR,
        ProcessTextResponse,
    )
    from redup_proto_textprocessor.redup.textprocessor.v1.textprocessor_pb2_grpc import (
        TextProcessorServicer,
        add_TextProcessorServicer_to_server,
    )
    from redup_servicekit.grpc.decorators import grpc_init_wrapper, aio_grpc_method_wrapper

    class Server(TextProcessorServicer):
        @grpc_init_wrapper
        def __init__(self, worker):
            self._worker = worker

        @aio_grpc_method_wrapper
        async def ProcessText(self, request, context, metrics, **kwargs):
            result, request_metrics = await self._worker.process_text(
                request_id=request.request_id,
                text=request.text,
            )
            metrics.update(request_metrics)
            return ProcessTextResponse(**result)

Entry point and config
======================

.. code-block:: python

    import asyncio
    import sys
    import grpc
    from your_app.prototype import Example  # worker class
    from redup_servicekit.config import ConfigSingleton
    from redup_servicekit.logging import init_console_log
    from redup_servicekit.health import _configure_health_server
    from redup_servicekit.monitoring import MonitorServer

    async def serve(config_path):
        ConfigSingleton.load(config_path)
        ConfigSingleton.inject_os_envs()
        config = ConfigSingleton.get()

        init_console_log(config["service"]["console_log_level"])
        worker = Example(config.get("Example", {}))
        MonitorServer().run(
            config.get("MonitorServer", {}),
            max_workers=int(config["service"]["max_workers"]),
            hpa_max_workers=int(config["service"].get("hpa_max_workers", 1)),
        )

        server = grpc.aio.server(
            options=config["service"].get("grpc_msg_opts", {}).get("options", []),
            maximum_concurrent_rpcs=config["service"].get("grpc_queue_size") or None,
        )
        add_TextProcessorServicer_to_server(Server(worker), server)
        _configure_health_server(server, DESCRIPTOR.services_by_name["TextProcessor"].full_name)
        server.add_insecure_port(config["service"]["port"])
        await server.start()
        await server.wait_for_termination()

    def start():
        asyncio.run(serve(sys.argv[1]))

Config YAML needs ``service`` (port, console_log_level, max_workers, hpa_max_workers, grpc_queue_size, grpc_msg_opts) and optionally ``MonitorServer``. Worker config lives under ``Example`` (or your worker section).

See also
========

Full service: `redup.python.service.example <https://github.com/redup-ai/redup.python.service.example>`_.
