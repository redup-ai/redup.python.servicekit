import futures
import threading

from grpc_health.v1 import health as health_servicer_module
from grpc_health.v1 import health_pb2, health_pb2_grpc

HEALTH_DESCRIPTOR = health_pb2.DESCRIPTOR


def _toggle_health(health_servicer, service):
    health_servicer.set(service, health_pb2.HealthCheckResponse.SERVING)


def _configure_health_server(server, servicer):
    health_servicer = health_servicer_module.HealthServicer(
        experimental_non_blocking=True,
        experimental_thread_pool=futures.ThreadPoolExecutor(max_workers=10),
    )
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    threading.Thread(
        target=_toggle_health, args=(health_servicer, servicer), daemon=True
    ).start()
