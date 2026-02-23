import grpc
from redup_servicekit.health import _configure_health_server

def test_health_server():
    _configure_health_server(
        grpc.aio.server(), "TestServer"
    )
