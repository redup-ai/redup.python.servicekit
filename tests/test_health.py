import grpc
import pytest

from redup_servicekit.health import _configure_health_server


@pytest.mark.asyncio
async def test_health_server():
    _configure_health_server(
        grpc.aio.server(), "TestServer"
    )
