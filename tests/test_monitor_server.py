import pytest

from redup_servicekit.monitoring import MonitorServer

def test_running():
    MonitorServer().run()
    monitoring = MonitorServer.get_instance()
    assert monitoring is not None

@pytest.mark.asyncio
async def test_stats():
    monitoring = MonitorServer.get_instance()
    assert len(await monitoring.get_stats()) == 0
