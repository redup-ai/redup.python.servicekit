import requests

import pytest

from redup_servicekit.monitoring import MonitorServer

def test_running():
    MonitorServer().run()
    monitoring = MonitorServer.get_instance()
    assert monitoring is not None

def test_prometheus():
    prometheus_lines = requests.get(
        'http://localhost:9999/metrics'
    ).text.split('\n')

    keys = set()
    for prometheus_line in prometheus_lines:
        prometheus_line = prometheus_line.strip()
        if not prometheus_line.startswith('#') and len(prometheus_line) > 0:
            keys.add(prometheus_line.split()[0].split('{')[0])

    for key in ['started_total', 'completed_total']:
        assert key in keys

@pytest.mark.asyncio
async def test_stats():
    monitoring = MonitorServer.get_instance()
    assert len(await monitoring.get_stats()) == 0
