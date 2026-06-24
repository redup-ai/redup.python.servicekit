import pytest

from redup_servicekit.monitoring import (
    MonitorStorage,
    _STATS_EMA_ALPHA_LONG,
    _STATS_EMA_ALPHA_SHORT,
)


@pytest.mark.asyncio
async def test_stats_ema_replaces_rolling_history():
    storage = MonitorStorage()
    metric_key = "time full___method__foo"

    await storage.append_stats("stats", {metric_key: 10.0})
    await storage.append_stats("stats", {metric_key: 20.0})

    stats = await storage.get_stats()
    assert "stats" not in stats
    assert stats["stats_ema_long"][metric_key] == pytest.approx(
        _STATS_EMA_ALPHA_LONG * 20.0
        + (1.0 - _STATS_EMA_ALPHA_LONG) * 10.0
    )
    assert stats["stats_ema_short"][metric_key] == pytest.approx(
        _STATS_EMA_ALPHA_SHORT * 20.0
        + (1.0 - _STATS_EMA_ALPHA_SHORT) * 10.0
    )


@pytest.mark.asyncio
async def test_metrics_snapshot_and_refresh():
    storage = MonitorStorage()
    metric_key = "time full___method__foo"

    await storage.inc_stats("request___method__foo")
    await storage.append_stats("stats", {metric_key: 1.5})
    with storage._lock:
        storage._stats["tasks"] = {"task-1": 1000.0}

    snapshot = storage._snapshot_stats_for_metrics()
    assert snapshot["counters"] == [("request___method__foo", 1)]
    assert snapshot["task_start_times"] == [1000.0]
    assert snapshot["stats_ema_long"] == [(metric_key, 1.5)]