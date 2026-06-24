import asyncio
from collections import defaultdict
from unittest.mock import MagicMock

import grpc
import pytest
from grpc.aio import AbortError

from redup_servicekit.grpc.decorators import aio_grpc_method_wrapper
from redup_servicekit.metrics import PROMETHEUS_METRICS_REGISTRY
from redup_servicekit.monitoring import MonitorServer


class _RecordingContext:
    def __init__(self):
        self.abort_calls = []

    async def abort(self, code, details=""):
        self.abort_calls.append((code, details))
        raise AbortError()


class _FakeContext:
    pass


@pytest.fixture
def decorator_env(monkeypatch):
    monkeypatch.setattr(MonitorServer, "__instance", None, raising=False)

    mock_histogram = MagicMock()
    mock_histogram.labels.return_value.time.return_value.__enter__ = MagicMock(
        return_value=None
    )
    mock_histogram.labels.return_value.time.return_value.__exit__ = MagicMock(
        return_value=False
    )
    PROMETHEUS_METRICS_REGISTRY["stats_tasks_time_spent_quantile"] = mock_histogram
    MonitorServer.async_service_threads = defaultdict(lambda: asyncio.Semaphore(1))
    monkeypatch.setattr(
        MonitorServer,
        "_get_labels",
        staticmethod(lambda extra=None: ["test"]),
    )


def _aio_rpc_error(code, details=""):
    return grpc.aio.AioRpcError(
        code,
        initial_metadata=None,
        trailing_metadata=None,
        details=details,
        debug_error_string="",
    )


async def _call_wrapped(handler, request, context):
    servicer = object()
    wrapped = aio_grpc_method_wrapper(handler)
    return await wrapped(servicer, request, context, metrics={})


@pytest.mark.asyncio
async def test_upstream_unavailable_is_aborted(decorator_env):
    context = _RecordingContext()

    async def handler(self, request, context, metrics, **kwargs):
        raise _aio_rpc_error(grpc.StatusCode.UNAVAILABLE, "connection refused")

    with pytest.raises(AbortError):
        await _call_wrapped(handler, {"x": 1}, context)

    assert context.abort_calls == [
        (grpc.StatusCode.UNAVAILABLE, "connection refused")
    ]


@pytest.mark.asyncio
async def test_upstream_internal_is_mapped_to_unavailable(decorator_env):
    context = _RecordingContext()

    async def handler(self, request, context, metrics, **kwargs):
        raise _aio_rpc_error(grpc.StatusCode.INTERNAL, "panic")

    with pytest.raises(AbortError):
        await _call_wrapped(handler, {"x": 1}, context)

    assert context.abort_calls == [(grpc.StatusCode.UNAVAILABLE, "panic")]


@pytest.mark.asyncio
async def test_value_error_is_invalid_argument(decorator_env):
    context = _RecordingContext()

    async def handler(self, request, context, metrics, **kwargs):
        raise ValueError("bad languages")

    with pytest.raises(AbortError):
        await _call_wrapped(handler, {"x": 1}, context)

    assert context.abort_calls == [
        (grpc.StatusCode.INVALID_ARGUMENT, "bad languages")
    ]


@pytest.mark.asyncio
async def test_runtime_error_is_internal(decorator_env):
    context = _RecordingContext()

    async def handler(self, request, context, metrics, **kwargs):
        raise RuntimeError("bug")

    with pytest.raises(AbortError):
        await _call_wrapped(handler, {"x": 1}, context)

    assert context.abort_calls == [(grpc.StatusCode.INTERNAL, "bug")]


@pytest.mark.asyncio
async def test_fake_context_does_not_abort(decorator_env):
    context = _FakeContext()

    async def handler(self, request, context, metrics, **kwargs):
        raise RuntimeError("bug")

    with pytest.raises(RuntimeError, match="bug"):
        await _call_wrapped(handler, {"x": 1}, context)


@pytest.mark.asyncio
async def test_handler_abort_is_not_wrapped_again(decorator_env):
    context = _RecordingContext()

    async def handler(self, request, context, metrics, **kwargs):
        await context.abort(grpc.StatusCode.NOT_FOUND, "missing")

    with pytest.raises(AbortError):
        await _call_wrapped(handler, {"x": 1}, context)

    assert context.abort_calls == [(grpc.StatusCode.NOT_FOUND, "missing")]
