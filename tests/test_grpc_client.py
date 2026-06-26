from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from redup_servicekit.grpc.client import BasicAsyncClient, DEFAULT_MAX_MESSAGE_LENGTH


class _EchoStub:
    def __init__(self, channel):
        self.channel = channel


def _make_channel(state=grpc.ChannelConnectivity.READY):
    channel = MagicMock()
    channel.get_state = MagicMock(return_value=state)
    channel.close = AsyncMock()
    return channel


def _install_channel_factory(channels_created):
    def factory(*args, **kwargs):
        channel = _make_channel()
        channels_created.append(channel)
        return channel

    return factory


def _aio_rpc_error(code, details=""):
    return grpc.aio.AioRpcError(
        code,
        initial_metadata=None,
        trailing_metadata=None,
        details=details,
        debug_error_string="",
    )


def _bind_stub(client, stub_method):
    def stub_factory(channel):
        stub = MagicMock()
        setattr(stub, "Method", stub_method)
        return stub

    client._stub_cls = stub_factory


@pytest.mark.asyncio
async def test_unary_reuses_shared_channel():
    channels_created = []
    factory = _install_channel_factory(channels_created)

    with patch("redup_servicekit.grpc.client.grpc.aio.insecure_channel", side_effect=factory):
        client = BasicAsyncClient("grpc://localhost:50051", _EchoStub)
        stub_method = AsyncMock(return_value="ok")
        _bind_stub(client, stub_method)

        await client.send(object(), "Method")
        await client.send(object(), "Method")

    assert len(channels_created) == 1
    channels_created[0].close.assert_not_called()
    assert stub_method.await_count == 2


@pytest.mark.asyncio
async def test_unary_recreates_channel_in_transient_failure():
    channels_created = []
    factory = _install_channel_factory(channels_created)

    with patch("redup_servicekit.grpc.client.grpc.aio.insecure_channel", side_effect=factory):
        client = BasicAsyncClient("grpc://localhost:50051", _EchoStub)
        _bind_stub(client, AsyncMock(return_value="ok"))

        await client.send(object(), "Method")
        channels_created[0].get_state.return_value = (
            grpc.ChannelConnectivity.TRANSIENT_FAILURE
        )
        await client.send(object(), "Method")

    assert len(channels_created) == 2
    channels_created[0].close.assert_awaited_once()


@pytest.mark.asyncio
async def test_unary_keeps_channel_on_rpc_error():
    channels_created = []
    factory = _install_channel_factory(channels_created)

    with patch("redup_servicekit.grpc.client.grpc.aio.insecure_channel", side_effect=factory):
        client = BasicAsyncClient("grpc://localhost:50051", _EchoStub)
        stub_method = AsyncMock(
            side_effect=[
                _aio_rpc_error(
                    grpc.StatusCode.UNAVAILABLE,
                    "upstream connect error",
                ),
                "ok",
            ]
        )
        _bind_stub(client, stub_method)

        with pytest.raises(grpc.aio.AioRpcError):
            await client.send(object(), "Method")

        result = await client.send(object(), "Method")

    assert result == "ok"
    assert len(channels_created) == 1
    channels_created[0].close.assert_not_called()


@pytest.mark.asyncio
async def test_per_call_max_message_length_keeps_shared_channel():
    channels_created = []
    factory = _install_channel_factory(channels_created)

    with patch("redup_servicekit.grpc.client.grpc.aio.insecure_channel", side_effect=factory):
        client = BasicAsyncClient("grpc://localhost:50051", _EchoStub)
        _bind_stub(client, AsyncMock(return_value="ok"))

        await client.send(
            object(),
            "Method",
            max_message_length=DEFAULT_MAX_MESSAGE_LENGTH // 2,
        )
        await client.send(object(), "Method")

    assert len(channels_created) == 1


@pytest.mark.asyncio
async def test_aclose_closes_shared_channel():
    channels_created = []
    factory = _install_channel_factory(channels_created)

    with patch("redup_servicekit.grpc.client.grpc.aio.insecure_channel", side_effect=factory):
        client = BasicAsyncClient("grpc://localhost:50051", _EchoStub)
        _bind_stub(client, AsyncMock(return_value="ok"))

        await client.send(object(), "Method")
        await client.aclose()

    channels_created[0].close.assert_awaited_once()
    assert client._channel is None
