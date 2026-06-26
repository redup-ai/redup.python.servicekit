"""Async gRPC client with TLS, compression and configurable limits.

The :mod:`redup_servicekit.grpc.client` module contains:

- :class:`redup_servicekit.grpc.client.BasicAsyncClient`
"""
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, Tuple
from urllib.parse import urlparse

import grpc

DEFAULT_MAX_MESSAGE_LENGTH = 100 * 1024 * 1024

COMPRESSION = {
    "gzip": grpc.Compression.Gzip,
    "deflate": grpc.Compression.Deflate,
    "no_compression": grpc.Compression.NoCompression,
}

KEEPALIVE_MS = 10000
KEEPALIVE_TIMEOUT_MS = 5000
KEEPALIVE_PERMIT_WITHOUT_CALLS = 1
HTTP2_MAX_PINGS_WITHOUT_DATA = 0

_BAD_CHANNEL_STATES = (
    grpc.ChannelConnectivity.SHUTDOWN,
    grpc.ChannelConnectivity.TRANSIENT_FAILURE,
)


class _AsyncNoop:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class BasicAsyncClient:
    r"""Async gRPC client with a shared channel per endpoint, TLS and compression.

    All calls on one client instance reuse a single ``grpc.aio`` channel. The channel
    is recreated only when its connectivity state is ``SHUTDOWN`` or
    ``TRANSIENT_FAILURE``. An ``UNAVAILABLE`` RPC (e.g. one backend pod behind Istio)
    does not close the channel.

    :param host: Server address with protocol: ``grpc://host:port`` (non-TLS) or ``https://host:port`` (TLS). Do not omit the scheme.
    :type host: str
    :param ServiceStub: Generated gRPC stub class (e.g. ``MyServiceStub``).
    :param max_message_length: Max send/receive message length in bytes. Default 100 MiB.
    :type max_message_length: int
    :param request_compression_algo: ``gzip``, ``deflate``, or ``no_compression``.
    :type request_compression_algo: str
    :param response_compression_algo: Same options; sent as metadata.
    :type response_compression_algo: str
    :param max_concurrent_requests: Max parallel outgoing calls. Omitted or zero — no limit.

    Example:

    >>> from redup_servicekit.grpc import BasicAsyncClient
    >>> client = BasicAsyncClient("grpc://localhost:50051", MyServiceStub, request_compression_algo="gzip")
    >>> response = await client.send(MyRequest(field="value"), Method="MyRpc", timeout=30)
    """

    @staticmethod
    def _channel_options(max_message_length: int):
        return [
            ("grpc.max_send_message_length", max_message_length),
            ("grpc.max_message_length", max_message_length),
            ("grpc.max_receive_message_length", max_message_length),
            ("grpc.keepalive_time_ms", KEEPALIVE_MS),
            ("grpc.keepalive_timeout_ms", KEEPALIVE_TIMEOUT_MS),
            ("grpc.http2.max_pings_without_data", HTTP2_MAX_PINGS_WITHOUT_DATA),
            ("grpc.keepalive_permit_without_calls", KEEPALIVE_PERMIT_WITHOUT_CALLS),
            ("grpc.lb_policy_name", "round_robin"),
        ]

    @staticmethod
    def _parse_endpoint_and_tls(host: str):
        parsed = urlparse(host)
        location = parsed.netloc.strip() or parsed.path
        return location, parsed.scheme == "https"

    def __init__(
        self,
        host: str,
        ServiceStub,
        max_message_length: int = DEFAULT_MAX_MESSAGE_LENGTH,
        request_compression_algo: str = "gzip",
        response_compression_algo: str = "gzip",
        max_concurrent_requests: Optional[int] = None,
    ):
        self._server_address = host
        self._stub_cls = ServiceStub
        self._base_metadata = (("response-compression", response_compression_algo),)
        self._channel_opts = BasicAsyncClient._channel_options(max_message_length)
        self._endpoint, self._use_tls = BasicAsyncClient._parse_endpoint_and_tls(host)
        self._compression = COMPRESSION.get(request_compression_algo, grpc.Compression.Gzip)
        self._ssl_credentials = (
            grpc.ssl_channel_credentials() if self._use_tls else None
        )
        self._channel = None
        self._channel_lock = None
        self._semaphore = asyncio.Semaphore(max_concurrent_requests) if max_concurrent_requests else None

    def _lock(self):
        if self._channel_lock is None:
            self._channel_lock = asyncio.Lock()
        return self._channel_lock

    async def _drop_channel(self):
        async with self._lock():
            if self._channel is not None:
                await self._channel.close()
                self._channel = None

    async def _get_channel(self):
        async with self._lock():
            if self._channel is not None:
                if self._channel.get_state(try_to_connect=False) not in _BAD_CHANNEL_STATES:
                    return self._channel
                await self._channel.close()
            if self._use_tls:
                self._channel = grpc.aio.secure_channel(
                    self._endpoint,
                    self._ssl_credentials,
                    options=self._channel_opts,
                    compression=self._compression,
                )
            else:
                self._channel = grpc.aio.insecure_channel(
                    self._endpoint,
                    options=self._channel_opts,
                    compression=self._compression,
                )
            return self._channel

    @asynccontextmanager
    async def _open_channel(self):
        yield await self._get_channel()

    async def aclose(self):
        """Close the shared channel."""
        await self._drop_channel()

    async def _invoke(self, channel, method_name: str, request, metadata, timeout, stream: bool):
        stub = self._stub_cls(channel)
        stub_method = getattr(stub, method_name)
        if stream:
            async for response_message in stub_method(request, metadata=metadata, timeout=timeout):
                yield response_message
        else:
            yield await stub_method(request, metadata=metadata, timeout=timeout)

    async def send(
        self,
        request,
        Method: str,
        metadata: Tuple[Tuple[str, str], ...] = (),
        max_message_length: int = DEFAULT_MAX_MESSAGE_LENGTH,
        timeout=None,
        stream: bool = False,
    ):
        r"""Send a request and return a single response (unary) or async stream.

        :param request: The request message instance.
        :param Method: Method name string (e.g. ``"MyRpc"``).
        :type Method: str
        :param metadata: Optional tuple of (key, value) metadata.
        :param max_message_length: Kept for API compatibility; channel limits come from ``__init__``.
        :param timeout: Optional timeout in seconds.
        :param stream: If True, return an async generator of response messages.
        :type stream: bool
        :return: Single response message (unary) or async generator (stream).
        """
        del max_message_length  # per-call override is not supported with a shared channel
        request_metadata = metadata + self._base_metadata

        if not stream:
            async with self._semaphore if self._semaphore is not None else _AsyncNoop():
                async with self._open_channel() as channel:
                    response_generator = self._invoke(
                        channel, Method, request, request_metadata, timeout, False
                    )
                    return await response_generator.__anext__()

        async def stream_response_generator():
            async with self._semaphore if self._semaphore is not None else _AsyncNoop():
                async with self._open_channel() as channel:
                    async for response_message in self._invoke(
                        channel, Method, request, request_metadata, timeout, True
                    ):
                        yield response_message

        return stream_response_generator()
