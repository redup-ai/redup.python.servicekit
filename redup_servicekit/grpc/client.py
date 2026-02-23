from typing import Tuple
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


class BasicAsyncClient:
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
    ):
        self._server_address = host
        self._stub_cls = ServiceStub
        self._compression_in = request_compression_algo
        self._compression_out = response_compression_algo
        self._base_metadata = (("response-compression", response_compression_algo),)
        self._channel_opts = BasicAsyncClient._channel_options(max_message_length)
        self._endpoint, self._use_tls = BasicAsyncClient._parse_endpoint_and_tls(host)
        self._open_channels = {}

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
        request_metadata = metadata + self._base_metadata
        channel_options = BasicAsyncClient._channel_options(max_message_length) if max_message_length != DEFAULT_MAX_MESSAGE_LENGTH else self._channel_opts
        if self._use_tls:
            ssl_credentials = grpc.ssl_channel_credentials()
            compression_algorithm = COMPRESSION.get(self._compression_in, grpc.Compression.Gzip)
        else:
            ssl_credentials = None
            compression_algorithm = COMPRESSION.get(self._compression_in, grpc.Compression.Gzip)

        if not stream:
            if self._use_tls:
                async with grpc.aio.secure_channel(self._endpoint, ssl_credentials, options=channel_options, compression=compression_algorithm) as channel:
                    response_generator = self._invoke(channel, Method, request, request_metadata, timeout, False)
                    return await response_generator.__anext__()
            async with grpc.aio.insecure_channel(self._endpoint, options=channel_options, compression=compression_algorithm) as channel:
                response_generator = self._invoke(channel, Method, request, request_metadata, timeout, False)
                return await response_generator.__anext__()

        if self._use_tls:
            channel = grpc.aio.secure_channel(self._endpoint, ssl_credentials, options=channel_options, compression=compression_algorithm)
        else:
            channel = grpc.aio.insecure_channel(self._endpoint, options=channel_options, compression=compression_algorithm)
        channel_identifier = id(channel)
        self._open_channels[channel_identifier] = channel

        async def stream_response_generator():
            try:
                async for response_message in self._invoke(channel, Method, request, request_metadata, timeout, True):
                    yield response_message
            finally:
                self._open_channels.pop(channel_identifier, None)

        return stream_response_generator()
