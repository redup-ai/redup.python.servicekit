********************
Example: gRPC client
********************

Client as a subclass of :class:`redup_servicekit.grpc.client.BasicAsyncClient`: pass the stub in ``__init__`` and expose a method that builds the request, calls ``send``, and returns the result (e.g. as a dict via ``MessageToDict``). Matches the pattern used in redup.proto.textprocessor. The host must include the protocol: use ``grpc://host:port`` for non-TLS or ``https://host:port`` for TLS.

.. code-block:: python

    from typing import Tuple, Dict
    from google.protobuf.json_format import MessageToDict
    from redup_servicekit.grpc.client import BasicAsyncClient

    from .redup.textprocessor.v1.textprocessor_pb2 import ProcessTextRequest
    from .redup.textprocessor.v1.textprocessor_pb2_grpc import TextProcessorStub

    class Client(BasicAsyncClient):

        def __init__(self, host: str, *argc, **argv):
            super(Client, self).__init__(host, TextProcessorStub, *argc, **argv)

        async def process_text(
            self,
            request_id: str,
            text: str,
            timeout: int = None,
            metadata: Tuple[Tuple[str, str]] = (),
        ) -> Dict:
            return MessageToDict(
                await self.send(
                    ProcessTextRequest(
                        request_id=request_id,
                        text=text
                    ),
                    "ProcessText",
                    timeout=timeout,
                    metadata=metadata,
                ),
                preserving_proto_field_name=True,
                use_integers_for_enums=False,
                always_print_fields_with_no_presence=True,
            )

Usage:

.. code-block:: python

    import asyncio
    client = Client("grpc://localhost:9878")
    result = asyncio.run(client.process_text(request_id="req-1", text="Hello"))
    print(result["text"])

See also
========

Full client and proto: `redup.proto.textprocessor <https://github.com/redup-ai/redup.proto.textprocessor>`_.
