import asyncio

import grpc

from redup_servicekit.grpc.server import run_grpc_application


def _patch_grpc_runtime(monkeypatch, on_monitor_run=None):
    class FakeMonitorServer:
        def run(self, *args, **kwargs):
            if on_monitor_run is not None:
                on_monitor_run(self, *args, **kwargs)

    monkeypatch.setattr(
        "redup_servicekit.grpc.server.MonitorServer",
        FakeMonitorServer,
    )
    monkeypatch.setattr(
        "redup_servicekit.grpc.server.reflection.enable_server_reflection",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "redup_servicekit.grpc.server._configure_health_server",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "redup_servicekit.grpc.server.asyncio.get_event_loop",
        lambda: type("Loop", (), {"add_signal_handler": lambda *args, **kwargs: None})(),
    )


def test_run_grpc_application_server_kwargs(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
service:
  console_log_level: ERROR
  port: "[::]:9878"
  max_workers: 1
  hpa_max_workers: 1
  grpc_queue_size: 8
  grpc_compression: gzip
  grpc_msg_opts:
    options:
      - ['grpc.max_message_length', 100]
""".strip()
    )

    captured = {}

    class FakeServer:
        def add_insecure_port(self, port):
            captured["port"] = port

        async def start(self):
            captured["started"] = True

        async def stop(self, grace):
            captured["stopped"] = True

        async def wait_for_termination(self):
            captured["waited"] = True

    def fake_server(**kwargs):
        captured["server_kwargs"] = kwargs
        return FakeServer()

    monkeypatch.setattr("redup_servicekit.grpc.server.grpc.aio.server", fake_server)
    _patch_grpc_runtime(monkeypatch)

    def build_servicers(config_obj):
        return [(lambda servicer, server: None, object())]

    asyncio.run(
        run_grpc_application(
            str(config_path),
            build_servicers=build_servicers,
            service_names=[],
        )
    )

    assert captured["server_kwargs"]["maximum_concurrent_rpcs"] == 8
    assert captured["server_kwargs"]["compression"] == grpc.Compression.Gzip
    assert captured["server_kwargs"]["options"] == [["grpc.max_message_length", 100]]


def test_run_grpc_application_unlimited_concurrent_rpcs(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
service:
  console_log_level: ERROR
  port: "[::]:9878"
  max_workers: 1
  hpa_max_workers: 1
  grpc_queue_size: -1
  grpc_msg_opts:
    options: []
""".strip()
    )

    captured = {}

    class FakeServer:
        def add_insecure_port(self, port):
            pass

        async def start(self):
            pass

        async def stop(self, grace):
            pass

        async def wait_for_termination(self):
            pass

    monkeypatch.setattr(
        "redup_servicekit.grpc.server.grpc.aio.server",
        lambda **kwargs: (captured.update({"server_kwargs": kwargs}) or FakeServer()),
    )
    _patch_grpc_runtime(monkeypatch)

    asyncio.run(
        run_grpc_application(
            str(config_path),
            build_servicers=lambda config: [(lambda s, srv: None, object())],
            service_names=[],
        )
    )

    assert captured["server_kwargs"]["maximum_concurrent_rpcs"] is None


def test_run_grpc_application_builds_servicers_after_logging(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
service:
  console_log_level: ERROR
  port: "[::]:9878"
  max_workers: 1
  hpa_max_workers: 1
  grpc_queue_size: 0
  grpc_msg_opts:
    options: []
Example:
  value: 42
""".strip()
    )

    calls = []

    class Example:
        def __init__(self, config):
            self.value = config["value"]

    class Servicer:
        def __init__(self, worker):
            self.worker = worker

    def add_servicer(servicer, server):
        calls.append(("add", servicer.worker.value))

    def build_servicers(config_obj):
        calls.append("build")
        worker = Example(config_obj["Example"])
        return [(add_servicer, Servicer(worker))]

    class FakeServer:
        def add_insecure_port(self, port):
            pass

        async def start(self):
            pass

        async def stop(self, grace):
            pass

        async def wait_for_termination(self):
            calls.append("run")

    monkeypatch.setattr(
        "redup_servicekit.grpc.server.grpc.aio.server",
        lambda **kwargs: FakeServer(),
    )
    _patch_grpc_runtime(
        monkeypatch,
        on_monitor_run=lambda self, *args, **kwargs: calls.append("metrics"),
    )

    asyncio.run(
        run_grpc_application(
            str(config_path),
            build_servicers=build_servicers,
            service_names=["test.Service"],
        )
    )

    assert calls.index("build") < calls.index("metrics")
    assert ("add", 42) in calls
    assert calls[-1] == "run"
