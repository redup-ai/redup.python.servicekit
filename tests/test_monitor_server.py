from redup_servicekit.monitoring import MonitorServer

def test_monitor_server_running():
    MonitorServer().run()
    monitoring = MonitorServer.get_instance()
    assert monitoring is not None
