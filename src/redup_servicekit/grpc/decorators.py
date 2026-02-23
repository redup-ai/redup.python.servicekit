import logging
import time
import traceback
import uuid
from functools import wraps
from pickle import dumps

from ..metrics import PROMETHEUS_METRICS_REGISTRY
from ..monitoring import ErrorParser, MonitorServer, StatusParser


def grpc_init_wrapper(func):
    @wraps(func)
    def init_and_run(*args, **kwargs):
        ErrorParser.init()
        server = MonitorServer.get_instance()
        service_instance = args[0]
        if server:
            for name in dir(service_instance):
                if name.startswith("_") or not callable(getattr(service_instance, name)):
                    continue
                server.set_stat_sync("request___method__%s" % name, 0)
                for outcome_status in StatusParser._types:
                    server.set_stat_sync(
                        "processed___method__%s___status__%s" % (name, outcome_status), 0
                    )
                for error_kind in ErrorParser._types:
                    server.set_stat_sync(
                        "errors___method__%s___type__%s" % (name, error_kind), 0
                    )
                server.set_stat_sync("request_size___method__%s" % name, 0)
                server.set_stat_sync("response_size___method__%s" % name, 0)
        func(*args, **kwargs)

    return init_and_run


def aio_grpc_method_wrapper(func):
    @wraps(func)
    async def run_with_metrics(*args, **kwargs):
        rpc_method_name = getattr(func, "__name__", "UnknownObject")
        server = MonitorServer.get_instance()
        request_trace_id = str(uuid.uuid4())
        request_start_timestamp_seconds = time.time()

        if server:
            await server.inc_stats("request___method__%s" % rpc_method_name)
            await server.add_key_value("tasks", (request_trace_id, request_start_timestamp_seconds))

        kwargs["info"] = {"context": args[2]}
        request_counters = {"request_size___method__%s" % rpc_method_name: len(dumps(args[1]))}
        grpc_servicer_context = args[2]
        if grpc_servicer_context is not None and "FakeContext" not in str(type(grpc_servicer_context)) and getattr(grpc_servicer_context, "time_remaining", lambda: None)() is not None:
            deadline_remaining_seconds = grpc_servicer_context.time_remaining()
            request_counters["time_remaining_time___method__%s" % rpc_method_name] = deadline_remaining_seconds
            kwargs["info"]["time_remaining"] = lambda: max(0, deadline_remaining_seconds - (time.time() - request_start_timestamp_seconds)) if time.time() - request_start_timestamp_seconds < deadline_remaining_seconds else 0
        else:
            kwargs["info"]["time_remaining"] = lambda: None

        custom_metrics_from_handler = {}
        kwargs["metrics"] = custom_metrics_from_handler
        request_failed = False
        error_status_for_health = None
        try:
            with PROMETHEUS_METRICS_REGISTRY["stats_tasks_time_spent_quantile"].labels(*MonitorServer._get_labels()).time():
                async with MonitorServer.async_service_threads[rpc_method_name]:
                    handler_result = await func(*args, **kwargs)
            request_counters["response_size___method__%s" % rpc_method_name] = len(dumps(handler_result))
            return handler_result
        except Exception as caught_exception:
            logging.error(traceback.format_exc())
            request_failed = True
            error_status_for_health = ErrorParser.parse(caught_exception)
            if server:
                await server.inc_stats("errors___method__%s___type__%s" % (rpc_method_name, error_status_for_health))
            raise
        finally:
            if server:
                await server.del_key_value("tasks", request_trace_id)
            for metric_name, metric_value in custom_metrics_from_handler.items():
                request_counters["%s___method__%s" % (metric_name, rpc_method_name)] = metric_value
            request_counters["time full___method__%s" % rpc_method_name] = time.time() - request_start_timestamp_seconds
            if grpc_servicer_context is not None and "FakeContext" not in str(type(grpc_servicer_context)) and getattr(grpc_servicer_context, "time_remaining", lambda: None)() is not None:
                request_counters["time_remaining_after_time___method__%s" % rpc_method_name] = grpc_servicer_context.time_remaining()
            if server:
                await server.inc_stats("processed___method__%s___status__%s" % (rpc_method_name, StatusParser.parse(request_failed)))
                await server.append_stats("stats", request_counters, 100)
            await ErrorParser.set_status(error_status_for_health)

    return run_with_metrics
