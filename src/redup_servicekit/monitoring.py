import asyncio
import copy
import datetime
import logging
import socket
import threading
import time
import traceback
from collections import defaultdict
from enum import Enum
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

from prometheus_client.exposition import (
    ThreadingWSGIServer,
    _bake_output,
    _SilentHandler,
)
from prometheus_client.metrics import Counter, Gauge, Histogram
from prometheus_client.registry import REGISTRY

from .metrics import (
    BUCKETS_HIST,
    OPERATION_EXPAND_METRICS,
    PROMETHEUS_METRICS_REGISTRY,
)


class TaskStatus(Enum):
    OK = 1
    FATAL = 2
    CONNECTION_ERROR = 3
    OTHER_EXCEPTION = -1
    INTERNAL = 4
    UNKNOWN = 5


class ErrorParser:
    _types = []
    _taskstatus2types = {}

    @staticmethod
    def init():
        for status_key, entry in MonitorServer.errors.items():
            ErrorParser._taskstatus2types[status_key] = entry["service_errors"]
            ErrorParser._types.extend(entry["service_errors"])

    @staticmethod
    def parse(error):
        traceback_string = traceback.format_exc().lower()
        for error_type_name in ErrorParser._types:
            if error_type_name == "other":
                continue
            if error_type_name in traceback_string:
                return error_type_name
        return "other"

    @staticmethod
    async def set_status(error_type=None):
        server = MonitorServer.get_instance()
        if not server:
            return
        if error_type is None:
            await server.set_task_status(TaskStatus.OK)
            return
        mapped_task_status = None
        for status_key, err_list in ErrorParser._taskstatus2types.items():
            if error_type in err_list:
                mapped_task_status = (
                    getattr(TaskStatus, status_key,
                            None) or TaskStatus.OTHER_EXCEPTION
                )
                break
        if mapped_task_status is None:
            mapped_task_status = TaskStatus.OK
        await server.set_task_status(mapped_task_status)


class StatusParser:
    _types = ["failure", "success"]

    @staticmethod
    def parse(error=False):
        return StatusParser._types[0] if error else StatusParser._types[1]


class MonitorStorage:
    """Async storage for monitoring state. Registry is updated only when /metrics is requested."""

    @staticmethod
    def _sanitize_metric_name(name):
        return name.replace(" ", "_").replace("-", "_").replace(".", "_").lower()

    def _gauge_get(self, metric):
        """Return current value of a gauge/counter with default labels (for combining metrics)."""
        return metric.labels(*MonitorServer._get_labels())._value.get()

    def __init__(self):
        self._lock = threading.Lock()
        self._stats = {}
        self._status_series = {}
        self._init_time = str(datetime.datetime.now())

    def get_time(self):
        return self._init_time

    def refresh_registry_for_metrics(self):
        """Update PROMETHEUS_METRICS_REGISTRY from current stats. Call from MetricServer on /metrics request (e.g. every 30s)."""
        with self._lock:
            snapshot = copy.deepcopy(self._stats)
        self._refresh_registry_from_stats(snapshot)

    @staticmethod
    def _get_aggregate_stats(all_stats, show_tasks=True, last_count=-1):
        aggregated_by_key = (
            {"tasks": [len(all_stats.get("tasks", {}))]} if show_tasks else {}
        )
        stats_series = all_stats.get("stats", [])
        if last_count > 0:
            stats_series = stats_series[-last_count:]
        for stats_point in stats_series:
            for stat_key in stats_point:
                aggregated_by_key.setdefault(stat_key, []).append(stats_point[stat_key])
        for aggregation_key in aggregated_by_key:
            try:
                aggregated_by_key[aggregation_key] = (
                    sum(aggregated_by_key[aggregation_key]) * 1.0
                    / len(aggregated_by_key[aggregation_key])
                )
            except Exception:
                aggregated_by_key[aggregation_key] = 0.0
        return aggregated_by_key

    def _apply_operations(
        self,
        values_to_aggregate,
        operation_names=None,
        prefix="",
        postfix="",
        skip_op_in_suffix=None,
    ):
        operation_names = operation_names or ["sum"]
        for operation_name in operation_names:
            aggregation_function = OPERATION_EXPAND_METRICS.get(operation_name)
            if aggregation_function is None:
                raise NotImplementedError(
                    "Operation %s not implemented" % operation_name
                )
            operation_name_suffix_for_metric = (
                "_" if operation_name == skip_op_in_suffix else "_%s_" % operation_name
            )
            prometheus_metric_key = prefix + operation_name_suffix_for_metric + postfix
            if prometheus_metric_key not in PROMETHEUS_METRICS_REGISTRY:
                PROMETHEUS_METRICS_REGISTRY[prometheus_metric_key] = Gauge(
                    prometheus_metric_key,
                    prometheus_metric_key.replace("_", " "),
                    MonitorServer._label_names(),
                )
            PROMETHEUS_METRICS_REGISTRY[prometheus_metric_key].labels(
                *MonitorServer._get_labels()
            ).set(aggregation_function(values_to_aggregate))

    def _prom_set_value(self, stat_key, numeric_value, prefix):
        key_parts = stat_key.split("___")
        metric_name_from_stat_key = key_parts[0]
        prometheus_label_names = []
        prometheus_label_values = []
        for label_part in key_parts[1:]:
            if len(label_part.split("__")) != 2:
                logging.warning(
                    "Label %s has bad format. Must be <label2name__label2value>.",
                    label_part,
                )
            else:
                label_name_part, label_value_part = label_part.split("__", 1)
                prometheus_label_names.append(label_name_part)
                prometheus_label_values.append(label_value_part)
        prometheus_metric_key_sanitized = prefix + self._sanitize_metric_name(
            metric_name_from_stat_key
        )
        if prometheus_metric_key_sanitized not in PROMETHEUS_METRICS_REGISTRY:
            PROMETHEUS_METRICS_REGISTRY[prometheus_metric_key_sanitized] = Gauge(
                prometheus_metric_key_sanitized,
                "This is metric `%s` from MonitorServer." % metric_name_from_stat_key,
                MonitorServer._label_names(prometheus_label_names),
            )
        PROMETHEUS_METRICS_REGISTRY[prometheus_metric_key_sanitized].labels(
            *MonitorServer._get_labels(prometheus_label_values)
        ).set(numeric_value)

    def _refresh_registry_from_stats(self, all_stats):
        for aggregation_key, aggregation_value in list(
            self._get_aggregate_stats(all_stats, False).items()
        ):
            if isinstance(aggregation_value, (float, int)):
                self._prom_set_value(aggregation_key, aggregation_value, "stats_aggregation_")
        for aggregation_key, aggregation_value in list(
            self._get_aggregate_stats(all_stats, False, 5).items()
        ):
            if isinstance(aggregation_value, (float, int)):
                self._prom_set_value(
                    aggregation_key, aggregation_value, "stats_aggregation_last_"
                )
        for top_level_stat_key in all_stats:
            if top_level_stat_key in ("tasks", "stats"):
                continue
            if isinstance(all_stats[top_level_stat_key], (int, float)):
                self._prom_set_value(
                    top_level_stat_key, all_stats[top_level_stat_key], "stats_"
                )
        task_id_to_start_timestamp = all_stats.get("tasks", [])
        self._apply_operations(
            task_id_to_start_timestamp,
            ["len"],
            prefix="tasks",
            postfix="count",
            skip_op_in_suffix="len",
        )
        current_timestamp_seconds = time.time()
        self._apply_operations(
            [
                (current_timestamp_seconds - task_id_to_start_timestamp[task_id])
                for task_id in task_id_to_start_timestamp
            ],
            ["sum", "max"],
            prefix="current_tasks",
            postfix="time_spend",
            skip_op_in_suffix="sum",
        )
        combined_time_spend_metric_key = "stats_tasks_with_current_time_spend_total"
        current_val = self._gauge_get(PROMETHEUS_METRICS_REGISTRY["current_tasks_time_spend"])
        spent_val = self._gauge_get(PROMETHEUS_METRICS_REGISTRY["stats_tasks_time_spend"])
        PROMETHEUS_METRICS_REGISTRY[combined_time_spend_metric_key].labels(
            *MonitorServer._get_labels()
        ).set(current_val + spent_val)

    def _record_histogram_observation(
        self,
        histogram_metric_key_string,
        observed_value,
        metric_prefix,
        bucket_type="time",
    ):
        key_parts = histogram_metric_key_string.split("___")
        metric_name_part, label_parts = key_parts[0], key_parts[1:]
        histogram_label_names = []
        histogram_label_values = []
        for label_part in label_parts:
            if len(label_part.split("__")) != 2:
                logging.warning(
                    "The label %s is not in the correct format. It should be <label2name__label2value>.",
                    label_part,
                )
            else:
                label_name_part, label_value_part = label_part.split("__", 1)
                histogram_label_names.append(label_name_part)
                histogram_label_values.append(label_value_part)
        sanitized_histogram_metric_key = metric_prefix + self._sanitize_metric_name(
            metric_name_part
        )
        if sanitized_histogram_metric_key not in PROMETHEUS_METRICS_REGISTRY:
            PROMETHEUS_METRICS_REGISTRY[sanitized_histogram_metric_key] = Histogram(
                sanitized_histogram_metric_key,
                "this is hist metric `%s` in stats from MonitorServers"
                % metric_name_part,
                MonitorServer._label_names(histogram_label_names),
                buckets=BUCKETS_HIST[bucket_type],
            )
        PROMETHEUS_METRICS_REGISTRY[sanitized_histogram_metric_key].labels(
            *MonitorServer._get_labels(histogram_label_values)
        ).observe(observed_value)

    async def inc_stats(self, stat_key, count=1):
        with self._lock:
            self._stats[stat_key] = self._stats.get(stat_key, 0) + count
            result = self._stats[stat_key]
        return result

    async def set_stats(self, stat_key, value):
        with self._lock:
            self._stats[stat_key] = value
        return value

    async def set_task_status(self, status, task_type="default"):
        with self._lock:
            old_status, old_series = self._status_series.get(
                task_type, (TaskStatus.OK.name, 0)
            )
            if old_status == status.name:
                new_status, new_series = old_status, old_series + 1
            else:
                new_status, new_series = status.name, 1
            self._status_series[task_type] = (new_status, new_series)
            self._stats["series." + task_type] = (status.name, new_series)

    async def append_stats(self, stat_key, stat_value, max_count=-1):
        with self._lock:
            if stat_key not in self._stats:
                self._stats[stat_key] = []
            self._stats[stat_key].append(stat_value)
            if max_count >= 0:
                self._stats[stat_key] = self._stats[stat_key][-max_count:]
            if isinstance(stat_value, dict):
                for histogram_stat_key, histogram_observed_value in stat_value.items():
                    bucket_type = "time" if "time" in histogram_stat_key else "size"
                    if "time" in histogram_stat_key or "size" in histogram_stat_key:
                        self._record_histogram_observation(
                            histogram_stat_key,
                            histogram_observed_value,
                            "stats_hist_",
                            bucket_type=bucket_type,
                        )
            elif ("time" in stat_key or "size" in stat_key) and isinstance(
                stat_value, (int, float)
            ):
                self._record_histogram_observation(
                    stat_key,
                    stat_value,
                    "stats_hist_",
                    bucket_type="time" if "time" in stat_key else "size",
                )

    async def add_key_value(self, section_key, key_value_tuple):
        with self._lock:
            if section_key not in self._stats:
                self._stats[section_key] = {}
            if section_key == "tasks":
                PROMETHEUS_METRICS_REGISTRY["started_total"].labels(
                    *MonitorServer._get_labels()
                ).inc()
            self._stats[section_key][key_value_tuple[0]] = key_value_tuple[1]

    async def del_key_value(self, section_key, key_to_remove):
        with self._lock:
            if section_key in self._stats and key_to_remove in self._stats[section_key]:
                if section_key == "tasks":
                    PROMETHEUS_METRICS_REGISTRY["stats_tasks_time_spend"].labels(
                        *MonitorServer._get_labels()
                    ).inc(time.time() - self._stats["tasks"][key_to_remove])
                    PROMETHEUS_METRICS_REGISTRY["completed_total"].labels(
                        *MonitorServer._get_labels()
                    ).inc()
                del self._stats[section_key][key_to_remove]

    async def get_stats(self):
        with self._lock:
            return copy.deepcopy(self._stats)

    async def get_statuses(self):
        with self._lock:
            return copy.deepcopy(self._status_series)


class MetricServer:
    """HTTP server in a thread: serves /metrics. Updates registry from storage only when /metrics is requested."""

    def __init__(self, registry=REGISTRY, disable_compression=False, storage=None):
        self._registry = registry
        self._disable_compression = disable_compression
        self._storage = storage

    def _bake_response(self, environ):
        """Build (status, headers, output) for /metrics from self._registry."""
        return _bake_output(
            self._registry,
            environ.get("HTTP_ACCEPT"),
            environ.get("HTTP_ACCEPT_ENCODING"),
            parse_qs(environ.get("QUERY_STRING", "")),
            self._disable_compression,
        )

    def get_wsgi_app(self):
        """Return WSGI app: on /metrics refreshes registry from storage then bakes output."""
        storage = self._storage

        def wsgi_application(environ, start_response):
            if environ["PATH_INFO"] != "/metrics":
                start_response("503 Service Unavailable", [("", "")])
                return [b""]
            if storage:
                storage.refresh_registry_for_metrics()
            status, headers, output = self._bake_response(environ)
            start_response(status, headers)
            return [output]

        return wsgi_application

    def run_in_thread(self, port):
        """Start HTTP server in a daemon thread. Returns the server object."""
        wsgi_app = self.get_wsgi_app()
        while True:
            try:
                http_server = make_server(
                    "",
                    port,
                    wsgi_app,
                    ThreadingWSGIServer,
                    handler_class=_SilentHandler,
                )
                break
            except socket.error:
                time.sleep(1)
        logging.info("Start metric server on port: %s.", port)
        thread = threading.Thread(target=http_server.serve_forever)
        thread.daemon = True
        thread.start()
        return http_server


class MonitorServer:
    """Single entry point: forwards all requests to MonitorStorage. Runs MetricServer in a thread."""

    __instance = None
    __time = None

    @staticmethod
    def _get_labels(extra=None):
        return [MonitorServer.get_time()] + (extra or [])

    @staticmethod
    def _label_names(extra=None):
        return ["inittime"] + (extra or [])

    @staticmethod
    def get_instance():
        return MonitorServer.__instance

    @staticmethod
    def get_time():
        return MonitorServer.__time

    async def wait_for_ending_tasks(self):
        """Wait until no tasks are in progress. Use: await server.wait_for_ending_tasks()."""
        while True:
            stats = await self.get_stats()
            if not stats.get("tasks"):
                return 0
            await asyncio.sleep(1)

    def __init__(self):
        if MonitorServer.__instance:
            raise Exception("This class is a singleton!")
        self._storage = MonitorStorage()
        MonitorServer.__instance = self
        MonitorServer.__time = self._storage.get_time()

    def set_stat_sync(self, stat_key, value):
        """Set one stat key (sync, for init from sync context)."""
        if self._storage:
            with self._storage._lock:
                self._storage._stats[stat_key] = value

    def _init_metrics(self):
        PROMETHEUS_METRICS_REGISTRY["started_total"] = Counter(
            "started", "Total number of started tasks.", MonitorServer._label_names()
        )
        PROMETHEUS_METRICS_REGISTRY["started_total"].labels(
            *MonitorServer._get_labels()).inc(0)
        PROMETHEUS_METRICS_REGISTRY["completed_total"] = Counter(
            "completed", "Total number of completed tasks.", MonitorServer._label_names()
        )
        PROMETHEUS_METRICS_REGISTRY["completed_total"].labels(
            *MonitorServer._get_labels()).inc(0)
        PROMETHEUS_METRICS_REGISTRY["tasks_count"] = Gauge(
            "tasks_count", "The total number of tasks currently being processed.", MonitorServer._label_names()
        )
        PROMETHEUS_METRICS_REGISTRY["tasks_count"].labels(*MonitorServer._get_labels()).inc(0)
        PROMETHEUS_METRICS_REGISTRY["current_tasks_time_spend"] = Gauge(
            "current_tasks_time_spend",
            "Total time spent processing current tasks.",
            MonitorServer._label_names(),
        )
        PROMETHEUS_METRICS_REGISTRY["current_tasks_max_time_spend"] = Gauge(
            "current_tasks_max_time_spend",
            "Maximum processing time for current tasks.",
            MonitorServer._label_names(),
        )
        PROMETHEUS_METRICS_REGISTRY["current_tasks_time_spend"].labels(
            *MonitorServer._get_labels()
        ).inc(0)
        PROMETHEUS_METRICS_REGISTRY["stats_tasks_time_spend"] = Counter(
            "stats_tasks_time_spend",
            "Total time spent on completed tasks.",
            MonitorServer._label_names(),
        )
        PROMETHEUS_METRICS_REGISTRY["stats_tasks_time_spend"].labels(
            *MonitorServer._get_labels()
        ).inc(0)
        PROMETHEUS_METRICS_REGISTRY[
            "stats_tasks_with_current_time_spend_total"
        ] = Gauge(
            "stats_tasks_with_current_time_spend_total",
            "Total time spent processing current and previous tasks.",
            MonitorServer._label_names(),
        )
        PROMETHEUS_METRICS_REGISTRY["stats_tasks_with_current_time_spend_total"].labels(
            *MonitorServer._get_labels()
        ).inc(0)
        PROMETHEUS_METRICS_REGISTRY[
            "stats_tasks_time_spent_quantile"
        ] = Histogram(
            "tasks_time_spent_quantile",
            "Time spent on completing tasks.",
            MonitorServer._label_names(),
            buckets=BUCKETS_HIST["time"],
        )
        for service_info_key in MonitorServer.service_info:
            PROMETHEUS_METRICS_REGISTRY[service_info_key] = Gauge(
                service_info_key,
                MonitorServer.service_info[service_info_key]["info"],
                MonitorServer._label_names(),
            )
            PROMETHEUS_METRICS_REGISTRY[service_info_key].labels(*MonitorServer._get_labels()).set(
                float(MonitorServer.service_info[service_info_key]["value"])
            )

    def run(self, server_config=None, max_workers=1, hpa_max_workers=None):
        server_config = server_config or {}
        http_port = int(server_config.get("port", 9999))
        configured_max_workers = (
            hpa_max_workers if hpa_max_workers is not None else max_workers
        )
        MonitorServer.async_service_threads = defaultdict(
            lambda: asyncio.Semaphore(max_workers)
        )
        MonitorServer.service_threads = defaultdict(
            lambda: threading.Semaphore(max_workers)
        )
        MonitorServer.service_info = dict(
            server_config.get("service info", {}))
        MonitorServer.service_info["service_threads"] = {
            "info": "number of service threads setups by the config",
            "value": configured_max_workers,
        }
        MonitorServer.series_unhealthy = {}
        MonitorServer.errors = {
            "CONNECTION_ERROR": {"service_errors": [], "unhealthy": -1},
            "FATAL": {"service_errors": [], "unhealthy": -1},
            "INTERNAL": {"service_errors": [], "unhealthy": -1},
            "OTHER_EXCEPTION": {"service_errors": ["other"], "unhealthy": -1},
            "UNKNOWN": {"service_errors": [], "unhealthy": -1},
        }
        MonitorServer.errors.update(server_config.get("errors", {}))
        for task_status_enum in TaskStatus:
            if task_status_enum == TaskStatus.OK:
                continue
            MonitorServer.series_unhealthy[task_status_enum.name] = int(
                MonitorServer.errors[task_status_enum.name]["unhealthy"]
            )
        MonitorServer.__time = self._storage.get_time()
        self._init_metrics()
        MetricServer(registry=REGISTRY, storage=self._storage).run_in_thread(http_port)

    async def inc_stats(self, stat_key, value=1):
        if self._storage:
            await self._storage.inc_stats(stat_key, value)
        else:
            logging.warning("The monitor server is not initialized.")

    async def set_task_status(self, status, task_type="default"):
        if self._storage:
            await self._storage.set_task_status(status, task_type=task_type)
        else:
            logging.warning("The monitor server is not initialized.")

    async def append_stats(self, stat_key, stat_value, max_count=-1):
        if self._storage:
            await self._storage.append_stats(stat_key, stat_value, max_count=max_count)
        else:
            logging.warning("The monitor server is not initialized.")

    async def add_key_value(self, section_key, key_value_tuple):
        if self._storage:
            await self._storage.add_key_value(section_key, key_value_tuple)
        else:
            logging.warning("The monitor server is not initialized.")

    async def del_key_value(self, section_key, key_to_remove):
        if self._storage:
            await self._storage.del_key_value(section_key, key_to_remove)
        else:
            logging.warning("The monitor server is not initialized.")

    async def get_stats(self):
        if self._storage:
            return await self._storage.get_stats()
        logging.warning("The monitor server is not initialized.")
        return {}

    async def get_statuses(self):
        if self._storage:
            return await self._storage.get_statuses()
        logging.warning("The monitor server is not initialized.")
        return {}