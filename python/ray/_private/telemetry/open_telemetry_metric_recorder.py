import logging
import os
import re
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Dict, FrozenSet, List, Optional, Tuple
from urllib.parse import unquote

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client.core import REGISTRY, CollectorRegistry, HistogramMetricFamily

import ray
from ray._private.metrics_agent import Record
from ray._private.telemetry.metric_cardinality import MetricCardinality
from ray._private.telemetry.metric_types import MetricType

logger = logging.getLogger(__name__)

NAMESPACE = "ray"

# Mirror the name sanitization of the OpenTelemetry Prometheus exporter
# (opentelemetry/exporter/prometheus/_mapping.py), so histograms served by
# _AggregatedHistogramCollector keep the series names and labels they had when they
# went through the exporter.
_SANITIZE_NAME_RE = re.compile(r"[^a-zA-Z0-9:]+")
_SANITIZE_LABEL_KEY_RE = re.compile(r"[^a-zA-Z0-9]+")


def _sanitize_name(name: str) -> str:
    if name and name[0].isdigit():
        name = "_" + name[1:]
    return _SANITIZE_NAME_RE.sub("_", name)


def _sanitize_label_key(key: str) -> str:
    if key and key[0].isdigit():
        key = "_" + key[1:]
    return _SANITIZE_LABEL_KEY_RE.sub("_", key)


@dataclass
class _HistogramSeries:
    """Cumulative bucket counts and sum of one histogram label set."""

    bucket_counts: List[int]
    sum: float = 0.0


@dataclass
class _AggregatedHistogram:
    """A histogram fed with pre-aggregated bucket counts (see
    OpenTelemetryMetricRecorder.record_histogram_aggregated_batch)."""

    description: str
    # Upper bounds of the buckets, excluding the implicit +Inf bucket.
    bounds: Tuple[float, ...]
    series: Dict[FrozenSet[Tuple[str, str]], _HistogramSeries] = field(
        default_factory=dict
    )


class _AggregatedHistogramCollector:
    """prometheus_client collector that serves the recorder's pre-aggregated
    histograms."""

    def __init__(self, recorder: "OpenTelemetryMetricRecorder"):
        self._recorder = recorder

    def collect(self):
        return self._recorder._collect_aggregated_histograms()


def _get_service_name(default_name: str) -> str:
    otel_service_name = os.environ.get("OTEL_SERVICE_NAME")
    if otel_service_name:
        return otel_service_name

    otel_resource_attributes = os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "")

    for attribute in otel_resource_attributes.split(","):
        key, sep, value = attribute.partition("=")
        if sep and key.strip() == "service.name" and value.strip():
            return unquote(value.strip())

    return default_name


class OpenTelemetryMetricRecorder:
    """
    A class to record OpenTelemetry metrics. This is the main entry point for exporting
    all ray telemetries to Prometheus server.
    It uses OpenTelemetry's Prometheus exporter to export metrics.
    """

    _metrics_initialized = False
    _metrics_initialized_lock = threading.Lock()

    def __init__(
        self,
        gauge_metric_ttl_seconds: Optional[float] = None,
        registry: CollectorRegistry = REGISTRY,
    ):
        # Lock-ordering contract:
        #   _registration_lock -> SDK meter locks (via meter.create_*)
        #   _registration_lock -> _lock
        #   SDK measurement-consumer lock -> _lock (the SDK holds its own lock
        #       while invoking our observable callbacks during collect(), and the
        #       callbacks acquire _lock)
        # _lock must therefore never be held across an SDK call that can acquire
        # the measurement-consumer lock — i.e. observable-instrument creation
        # (meter.create_observable_*, which registers the instrument with the
        # consumer). Holding _lock there deadlocks with a concurrent scrape:
        # registration holds _lock and waits on the consumer lock inside
        # create_*, while collect() holds the consumer lock and waits on _lock
        # inside a callback. _registration_lock exists so that instrument
        # registration stays serialized (no duplicate instruments on a race)
        # without _lock being held across those SDK entry points.
        #
        # Note: the synchronous histogram path (set_metric_value) does call
        # instrument.record() while holding _lock. That is safe from this deadlock: record() only takes
        # per-reader-storage locks, never the measurement-consumer lock, and
        # reader-storage code never calls back into this recorder, so no lock
        # cycle can form. Keep any new SDK call out of _lock unless it has the
        # same property.
        self._lock = threading.Lock()
        self._registration_lock = threading.Lock()
        self._registered_instruments = {}
        # Gauge observations are stored as tag_key -> (value, last_update_monotonic).
        # Unlike counters/sums, gauges are evicted once they have not been refreshed
        # within `_gauge_metric_ttl_s` (see the scrape callback below).
        self._gauge_observations_by_name = defaultdict(dict)
        self._counter_observations_by_name = defaultdict(dict)
        self._sum_observations_by_name = defaultdict(dict)
        self._aggregated_histograms: Dict[str, _AggregatedHistogram] = {}
        self._histogram_bucket_mismatch_warned = set()
        self._gauge_metric_ttl_s = self._resolve_gauge_ttl_seconds(
            gauge_metric_ttl_seconds
        )
        self._init_metrics()
        self.meter = metrics.get_meter(__name__)
        # Pre-aggregated histograms bypass the OpenTelemetry SDK, whose synchronous
        # histogram only accepts individual measurements, and are served to
        # Prometheus by this collector. Its collect() only takes _lock, and the
        # registry runs it separately from the OpenTelemetry reader's collector, so
        # it adds no lock-ordering constraints.
        registry.register(_AggregatedHistogramCollector(self))

    @staticmethod
    def _resolve_gauge_ttl_seconds(override: Optional[float]) -> float:
        """Returns how long (in seconds) a gauge observation is retained without a
        refresh before it is evicted on scrape.

        Emitters export their live gauge values to the agent every export interval.
        The effective export interval is ``max(metrics_report_interval_ms, 1000)`` --
        the emitter floors it at 1000ms via ``SetReportInterval`` in
        ``src/ray/stats/stats.h`` (``GetReportInterval()`` is what actually drives the
        OTLP export in ``InitOpenTelemetryExporter``). We apply the same floor here so
        the TTL is exactly 2x the true export cadence, even when the raw config value
        is below 1000ms. Retaining a value for 2 export intervals lets an
        actively-reported series survive a missed/late export, while a series that
        stops being reported (finished task, dead worker) ages out after ~2 intervals.

        NOTE: this mirrors the ``max(..., 1000)`` clamp and the export cadence in
        ``stats.h``. If either changes, update this derivation.

        Callers may pass an explicit ``override`` (used by tests and available for
        future injection from above).
        """
        if override is not None:
            return override
        # Mirror the emitter's SetReportInterval floor (stats.h): the export cadence is
        # max(metrics_report_interval_ms, 1000ms), never less than 1s.
        effective_report_interval_ms = max(
            ray._config.metrics_report_interval_ms(), 1000
        )
        return 2 * effective_report_interval_ms / 1000.0

    def _create_observable_callback(
        self, metric_name: str, metric_type: MetricType
    ) -> Callable[[dict], List[Observation]]:
        """
        Factory method to create callbacks for observable metrics.

        Args:
            metric_name: name of the metric for which the callback is being created
            metric_type: type of the metric for which the callback is being created

        Returns:
            Callable: A callback function that can be used to record observations for the metric.
        """

        def callback(options):
            with self._lock:
                # Select appropriate storage based on metric type
                if metric_type == MetricType.GAUGE:
                    # Gauges report the last value. Instead of clearing after each
                    # scrape (which drops a series between reports if the emitter
                    # hasn't re-reported in time), retain each value for a TTL and
                    # evict only observations that have gone stale.
                    stored = self._gauge_observations_by_name.get(metric_name, {})
                    now = time.monotonic()
                    retained = {}
                    observations = {}
                    for tag_set, (val, ts) in stored.items():
                        if now - ts <= self._gauge_metric_ttl_s:
                            retained[tag_set] = (val, ts)
                            observations[tag_set] = val
                    self._gauge_observations_by_name[metric_name] = retained
                elif metric_type == MetricType.COUNTER:
                    observations = self._counter_observations_by_name.get(
                        metric_name, {}
                    )
                    # Don't clear - counters are cumulative
                elif metric_type == MetricType.SUM:
                    observations = self._sum_observations_by_name.get(metric_name, {})
                    # Don't clear - sums are cumulative
                else:
                    return []

                # Aggregate by filtered tags (drop high cardinality labels)
                high_cardinality_labels = (
                    MetricCardinality.get_high_cardinality_labels_to_drop(metric_name)
                )
                # First, collect all values that share the same filtered tag set
                values_by_filtered_tags = defaultdict(list)
                for tag_set, val in observations.items():
                    filtered = frozenset(
                        (k, v) for k, v in tag_set if k not in high_cardinality_labels
                    )
                    values_by_filtered_tags[filtered].append(val)

                # Then aggregate each group using the appropriate aggregation function
                agg_fn = MetricCardinality.get_aggregation_function(
                    metric_name, metric_type
                )
                # Keep a single label schema for each metric before passing
                # observations to the Prometheus exporter.
                all_keys = sorted(
                    {k for filtered in values_by_filtered_tags for k, _ in filtered}
                )

                observations = []
                for filtered, values in values_by_filtered_tags.items():
                    attrs = dict(filtered)
                    observations.append(
                        Observation(
                            agg_fn(values),
                            attributes={k: attrs.get(k, "") for k in all_keys},
                        )
                    )
                return observations

        return callback

    def _init_metrics(self):
        # Initialize the global metrics provider and meter. We only do this once on
        # the first initialization of the class, because re-setting the meter provider
        # can result in loss of metrics.
        with OpenTelemetryMetricRecorder._metrics_initialized_lock:
            if OpenTelemetryMetricRecorder._metrics_initialized:
                return
            from opentelemetry.sdk.resources import Resource

            prometheus_reader = PrometheusMetricReader()
            provider = MeterProvider(
                resource=Resource.create(
                    {
                        "service.name": _get_service_name("ray-dashboard-agent"),
                    }
                ),
                metric_readers=[prometheus_reader],
            )
            metrics.set_meter_provider(provider)
            OpenTelemetryMetricRecorder._metrics_initialized = True

    def register_gauge_metric(self, name: str, description: str) -> None:
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Gauge with the same name is already registered.
                    return

            callback = self._create_observable_callback(name, MetricType.GAUGE)
            # Created without holding self._lock: create_observable_gauge acquires
            # SDK-internal locks that are also held around our callbacks during
            # collect() (see the lock-ordering contract in __init__).
            instrument = self.meter.create_observable_gauge(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            with self._lock:
                self._registered_instruments[name] = instrument
                self._gauge_observations_by_name[name] = {}

    def register_counter_metric(self, name: str, description: str) -> None:
        """
        Register an observable counter metric with the given name and description.
        """
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Counter with the same name is already registered. This is a
                    # common case when metrics are exported from multiple Ray
                    # components (e.g., raylet, worker, etc.) running in the same
                    # node. Since each component may export metrics with the same
                    # name, the same metric might be registered multiple times.
                    return

            callback = self._create_observable_callback(name, MetricType.COUNTER)
            # Created without holding self._lock (see __init__ lock-ordering
            # contract).
            instrument = self.meter.create_observable_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            with self._lock:
                self._registered_instruments[name] = instrument
                self._counter_observations_by_name[name] = {}

    def register_sum_metric(self, name: str, description: str) -> None:
        """
        Register an observable sum metric with the given name and description.
        """
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Sum with the same name is already registered. This is a common
                    # case when metrics are exported from multiple Ray components
                    # (e.g., raylet, worker, etc.) running in the same node. Since
                    # each component may export metrics with the same name, the same
                    # metric might be registered multiple times.
                    return

            callback = self._create_observable_callback(name, MetricType.SUM)
            # Created without holding self._lock (see __init__ lock-ordering
            # contract).
            instrument = self.meter.create_observable_up_down_counter(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                callbacks=[callback],
            )
            with self._lock:
                self._registered_instruments[name] = instrument
                self._sum_observations_by_name[name] = {}

    def register_histogram_metric(
        self, name: str, description: str, buckets: List[float]
    ) -> None:
        """
        Register a histogram metric with the given name and description.
        """
        with self._registration_lock:
            with self._lock:
                if name in self._registered_instruments:
                    # Histogram with the same name is already registered. This is a
                    # common case when metrics are exported from multiple Ray
                    # components (e.g., raylet, worker, etc.) running in the same
                    # node. Since each component may export metrics with the same
                    # name, the same metric might be registered multiple times.
                    return

            # Created without holding self._lock (see __init__ lock-ordering
            # contract).
            instrument = self.meter.create_histogram(
                name=f"{NAMESPACE}_{name}",
                description=description,
                unit="1",
                explicit_bucket_boundaries_advisory=buckets,
            )

            with self._lock:
                self._registered_instruments[name] = instrument

    def set_metric_value(self, name: str, tags: dict, value: float):
        """
        Set the value of a metric with the given name and tags.

        For observable metrics (gauge, counter, sum), this stores the value internally
        and returns immediately. The value will be exported asynchronously when
        OpenTelemetry collects metrics.

        For histograms, this calls record() synchronously since there is no observable
        histogram in OpenTelemetry.

        If the metric is not registered, it lazily records the value for observable metrics or is a no-op for
        synchronous metrics.
        """
        with self._lock:
            tag_key = frozenset(tags.items())
            if self._gauge_observations_by_name.get(name) is not None:
                # Gauge - store the most recent value and its timestamp for the given
                # tags. The timestamp is used to evict stale observations on scrape.
                self._gauge_observations_by_name[name][tag_key] = (
                    value,
                    time.monotonic(),
                )
            elif name in self._counter_observations_by_name:
                # Counter - increment the value for the given tags.
                self._counter_observations_by_name[name][tag_key] = (
                    self._counter_observations_by_name[name].get(tag_key, 0) + value
                )
            elif name in self._sum_observations_by_name:
                # Sum - add the value for the given tags.
                self._sum_observations_by_name[name][tag_key] = (
                    self._sum_observations_by_name[name].get(tag_key, 0) + value
                )
            else:
                # Histogram - record the value synchronously.
                instrument = self._registered_instruments.get(name)
                if isinstance(instrument, metrics.Histogram):
                    # Filter out high cardinality labels.
                    filtered_tags = {
                        k: v
                        for k, v in tags.items()
                        if k
                        not in MetricCardinality.get_high_cardinality_labels_to_drop(
                            name
                        )
                    }
                    instrument.record(value, attributes=filtered_tags)
                else:
                    logger.warning(
                        f"Metric {name} is not registered or unsupported type."
                    )

    def record_histogram_aggregated_batch(
        self,
        name: str,
        description: str,
        data_points: List[dict],
    ) -> None:
        """
        Add pre-aggregated histogram data points to the histogram with the given name.

        Each data point is a dict with "tags", "explicit_bounds" (bucket upper bounds,
        excluding +Inf), "bucket_counts" and "sum", holding the delta since the
        emitter's previous export. The bucket counts and sum are added exactly to the
        cumulative series of the data point's tags, after dropping high cardinality
        labels, so the cost is per data point rather than per observation.

        The first data point seen for a name fixes its bounds. Several processes may
        emit the same name with different bounds (e.g. two vLLM engines configured
        with different max_model_len), whose counts cannot be added together. Those
        data points are skipped with a warning rather than raising, which would
        otherwise abort ingestion for the rest of the reporting component's metrics.
        """
        if not data_points:
            return
        with self._lock:
            histogram = self._aggregated_histograms.get(name)
            if histogram is None:
                histogram = _AggregatedHistogram(
                    description=description,
                    bounds=tuple(data_points[0]["explicit_bounds"]),
                )
                self._aggregated_histograms[name] = histogram

            high_cardinality_labels = (
                MetricCardinality.get_high_cardinality_labels_to_drop(name)
            )
            num_buckets = len(histogram.bounds) + 1
            for dp in data_points:
                bucket_counts = dp["bucket_counts"]
                if (
                    tuple(dp["explicit_bounds"]) != histogram.bounds
                    or len(bucket_counts) != num_buckets
                ):
                    if name not in self._histogram_bucket_mismatch_warned:
                        self._histogram_bucket_mismatch_warned.add(name)
                        logger.warning(
                            f"Metric {name} was reported with bucket bounds "
                            f"{list(dp['explicit_bounds'])} but "
                            f"{list(histogram.bounds)} were expected; skipping the "
                            "data point."
                        )
                    continue

                # Prometheus treats an empty label value as an absent label, so drop
                # empty values to merge data points that differ only in that.
                tag_key = frozenset(
                    (k, v)
                    for k, v in dp["tags"].items()
                    if k not in high_cardinality_labels and v != ""
                )
                series = histogram.series.get(tag_key)
                if series is None:
                    series = _HistogramSeries(bucket_counts=[0] * num_buckets)
                    histogram.series[tag_key] = series
                for i, count in enumerate(bucket_counts):
                    series.bucket_counts[i] += count
                series.sum += dp["sum"]

    def _collect_aggregated_histograms(self) -> List[HistogramMetricFamily]:
        """Build the Prometheus families of the pre-aggregated histograms, in the
        same format the OpenTelemetry Prometheus exporter produces."""
        with self._lock:
            snapshot = [
                (
                    name,
                    histogram.description,
                    histogram.bounds,
                    [
                        (dict(tag_key), list(series.bucket_counts), series.sum)
                        for tag_key, series in histogram.series.items()
                    ],
                )
                for name, histogram in self._aggregated_histograms.items()
            ]

        families = []
        for name, description, bounds, series_list in snapshot:
            if not series_list:
                continue
            # Keep a single label schema for the metric.
            label_keys = sorted({k for tags, _, _ in series_list for k in tags})
            family = HistogramMetricFamily(
                name=_sanitize_name(f"{NAMESPACE}_{name}"),
                documentation=description,
                labels=[_sanitize_label_key(k) for k in label_keys],
            )
            upper_bounds = [str(bound) for bound in bounds] + ["+Inf"]
            for tags, bucket_counts, total in series_list:
                cumulative_count = 0
                buckets = []
                for upper_bound, count in zip(upper_bounds, bucket_counts):
                    cumulative_count += count
                    buckets.append((upper_bound, cumulative_count))
                family.add_metric(
                    labels=[tags.get(k, "") for k in label_keys],
                    buckets=buckets,
                    sum_value=total,
                )
            families.append(family)
        return families

    def record_and_export(self, records: List[Record], global_tags=None):
        """
        Record a list of telemetry records and export them to Prometheus.
        """
        global_tags = global_tags or {}

        for record in records:
            gauge = record.gauge
            value = record.value
            tags = {**record.tags, **global_tags}
            try:
                self.register_gauge_metric(gauge.name, gauge.description or "")
                self.set_metric_value(gauge.name, tags, value)
            except Exception as e:
                logger.error(
                    f"Failed to record metric {gauge.name} with value {value} with tags {tags!r} and global tags {global_tags!r} due to: {e!r}"
                )
