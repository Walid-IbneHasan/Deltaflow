from __future__ import annotations

import json
import logging

import pytest

from deltaflow.observability.logs import JsonFormatter
from deltaflow.observability.metrics import Registry


def test_prometheus_text_exposition() -> None:
    reg = Registry()
    txns = reg.counter("deltaflow_transactions_total", "Committed transactions applied")
    deltas = reg.counter("deltaflow_operator_deltas_total", "Deltas", labels=("operator", "dir"))
    depth = reg.gauge("deltaflow_queue_depth", "Queued transactions")
    txns.inc()
    txns.inc(2)
    deltas.labels(operator='join "x"\\1', dir="in").inc(5)
    depth.set(3.5)
    text = reg.expose()
    assert "# HELP deltaflow_transactions_total Committed transactions applied\n" in text
    assert "# TYPE deltaflow_transactions_total counter\n" in text
    assert "deltaflow_transactions_total 3\n" in text
    assert 'deltaflow_operator_deltas_total{operator="join \\"x\\"\\\\1",dir="in"} 5\n' in text
    assert "# TYPE deltaflow_queue_depth gauge\ndeltaflow_queue_depth 3.5\n" in text
    assert text.endswith("\n")


def test_histogram_buckets_are_cumulative_and_quantiles_estimated() -> None:
    reg = Registry()
    lag = reg.histogram("deltaflow_lag_seconds", "Lag", buckets=(0.01, 0.1, 1.0))
    for v in (0.005, 0.05, 0.05, 0.5, 5.0):
        lag.observe(v)
    text = reg.expose()
    assert 'deltaflow_lag_seconds_bucket{le="0.01"} 1\n' in text
    assert 'deltaflow_lag_seconds_bucket{le="0.1"} 3\n' in text
    assert 'deltaflow_lag_seconds_bucket{le="1"} 4\n' in text
    assert 'deltaflow_lag_seconds_bucket{le="+Inf"} 5\n' in text
    assert "deltaflow_lag_seconds_count 5\n" in text
    assert "deltaflow_lag_seconds_sum 5.605\n" in text
    # Quantiles are estimated from the bucket counts (upper bound of the
    # bucket the quantile falls in), like histogram_quantile without
    # interpolation; above the last bucket, the largest observation.
    assert lag.quantile(0.5) == 0.1
    assert lag.quantile(0.8) == 1.0
    assert lag.quantile(0.99) == 5.0
    assert lag.max == 5.0


def test_counters_only_go_up_and_names_are_validated() -> None:
    reg = Registry()
    c = reg.counter("deltaflow_x_total", "x")
    with pytest.raises(ValueError, match="only increase"):
        c.inc(-1)
    with pytest.raises(ValueError, match="already registered"):
        reg.counter("deltaflow_x_total", "again")
    with pytest.raises(ValueError, match="invalid metric name"):
        reg.gauge("bad-name", "x")
    labelled = reg.counter("deltaflow_y_total", "y", labels=("a",))
    with pytest.raises(ValueError, match="labels"):
        labelled.inc()


def test_snapshot_is_json_able() -> None:
    reg = Registry()
    reg.counter("deltaflow_c_total", "c", labels=("op",)).labels(op="a").inc(2)
    reg.histogram("deltaflow_h_seconds", "h", buckets=(1.0,)).observe(0.5)
    snap = reg.snapshot()
    assert json.loads(json.dumps(snap)) == snap
    assert snap["deltaflow_c_total"] == [{"labels": {"op": "a"}, "value": 2}]
    assert snap["deltaflow_h_seconds"][0]["count"] == 1


def test_json_log_lines() -> None:
    record = logging.LogRecord(
        "deltaflow.runtime", logging.INFO, "f.py", 1, "checkpoint at %s", ("0/1",), None
    )
    record.lsn = "0/1"
    record.seconds = 0.25
    line = json.loads(JsonFormatter().format(record))
    assert line["level"] == "info"
    assert line["logger"] == "deltaflow.runtime"
    assert line["msg"] == "checkpoint at 0/1"
    assert line["lsn"] == "0/1"
    assert line["seconds"] == 0.25
    assert "ts" in line


def test_an_unlabelled_metric_is_exposed_before_it_is_touched() -> None:
    reg = Registry()
    reg.counter("deltaflow_untouched_total", "x")
    assert "deltaflow_untouched_total 0\n" in reg.expose()
