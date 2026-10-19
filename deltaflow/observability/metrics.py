"""Prometheus metrics: counters, gauges, histograms, in the text exposition
format (version 0.0.4).

Hand-written instead of `prometheus_client` (decision 0006): Deltaflow
serves metrics from its own asyncio HTTP server, per runner, alongside the
console's JSON API. The client library brings a global registry and a
thread-based server. The exposition format is a few lines of text per
metric.
"""

from __future__ import annotations

import bisect
import math
import re
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

_NAME = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")
type LabelValues = tuple[str, ...]

DEFAULT_BUCKETS = (
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
)  # fmt: skip


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _number(v: float) -> str:
    if v == math.inf:
        return "+Inf"
    if isinstance(v, int) or v.is_integer():
        return str(int(v))
    return repr(v)


class _Metric:
    kind = ""

    def __init__(self, name: str, help: str, labels: Sequence[str]) -> None:
        self.name, self.help, self.label_names = name, help, tuple(labels)

    def _key(self, labels: dict[str, str]) -> LabelValues:
        if set(labels) != set(self.label_names):
            raise ValueError(f"{self.name} takes labels {self.label_names}, got {tuple(labels)}")
        return tuple(str(labels[n]) for n in self.label_names)

    def _render_labels(self, values: LabelValues, extra: str = "") -> str:
        parts = [f'{n}="{_escape(v)}"' for n, v in zip(self.label_names, values, strict=True)]
        if extra:
            parts.append(extra)
        return "{" + ",".join(parts) + "}" if parts else ""

    def header(self) -> Iterator[str]:
        yield f"# HELP {self.name} {self.help}"
        yield f"# TYPE {self.name} {self.kind}"

    def samples(self, extra: str = "") -> Iterator[str]:
        """The sample lines; `extra` is one more rendered label, `k="v"`."""
        return iter(())

    def expose(self) -> Iterator[str]:
        yield from self.header()
        yield from self.samples()


class _Value:
    def __init__(self) -> None:
        self.value = 0.0


class _CounterValue(_Value):
    def inc(self, n: float = 1) -> None:
        if n < 0:
            raise ValueError("counters only increase")
        self.value += n


class _GaugeValue(_Value):
    def set(self, v: float) -> None:
        self.value = v

    def inc(self, n: float = 1) -> None:
        self.value += n


class _Family[V: _Value](_Metric):
    """A metric with one value per combination of label values."""

    def __init__(self, name: str, help: str, labels: Sequence[str], make: type[V]) -> None:
        super().__init__(name, help, labels)
        self._make = make
        self._children: dict[LabelValues, V] = {}
        if not self.label_names:
            self._children[()] = make()  # an unlabelled metric is exposed as 0 from the start

    def labels(self, **labels: str) -> V:
        key = self._key(labels)
        child = self._children.get(key)
        if child is None:
            child = self._children[key] = self._make()
        return child

    def value(self, **labels: str) -> float:
        child = self._children.get(self._key(labels))
        return child.value if child else 0.0

    def samples(self, extra: str = "") -> Iterator[str]:
        for key, child in self._children.items():
            yield f"{self.name}{self._render_labels(key, extra)} {_number(child.value)}"

    def snapshot(self) -> list[dict[str, Any]]:
        return [
            {"labels": dict(zip(self.label_names, k, strict=True)), "value": c.value}
            for k, c in self._children.items()
        ]


class Counter(_Family[_CounterValue]):
    kind = "counter"

    def __init__(self, name: str, help: str, labels: Sequence[str]) -> None:
        super().__init__(name, help, labels, _CounterValue)

    def inc(self, n: float = 1) -> None:
        self.labels().inc(n)


class Gauge(_Family[_GaugeValue]):
    kind = "gauge"

    def __init__(self, name: str, help: str, labels: Sequence[str]) -> None:
        super().__init__(name, help, labels, _GaugeValue)

    def set(self, v: float) -> None:
        self.labels().set(v)


class Histogram(_Metric):
    """Bucketed observations. `quantile` estimates a quantile as the upper
    bound of the bucket it falls in (no interpolation), or the largest
    observation when it falls above the last bucket. Coarse but honest: the
    true value is at most that."""

    kind = "histogram"

    def __init__(self, name: str, help: str, buckets: Sequence[float]) -> None:
        super().__init__(name, help, ())
        self.buckets = tuple(sorted(buckets))
        self.counts = [0] * (len(self.buckets) + 1)
        self.count = 0
        self.sum = 0.0
        self.max = 0.0

    def observe(self, v: float) -> None:
        self.counts[bisect.bisect_left(self.buckets, v)] += 1
        self.count += 1
        self.sum += v
        self.max = max(self.max, v)

    def quantile(self, q: float) -> float:
        if not self.count:
            return math.nan
        rank = q * self.count
        seen = 0
        for i, n in enumerate(self.counts):
            seen += n
            if seen >= rank and n:
                return self.buckets[i] if i < len(self.buckets) else self.max
        return self.max

    def samples(self, extra: str = "") -> Iterator[str]:
        seen = 0
        prefix = f"{extra}," if extra else ""
        for bound, n in zip([*self.buckets, math.inf], self.counts, strict=True):
            seen += n
            yield f'{self.name}_bucket{{{prefix}le="{_number(bound)}"}} {seen}'
        labels = f"{{{extra}}}" if extra else ""
        yield f"{self.name}_sum{labels} {_number(round(self.sum, 9))}"
        yield f"{self.name}_count{labels} {self.count}"

    def snapshot(self) -> list[dict[str, Any]]:
        return [
            {
                "count": self.count,
                "sum": self.sum,
                "max": self.max,
                "p50": self.quantile(0.5) if self.count else None,
                "p99": self.quantile(0.99) if self.count else None,
            }
        ]


class Registry:
    def __init__(self) -> None:
        self._metrics: dict[str, _Metric] = {}

    def _add[M: _Metric](self, metric: M) -> M:
        if not _NAME.fullmatch(metric.name):
            raise ValueError(f"invalid metric name {metric.name!r}")
        if metric.name in self._metrics:
            raise ValueError(f"metric {metric.name} already registered")
        self._metrics[metric.name] = metric
        return metric

    def counter(self, name: str, help: str, labels: Sequence[str] = ()) -> Counter:
        return self._add(Counter(name, help, labels))

    def gauge(self, name: str, help: str, labels: Sequence[str] = ()) -> Gauge:
        return self._add(Gauge(name, help, labels))

    def histogram(
        self, name: str, help: str, buckets: Sequence[float] = DEFAULT_BUCKETS
    ) -> Histogram:
        return self._add(Histogram(name, help, buckets))

    def expose(self) -> str:
        return "".join(line + "\n" for m in self._metrics.values() for line in m.expose())

    def snapshot(self) -> dict[str, list[dict[str, Any]]]:
        out: dict[str, list[dict[str, Any]]] = {}
        for name, m in self._metrics.items():
            if isinstance(m, Counter | Gauge | Histogram):
                out[name] = m.snapshot()
        return out

    def metrics(self) -> Iterator[_Metric]:
        return iter(self._metrics.values())


def merge(registries: Mapping[str, Registry], *, label: str) -> str:
    """Several registries of the same metrics as one exposition: each family
    once, its samples from every registry labelled `label="<key>"`. This is
    how a cluster worker serves all of its shards at one endpoint; listing
    the registries one after another would repeat every family, which the
    format does not allow."""
    if not _NAME.fullmatch(label):
        raise ValueError(f"invalid label name {label!r}")
    families: dict[str, list[tuple[str, _Metric]]] = {}
    for key, registry in registries.items():
        for metric in registry.metrics():
            families.setdefault(metric.name, []).append((key, metric))
    lines: list[str] = []
    for members in families.values():
        lines.extend(members[0][1].header())
        for key, metric in members:
            lines.extend(metric.samples(f'{label}="{_escape(key)}"'))
    return "".join(line + "\n" for line in lines)
