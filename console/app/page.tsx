"use client";

import { useEffect, useRef, useState } from "react";

import { Graph } from "@/components/Graph.tsx";
import { DEFAULT_API, fetchStats, type Stats } from "@/lib/api.ts";
import { bytes, rates, seconds, type Rate } from "@/lib/graph.ts";

const POLL_MS = 1000;

function apiBase(): string {
  if (typeof window !== "undefined") {
    const fromQuery = new URLSearchParams(window.location.search).get("api");
    if (fromQuery) return fromQuery.replace(/\/$/, "");
  }
  return process.env.NEXT_PUBLIC_DELTAFLOW_API ?? DEFAULT_API;
}

function Metric({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="metric" title={hint}>
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value}</div>
    </div>
  );
}

export default function Console() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [rateMap, setRateMap] = useState<Map<string, Rate>>(new Map());
  const [error, setError] = useState<string | null>(null);
  const previous = useRef<Stats | null>(null);
  const [api, setApi] = useState(DEFAULT_API);

  useEffect(() => {
    const base = apiBase();
    setApi(base);
    let stopped = false;
    const controller = new AbortController();
    const poll = async () => {
      try {
        const next = await fetchStats(base, controller.signal);
        if (stopped) return;
        setRateMap(rates(previous.current, next));
        previous.current = next;
        setStats(next);
        setError(null);
      } catch (e) {
        if (!stopped) setError(e instanceof Error ? e.message : String(e));
      }
    };
    void poll();
    const timer = setInterval(() => void poll(), POLL_MS);
    return () => {
      stopped = true;
      controller.abort();
      clearInterval(timer);
    };
  }, []);

  const ops = stats?.graph.nodes.filter((n) => n.kind !== "sink") ?? [];
  const fresh = stats?.freshness_seconds;

  return (
    <main>
      <header>
        <h1>Deltaflow</h1>
        <span className="subtitle">
          {stats ? `slot ${stats.slot}` : "connecting"} · {api}
        </span>
        <span className={error ? "status status-down" : "status status-up"}>
          {error ? `unreachable: ${error}` : stats ? "live" : "…"}
        </span>
      </header>

      {stats && (
        <>
          <section className="metrics" aria-label="Runner">
            <Metric label="freshness p50" value={seconds(fresh?.p50)} hint="source commit → sink commit" />
            <Metric label="freshness p99" value={seconds(fresh?.p99)} hint="bucket upper bound" />
            <Metric label="frontier" value={stats.frontier ?? "–"} hint="engine: complete through" />
            <Metric label="sink watermark" value={stats.watermark ?? "–"} />
            <Metric label="checkpoint" value={stats.checkpoint ?? "–"} />
            <Metric label="apply lag" value={bytes(stats.apply_lag_bytes)} hint="received, not yet applied" />
            <Metric label="slot lag" value={bytes(stats.slot_lag_bytes)} hint="WAL the server retains" />
            <Metric
              label="queued"
              value={`${stats.queued} / ${stats.max_queued}`}
              hint="decoded transactions waiting; at the bound, backpressure"
            />
            <Metric label="state on disk" value={bytes(stats.state.sst_bytes)} />
            <Metric
              label="cache"
              value={`${bytes(stats.state.cache_usage)} / ${bytes(stats.state.memory_budget)}`}
            />
            <Metric label="resident" value={bytes(stats.rss_bytes)} />
            <Metric label="transactions" value={String(stats.counters.transactions ?? 0)} />
          </section>

          <section aria-label="Operator graph">
            <h2>Operators</h2>
            <Graph nodes={stats.graph.nodes} rates={rateMap} />
          </section>

          <section aria-label="Operator table">
            <table>
              <thead>
                <tr>
                  <th>operator</th>
                  <th>kind</th>
                  <th>in/s</th>
                  <th>out/s</th>
                  <th>busy</th>
                  <th>deltas in</th>
                  <th>deltas out</th>
                  <th>views</th>
                </tr>
              </thead>
              <tbody>
                {ops.map((n) => {
                  const r = rateMap.get(n.id);
                  return (
                    <tr key={n.id}>
                      <td title={n.label}>{n.id}</td>
                      <td>{n.kind}</td>
                      <td className="num">{(r?.inPerSecond ?? 0).toFixed(1)}</td>
                      <td className="num">{(r?.outPerSecond ?? 0).toFixed(1)}</td>
                      <td className="num">{Math.round((r?.busy ?? 0) * 100)}%</td>
                      <td className="num">{n.deltas_in ?? 0}</td>
                      <td className="num">{n.deltas_out ?? 0}</td>
                      <td>{n.views.join(", ")}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </section>
        </>
      )}
    </main>
  );
}
