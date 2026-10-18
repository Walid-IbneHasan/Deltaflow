// Shape of a Deltaflow worker's GET /api/stats (deltaflow/runtime/durable.py, DurableRunner.stats).

export type GraphNode = {
  id: string;
  kind: "source" | "filter" | "map" | "join" | "group_by" | "distinct" | "sink";
  label: string;
  inputs: string[];
  views: string[];
  deltas_in?: number;
  deltas_out?: number;
  seconds?: number;
};

export type Stats = {
  time: string;
  slot: string;
  frontier: string | null;
  watermark: string | null;
  checkpoint: string | null;
  apply_lag_bytes: number;
  slot_lag_bytes: number | null;
  queued: number;
  max_queued: number;
  freshness_seconds: { count: number; p50: number | null; p99: number | null; max: number | null };
  counters: Record<string, number>;
  state: {
    memory_budget: number;
    cache_usage: number;
    memtable_bytes: number;
    sst_bytes: number;
    sst_files: number;
  };
  rss_bytes: number;
  graph: { nodes: GraphNode[] };
};

export const DEFAULT_API = "http://127.0.0.1:9187";

export async function fetchStats(api: string, signal?: AbortSignal): Promise<Stats> {
  const response = await fetch(`${api}/api/stats`, { cache: "no-store", signal });
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return (await response.json()) as Stats;
}
