import type { GraphNode, Stats } from "./api.ts";

export type Placed = GraphNode & { column: number; row: number };

/** Column = length of the longest path from a source, so every edge points
 * right. Rows keep the order nodes arrive in, which is the planner's order. */
export function layout(nodes: GraphNode[]): Placed[] {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const depth = new Map<string, number>();
  const visit = (id: string, seen: Set<string>): number => {
    const known = depth.get(id);
    if (known !== undefined) return known;
    if (seen.has(id)) throw new Error(`cycle through ${id}`);
    seen.add(id);
    const node = byId.get(id);
    const d = node && node.inputs.length ? 1 + Math.max(...node.inputs.map((i) => visit(i, seen))) : 0;
    depth.set(id, d);
    return d;
  };
  const rows = new Map<number, number>();
  return nodes.map((n) => {
    const column = visit(n.id, new Set());
    const row = rows.get(column) ?? 0;
    rows.set(column, row + 1);
    return { ...n, column, row };
  });
}

export type Rate = { inPerSecond: number; outPerSecond: number; busy: number };

/** Per-operator rates between two samples: deltas per second in and out,
 * and the fraction of wall time the operator was busy. Counters only grow;
 * if one went down (the worker restarted), report 0 rather than a negative
 * rate. */
export function rates(previous: Stats | null, current: Stats): Map<string, Rate> {
  const out = new Map<string, Rate>();
  const dt = previous ? (Date.parse(current.time) - Date.parse(previous.time)) / 1000 : 0;
  const before = new Map((previous?.graph.nodes ?? []).map((n) => [n.id, n]));
  for (const node of current.graph.nodes) {
    const prev = before.get(node.id);
    const per = (now?: number, then?: number) =>
      dt > 0 && now !== undefined && then !== undefined && now >= then ? (now - then) / dt : 0;
    out.set(node.id, {
      inPerSecond: per(node.deltas_in, prev?.deltas_in),
      outPerSecond: per(node.deltas_out, prev?.deltas_out),
      busy: per(node.seconds, prev?.seconds),
    });
  }
  return out;
}

export function bytes(n: number | null | undefined): string {
  if (n === null || n === undefined) return "–";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = n;
  let i = 0;
  while (Math.abs(v) >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

export function seconds(s: number | null | undefined): string {
  if (s === null || s === undefined) return "–";
  return s < 1 ? `${(s * 1000).toFixed(s < 0.01 ? 1 : 0)} ms` : `${s.toFixed(2)} s`;
}
