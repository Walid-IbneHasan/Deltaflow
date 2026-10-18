import type { GraphNode } from "@/lib/api.ts";
import { layout, type Rate } from "@/lib/graph.ts";

const W = 230;
const H = 58;
const COL = 290;
const ROW = 74;
const PAD = 16;

const KIND_COLOUR: Record<GraphNode["kind"], string> = {
  source: "var(--source)",
  filter: "var(--linear)",
  map: "var(--linear)",
  join: "var(--join)",
  group_by: "var(--aggregate)",
  distinct: "var(--aggregate)",
  sink: "var(--sink)",
};

function rate(perSecond: number): string {
  if (perSecond >= 1000) return `${(perSecond / 1000).toFixed(1)}k/s`;
  return `${perSecond.toFixed(perSecond < 10 ? 1 : 0)}/s`;
}

export function Graph({ nodes, rates }: { nodes: GraphNode[]; rates: Map<string, Rate> }) {
  const placed = layout(nodes);
  const at = new Map(placed.map((p) => [p.id, p]));
  const width = PAD * 2 + (Math.max(0, ...placed.map((p) => p.column)) * COL + W);
  const height = PAD * 2 + (Math.max(0, ...placed.map((p) => p.row)) * ROW + H);
  const x = (column: number) => PAD + column * COL;
  const y = (row: number) => PAD + row * ROW;

  return (
    <svg className="graph" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Operator graph">
      {placed.flatMap((p) =>
        p.inputs.map((input) => {
          const from = at.get(input);
          if (!from) return null;
          const x1 = x(from.column) + W;
          const y1 = y(from.row) + H / 2;
          const x2 = x(p.column);
          const y2 = y(p.row) + H / 2;
          const busy = (rates.get(input)?.outPerSecond ?? 0) > 0;
          return (
            <path
              key={`${input}->${p.id}`}
              className={busy ? "edge edge-active" : "edge"}
              d={`M ${x1} ${y1} C ${x1 + 40} ${y1}, ${x2 - 40} ${y2}, ${x2} ${y2}`}
            />
          );
        }),
      )}
      {placed.map((p) => {
        const r = rates.get(p.id);
        const busyPct = Math.min(100, Math.round((r?.busy ?? 0) * 100));
        return (
          <g key={p.id} transform={`translate(${x(p.column)}, ${y(p.row)})`}>
            <title>{`${p.id}\n${p.label}\nviews: ${p.views.join(", ")}`}</title>
            <rect className="node" width={W} height={H} rx={8} />
            <rect width={5} height={H} rx={2} fill={KIND_COLOUR[p.kind]} />
            <text className="node-kind" x={14} y={18}>
              {p.kind}
            </text>
            <text className="node-label" x={14} y={36}>
              {p.label.length > 30 ? `${p.label.slice(0, 29)}…` : p.label}
            </text>
            {p.kind !== "sink" && (
              <text className="node-rate" x={14} y={51}>
                {`in ${rate(r?.inPerSecond ?? 0)} · out ${rate(r?.outPerSecond ?? 0)} · busy ${busyPct}%`}
              </text>
            )}
          </g>
        );
      })}
    </svg>
  );
}
