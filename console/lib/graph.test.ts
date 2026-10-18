import assert from "node:assert/strict";
import { test } from "node:test";

import type { GraphNode, Stats } from "./api.ts";
import { bytes, layout, rates } from "./graph.ts";

const node = (id: string, inputs: string[] = [], extra: Partial<GraphNode> = {}): GraphNode => ({
  id,
  kind: inputs.length ? "map" : "source",
  label: id,
  inputs,
  views: ["v"],
  ...extra,
});

test("columns follow the longest path, so every edge points right", () => {
  const placed = layout([
    node("source:a"),
    node("source:b"),
    node("v/0", ["source:a", "source:b"]),
    node("v/1", ["v/0"]),
    node("w/0", ["source:a"]),
    node("v/sink", ["v/1"]),
  ]);
  const at = new Map(placed.map((p) => [p.id, [p.column, p.row]] as const));
  assert.deepEqual(Object.fromEntries(at), {
    "source:a": [0, 0],
    "source:b": [0, 1],
    "v/0": [1, 0],
    "v/1": [2, 0],
    "w/0": [1, 1],
    "v/sink": [3, 0],
  });
  for (const p of placed) {
    for (const i of p.inputs) assert.ok(at.get(i)![0] < p.column, `${i} -> ${p.id}`);
  }
});

test("a cycle is reported, not looped on", () => {
  assert.throws(() => layout([node("a", ["b"]), node("b", ["a"])]), /cycle/);
});

const stats = (time: string, nodes: GraphNode[]): Stats =>
  ({ time, graph: { nodes } }) as unknown as Stats;

test("rates are per second between samples, and never negative", () => {
  const before = stats("2026-10-19T00:00:00Z", [node("j", ["x"], { deltas_in: 100, deltas_out: 40, seconds: 0.5 })]);
  const after = stats("2026-10-19T00:00:02Z", [node("j", ["x"], { deltas_in: 300, deltas_out: 140, seconds: 0.9 })]);
  assert.deepEqual(rates(before, after).get("j"), { inPerSecond: 100, outPerSecond: 50, busy: 0.2 });
  // a restarted worker's counters start again from zero
  const restarted = stats("2026-10-19T00:00:03Z", [node("j", ["x"], { deltas_in: 5, deltas_out: 1, seconds: 0 })]);
  assert.deepEqual(rates(after, restarted).get("j"), { inPerSecond: 0, outPerSecond: 0, busy: 0 });
  assert.deepEqual(rates(null, after).get("j"), { inPerSecond: 0, outPerSecond: 0, busy: 0 });
});

test("byte formatting", () => {
  assert.equal(bytes(512), "512 B");
  assert.equal(bytes(1536), "1.5 KiB");
  assert.equal(bytes(3 * 1024 ** 3), "3.0 GiB");
  assert.equal(bytes(null), "–");
});
