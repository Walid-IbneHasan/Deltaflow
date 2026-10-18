# 0004 — State lives in RocksDB, via `rocksdict` (M3)

Status: accepted, 2026-10-18

## Context

Arrangements (M2) and join indices (M3) must hold state larger than memory.
The M3 acceptance test holds *resident memory* under a configured budget.
The spec allows RocksDB (`rocksdict`) or LMDB.

## Options

**(a) RocksDB via `rocksdict`.** An LSM tree with an explicit, sized block
cache. Memtable memory can be charged to the same cache through a
WriteBufferManager, so RocksDB's memory is governed by one number we set and
can read back (`Cache.get_usage`, `rocksdb.cur-size-all-mem-tables`). The
costs:
- a large native extension (RocksDB statically linked, via Rust);
- a binding less widely used than the C++ library itself;
- background compaction, which adds write amplification and needs tuning.

**(b) LMDB via `py-lmdb`.** A B+tree over a memory-mapped file: small,
extremely mature, single-writer (as we are), with zero-copy reads. Its
"cache" is the OS page cache over the mapped file. Pages it touches count
toward the process's resident set, and the OS, not Deltaflow, decides when
they are dropped. A resident-memory budget would therefore be a property of
the kernel's reclaim policy, which can't be configured from the process and
differs between Linux and Windows.

## Decision

(a). The acceptance criterion is a memory budget, and only (a) lets the
process enforce one.

## The dependency

| | |
|---|---|
| Package | `rocksdict` 0.3.29 (2025-12). Wheels for CPython 3.10–3.14 on Linux (manylinux 2.17/2.28), Windows and macOS, so neither CI nor Windows needs a compiler. Ships `py.typed` and a `.pyi`. |
| Why not the stdlib | `dbm` / `sqlite3` have no memory-bounded cache shared with write buffers. `sqlite3`'s page cache is per connection and its write path is a B-tree with a journal; a budget would be approximate in the same way as LMDB's. |
| Stub defect worked around | `WriteOptions.disable_wal` is a property at runtime but a method in the stub. `store.py` sets it with `setattr` and asserts that it took. |
| Use | raw byte keys and values only (`raw_mode=True`). The encodings are ours (`state/codec.py`), so nothing depends on rocksdict's own pickling. |
