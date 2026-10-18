"""The embedded state store: one RocksDB instance per engine.

Memory budget
-------------
RocksDB's memory goes mainly to three places:

* the block cache, holding uncompressed data blocks read from SST files;
* memtables, holding recent writes not yet flushed;
* index and filter blocks of open SST files.

All three are tied to one LRU cache of `memory_budget` bytes:

* the block cache *is* that cache;
* memtable memory is charged to it through a WriteBufferManager, whose
  limit is a quarter of the budget;
* index and filter blocks are cached in it (`cache_index_and_filter_blocks`)
  rather than held per open file.

Once the cache is full, the least recently used blocks are evicted, and
memtables flush to SST files on disk. State beyond the budget lives in those
files and is read back through the cache on demand.

What the budget does *not* cover is accounted for in docs/join.md:
- Python objects in flight (one transaction's deltas, one snapshot chunk);
- iterator buffers;
- compaction scratch space;
- the allocator's own overhead.

Durability
----------
None in M3. The WAL is disabled and the directory is wiped on open, because
state is rebuilt from a fresh snapshot on every start. Making state survive
a restart, atomically with the sink's watermark, is M5.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from rocksdict import (
    BlockBasedOptions,
    Cache,
    Options,
    Rdict,
    ReadOptions,
    WriteBatch,
    WriteBufferManager,
    WriteOptions,
)

MIN_BUDGET = 1 << 20


def _length_prefixed(name: str) -> bytes:
    raw = name.encode()
    if len(raw) > 255:
        raise ValueError(f"keyspace name too long: {name!r}")
    return bytes([len(raw)]) + raw


def _successor(prefix: bytes) -> bytes | None:
    """Smallest byte string greater than every string starting with `prefix`."""
    trimmed = prefix.rstrip(b"\xff")
    if not trimmed:
        return None
    return trimmed[:-1] + bytes([trimmed[-1] + 1])


@dataclass(frozen=True, slots=True)
class StoreStats:
    memory_budget: int
    #: bytes held in the shared cache: data blocks, index/filter blocks and
    #: memtable reservations
    cache_usage: int
    memtable_bytes: int
    #: live data on disk
    sst_bytes: int
    sst_files: int


class StateStore:
    def __init__(self, path: Path, *, memory_budget: int) -> None:
        if memory_budget < MIN_BUDGET:
            raise ValueError(f"memory_budget must be at least {MIN_BUDGET} bytes")
        self.path = path
        self.memory_budget = memory_budget
        if path.exists():
            shutil.rmtree(path)  # M3 state is never resumed; see module docstring
        path.mkdir(parents=True)

        self._cache = Cache(memory_budget)
        table = BlockBasedOptions()
        table.set_block_cache(self._cache)
        table.set_cache_index_and_filter_blocks(True)
        table.set_pin_l0_filter_and_index_blocks_in_cache(True)
        table.set_bloom_filter(10, False)
        opts = Options(raw_mode=True)
        opts.create_if_missing(True)
        opts.set_block_based_table_factory(table)
        memtables = memory_budget // 4
        opts.set_write_buffer_manager(
            WriteBufferManager.new_write_buffer_manager_with_cache(memtables, False, self._cache)
        )
        opts.set_write_buffer_size(max(memtables // 2, 64 << 10))
        opts.set_max_write_buffer_number(2)
        opts.set_max_open_files(512)
        self._db = Rdict(str(path), opts)
        write = WriteOptions()
        # rocksdict's stub declares disable_wal as a method; at runtime it is
        # a property (getset descriptor), so assign and check it took.
        setattr(write, "disable_wal", True)  # noqa: B010
        assert getattr(write, "disable_wal") is True  # noqa: B009
        self._db.set_write_options(write)
        self._write: WriteOptions = write

    def keyspace(self, name: str) -> Keyspace:
        return Keyspace(self, _length_prefixed(name))

    def batch(self) -> Batch:
        return Batch(self)

    def stats(self) -> StoreStats:
        def prop(name: str) -> int:
            return self._db.property_int_value(name) or 0

        return StoreStats(
            memory_budget=self.memory_budget,
            cache_usage=self._cache.get_usage(),
            memtable_bytes=prop("rocksdb.cur-size-all-mem-tables"),
            sst_bytes=prop("rocksdb.total-sst-files-size"),
            sst_files=len(self._db.live_files()),
        )

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> StateStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class Keyspace:
    """A named slice of the store: every key is stored behind its prefix."""

    __slots__ = ("_db", "_prefix", "_store")

    def __init__(self, store: StateStore, prefix: bytes) -> None:
        self._store = store
        self._db = store._db
        self._prefix = prefix

    def get(self, key: bytes) -> bytes | None:
        value = self._db.get(self._prefix + key)
        assert value is None or isinstance(value, bytes)
        return value

    def put(self, key: bytes, value: bytes) -> None:
        self._db.put(self._prefix + key, value)

    def delete(self, key: bytes) -> None:
        self._db.delete(self._prefix + key)

    def scan(self, prefix: bytes = b"") -> Iterator[tuple[bytes, bytes]]:
        """Entries whose key starts with `prefix`, in key order."""
        full = self._prefix + prefix
        read = ReadOptions()
        upper = _successor(full)
        if upper is not None:
            read.set_iterate_upper_bound(upper)
        it = self._db.iter(read)
        it.seek(full)
        cut = len(self._prefix)
        while it.valid():
            key = it.key()
            assert isinstance(key, bytes)
            if not key.startswith(full):
                return
            yield key[cut:], it.value()
            it.next()

    def clear(self) -> None:
        upper = _successor(self._prefix)
        assert upper is not None  # the prefix starts with a length byte < 0xff
        self._db.delete_range(self._prefix, upper)

    @property
    def full_prefix(self) -> bytes:
        return self._prefix


class Batch:
    """Writes applied atomically when the `with` block exits normally."""

    def __init__(self, store: StateStore) -> None:
        self._store = store
        self._batch = WriteBatch(raw_mode=True)

    def put(self, ks: Keyspace, key: bytes, value: bytes) -> None:
        self._batch.put(ks.full_prefix + key, value)

    def delete(self, ks: Keyspace, key: bytes) -> None:
        self._batch.delete(ks.full_prefix + key)

    def __enter__(self) -> Batch:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self._store._db.write(self._batch, self._store._write)
