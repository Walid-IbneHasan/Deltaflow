"""Typed row changes, grouped into committed transactions.

`TransactionAssembler` consumes decoded pgoutput messages in stream order and
returns a `Transaction` only when its COMMIT arrives. Nothing inside a
transaction is visible before then, so a transaction that is never committed
can never reach a consumer. pgoutput without streaming already omits aborted
transactions; the assembler does not rely on that.

**Large transactions.** With `max_changes`, a transaction is also returned
in `TransactionPart`s of that many changes, as they arrive, and its final
`Transaction` holds only the rest (`continued`). That is sound because
pgoutput without streaming sends a transaction only once it has committed:
the parts belong to a committed transaction, at its commit LSN, and only
its end is still to come. A ten-million-row UPDATE is then never in memory
whole.

Column values are converted to Python with psycopg's loaders, chosen by the
column type OID from the RELATION message. Types psycopg has no loader for
(e.g. user-defined enums) come through as `str` holding Postgres's own text
representation, so nothing is lost. The unchanged-TOAST placeholder is
preserved as `UNCHANGED_TOAST` and never turned into a value. json, jsonb
and arrays become `Json`, `Jsonb` and `PgArray` (core/values.py): exact,
and hashable, where psycopg's dicts and lists are neither.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg import adapt
from psycopg.abc import Buffer, Loader
from psycopg.adapt import AdaptersMap
from psycopg.pq import Format

from deltaflow.core.values import Json, Jsonb, check_bounds, to_array
from deltaflow.replication.lsn import Lsn
from deltaflow.replication.pgoutput import (
    Begin,
    Binary,
    Commit,
    Delete,
    Insert,
    Message,
    OldKind,
    Origin,
    PgOutputMessage,
    ProtocolError,
    RawValue,
    Relation,
    Toast,
    Truncate,
    TypeInfo,
    Update,
)

#: Column name -> Python value, `None` for SQL NULL, or `UNCHANGED_TOAST`.
type Row = dict[str, Any]


@dataclass(frozen=True, slots=True)
class RowInsert:
    relation: Relation
    lsn: Lsn
    new: Row


@dataclass(frozen=True, slots=True)
class RowUpdate:
    relation: Relation
    lsn: Lsn
    #: None: identity columns unchanged and not FULL, so the key is in `new`.
    #: "key": `old` holds only the replica identity columns' previous values.
    #: "full": `old` holds the entire previous row (REPLICA IDENTITY FULL).
    old_kind: OldKind | None
    old: Row | None
    new: Row

    @property
    def key(self) -> Row:
        """Replica identity of the row *before* the update."""
        return _key(self.relation, self.old if self.old is not None else self.new)


@dataclass(frozen=True, slots=True)
class RowDelete:
    relation: Relation
    lsn: Lsn
    old_kind: OldKind
    old: Row

    @property
    def key(self) -> Row:
        return _key(self.relation, self.old)


@dataclass(frozen=True, slots=True)
class TableTruncate:
    relations: tuple[Relation, ...]
    lsn: Lsn
    cascade: bool
    restart_identity: bool


type Change = RowInsert | RowUpdate | RowDelete | TableTruncate | Message


@dataclass(frozen=True, slots=True)
class Transaction:
    xid: int
    #: LSN of the commit record; also the BEGIN message's final_lsn.
    commit_lsn: Lsn
    #: First LSN after the transaction. Acknowledging this as flushed tells
    #: the server never to send this transaction again.
    end_lsn: Lsn
    commit_time: datetime
    origin: str | None
    changes: tuple[Change, ...]
    #: parts of it came before (`TransactionPart`); `changes` is the rest
    continued: bool = False


@dataclass(frozen=True, slots=True)
class TransactionPart:
    """Changes of a committed transaction whose end has not arrived yet
    (only with `TransactionAssembler(max_changes=...)`)."""

    xid: int
    #: the transaction's commit LSN, known from its BEGIN
    commit_lsn: Lsn
    changes: tuple[Change, ...]


def _key(relation: Relation, row: Row) -> Row:
    return {c.name: row[c.name] for c in relation.columns if c.is_key}


JSON_OID, JSONB_OID = 114, 3802


class _JsonLoader(adapt.Loader):
    def load(self, data: Buffer) -> Json:
        return Json(bytes(data).decode())


class _JsonbLoader(adapt.Loader):
    def load(self, data: Buffer) -> Jsonb:
        return Jsonb(bytes(data).decode())


class _JsonBinaryLoader(_JsonLoader):
    format = Format.BINARY  # json's binary form is its text


class _JsonbBinaryLoader(adapt.Loader):
    format = Format.BINARY

    def load(self, data: Buffer) -> Jsonb:
        raw = bytes(data)
        if raw[:1] != bytes([1]):
            raise ProtocolError(f"unknown jsonb binary format version {raw[:1]!r}")
        return Jsonb(raw[1:].decode())  # version 1: jsonb_out's text


def register_value_loaders(adapters: AdaptersMap) -> None:
    """json and jsonb, in either format, as `Json` and `Jsonb` (arrays of
    them too). Results of other queries compared with views (tests) are
    read with the same loaders."""
    adapters.register_loader(JSON_OID, _JsonLoader)
    adapters.register_loader(JSONB_OID, _JsonbLoader)
    adapters.register_loader(JSON_OID, _JsonBinaryLoader)
    adapters.register_loader(JSONB_OID, _JsonbBinaryLoader)


class ValueDecoder:
    """Raw pgoutput column -> Python value, by type OID; loaders are cached.

    Its loaders are psycopg's, from a map of its own in which json and
    jsonb load as `Json` and `Jsonb`, so array elements do too."""

    def __init__(self) -> None:
        self._loaders: dict[tuple[int, Format], Loader] = {}
        self.adapters = AdaptersMap(psycopg.adapters)
        register_value_loaders(self.adapters)

    def __call__(self, type_oid: int, raw: RawValue) -> Any:
        if raw is None or isinstance(raw, Toast):
            return raw
        if isinstance(raw, Binary):
            return to_array(self._loader(type_oid, Format.BINARY).load(raw.data))
        value = self._loader(type_oid, Format.TEXT).load(raw)
        if isinstance(value, list):
            check_bounds(bytes(raw[:1]).decode())
            return to_array(value)
        return value

    def text(self, type_oid: int, text: str | None) -> Any:
        """A value from its text form (as `::text` reads it in a snapshot)."""
        return None if text is None else self(type_oid, text.encode())

    def reads_as_text(self, type_oid: int) -> bool:
        """Whether a snapshot must read this type as text to get the value
        the stream gets: json and jsonb (exactness), arrays (their bounds),
        and types without a binary loader (enums, domains)."""
        if type_oid in (JSON_OID, JSONB_OID):
            return True
        info = psycopg.postgres.types.get(type_oid)
        if info is not None and info.array_oid == type_oid:  # an array type
            return True
        return self.adapters.get_loader(type_oid, Format.BINARY) is None

    def _loader(self, type_oid: int, fmt: Format) -> Loader:
        loader = self._loaders.get((type_oid, fmt))
        if loader is None:
            cls = self.adapters.get_loader(type_oid, fmt)
            if cls is None:
                # No loader for this type: fall back to the text loader, which
                # returns the server's text representation unchanged.
                if fmt is Format.BINARY:
                    raise ProtocolError(f"no binary loader for type OID {type_oid}")
                cls = self.adapters.get_loader(0, Format.TEXT)
                assert cls is not None
            loader = cls(type_oid, self.adapters)
            self._loaders[type_oid, fmt] = loader
        return loader


@dataclass(slots=True)
class _OpenTransaction:
    begin: Begin
    origin: str | None
    changes: list[Change]
    parts: int = 0


class TransactionAssembler:
    def __init__(
        self, decode_value: ValueDecoder | None = None, *, max_changes: int | None = None
    ) -> None:
        if max_changes is not None and max_changes < 1:
            raise ValueError("max_changes must be at least 1")
        self._max_changes = max_changes
        self._decode = decode_value or ValueDecoder()
        self._relations: dict[int, Relation] = {}
        self._types: dict[int, TypeInfo] = {}
        self._open: _OpenTransaction | None = None

    @property
    def in_transaction(self) -> bool:
        return self._open is not None

    def feed(
        self, lsn: Lsn, msg: PgOutputMessage
    ) -> Transaction | TransactionPart | Message | None:
        """Consume one message located at WAL position `lsn`.

        Returns a committed `Transaction`, a non-transactional `Message`, a
        `TransactionPart` (with `max_changes`), or None while a transaction
        is still open.
        """
        event = self._feed(lsn, msg)
        if (
            event is None
            and self._max_changes is not None
            and self._open is not None
            and len(self._open.changes) >= self._max_changes
        ):
            return self._part(self._open)
        return event

    def _feed(self, lsn: Lsn, msg: PgOutputMessage) -> Transaction | Message | None:
        match msg:
            case Relation():
                self._relations[msg.oid] = msg
            case TypeInfo():
                self._types[msg.oid] = msg
            case Begin():
                if self._open is not None:
                    raise ProtocolError(
                        f"nested BEGIN (xid {msg.xid} inside {self._open.begin.xid})"
                    )
                self._open = _OpenTransaction(msg, None, [])
            case Origin():
                self._txn("ORIGIN").origin = msg.name
            case Commit():
                return self._commit(msg)
            case Message() if not msg.transactional:
                return msg
            case Message():
                self._txn("MESSAGE").changes.append(msg)
            case Insert():
                txn, rel = self._txn("INSERT"), self._relation(msg.relation_oid)
                txn.changes.append(RowInsert(rel, lsn, self._row(rel, msg.new)))
            case Update():
                txn, rel = self._txn("UPDATE"), self._relation(msg.relation_oid)
                old = self._old_row(rel, msg.old_kind, msg.old) if msg.old is not None else None
                txn.changes.append(RowUpdate(rel, lsn, msg.old_kind, old, self._row(rel, msg.new)))
            case Delete():
                txn, rel = self._txn("DELETE"), self._relation(msg.relation_oid)
                old = self._old_row(rel, msg.old_kind, msg.old)
                txn.changes.append(RowDelete(rel, lsn, msg.old_kind, old))
            case Truncate():
                txn = self._txn("TRUNCATE")
                rels = tuple(self._relation(oid) for oid in msg.relation_oids)
                txn.changes.append(TableTruncate(rels, lsn, msg.cascade, msg.restart_identity))
        return None

    def _part(self, txn: _OpenTransaction) -> TransactionPart:
        part = TransactionPart(txn.begin.xid, txn.begin.final_lsn, tuple(txn.changes))
        txn.changes = []
        txn.parts += 1
        return part

    def _commit(self, msg: Commit) -> Transaction:
        if self._open is None:
            raise ProtocolError(f"COMMIT without BEGIN at {msg.commit_lsn}")
        begin = self._open.begin
        if msg.commit_lsn != begin.final_lsn:
            raise ProtocolError(
                f"commit LSN {msg.commit_lsn} does not match BEGIN final LSN {begin.final_lsn}"
            )
        txn = Transaction(
            begin.xid,
            msg.commit_lsn,
            msg.end_lsn,
            msg.commit_time,
            self._open.origin,
            tuple(self._open.changes),
            continued=self._open.parts > 0,
        )
        self._open = None
        return txn

    def _txn(self, what: str) -> _OpenTransaction:
        if self._open is None:
            raise ProtocolError(f"{what} outside a transaction")
        return self._open

    def _relation(self, oid: int) -> Relation:
        try:
            return self._relations[oid]
        except KeyError:
            raise ProtocolError(f"change for unknown relation {oid} (no RELATION seen)") from None

    def _row(self, rel: Relation, values: tuple[RawValue, ...]) -> Row:
        if len(values) != len(rel.columns):
            raise ProtocolError(
                f"{rel.qualified_name} has {len(rel.columns)} columns, tuple has {len(values)}"
            )
        return {
            col.name: self._decode(col.type_oid, raw)
            for col, raw in zip(rel.columns, values, strict=True)
        }

    def _old_row(self, rel: Relation, kind: OldKind | None, values: tuple[RawValue, ...]) -> Row:
        row = self._row(rel, values)
        if kind == "key":
            # Non-identity columns are sent as NULL placeholders; they are not
            # the old values, so they are dropped rather than passed on.
            return _key(rel, row)
        return row
