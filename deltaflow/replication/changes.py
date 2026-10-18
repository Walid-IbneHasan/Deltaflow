"""Typed row changes, grouped into committed transactions.

`TransactionAssembler` consumes decoded pgoutput messages in stream order and
returns a `Transaction` only when its COMMIT arrives. Nothing inside a
transaction is visible before then, so a transaction that is never committed
can never reach a consumer. pgoutput without streaming already omits aborted
transactions; the assembler does not rely on that.

Column values are converted to Python with psycopg's loaders, chosen by the
column type OID from the RELATION message. Types psycopg has no loader for
(e.g. user-defined enums) come through as `str` holding Postgres's own text
representation, so nothing is lost. The unchanged-TOAST placeholder is
preserved as `UNCHANGED_TOAST` and never turned into a value.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.abc import Loader
from psycopg.pq import Format

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


def _key(relation: Relation, row: Row) -> Row:
    return {c.name: row[c.name] for c in relation.columns if c.is_key}


class ValueDecoder:
    """Raw pgoutput column -> Python value, by type OID; loaders are cached."""

    def __init__(self) -> None:
        self._loaders: dict[tuple[int, Format], Loader] = {}

    def __call__(self, type_oid: int, raw: RawValue) -> Any:
        if raw is None or isinstance(raw, Toast):
            return raw
        if isinstance(raw, Binary):
            return self._loader(type_oid, Format.BINARY).load(raw.data)
        return self._loader(type_oid, Format.TEXT).load(raw)

    def _loader(self, type_oid: int, fmt: Format) -> Loader:
        loader = self._loaders.get((type_oid, fmt))
        if loader is None:
            cls = psycopg.adapters.get_loader(type_oid, fmt)
            if cls is None:
                # No loader for this type: fall back to the text loader, which
                # returns the server's text representation unchanged.
                if fmt is Format.BINARY:
                    raise ProtocolError(f"no binary loader for type OID {type_oid}")
                cls = psycopg.adapters.get_loader(0, Format.TEXT)
                assert cls is not None
            loader = cls(type_oid, None)
            self._loaders[type_oid, fmt] = loader
        return loader


@dataclass(slots=True)
class _OpenTransaction:
    begin: Begin
    origin: str | None
    changes: list[Change]


class TransactionAssembler:
    def __init__(self, decode_value: ValueDecoder | None = None) -> None:
        self._decode = decode_value or ValueDecoder()
        self._relations: dict[int, Relation] = {}
        self._types: dict[int, TypeInfo] = {}
        self._open: _OpenTransaction | None = None

    @property
    def in_transaction(self) -> bool:
        return self._open is not None

    def feed(self, lsn: Lsn, msg: PgOutputMessage) -> Transaction | Message | None:
        """Consume one message located at WAL position `lsn`.

        Returns a committed `Transaction`, a non-transactional `Message`, or
        None while a transaction is still open.
        """
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
