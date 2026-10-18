"""Deterministic randomized workload over the seed schema (`customers`, `orders`).

The generator keeps an exact in-memory model of the rows it has produced. That
model serves two purposes:

* every UPDATE/DELETE targets a row that exists, and every order references a
  live customer, so statements never fail for workload reasons;
* tests can compare the database against the model, which makes the model an
  oracle for the replication reader and the differential suites later on.

Given the same starting state and seed, the sequence of transactions is
identical. Wall-clock time is never consulted.
"""

from __future__ import annotations

import random
import string
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

COUNTRIES = ("NL", "DE", "FR", "GB", "US", "JP", "BR", "IN")
STATUSES = ("pending", "paid", "shipped", "cancelled")
_FIRST = ("Ada", "Alan", "Barbara", "Edsger", "Grace", "Ken", "Leslie", "Margaret", "Niklaus")
_LAST = ("Lovelace", "Turing", "Liskov", "Dijkstra", "Hopper", "Thompson", "Lamport", "Wirth")
_EPOCH = datetime(2024, 1, 1, tzinfo=UTC)
_ALPHABET = string.ascii_letters + string.digits

#: Postgres compresses, then moves out of line, any value that pushes a row past
#: ~2 kB (TOAST_TUPLE_THRESHOLD). Values at least this long, drawn from 62
#: symbols, defeat pglz and are therefore always stored out of line.
TOAST_VALUE_MIN_LEN = 8192
_TOAST_VALUE_MAX_LEN = 24576


class OpKind(StrEnum):
    INSERT_CUSTOMER = "insert_customer"
    UPDATE_CUSTOMER = "update_customer"
    DELETE_CUSTOMER = "delete_customer"  # cascades to the customer's orders
    INSERT_ORDER = "insert_order"
    UPDATE_ORDER = "update_order"
    REKEY_ORDER = "rekey_order"  # UPDATE of the primary key itself
    DELETE_ORDER = "delete_order"


DEFAULT_WEIGHTS: dict[OpKind, int] = {
    OpKind.INSERT_CUSTOMER: 8,
    OpKind.UPDATE_CUSTOMER: 12,
    OpKind.DELETE_CUSTOMER: 3,
    OpKind.INSERT_ORDER: 30,
    OpKind.UPDATE_ORDER: 30,
    OpKind.REKEY_ORDER: 2,
    OpKind.DELETE_ORDER: 15,
}


@dataclass(frozen=True, slots=True)
class Customer:
    id: int
    name: str
    country: str
    email: str | None
    bio: str | None


@dataclass(frozen=True, slots=True)
class Order:
    id: int
    customer_id: int
    status: str
    amount: Decimal
    note: str | None
    created_at: datetime


@dataclass(slots=True)
class TableState:
    """Expected contents of both tables, keyed by primary key.

    Dict order is insertion order, which keeps random choices reproducible.
    """

    customers: dict[int, Customer]
    orders: dict[int, Order]

    def copy(self) -> TableState:
        # Rows are immutable, so a shallow copy is a full snapshot.
        return TableState(dict(self.customers), dict(self.orders))


@dataclass(frozen=True, slots=True)
class Statement:
    kind: OpKind
    sql: str
    params: tuple[object, ...]
    #: Rows the statement must report as affected; anything else means the
    #: model and the database have diverged.
    expected_rowcount: int = 1


@dataclass(frozen=True, slots=True)
class Transaction:
    statements: tuple[Statement, ...]
    commit: bool


@dataclass(frozen=True, slots=True)
class WorkloadConfig:
    max_statements_per_txn: int = 6
    rollback_probability: float = 0.05
    null_probability: float = 0.15
    toast_probability: float = 0.1
    weights: dict[OpKind, int] = field(default_factory=lambda: dict(DEFAULT_WEIGHTS))


class RowFactory:
    """Random column values, shared by seeding and the load generator."""

    def __init__(self, rng: random.Random, config: WorkloadConfig) -> None:
        self._rng = rng
        self._config = config

    def name(self) -> str:
        return f"{self._rng.choice(_FIRST)} {self._rng.choice(_LAST)}"

    def country(self) -> str:
        return self._rng.choice(COUNTRIES)

    def email(self) -> str | None:
        if self._rng.random() < self._config.null_probability:
            return None
        return f"user{self._rng.randrange(10**6)}@example.com"

    def bio(self) -> str | None:
        roll = self._rng.random()
        if roll < self._config.null_probability:
            return None
        if roll < self._config.null_probability + self._config.toast_probability:
            n = self._rng.randint(TOAST_VALUE_MIN_LEN, _TOAST_VALUE_MAX_LEN)
            return "".join(self._rng.choices(_ALPHABET, k=n))
        return "".join(self._rng.choices(_ALPHABET + " ", k=self._rng.randint(0, 80)))

    def status(self) -> str:
        return self._rng.choices(STATUSES, weights=(3, 4, 4, 1))[0]

    def amount(self) -> Decimal:
        return Decimal(self._rng.randint(1, 500_000)).scaleb(-2)

    def note(self) -> str | None:
        if self._rng.random() < 0.5:
            return None
        return self._rng.choice(("gift", "express", "", "leave at door", "fragile ✓"))

    def created_at(self) -> datetime:
        return _EPOCH + timedelta(microseconds=self._rng.randrange(365 * 86_400 * 10**6))

    def customer(self, id_: int) -> Customer:
        return Customer(id_, self.name(), self.country(), self.email(), self.bio())

    def order(self, id_: int, customer_id: int) -> Order:
        return Order(id_, customer_id, self.status(), self.amount(), self.note(), self.created_at())


def initial_state(seed: int, customers: int, orders: int) -> TableState:
    """Seed contents: ids 1..n, every order owned by a random customer."""
    if orders and not customers:
        raise ValueError("orders need at least one customer")
    rng = random.Random(seed)
    rows = RowFactory(rng, WorkloadConfig())
    cs = {i: rows.customer(i) for i in range(1, customers + 1)}
    os_ = {i: rows.order(i, rng.randint(1, customers)) for i in range(1, orders + 1)}
    return TableState(cs, os_)


_CUSTOMER_COLUMNS = ("name", "country", "email", "bio")
_ORDER_COLUMNS = ("customer_id", "status", "amount", "note")


class Workload:
    """Produces transactions and applies committed ones to `state`."""

    def __init__(self, seed: int, state: TableState, config: WorkloadConfig | None = None) -> None:
        self.seed = seed
        self.state = state
        self._config = config or WorkloadConfig()
        self._rng = random.Random(seed)
        self._rows = RowFactory(self._rng, self._config)
        # Like sequences: ids handed out are never reused, even if the
        # transaction rolls back or the row is later deleted.
        self._next_customer_id = max(state.customers, default=0) + 1
        self._next_order_id = max(state.orders, default=0) + 1
        self._kinds = tuple(self._config.weights)
        self._weights = tuple(self._config.weights.values())
        self._generators: dict[OpKind, Callable[[TableState], Statement]] = {
            OpKind.INSERT_CUSTOMER: self._insert_customer,
            OpKind.UPDATE_CUSTOMER: self._update_customer,
            OpKind.DELETE_CUSTOMER: self._delete_customer,
            OpKind.INSERT_ORDER: self._insert_order,
            OpKind.UPDATE_ORDER: self._update_order,
            OpKind.REKEY_ORDER: self._rekey_order,
            OpKind.DELETE_ORDER: self._delete_order,
        }

    def next_transaction(self) -> Transaction:
        commit = self._rng.random() >= self._config.rollback_probability
        # A transaction that will roll back is planned against a scratch copy,
        # so its effects are discarded exactly as Postgres discards them.
        target = self.state if commit else self.state.copy()
        n = self._rng.randint(1, self._config.max_statements_per_txn)
        return Transaction(tuple(self._statement(target) for _ in range(n)), commit)

    def _statement(self, s: TableState) -> Statement:
        kind = self._rng.choices(self._kinds, weights=self._weights)[0]
        needs_customer = kind in {
            OpKind.UPDATE_CUSTOMER,
            OpKind.DELETE_CUSTOMER,
            OpKind.INSERT_ORDER,
        }
        needs_order = kind in {OpKind.UPDATE_ORDER, OpKind.REKEY_ORDER, OpKind.DELETE_ORDER}
        if (needs_customer and not s.customers) or (needs_order and not s.orders):
            kind = OpKind.INSERT_CUSTOMER if not s.customers else OpKind.INSERT_ORDER

        return self._generators[kind](s)

    def _pick[K](self, rows: Mapping[K, object]) -> K:
        return self._rng.choice(list(rows))

    def _insert_customer(self, s: TableState) -> Statement:
        c = self._rows.customer(self._next_customer_id)
        self._next_customer_id += 1
        s.customers[c.id] = c
        return Statement(
            OpKind.INSERT_CUSTOMER,
            "INSERT INTO customers (id, name, country, email, bio) VALUES (%s, %s, %s, %s, %s)",
            (c.id, c.name, c.country, c.email, c.bio),
        )

    def _update_customer(self, s: TableState) -> Statement:
        old = s.customers[self._pick(s.customers)]
        cols = self._subset(_CUSTOMER_COLUMNS)
        new = old
        if "name" in cols:
            new = replace(new, name=self._rows.name())
        if "country" in cols:
            new = replace(new, country=self._rows.country())
        if "email" in cols:
            new = replace(new, email=self._rows.email())
        if "bio" in cols:
            new = replace(new, bio=self._rows.bio())
        s.customers[old.id] = new
        assignments = ", ".join(f"{c} = %s" for c in cols)
        return Statement(
            OpKind.UPDATE_CUSTOMER,
            f"UPDATE customers SET {assignments} WHERE id = %s",
            (*(getattr(new, c) for c in cols), old.id),
        )

    def _delete_customer(self, s: TableState) -> Statement:
        cid = self._pick(s.customers)
        del s.customers[cid]
        for oid in [o.id for o in s.orders.values() if o.customer_id == cid]:
            del s.orders[oid]
        return Statement(OpKind.DELETE_CUSTOMER, "DELETE FROM customers WHERE id = %s", (cid,))

    def _insert_order(self, s: TableState) -> Statement:
        o = self._rows.order(self._next_order_id, self._pick(s.customers))
        self._next_order_id += 1
        s.orders[o.id] = o
        return Statement(
            OpKind.INSERT_ORDER,
            "INSERT INTO orders (id, customer_id, status, amount, note, created_at)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            (o.id, o.customer_id, o.status, o.amount, o.note, o.created_at),
        )

    def _update_order(self, s: TableState) -> Statement:
        old = s.orders[self._pick(s.orders)]
        cols = self._subset(_ORDER_COLUMNS)
        new = old
        if "customer_id" in cols:
            new = replace(new, customer_id=self._pick(s.customers))
        if "status" in cols:
            new = replace(new, status=self._rows.status())
        if "amount" in cols:
            new = replace(new, amount=self._rows.amount())
        if "note" in cols:
            new = replace(new, note=self._rows.note())
        s.orders[old.id] = new
        assignments = ", ".join(f"{c} = %s" for c in cols)
        return Statement(
            OpKind.UPDATE_ORDER,
            f"UPDATE orders SET {assignments} WHERE id = %s",
            (*(getattr(new, c) for c in cols), old.id),
        )

    def _rekey_order(self, s: TableState) -> Statement:
        old = s.orders.pop(self._pick(s.orders))
        new_id = self._next_order_id
        self._next_order_id += 1
        s.orders[new_id] = replace(old, id=new_id)
        return Statement(
            OpKind.REKEY_ORDER, "UPDATE orders SET id = %s WHERE id = %s", (new_id, old.id)
        )

    def _delete_order(self, s: TableState) -> Statement:
        oid = self._pick(s.orders)
        del s.orders[oid]
        return Statement(OpKind.DELETE_ORDER, "DELETE FROM orders WHERE id = %s", (oid,))

    def _subset(self, columns: tuple[str, ...]) -> tuple[str, ...]:
        """A random non-empty subset of `columns`, in schema order."""
        k = self._rng.randint(1, len(columns))
        chosen = set(self._rng.sample(columns, k))
        return tuple(c for c in columns if c in chosen)
