"""Operator state. M2: in-memory source arrangements. The embedded KV
backend, join indices and spilling arrive in M3."""

from deltaflow.state.arrangement import (
    SchemaChangedError,
    SourceStateError,
    TableArrangement,
    TableSchema,
)

__all__ = ["SchemaChangedError", "SourceStateError", "TableArrangement", "TableSchema"]
