"""Views for the deltaflow command's end-to-end test (test_cli.py), named
on its command line as `tests.integration.cli_views:views` and `:sharded`."""

from __future__ import annotations

from deltaflow.planner import Count, Deltaflow, Sum
from deltaflow.sinks import PostgresSink

SCHEMA = "cli_it"
DDL = f"""
DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;
CREATE SCHEMA {SCHEMA};
CREATE TABLE {SCHEMA}.orders_by_status (
    status text PRIMARY KEY, orders bigint NOT NULL, revenue numeric NOT NULL);
CREATE TABLE {SCHEMA}.orders_per_customer__shards (
    shard int, customer_id bigint, orders bigint NOT NULL, PRIMARY KEY (shard, customer_id));
"""
SQL = "SELECT status, count(*) AS orders, sum(amount) AS revenue FROM orders GROUP BY status"


def views() -> Deltaflow:
    df = Deltaflow()
    (
        df.view("orders_by_status")
        .source("public.orders")
        .group_by("status")
        .agg(orders=Count(), revenue=Sum("amount"))
        .sink(PostgresSink(f"{SCHEMA}.orders_by_status"))
    )
    return df


def sharded() -> Deltaflow:
    df = Deltaflow(shards=2, shard_by={"public.orders": "customer_id"})
    (
        df.view("orders_per_customer")
        .source("public.orders")
        .group_by("customer_id")
        .agg(orders=Count())
        .sink(PostgresSink(f"{SCHEMA}.orders_per_customer"))
    )
    return df
