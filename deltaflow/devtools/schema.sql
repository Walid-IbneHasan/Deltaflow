-- Seed schema for the Deltaflow harness. Applied by `make seed`, which drops
-- and recreates both tables.
--
-- Replica identity is left at the default (primary key). Whether views need
-- the full old row from the WAL (REPLICA IDENTITY FULL) or keep their own copy
-- of source rows is an M1 decision, recorded in docs/decisions/ then.

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    id      bigint PRIMARY KEY,
    name    text   NOT NULL,
    country text   NOT NULL,
    email   text,           -- nullable: exercises NULL decoding
    bio     text            -- nullable; sometimes >8 kB, so stored out of line (TOAST)
);

CREATE TABLE orders (
    id          bigint        PRIMARY KEY,
    customer_id bigint        NOT NULL REFERENCES customers (id) ON DELETE CASCADE,
    status      text          NOT NULL
                CHECK (status IN ('pending', 'paid', 'shipped', 'cancelled')),
    amount      numeric(12, 2) NOT NULL,
    note        text,
    created_at  timestamptz   NOT NULL
);

CREATE INDEX orders_customer_id_idx ON orders (customer_id);
