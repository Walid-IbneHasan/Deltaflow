-- Seed schema for the Deltaflow harness. Applied by `make seed`, which drops
-- and recreates both tables.
--
-- Names are schema-qualified: with the default search_path ("$user", public)
-- an unqualified CREATE TABLE lands in a schema named after the role, if one
-- exists.
--
-- Replica identity is left at the default (primary key): Deltaflow keeps its
-- own copy of source rows instead (docs/decisions/0003).

DROP TABLE IF EXISTS public.orders;
DROP TABLE IF EXISTS public.customers;

CREATE TABLE public.customers (
    id      bigint PRIMARY KEY,
    name    text   NOT NULL,
    country text   NOT NULL,
    email   text,           -- nullable: exercises NULL decoding
    bio     text            -- nullable; sometimes >8 kB, so stored out of line (TOAST)
);

CREATE TABLE public.orders (
    id          bigint        PRIMARY KEY,
    customer_id bigint        NOT NULL REFERENCES public.customers (id) ON DELETE CASCADE,
    status      text          NOT NULL
                CHECK (status IN ('pending', 'paid', 'shipped', 'cancelled')),
    amount      numeric(12, 2) NOT NULL,
    note        text,
    created_at  timestamptz   NOT NULL
);

CREATE INDEX orders_customer_id_idx ON public.orders (customer_id);
