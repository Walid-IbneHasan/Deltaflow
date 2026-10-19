# Views in SQL

```python
df = Deltaflow()
df.sql("""
    CREATE VIEW analytics.revenue_by_country AS
    SELECT c.country, count(*) AS orders, sum(o.amount) AS revenue, avg(o.amount) AS mean
    FROM orders o JOIN customers c ON c.id = o.customer_id
    WHERE o.status <> 'cancelled'
    GROUP BY c.country
""")
```

- **Parsing.** Postgres's own parser (pglast) parses the statement.
- **Translation.** It becomes the same plan a Python view would, on the
  same engine.
- **Naming.** `CREATE [MATERIALIZED] VIEW schema.name` names the view and
  its sink table; a bare SELECT needs `name=` and `sink=`.
- **The fingerprint covers the SQL.** Change the view and the next start
  rebuilds it; a Python view's function bodies are invisible to the
  fingerprint.

## What is maintained

| Clause | Supported |
|---|---|
| FROM | tables (unqualified means `public`), aliases; `[INNER] JOIN` and `LEFT JOIN ... ON a.x = b.y [AND ...]`, left-deep. An inner join's other ON conditions move to WHERE when every join is inner |
| WHERE, HAVING | any supported expression; a row is kept when it is TRUE (not NULL) |
| GROUP BY | columns, expressions, positions (`GROUP BY 1`) |
| Aggregates | `count(*)`, `count(x)`, `count(DISTINCT x)`, `sum`, `min`, `max`, `avg`, of expressions too |
| SELECT | expressions over columns, or over keys and aggregates (`sum(x) / count(*)`); `SELECT *` over one table |
| DISTINCT | `SELECT DISTINCT` |

Expressions:

- **Operators:** `+ - * / %`, `||`, comparisons, `AND OR NOT`;
- **Tests:** `IS [NOT] NULL`, `IS [NOT] TRUE / FALSE / UNKNOWN`,
  `[NOT] IN (...)`, `[NOT] BETWEEN [SYMMETRIC]`,
  `IS [NOT] DISTINCT FROM`, `[NOT] LIKE`;
- **Conditionals and functions:** CASE, COALESCE, NULLIF, GREATEST,
  LEAST, `abs`, `round`, `length`;
- **Types:** casts to integer types, `numeric[(p, s)]`, text, boolean,
  date, timestamp[tz] and float8;
- **Collation:** `COLLATE "C"`.

## Exactly Postgres's results, or refused

Every expression returns what Postgres returns: the same value, the same
numeric scale (`1.50 / 3` has 20 decimals, as in Postgres), NULL where
Postgres gives NULL, and an error where Postgres raises one. The rules
are in [decision 0008](decisions/0008-sql-views.md). Two tests check it:

- `tests/integration/test_sql_expressions.py` compares random expressions
  with Postgres (20,000 in a soak run);
- `tests/differential/test_sql_views.py` maintains twelve SQL views under
  20,000 random writes and compares each with Postgres running the same
  SELECT after every commit. That covers joins and left joins with
  aggregates, HAVING, expressions before and after grouping, and
  `count(DISTINCT)`.

Refused at registration, with the reason:

- set operations, CTEs, subqueries;
- ORDER BY and LIMIT (a maintained table has no order);
- DISTINCT ON, window functions, GROUPING SETS;
- RIGHT and FULL joins, USING, NATURAL, comma joins;
- functions outside the list (`now()` among them), ILIKE;
- output columns Postgres would name `?column?` (alias them).

**Text order.** Ordering comparisons of text, and min/max over text,
follow the collation, and Deltaflow does not reproduce a locale's order.
Write `COLLATE "C"` (code-point order); without it they raise when they
meet text, instead of guessing.

**Not detected.** Overflow of an int2 or int4 column (Postgres raises at
2^15 and 2^31), because runtime values do not carry their column's
integer width. int8 overflow raises.
