# 0008 — Views written in SQL: Postgres's parser, Postgres's semantics

Status: accepted, 2026-10-19

Views were Python: a builder of steps whose filters and maps are Python
functions. Two problems follow, and they block anyone who would use
Deltaflow as a product:

- **Data teams write SQL.** A Python DSL is a barrier, and a second
  language for logic the team already has as queries.
- **A function's body is invisible.** The plan fingerprint covers a
  function's qualified name, not its body. Changing what a lambda computes
  goes unnoticed, and the checkpoint is reused under different logic
  (M5 documented this; `make worker-reset` was the remedy).

A view written in SQL has neither problem: the SQL is the logic, the
fingerprint covers it, and Postgres itself can say what the view should
contain.

## Fork 1: the parser

**(a) pglast: Postgres's own parser** (libpg_query, the parser source
extracted from Postgres), with prebuilt wheels for Linux, macOS and Windows.
Its parse trees are Postgres's node types. A query is parsed exactly as
Postgres parses it, and its deparser turns any node back into canonical SQL.

**(b) sqlglot**, pure Python, many dialects. Its grammar is its own
approximation of Postgres's, so a query can parse differently from how
Postgres parses it, or parse when Postgres would reject it.

**Chosen: (a).** "No silent approximation" starts at the grammar. The cost
is a compiled dependency, which ships as wheels on every platform CI and
development use. Justified under decision 0001's rule: it replaces what
would otherwise be a hand-written SQL parser.

## Fork 2: where expressions are evaluated

**(a) In Python, reproducing Postgres's semantics** for a deliberately
small subset, and refusing everything else at registration (`SqlError`).

**(b) In Postgres.** For example, evaluate each batch's expressions with
`SELECT expr FROM (VALUES ...)`. Exact by construction, but it puts a round
trip and load on the source database in the hot path of every change.

**Chosen: (a), with Postgres as the test oracle.**
`test_sql_expressions.py` asks Postgres for random, type-correct
expressions over random rows (20,000 in a soak run). It requires the same
value, the same numeric scale, NULL where Postgres gives NULL, and an error
where Postgres raises one.

What it took to agree, each found by that test rather than assumed:

- **Integer arithmetic.** `/` and `%` truncate toward zero; integer
  results outside int8 raise.
- **Numeric arithmetic.** Exact, with Postgres's result scales: `/`
  follows select_div_scale, rounding is half away from zero
  (`core/numeric.py`), and float8 → numeric goes through 15 significant
  digits as float8_numeric does.
- **Static types.** Postgres types an expression before it runs:
  - `round(integer)` resolves to `round(double precision)`;
  - CASE, COALESCE, GREATEST and LEAST take a common type over every
    branch, so `COALESCE(numeric, float8)` is float8 even when the numeric
    is not NULL;
  - a literal that cannot take the common type fails even in a branch
    never reached.

  Nodes therefore carry a static kind (from literals, casts, functions,
  operators and, when supplied, column types), and branches are promoted
  to the common one.
- **Constant folding.** Postgres folds constant subexpressions while
  planning, and its rules decide which ones it reaches:
  - `CASE WHEN x THEN 1/0 END` raises;
  - `FALSE AND 1/0 = 1` does not;
  - `i + NULL/0` is NULL.

  The compiler folds by the same rules (eval_const_expressions), so errors
  arise where Postgres's do.
- **LIKE.** Postgres raises on a trailing escape only when matching
  reaches it, so `'' LIKE '\'` is false. The matcher is MatchText,
  ported statement for statement; a regular expression gets this wrong.
- **Text ordering.** It depends on the collation, which Python does not
  have. Ordering comparisons of text, and min/max over text, require
  `COLLATE "C"` (code-point order); without it they raise rather than
  guess.

**What was wrong in the oracle itself.** A single-row `VALUES` is pulled
up into the query by Postgres's planner. Its values become constants, and
the whole expression folds with them, which a view's rows never do. The
oracle reads one row of a two-row `VALUES`.

## Consequences

- The supported subset is in `planner/sqlexpr.py`'s docstring; the rest is
  refused by name: subqueries, window functions, `now()`, ILIKE, arrays,
  locale collations.
- **Not detected:** overflow of an int2 or int4 column, which Postgres
  raises at 2^15 and 2^31. Runtime values do not say which integer type
  they came from.
- **Without column types,** a value's runtime type stands in for its
  column's static type. The difference is visible only when float8
  columns meet numeric literals in CASE or COALESCE; the column types are
  passed when the catalog is available.
