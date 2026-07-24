# INSERT INTO ... SELECT and CREATE TABLE ... AS SELECT — implementation note

## Status
Implemented, built (dev mode), and tested. Covered by
`test/cqlpy/test_insert_select.py` (13 tests) and
`test/cqlpy/test_create_table_as_select.py` (6 tests); the full cqlpy suite
shows no regressions. User docs: `docs/cql/cql-extensions.md`.

## Features
- `INSERT INTO <target> [(cols...)] SELECT ... FROM <source> [USING TIMESTAMP/TTL]`
  — copies the inner SELECT's result set into the target table. Distributed,
  paged, and (for full-ring scans) range-parallel. NOT atomic / NOT idempotent
  across coordinator failure.
- `CREATE TABLE [IF NOT EXISTS] <target> (PRIMARY KEY (...)) AS SELECT ...`
  — an alias for `CREATE TABLE` (columns/types inferred from the SELECT, primary
  key given explicitly) followed by `INSERT INTO <target> SELECT ...`.

## Files
- `cql3/Cql.g` — grammar: `insertStatement` extended with a SELECT source;
  `createTableStatement` extended with a trailing `AS SELECT` (single-token
  K_AS lookahead, no backtracking DFA).
- `cql3/statements/raw/insert_select_statement.{hh,cc}` — parse + prepare/validate.
- `cql3/statements/insert_select_statement.{hh,cc}` — prepared distributed exec.
- `cql3/statements/raw/create_table_select_statement.{hh,cc}` — CTAS parse:
  infer columns from the SELECT, build the CREATE TABLE, validate the PK.
- `cql3/statements/create_table_select_statement.{hh,cc}` — CTAS exec: create
  the table (group0) then run the insert-select.
- `cql3/statements/select_statement.{hh,cc}` — `can_be_split_into_token_ranges`
  + `execute_paged_for_token_range` (the range-restricted read seam).
- `cql3/statements/create_table_statement.hh` — `partition_key_columns()` /
  `clustering_key_columns()` accessors on `raw_statement`, reused by CTAS.
- `configure.py`, `cql3/CMakeLists.txt` — new .cc files wired in.

## Design

### INSERT ... SELECT
1. Grammar (`insertStatement`): the optional column list is factored so the
   parser deterministically branches VALUES vs SELECT; a no-column-list
   `INSERT INTO t SELECT ...` form is also accepted. Both build
   `raw::insert_select_statement`, which derives from `raw::modification_statement`
   only to fit the grammar return type. The inner SELECT's own optional
   `USING TIMEOUT`/`SERVICE LEVEL` clause is gated with a syntactic predicate
   (`(K_USING (K_TIMEOUT | serviceLevel)) =>`) so a trailing `USING TIMESTAMP`/
   `TTL` is left for the INSERT.

2. Prepare (`raw/insert_select_statement.cc`): prepares the inner SELECT;
   resolves target columns (explicit list positionally, else by name); records
   each mapping entry's source result-set column name (positional remap reads by
   the SELECTed column, not the target name); validates per-column type
   compatibility; rejects duplicate target columns; requires every target PK/CK
   column to be covered. `prepare_internal()` throws — blocks use inside BATCH.
   Forwards `prepare_keyspace` to the inner SELECT so unqualified names resolve
   under a `USE` keyspace.

3. Execute (`insert_select_statement.cc`): the write timestamp/TTL are resolved
   once. If the source is a splittable full-ring scan, the token ring is split
   into N (~ shards*4) disjoint sub-ranges processed concurrently under a bound
   (`max_concurrent_ranges`); otherwise a single full-ring pass runs. Each
   sub-range drives the prepared SELECT (restricted to its token range via
   `execute_paged_for_token_range`) with native paging, converts each page to
   target mutations, and applies them via `storage_proxy::mutate`. Read memory is
   bounded to one page per in-flight range; writes are batched.

### Range-parallel correctness
Splitting only happens when each row is read by exactly one sub-range and no
cross-range operation is involved — `can_be_split_into_token_ranges()` requires:
no aggregation/GROUP BY, no LIMIT/PER PARTITION LIMIT, no post-query ordering, no
secondary indexing, and a full-ring partition range. The sub-ranges are
half-open `[starting_at(b_i), starting_at(b_{i+1}))` covering `[min, max]`, so
their union is the whole ring and they are pairwise disjoint. Seastar shards are
single-threaded, so the concurrent per-range coroutines interleave cooperatively
— sharing `query_state` and the copied-row counter across them is race-free.

### CREATE TABLE ... AS SELECT
The grammar parses the normal create-table body (which already accepts
`(PRIMARY KEY (...))`), then a single-token `K_AS` lookahead switches to CTAS.
`raw::create_table_select_statement::prepare()` prepares the inner SELECT to read
its result columns/types, validates that every PK column is selected, builds the
equivalent `create_table_statement` (`data_type -> cql3_type::raw::from(...)`),
and produces a `create_table_select_statement`. The latter is a
`schema_altering_statement` (so the framework hands it a group0 guard); its
`execute()` creates the table, then — with `IF NOT EXISTS` honored as a whole-
statement no-op when the table pre-exists — builds and runs an
`insert_select_statement` against the freshly created target.

## Future work
- Phase 3 (read locality): push each sub-range to the node that owns it for
  node-local reads (MV-build/repair model). The per-range pipeline is unchanged
  by that move — only where the SELECT executes changes.
