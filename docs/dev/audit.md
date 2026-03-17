# Prototype design: auditing all keyspaces and per-role auditing

## Summary

Extend the existing `scylla.yaml`-driven audit subsystem with two focused capabilities:

1. allow auditing **all keyspaces** without enumerating them one by one
2. allow auditing only a configured set of **roles**

The prototype should stay close to the current implementation in `audit/`:

- keep the existing backends (`table`, `syslog`, or both)
- keep the existing category / keyspace / table filters
- preserve live updates for audit configuration
- avoid any schema change to `audit.audit_log`

This is intentionally a small extension of the current auditing model, not a redesign around new CQL statements such as `CREATE AUDIT`.

## Motivation

Today Scylla exposes three main audit selectors:

- `audit_categories`
- `audit_tables`
- `audit_keyspaces`

This leaves two operational gaps:

1. **Auditing all keyspaces is cumbersome.**
   Large installations may create keyspaces dynamically, or manage many tenant keyspaces. Requiring operators to keep
   `audit_keyspaces` synchronized with the full keyspace list is error-prone and defeats the point of cluster-wide auditing.
2. **Auditing is all-or-nothing with respect to users.**
   Once a category/keyspace/table combination matches, any authenticated user generating that traffic is audited.
   Operators want to narrow the scope to specific tenants, service accounts, or privileged roles.

These two additions also work well together: "audit all keyspaces, but only for selected roles" is a practical way to reduce
both audit volume and performance impact.

## Goals

- Add a way to express "all keyspaces" in the current configuration model.
- Add a new role filter that limits auditing to selected roles.
- Preserve backwards compatibility for existing configurations.
- Keep the evaluation cheap on the request path.
- Support live configuration updates, consistent with the existing audit options.

## Non-goals

- Introducing `CREATE AUDIT`, `ALTER AUDIT`, or other new CQL syntax.
- Adding per-role audit destinations.
- Adding different categories per role.
- Expanding role matching through the full granted-role graph in the prototype.
- Changing the on-disk audit table schema.

## Current behavior

At the moment, audit logging is controlled by:

- `audit`
- `audit_categories`
- `audit_tables`
- `audit_keyspaces`

The current decision rule in `audit::should_log()` is effectively:

```text
category matches
&& (
    keyspace is listed in audit_keyspaces
    || table is listed in audit_tables
    || category in {AUTH, ADMIN, DCL}
)
```

Observations:

- `AUTH`, `ADMIN`, and `DCL` are already global once their category is enabled.
- `DDL`, `DML`, and `QUERY` need a matching keyspace or table.
- An empty `audit_keyspaces` means "audit no keyspaces", not "audit every keyspace".
- There is no role-based filter; the authenticated user is recorded in the log but is not part of the decision.
- The exact implementation to preserve is in `audit/audit.cc` (`should_log()`, `inspect()`, and `inspect_login()`).

## Proposed configuration

### 1. Reuse `audit_keyspaces` for the all-keyspaces mode

Reserve `*` inside `audit_keyspaces` as a wildcard meaning "all keyspaces".

Examples:

```yaml
# Audit all keyspaces for matching categories
audit_keyspaces: "*"

# Audit all keyspaces (`audit_tables` is redundant here, but still legal)
audit_keyspaces: "*"
audit_tables: "ks1.tbl1,ks2.tbl2"
```

Semantics:

- `audit_keyspaces: ""` keeps the existing meaning: no keyspace-wide auditing.
- `audit_keyspaces: "*"` means every keyspace matches.
- `audit_keyspaces: "*,ks1,ks2"` is accepted but equivalent to `*` alone.
- the parser should normalize any list containing `*` into `audit_all_keyspaces = true` and an empty explicit keyspace set
- when `*` is combined with explicit keyspaces, Scylla should log an informational message explaining that the explicit keyspaces are redundant and ignored
- only the exact token `*` is special; tokens such as `**` or `ks*` should be rejected as invalid configuration, with an error that points users to `audit_keyspaces: "*"` as the valid form
- If `*` is present, `audit_tables` becomes redundant but remains legal.

### 2. Add `audit_roles`

Introduce a new live-update configuration option:

```yaml
audit_roles: "alice,bob,service_api"
```

Semantics:

- empty `audit_roles` means **no role filtering**, preserving today's behavior
- non-empty `audit_roles` means audit only requests whose effective logged username matches one of the configured roles
- matching is exact, using the same role name that is already written to the audit record's `username` column / syslog field
- the prototype should compare against the post-authentication role name exactly as exposed by the session and audit log, with no additional case folding or role-graph expansion

Examples:

```yaml
# Audit all roles in a single keyspace (current behavior, made explicit)
audit_keyspaces: "ks1"
audit_roles: ""

# Audit two roles across all keyspaces
audit_keyspaces: "*"
audit_roles: "alice,bob"

# Audit a service role, but only for selected tables
audit_tables: "ks1.orders,ks1.payments"
audit_roles: "billing_service"
```

## Decision rule after the change

After the prototype, the rule becomes:

```text
category matches
&& role matches
&& (
    category in {AUTH, ADMIN, DCL}
    || audit_all_keyspaces
    || keyspace is listed in audit_keyspaces
    || table is listed in audit_tables
)
```

Where:

- `role matches` is always true when `audit_roles` is empty
- `audit_all_keyspaces` is true when `*` appears in `audit_keyspaces`

For login auditing, the rule is simply:

```text
AUTH category enabled && role matches(login username)
```

## Implementation details

### Configuration parsing

Add a new config entry:

- `db::config::audit_roles`

It should mirror the existing audit selectors:

- type: `named_value<sstring>`
- liveness: `LiveUpdate`
- default: empty string

Parsing changes:

- keep `parse_audit_tables()` as-is
- extend `parse_audit_keyspaces()` so it can detect `*`
- add `parse_audit_roles()` that returns a set of role names
- reject malformed wildcard tokens, because only the exact token `*` should trigger all-keyspaces mode

To avoid re-parsing on every request, the `audit::audit` service should store:

```c++
bool _audit_all_keyspaces;
std::set<sstring> _audited_keyspaces;
std::set<sstring> _audited_roles;
```

Using a dedicated boolean is preferable to storing `*` inside `_audited_keyspaces`, because it avoids ambiguity and keeps the
hot-path check straightforward.

### Audit object changes

The current `audit_info` already carries:

- category
- keyspace
- table
- query text

The username is available separately from `service::query_state` and is already passed to storage helpers when an entry is written.
For the prototype there is no need to duplicate the username into `audit_info`.

Instead:

- change `should_log()` to take the effective username as an additional input
- change `should_log_login()` to check the username against `audit_roles`
- keep the storage helpers unchanged, because they already persist the username
- update the existing internal call sites in `inspect()` and `inspect_login()` to pass the username through

One possible interface shape is:

```c++
bool should_log(std::string_view username, const audit_info* info) const;
bool should_log_login(std::string_view username) const;
```

### Role semantics

For the prototype, "role" means the role name already associated with the current client session:

- successful authenticated sessions use the session's user name
- failed login events use the login name from the authentication attempt

This keeps the feature easy to explain and aligns the filter with what users already see in audit output.

The prototype should **not** try to expand inherited roles. If a user logs in as `alice` and inherits permissions from another role,
the audit filter still matches `alice`. This keeps the behavior deterministic and avoids expensive role graph lookups on the request path.

### Keyspace semantics

`audit_keyspaces: "*"` should affect any statement whose `audit_info` carries a keyspace name.

Important consequences:

- it makes `DDL` / `DML` / `QUERY` auditing effectively cluster-wide
- it does not change the existing global handling of `AUTH`, `ADMIN`, and `DCL`
- statements that naturally have no keyspace name continue to depend on their category-specific behavior

No extra schema or metadata scan is required: the request already carries the keyspace information needed for the decision.

## Backwards compatibility

This design keeps existing behavior intact:

- existing clusters that do not set `audit_roles` continue to audit all roles
- existing clusters that leave `audit_keyspaces` empty continue to audit no keyspaces
- existing explicit keyspace/table lists keep their current meaning

The only newly reserved value is `*` in `audit_keyspaces`.

## Operational considerations

### Performance and volume

`audit_keyspaces: "*"` can significantly increase audit volume, especially with `QUERY` and `DML`.

The intended mitigation is to combine it with:

- a narrow `audit_categories`
- a narrow `audit_roles`

That combination gives operators a simple and cheap filter model:

- first by category
- then by role
- then by keyspace/table scope

### Live updates

`audit_roles` should follow the same live-update behavior as the current audit filters.

Changing:

- `audit_roles`
- `audit_keyspaces`
- `audit_tables`
- `audit_categories`

should update the in-memory selectors on all shards without restarting the node.

### Prototype limitation

Because matching is done against the session's logged-in role name, `audit_roles` cannot express "audit everyone who inherits role X".
Operators must list the concrete login roles they want to audit. This is a deliberate trade-off in the prototype to keep matching cheap
and avoid role graph lookups on every audited request.

Example: if `alice` inherits permissions from `admin_role`, configuring `audit_roles: "admin_role"` would not audit requests from
`alice`; to audit those requests, `alice` itself must be listed.

### Audit table schema

No schema change is needed. The audit table already includes `username`, which is sufficient for both storage and later analysis.

## Testing plan

The prototype should extend existing audit coverage rather than introduce a separate test framework.

### Parser / unit coverage

Add focused tests for:

- empty `audit_roles`
- specific `audit_roles`
- `audit_keyspaces: "*"`
- mixed `audit_keyspaces: "*,ks1"`
- empty or whitespace-only keyspace lists such as `",,,"` or `"  "`
- malformed comma-separated input and malformed wildcard tokens such as `**` or `ks*`, including verification of the resulting error messages

### Behavioral coverage

Extend the existing audit tests in `test/cluster/dtest/audit_test.py` with scenarios such as:

1. `audit_keyspaces: "*"` audits statements in multiple keyspaces without listing them explicitly
2. `audit_roles: "alice"` logs requests from `alice` but not from `bob`
3. `audit_keyspaces: "*"` + `audit_roles: "alice"` only logs `alice`'s traffic cluster-wide
4. login auditing respects `audit_roles`
5. live-updating `audit_roles` changes behavior without restart

## Future evolution

This prototype is deliberately small, but it fits a broader audit-spec design if we decide to revisit that later.

In a future CQL-driven design, these two additions map naturally to triggers such as:

- `TRIGGER KEYSPACE IN *`
- `TRIGGER ROLE IN (...)`

That means the prototype is not throwaway work: it improves the current operational model immediately while keeping a clean path
toward richer audit objects in the future.
