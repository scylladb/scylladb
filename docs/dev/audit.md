# Introduction

Similar to the approach described in CASSANDRA-14471, we add the
concept of an audit specification.  An audit has a target (syslog or a
table) and a set of events/actions that it wants recorded.  We
introduce new CQL syntax for Scylla users to describe and manipulate
audit specifications.

Prior art:
- Microsoft SQL Server [audit
  description](https://docs.microsoft.com/en-us/sql/relational-databases/security/auditing/sql-server-audit-database-engine?view=sql-server-ver15)
- pgAudit [docs](https://github.com/pgaudit/pgaudit/blob/master/README.md)
- MySQL audit_log docs in
  [MySQL](https://dev.mysql.com/doc/refman/8.0/en/audit-log.html) and
  [Azure](https://docs.microsoft.com/en-us/azure/mysql/concepts-audit-logs)
- DynamoDB can [use CloudTrail](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/logging-using-cloudtrail.html) to log all events

# CQL extensions

## Create an audit

```cql
CREATE AUDIT [IF NOT EXISTS] audit-name WITH TARGET { SYSLOG | table-name }
[ AND TRIGGER KEYSPACE IN (ks1, ks2, ks3) ]
[ AND TRIGGER TABLE IN (tbl1, tbl2, tbl3) ]
[ AND TRIGGER ROLE IN (usr1, usr2, usr3) ]
[ AND TRIGGER CATEGORY IN (cat1, cat2, cat3) ]
;
```

From this point on, every database event that matches all present
triggers will be recorded in the target.  When the target is a table,
it behaves like the [current
design](https://docs.scylladb.com/operating-scylla/security/auditing/#table-storage).

The audit name must be different from all other audits, unless IF NOT
EXISTS precedes it, in which case the existing audit must be identical
to the new definition.  Case sensitivity and length limit are the same
as for table names.

A trigger kind (ie, `KEYSPACE`, `TABLE`, `ROLE`, or `CATEGORY`) can be
specified at most once.

## Show an audit

```cql
DESCRIBE AUDIT [audit-name ...];
```

Prints definitions of all audits named herein.  If no names are
provided, prints all audits.

## Delete an audit

```cql
DROP AUDIT audit-name;
```

Stops logging events specified by this audit.  Doesn't impact the
already logged events.  If the target is a table, it remains as it is.

## Alter an audit

```cql
ALTER AUDIT audit-name WITH {same syntax as CREATE}
```

Any trigger provided will be updated (or newly created, if previously
absent).  To drop a trigger, use `IN *`.

## Permissions

Only superusers can modify audits or turn them on and off.

Only superusers can read tables that are audit targets; no user can
modify them.  Only superusers can drop tables that are audit targets,
after the audit itself is dropped.  If a superuser doesn't drop a
target table, it remains in existence indefinitely.

# Implementation

## Efficient trigger evaluation

```c++
namespace audit {

/// Stores triggers from an AUDIT statement.
class triggers {
    // Use trie structures for speedy string lookup.
    optional<trie> _ks_trigger, _tbl_trigger, _usr_trigger;

    // A logical-AND filter.
    optional<unsigned> _cat_trigger;

public:
    /// True iff every non-null trigger matches the corresponding ainf element.
    bool should_audit(const audit_info& ainf);
};

} // namespace audit
```

To prevent modification of target tables, `audit::inspect()` will
check the statement and throw if it is disallowed, similar to what
`check_access()` currently does.

## Persisting audit definitions

Obviously, an audit definition must survive a server restart and stay
consistent among all nodes in a cluster.  We'll accomplish both by
storing audits in a system table.  They will be cached in memory the
same way `permissions_cache` caches table contents in `permission_set`
objects resident in memory.
