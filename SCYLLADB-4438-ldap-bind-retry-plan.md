# SCYLLADB-4438: `LDAPSocketOpenError` flakiness in `ldap_test.TestLdap.test_multiple_users_with_modify_role`

## Ticket / symptom

SCYLLADB-4438 (duplicate: SCYLLADB-4439; related: SCYLLADB-3307, DTEST-242) reports that
`ldap_test.TestLdap.test_multiple_users_with_modify_role` fails intermittently with:

```
ldap3.core.exceptions.LDAPSocketOpenError: invalid server address
```

This happens because the per-worker LDAP Docker container occasionally does not yet have a
usable network address when the test's `ldap3.Connection.bind()` call runs — a startup race
between the container coming up and the test harness trying to connect to it.

DTEST-242 previously hardened the same startup race for a different symptom
(`ContainerNotRunningError`, raised by a Docker-SDK container-status check) by adding a bounded
retry loop around that status check. That retry loop does not cover `LDAPSocketOpenError`,
which is raised by `ldap3` itself, inside `bind()`, in a different call frame — after Docker has
already reported the container as running.

## Finding: the failing code does not exist in scylladb/scylladb

An exhaustive search of this repository (scylladb/scylladb) found:

- No `ldap_test.py`, no `TestLdap` class, no `ldap3` import or usage anywhere.
- No `ContainerNotRunningError` or any Docker-SDK-based LDAP container fixture.
- `.gitmodules` only lists seastar, swagger-ui, abseil, scylla-python3, tools/cqlsh, and fmt —
  there is no `scylla-dtest` submodule vendored here.
- This repo's own in-tree LDAP fixture, `test/pylib/ldap_server.py`, is unrelated to the failure:
  it drives native `slapd`/`saslauthd` subprocesses (not Docker), never imports `ldap3`, and
  already has TCP-connect readiness polling (`try_something_backoff()`,
  `test/pylib/ldap_server.py:98-105`). It cannot raise `LDAPSocketOpenError` and is not on the
  failing code path.

The real LDAP dtest (`ldap_test.TestLdap.test_multiple_users_with_modify_role`), its Docker-based
fixture, the DTEST-242 retry loop, and the code that constructs `ldap3.Server`/`Connection` and
calls `.bind()` all live in the separate `scylladb/scylla-dtest` repository, which is not
reachable from a scylladb/scylladb checkout or PR.

**Conclusion: no code change belonging to this ticket can be made in scylladb/scylladb. The fix
must land in scylladb/scylla-dtest.**

## Recommended fix shape (for scylla-dtest)

Whoever picks this up with access to `scylladb/scylla-dtest` should:

- Add a bounded retry around the `ldap3.Server(...)` construction **and** the
  `Connection.bind()` call *together* — not around `bind()` alone — since the server address is
  captured at `Server` construction time, and a stale/invalid address captured there will still
  fail even if only the `bind()` call is retried.
- Bound the retry to a short total time budget, catch only
  `ldap3.core.exceptions.LDAPSocketOpenError` — not a bare `Exception` — so unrelated failures
  still surface immediately, and re-raise after the final attempt. Leave the concrete attempt
  count and sleep interval to whoever implements this in scylla-dtest.
- Explicitly do **not** extend or widen the existing DTEST-242 container-readiness retry loop
  itself: `LDAPSocketOpenError` is raised in a different call frame (inside ldap3's own
  `bind()`), after Docker has already reported the container as running, so it is a distinct
  race window from the one DTEST-242 addressed.

## Testing note

No test can be run or added for this from within scylladb/scylladb: the failing test, its
Docker/ldap3-based fixture, and the DTEST-242 retry loop it should be paired with all live in
scylladb/scylla-dtest, not in this repository.
