# system\_distributed.snapshot\_attempts

## Purpose

Lists the attempts of each cluster snapshot tag. Every round of snapshot RPCs the
topology coordinator sends is one attempt, and the replicas write their catalog rows
and bucket references under `{tag}/{attempt}`. The coordinator inserts the row before
the round's RPCs and sets `committed` when the round seals the tag. Attempts that are
not committed are residue of failed or superseded rounds; the coordinator collects
them after the commit, and cluster clearsnapshot collects whatever is left.
See "Attempts" in [object_storage.md](object_storage.md) for the protocol.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `tag` | text (partition key) | User tag of the snapshot |
| `attempt` | uuid (clustering key) | Attempt id; rows and references live under `{tag}/{attempt}` |
| `committed` | boolean | True once the attempt sealed the tag |
