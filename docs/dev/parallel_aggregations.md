# Parallel aggregations

This document describes the design of a mechanism that parallelizes execution of aggregation queries.

## Architecture overview

New level of coordination was added. Node called super-coordinator splits aggregation query into sub-queries and distributes them across some group of coordinators. Super-coordinator is also responsible for merging results.

## Detection

To develop a mechanism for speeding up `count(*)` queries, there was a need to detect which queries have a `count(*)` selector. Due to this pull request being a proof of concept, detection was realized rather poorly. It is only allows catching the simplest cases of `count(*)` queries (with only one selector and no column name specified).

## Delegation

After detecting that a query is a `count(*)` it should be split into sub-queries and sent to another coordinators. Splitting part wasn't that difficult, it has been achieved by limiting original query's partition ranges. Sending modified query to another node was much harder. The easiest scenario would be to send whole `cql3::statements::select_statement`. Unfortunately `cql3::statements::select_statement` can't be [de]serialized, so sending it was out of the question. Even more unfortunately, some non-[de]serializable members of `cql3::statements::select_statement` are required to start the execution process of this statement. Finally, I have decided to send a `query::read_command` paired with required [de]serializable members. Objects, that cannot be [de]serialized (such as query's selector) are mocked on the receiving end.

## Distributing

When a super-coordinator receives a `count(*)` query, it splits it into sub-queries. It does so, by splitting original query's partition ranges into list of vnodes, grouping them by their owner and creating sub-queries with partition ranges set to successive results of such grouping. After creation, each sub-query is sent to the owner of its partition ranges. Owner dispatches received sub-query to all of its shards. Shards slice partition ranges of the received sub-query, so that they will only query data that is owned by them. Each shard becomes a coordinator and executes so prepared sub-query.

