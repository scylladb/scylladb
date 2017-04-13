# Tracing

### Motivation
Tracing is a ScyllaDB tool that is meant to help debugging and analysing the internal flows in the server. One example of such a flow is a CQL request processing.

### How to enable?
#### cqlsh
Users of a `cqlsh` may enable Tracing with `TRACING ON|OFF` command:
```
cqlsh> TRACING ON
Now Tracing is enabled
cqlsh> INSERT into keyspace1.standard1  (key, "C0") VALUES (0x12345679, bigintAsBlob(123456));

Tracing session: 227aff60-4f21-11e6-8835-000000000000

 activity                                                                                         | timestamp                  | source    | source_elapsed
--------------------------------------------------------------------------------------------------+----------------------------+-----------+----------------
                                                                               Execute CQL3 query | 2016-07-21 11:57:21.238000 | 127.0.0.2 |              0
                                                                    Parsing a statement [shard 1] | 2016-07-21 11:57:21.238335 | 127.0.0.2 |              1
                                                                 Processing a statement [shard 1] | 2016-07-21 11:57:21.238405 | 127.0.0.2 |             71
 Creating write handler for token: 2309717968349690594 natural: {127.0.0.1} pending: {} [shard 1] | 2016-07-21 11:57:21.238433 | 127.0.0.2 |             99
                                 Creating write handler with live: {127.0.0.1} dead: {} [shard 1] | 2016-07-21 11:57:21.238439 | 127.0.0.2 |            105
                                                       Sending a mutation to /127.0.0.1 [shard 1] | 2016-07-21 11:57:21.238490 | 127.0.0.2 |            156
                                                       Message received from /127.0.0.2 [shard 0] | 2016-07-21 11:57:21.238562 | 127.0.0.1 |             17
                                                    Sending mutation_done to /127.0.0.2 [shard 0] | 2016-07-21 11:57:21.238658 | 127.0.0.1 |            113
                                                              Mutation handling is done [shard 0] | 2016-07-21 11:57:21.238675 | 127.0.0.1 |            130
                                                         Got a response from /127.0.0.1 [shard 1] | 2016-07-21 11:57:21.238950 | 127.0.0.2 |            616
                                                        Mutation successfully completed [shard 1] | 2016-07-21 11:57:21.238958 | 127.0.0.2 |            624
                                                   Done processing - preparing a result [shard 1] | 2016-07-21 11:57:21.238962 | 127.0.0.2 |            628
                                                                                 Request complete | 2016-07-21 11:57:21.238639 | 127.0.0.2 |            639
```
**Note** how `source_elapsed` starts over on `127.0.0.1` when execution gets there.

The raw tracing data looks as follows:
```
cqlsh> select * from system_traces.sessions  where session_id=227aff60-4f21-11e6-8835-000000000000;

 session_id                           | client    | command | coordinator | duration | parameters                                                                                                                                                                                                                       | request            | started_at
--------------------------------------+-----------+---------+-------------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------+---------------------------------
 227aff60-4f21-11e6-8835-000000000000 | 127.0.0.1 |   QUERY |   127.0.0.2 |      639 | {'consistency_level': 'ONE', 'page_size': '100', 'query': 'INSERT into keyspace1.standard1  (key, "C0") VALUES (0x12345679, bigintAsBlob(123456));', 'serial_consistency_level': 'SERIAL', 'user_timestamp': '1469091441238107'} | Execute CQL3 query | 2016-07-21 08:57:21.238000+0000

(1 rows)
cqlsh> select * from system_traces.events  where session_id=227aff60-4f21-11e6-8835-000000000000;

 session_id                           | event_id                             | activity                                                                               | source    | source_elapsed | thread
--------------------------------------+--------------------------------------+----------------------------------------------------------------------------------------+-----------+----------------+--------
 227aff60-4f21-11e6-8835-000000000000 | 227b0c74-4f21-11e6-8835-000000000000 |                                                                    Parsing a statement | 127.0.0.2 |              1 | shard 1
 227aff60-4f21-11e6-8835-000000000000 | 227b0f34-4f21-11e6-8835-000000000000 |                                                                 Processing a statement | 127.0.0.2 |             71 | shard 1
 227aff60-4f21-11e6-8835-000000000000 | 227b1047-4f21-11e6-8835-000000000000 | Creating write handler for token: 2309717968349690594 natural: {127.0.0.1} pending: {} | 127.0.0.2 |             99 | shard 1
 227aff60-4f21-11e6-8835-000000000000 | 227b1087-4f21-11e6-8835-000000000000 |                                 Creating write handler with live: {127.0.0.1} dead: {} | 127.0.0.2 |            105 | shard 1
 227aff60-4f21-11e6-8835-000000000000 | 227b1284-4f21-11e6-8835-000000000000 |                                                       Sending a mutation to /127.0.0.1 | 127.0.0.2 |            156 | shard 1
 227aff60-4f21-11e6-8835-000000000000 | 227b1559-4f21-11e6-bf08-000000000000 |                                                       Message received from /127.0.0.2 | 127.0.0.1 |             17 | shard 0
 227aff60-4f21-11e6-8835-000000000000 | 227b1915-4f21-11e6-bf08-000000000000 |                                                    Sending mutation_done to /127.0.0.2 | 127.0.0.1 |            113 | shard 0
 227aff60-4f21-11e6-8835-000000000000 | 227b19bd-4f21-11e6-bf08-000000000000 |                                                              Mutation handling is done | 127.0.0.1 |            130 | shard 0
 227aff60-4f21-11e6-8835-000000000000 | 227b247e-4f21-11e6-8835-000000000000 |                                                         Got a response from /127.0.0.1 | 127.0.0.2 |            616 | shard 1
 227aff60-4f21-11e6-8835-000000000000 | 227b24ca-4f21-11e6-8835-000000000000 |                                                        Mutation successfully completed | 127.0.0.2 |            624 | shard 1
 227aff60-4f21-11e6-8835-000000000000 | 227b24f2-4f21-11e6-8835-000000000000 |                                                   Done processing - preparing a result | 127.0.0.2 |            628 | shard 1

(11 rows)
cqlsh>
```
#### Probabilistic tracing
Tracing implies a significant performance penalty on a cluster when it's enabled. Therefore if we want to enable tracing for some on going workload we don't want to enable it for every request but rather for some (small) portion of them. This may be achieved using the so called `probabilistic tracing`, which would randomly choose a request to be traced with some defined probability.

For instance, if we want to trace 0.01% or all queries in the cluster we shell set a `probabilistic tracing` with the probability 0.0001:

```
$ nodetool settraceprobability 0.0001
```

### How traces are stored?
Traces are stored in a `system_traces` keyspace for 24 hours, which consists of 2 tables with replication factor of 2:
```
CREATE TABLE system_traces.events (
    session_id uuid,
    event_id timeuuid,
    activity text,
    source inet,
    source_elapsed int,
    thread text,
    PRIMARY KEY (session_id, event_id)
)

CREATE TABLE system_traces.sessions (
    session_id uuid PRIMARY KEY,
    client inet,
    command text,
    coordinator inet,
    duration int,
    parameters map<text, text>,
    request text,
    started_at timestamp
)
```
Traces are created in a context of a `tracing session`. For instance, if we trace an `INSERT` CQL command, a tracing session with a unique ID (`session_id` column in the tables above) will be created and all trace points hit during the execution will be stored in a context of this session. And this defines the format in which tracing data is stored:
* `sessions` table contains a single row for each tracing session
* `events` table contains a single row for each trace point.

If we need trace points for a specific session we may query `events` table for this session's ID (see examples above).

##### `events` columns descripton
`events` columns are quite straight forward:

* `session_id`: ID of a session this trace
* `event_id`: ID of this specific trace entry
* `activity`: a trace message
* `source`: address of a Node where the trace entry has been created
* `source_elapsed`: a number of microseconds passed since the beginning of the tracing session on a specific Node (see examples above)
* `thread`: currently this contains a number of a shard on which this trace point has been taken


##### `sessions` columns description

* `session_id`: ID of this tracing session
* `command`: currently this may only have a "QUERY" value
* `client`: address of a Client that has sent this query
* `coordinator`: address of a coordinator that received this query from a Client
* `duration`: the total duration of this tracing session
* `parameters`: this map contains string pairs that describe the query which may include:
   * query string
   * consistency level
   * etc.
* `request`: a short string describing the current query, like "Execute CQL3 query"
* `started_at`: is a timestamp taken when tracing session has began

### Slow queries logging
#### The motivation
Many times in real life installations one of the most important parameters of the system is the longest response time. Naturally, the shorter it is - the better. Therefore capturing the request that take a long time and understanding why it took it so long is a very critical and challenging task.

#### The solution
The "slow query logging" is a ScyllaDB feature that is going to greatly ease the debugging related to the long requests.
When enabled it would record the queries with the handling time above the specified threshold. As a result there will be created a new record in a `system_traces.node_slow_log` table. All tracing records created in a context of this query on a Coordinator Node will be written as well. In addition, if handling on some replica takes too long, its traces are going to be stored too.

Thereby, when we detect a slow query we are going to get a valuable tracing data that would greatly help us to understand what was the query and why it took so long to complete.

#### How to enable and configure
By default slow query logging is disabled.
There is a REST API that allows configuring and querying the current configuration of this feature.
To set the parameters run:
`curl -X POST --header "Content-Type: application/json" --header "Accept: application/json" "http://<Node's address>:10000/storage_service/slow_query?enable=<true|false>&ttl=<in seconds>&threshold=<threshold in microseconds>"`

e.g. in order to disable the feature on a Node with an address `127.0.0.1`, set the `ttl` to 8600s and a threshold to 10000us:

`curl -X POST --header "Content-Type: application/json" --header "Accept: application/json" "http://127.0.0.1:10000/storage_service/slow_query?enable=false&ttl=8600&threshold=10000"`

To get the current configuration run:
`curl -X GET --header "Content-Type: application/json" --header "Accept: application/json" "http://<Node's
address>:10000/storage_service/slow_query"`

e.g. after the POST command above the query result will look as below:
```
$ curl -X GET --header "Content-Type: application/json" --header "Accept: application/json" "http://127.0.0.1:10000/storage_service/slow_query"
{"threshold": 10000, "enable": false, "ttl": 8600}
$
```

#### node_slow_log table
##### Schema
```
CREATE TABLE system_traces.node_slow_log (
    start_time timeuuid,
    node_ip inet,
    shard int,
    command text,
    date timestamp,
    duration int,
    parameters map<text, text>,
    session_id uuid,
    source_ip inet,
    table_names set<text>,
    username text,
    PRIMARY KEY (start_time, node_ip, shard)
)
```

##### Columns description
* `start_time` and `date`: time when the query has began
* `node_ip`: Address of a Coordinator Node
* `shard`: shard ID on a Coordinator, where the query has been handled
* `command`: the query command, e.g. `select * from my_ks.my_cf`
* `duration`: the duration of a query handling in microseconds
* `parameters`: query parameters like a `parameters` column in a system_traces.sessions table
* `session_id`: the corresponding Tracing session ID
* `source_ip`: Address of a Client that sent this query
* `table_names`: a list of tables used for this query, where applicable
* `username`: a user name used for authentication with this query

### How to get query traces?
Each query tracing session gets a unique ID - `session_id`, which serves as a partition key for `system_traces.sessions` and `system_traces.events` tables.

Once one has this key the rest is trivial: `SELECT * FROM system_traces.events WHERE session_id=<value>`.

If we invoke tracing from the `cqlsh` using a `TRACING ON` command the `session_id` is printed with traces themselves and one can easily grab it
and get the same traces later if needed using the query above.

If tracing was enabled using probabilistic tracing or with slow query log features (both described above) then one should use 
time based index tables: `system_traces.sessions_time_idx` and `system_traces.node_slow_log_time_idx`.

These indexes allow getting `session_id`s of all tracing sessions that were recorded in a specific time period.

For instance, to get `session_id`s of all tracing sessions that started between `2016-09-07 16:56:30-0000` and `2016-09-07 16:58:00-0000` one may run the following query:

```
SELECT session_id from system_traces.sessions_time_idx where minutes in ('2016-09-07 16:56:00-0000','2016-09-07 16:57:00-0000','2016-09-07 16:58:00-0000') and started_at > '2016-09-07 16:56:30-0000'
```

* `system_traces.node_slow_log_time_idx` contains entries that correspond to queries traced only in the context of slow query logging.
* `system_traces.sessions_time_idx` contains entries for all traced queries.

#### Index tables' schemas are as follows:

```
CREATE TABLE system_traces.sessions_time_idx (
    minute timestamp,
    started_at timestamp,
    session_id uuid,
    PRIMARY KEY (minute, started_at, session_id))

CREATE TABLE system_traces.node_slow_log_time_idx (
    minute timestamp,
    started_at timestamp,
    session_id uuid,
    start_time timeuuid,
    node_ip inet,
    shard int,
    PRIMARY KEY (minute, started_at, session_id))
```

As you may notice each `system_traces.node_slow_log_time_idx` record contains `system_traces.sessions`, `system_traces.events` and `system_traces.node_slow_log` keys allowing to get the corresponding entries from each of these tables.
