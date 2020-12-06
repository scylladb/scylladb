# Scylla Metrics
Scylla exposes dozens of different metrics which are valuable for
understanding the performance of a node, and for diagnosing performance
problems when those occur. Among other things, you can see counts of requests,
activity of disks, cpus and network, memory usage of different types,
activity in different individual tables, and many many more metrics.

Scylla's metrics are implemented using Seastar's metrics infrastructure.
Scylla's code updates metrics continuously in memory variables, and then
exposes them through an HTTP request, http://scyllanode:9180/metrics.
The response to this request is a text file listing the metrics and their
current values at the time of the query. This protocol, and the format of
the response was defined by the Prometheus metric collection system and
is described in detail here: https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md

> Note that the REST API in port 9180 is only devoted to publishing metrics.
> Scylla also has a separate and more powerful REST API on port 10000.

This very simple REST API is useful for quick scripting and development work,
but in Scylla production you'd usually want to collect metrics from multiple
Scylla nodes, collect a history of each metric over time, and provide a
graphical UI for viewing graphs of these histories. For this purpose,
we provide the separate scylla-grafana-monitoring project - see
https://github.com/scylladb/scylla-grafana-monitoring on how to install and
use it. The scylla-grafana-monitoring project allows you to continuously
collect metrics from several Scylla nodes into a Prometheus metric-collection
server, and then to visualize these metrics using Grafana and a web browser.
Prometheus and Grafana will be described in separate sections below.

## Metric labels: shard, instance and type
Different Scylla nodes will have different values for each metric (e.g.,
`scylla_cql_reads`, the total number of CQL read requests). Moreover, Scylla
is sharded, meaning that inside each node each core works on its own data
and keeps its own separate metrics. So in the metrics output, each metric
identifier contains, beyond the metric's name, also an additional label to
qualify which shard this metric comes from. For example:
```
scylla_cql_reads{shard="0",type="derive"} 20
```
In this example, this measurement comes from shard 0 (the first shard) of
the node which returned this metric.

When Prometheus collects measurements from multiple nodes, it further adds
an "instance" label to each measurement to remember from which node this
measurement came. The "instance" label has the form `ip_address:port` - see
https://prometheus.io/docs/concepts/jobs_instances/ for more information.
Note again that the instance label is not present in the metrics exposed by
Scylla (`http://scyllanode:9180/metrics`) but added later by Prometheus.

Saving instance and shard ids on each metric is what allows a single
Prometheus server to collect metrics from many Scylla nodes and their shards.
The visualization tool (such as Prometheus itself, or Grafana) can then show
the metrics of different nodes and shards separately, or to calculate and
display various sums - e.g., the sum on all shards of each node, or the total
sum of all shards and all nodes.

The "type" label should be ignored - it appears for historic reasons
(it was used by collectd) and is planned to be removed in the future.

## Additional metric labels
In some cases, we have several metrics which measure the same thing but for
different cases. For example, Scylla has about a dozen _scheduling groups_
(see isolation.md), and we would like to get some statistics - e.g. the
scheduler queue length - separately for each of these scheduling groups.

One option is to have a dozen different metrics with different names, e.g.,
`scylla_scheduler_queue_length_main`, `scylla_scheduler_queue_length_statement`
for the two scheduling groups called "main" and "statement".

However, there is a second option - which we chose in this case. The second
option is to have just one metric name, and qualify it by a **label** with
a value. In this case, we have one metric name `scylla_scheduler_queue_length`,
and metrics on different scheduling groups differ by the `group` label:
`scylla_scheduler_queue_length{group="main"}` and
`scylla_scheduler_queue_length{group="statement"}`.

Each metric reported by Scylla often has multiple labels, e.g.,
```
scylla_scheduler_queue_length{group="main",shard="0",type="gauge"} 0.000000
```
This metric has the `group` label, saying to which scheduling group this
measurement pertains, and also `shard` and `type` labels which we described
in the previous section.

## Per-table metrics
Most of Scylla's metrics are global (in each shard). Scylla also supports
per-table metrics, which are maintained separately for each table in the
database.

On a deployment with a large number of tables, this can result in a very
large number of metrics at each time, and overwhelm Scylla's HTTP
server and/or the Prometheus server collecting these metrics. For this
reason, the per-table metrics are currently **disabled** by default:
The per-table metrics are defined in the `table::set_metrics()` function,
and only added when the `enable_keyspace_column_family_metrics` flag is
enabled (and it is disabled by default).

To enable this flag and the per-table metrics, you can pass the parameters
`--enable-keyspace-column-family-metrics 1` in the Scylla command line, or
set this parameter in Scylla's configuration file. 

We are planning to rethink this approach in the future. In particular,
it's not great that we currently need to restart Scylla to make these
metrics available. Scylla already maintains these per-table metrics in
per-table memory variables, and we just need a way to optionally expose
them through the HTTP request.

To tell the metrics of the different tables apart, each metric's identifier
contains the "ks" (*keyspace*) and "cf" (*column family* - the old name
for table) as labels. For example,

```
scylla_column_family_pending_compaction{cf="IndexInfo",ks="system",shard="0",type="gauge"} 0.000000
```

Here we can see the "scylla_column_family_pending_compactions" metric
measured in shard 0 of this node, for the table "IndexInfo" in keyspace
"system".

## Types of metrics
Scylla metrics fall under three types: "counter", "gauge" and "histogram".

Most metrics are of the "counter" type. A counter metric tracks a cumulative
value over objects or events that existed throughout the lifetime of the
node. For example, the "total number of requests processed so far", or
"the total number of bytes written to disk". 

When visualizing counter metrics, it is often useful to look at the
*derivative*, or rate of change, of the number, instead of at the cumulative
number itself. Note that Scylla only provides the cumulative number - the
visualization tool used by the user (such as Grafana mentioned earlier) is
responsible for calculating the rate of change - by taking two measurements
of the cumulative value at two different times, and calculating the difference
of cumulative value divided by the time difference. For example, by
subtracting the "total number of requests" values queried one second apart,
we can show the number of requests handled during that second.

> In some contexts, we call counter metrics "derive" metrics. We do this
> mainly for historic reasons, because our previous focus on the "collectd"
> metric collection daemon - which Scylla still supports but is no longer
> our recommended choice. Collectd has both "derive" and "counter" metrics
> with a subtle difference: Both indicate cumulative values, but "counter"
> is a sum of non-negative values, while "derive" is a sum of values which
> may be negative. This distinction is not important in Scylla: all our
> cumulative metrics are sums of non-negative values, and are monotonically
> increasing. So in this document we picked the term "counter" and use it
> exclusively.

Contrary to counter metrics which accumulate a measurement throughout the
lifetime of the node, a **gauge** metric measures the state of objects
currently existing in the system. For example, the number of requests being
processed *right now*, the size of some queue, the amount of memory devoted
now to the row cache, or the amount of disk used now for the data storage.

Gauge metrics are less common than counter metrics. When visualizing them,
one usually wants to look at the metric itself rather than its rate of
change. However, even for gauge metrics it is sometimes useful to visualize
their derivative - for example, a user might want to visualize the rate of
change to the amount of disk storage.

Internally, Scylla calculates many of the gauge metrics just like calculates
counter metrics - as a cumulative value: For example, Scylla maintains a
metric of the number of requests being processed *right now* by adding 1 to
the metric when starting to process a request, and subtracting 1 when the
request's processing is complete. This metric is nevertheless labeled "gauge"
because it provides a metric over currently-existing objects in the system
(requests being processed), not a sum of historic information.

TODO: histogram metrics. They are described in the Prometheus document linked
above.

## List of metrics
Looking at the response for http://scyllanode:9180/metrics is the best
way to see the list of metrics currently exposed by Scylla, because it
includes a textual description in a comment above each metric.

TODO: mention source files in which a developer should add new metrics.

## Prometheus
So far, we described Scylla's internal metric-retrieval recapability,
a REST API for retrieving the current values of all metrics from a single
node. But in production, as well as more advanced debugging sessions, one
usually wants to collect metrics from multiple Scylla nodes, and to collect
and to graph a history of each metric over time. As already mentioned above,
we provide a separate project "scylla-grafana-monitoring" which does exactly
this using the Prometheus time-series database.

Prometheus is installed on a separate monitoring node (which we shall call
below "monitornode"). It connects to several Scylla nodes, and saves their
metrics into a time-series database. Prometheus then allows querying,
analyzing, and and graphing this data, via a Web interface at:

    http://monitornode:9090/

Through this Web interface, a user can search for a metric name (type
a word and see the list of all metrics with this word as part of their
name), and then see the current value of this metric over all shards and
nodes (the "Console" tab), and also see a graph of the value of this
metric over time (the "Graph" tab).

Prometheus allows querying and graphing not only the metric itself, but
also various functions and aggregates of these metrics. For example, if
a user asks to graph some metric `xyz` the result is a graph with multiple
lines, one line for each shard and node. The syntax `xyz{instance="..."}`
will limit the lines to all shards of just one node (given the node's IP
address), and the syntax `xyz{instance="...",shard="0"}` will show only
one shard of one node. The syntax `xyz{group=~"memtable.*"}` will show
only metrics where the `group` label matches the given regular expression.

The syntax `sum(xyz)` will plot just one line, with the total of the metric
`xyz` over all shards in all nodes. It's also possible to plot partial sums -
for example `sum(xyz) by (group)` generates a separate sum (and plot line)
for each value of the label `group`.

The expression `irate(xyz[1m])` graphs the rate of change (i.e.,
the derivative) of the metric `xyz`. In this last example, the "1m"
selector is ignored by the `irate()` function, but some duration is required
by the Prometheus syntax.

Prometheus supports many more functions and aggregations, which are described
in its documentation:
https://prometheus.io/docs/prometheus/latest/querying/basics/

## Grafana
While Prometheus already allows analyzing and graphing metric data, Grafana
is a more advanced user interface which allows displaying many of these graphs
in professional-looking "dashboards" which are more convenient for end-users
who don't know which metrics Scylla has and what they mean, and want to see
pre-canned dashboards of graphs that are useful for particular purposes.

The Grafana user interface is available in:

    http://monitornode:3000/
