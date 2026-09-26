# Choose a Compaction Strategy

ScyllaDB implements the following compaction strategies in order to reduce [read amplification](https://opensource.docs.scylladb.com/branch-6.1/reference/glossary.md#term-Read-Amplification), [write amplification](https://opensource.docs.scylladb.com/branch-6.1/reference/glossary.md#term-Write-Amplification), and [space amplification](https://opensource.docs.scylladb.com/branch-6.1/reference/glossary.md#term-Space-amplification), which causes bottlenecks and poor performance. These strategies include:

* [Size-tiered compaction strategy (STCS)]() - triggered when the system has enough (four by default) similarly sized SSTables.
* [Leveled compaction strategy (LCS)]() - the system uses small, fixed-size (by default 160 MB) SSTables distributed across different levels.
* [Incremental Compaction Strategy (ICS)]() - shares the same read and write amplification factors as STCS, but it fixes its 2x temporary space amplification issue by breaking huge sstables into SSTable runs, which are comprised of a sorted set of smaller (1 GB by default), non-overlapping SSTables.
* [Time-window compaction strategy (TWCS)]() - designed for time series data.

This document covers how to choose a compaction strategy and presents the benefits and disadvantages of each one. If you want more information on compaction in general or on any of these strategies, refer to the [Compaction Overview](https://opensource.docs.scylladb.com/branch-6.1/kb/compaction.md). If you want an explanation of the CQL commands used to create a compaction strategy, refer to [Compaction CQL Reference](https://opensource.docs.scylladb.com/branch-6.1/cql/compaction.md) .

Learn more in the [Compaction Strategies lesson](https://university.scylladb.com/courses/scylla-operations/lessons/compaction-strategies/) on ScyllaDB University

<a id="stcs1"></a>

## Size-tiered Compaction Strategy (STCS)

The premise of [Size-tiered Compaction Strategy (STCS)](https://opensource.docs.scylladb.com/branch-6.1/reference/glossary.md#term-Size-tiered-compaction-strategy) is to merge SSTables of approximately the same size.

### Size-tiered compaction benefits

This is a popular strategy for LSM workloads.  It results in a low and logarithmic (in size of data) number of SSTables, and the same data is copied during compaction a fairly low number of times. Use the table in [Which strategy is best]() to determine if this is the right strategy for your needs.

### Size-tiered compaction disadvantages

This strategy has the following drawbacks (particularly with writes):

* Continuously modifying existing rows results in each row being split across several SSTables, making reads slow, which doesn’t happen in [Leveled compaction](#lcs1).
* Obsolete data (overwritten or deleted columns) in a very large SSTable remains, wasting space, for a long time, until it is finally merged.  In overwrite-intensive loads for example, the overhead can be as much as 400%, as data will be duplicated 4X within a tier. On the other hand, the output SSTable will be the size of a single input SSTable. As a result, you will need 5X the amount of space (4 input SSTables plus one output SSTable), so 400% over the amount of data currently being stored. The allocated space will have to be checked and evaluated as your data set increases in size.
* Compaction requires a lot of temporary space as the new larger SSTable is written before the duplicates are purged. In the worst case up to half the disk space needs to be empty to allow this to happen.

**To implement this strategy**

Set the parameters for [Size-tiered compaction](https://opensource.docs.scylladb.com/branch-6.1/kb/compaction.md#size-tiered-compaction-strategy-stcs).

<a id="lcs1"></a>

## Leveled Compaction Strategy (LCS)

[Leveled Compaction Strategy](https://opensource.docs.scylladb.com/branch-6.1/reference/glossary.md#term-Leveled-compaction-strategy-LCS) (LCS) uses small, fixed-size (by default 160 MB) SSTables divided into different levels. Each level represents a run of a number of SSTables.

### Leveled Compaction benefits

With the leveled compaction strategy, the following benefits are noteworthy:

* SSTable reads are efficient. The great number of small SSTables doesn’t mean we need to look up a key in that many SSTables, because we know the SSTables in each level have disjoint ranges, so we only need to look in one SSTable in each level. In the typical case, only one SSTable needs to be read.
* The other factors making this compaction strategy efficient are that at most 10% of space will be wasted by obsolete rows, and only enough space for ~10x the small SSTable size needs to be reserved for temporary use by compaction.

Use the table in [Which strategy is best]() to determine if this is the right strategy for your needs.

### Leveled Compaction disadvantages

The downside of this method is there is two times more I/O on writes, so it is not as good for workloads which focus on writing mostly new data.

Only one compaction operation on the same table can run at a time, so compaction may be postponed if there is a compaction already in progress. As the size of the files is not too large, this is not really an issue.

**To implement this strategy**

Set the parameters for [Leveled Compaction](https://opensource.docs.scylladb.com/branch-6.1/kb/compaction.md#leveled-compaction-strategy-lcs).

<a id="ics1"></a>

## Incremental Compaction Strategy (ICS)

ICS is only available in ScyllaDB Enterprise. See the [ScyllaDB Enetrpise documentation](https://enterprise.docs.scylladb.com/stable/architecture/compaction/compaction-strategies.html) for details.

<a id="twcs1"></a>

## Time-window Compaction Strategy (TWCS)

Time-Window Compaction Strategy compacts SSTables within each time window using [Size-tiered Compaction Strategy (STCS)]().
SSTables from different time windows are never compacted together. You set the [TimeWindowCompactionStrategy](https://opensource.docs.scylladb.com/branch-6.1/kb/compaction.md#time-window-compactionstrategy-twcs) parameters when you create a table using a CQL command.

### Time-window Compaction benefits

* Keeps entries according to a time range, making searches for data within a given range easy to do, resulting in better read performance.
* Allows you to expire an entire SSTable at once (using a TTL) as the data is already organized within a time frame.

### Time-window Compaction deficits

* Time-window compaction is **only** ideal for time-series workloads

**To implement this strategy**

Set the parameters for [Time-window Compaction](https://opensource.docs.scylladb.com/branch-6.1/kb/compaction.md#time-window-compactionstrategy-twcs).

Use the table in [Which strategy is best]() to determine if this is the right strategy for your needs.

<a id="which-strategy-is-best"></a>

## Which strategy is best

Every workload type may not work well with every compaction strategy. Unfortunately, the more mixed your workload, the harder it is to pick the correct strategy. This table presents what can be expected depending on the strategy you use for the workload indicated, allowing you to make a more informed decision. Keep in mind that the best choice for our testing may not be the best choice for your environment. You may have to experiment to find which strategy works best for you.

<a id="csm1"></a>

### Compaction Strategy Matrix

The table presents which workload works best with which compaction strategy. In cases where you have the ability to use either STCS or ICS, always choose ICS.

| Workload/Compaction Strategy   | Size-tiered                                                | Leveled                                                    | Incremental                                                | Time-Window                                                | Comments                                        |
|--------------------------------|------------------------------------------------------------|------------------------------------------------------------|------------------------------------------------------------|------------------------------------------------------------|-------------------------------------------------|
| Write-only                     | <i class="inline-icon icon-check" aria-hidden="true"></i>  | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i>  | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <sup>[1](#id10)</sup> and <sup>[2](#id11)</sup> |
|                                | <i class="inline-icon icon-check" aria-hidden="true"></i>  | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i>  | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <sup>[3](#id12)</sup> and <sup>[4](#id13)</sup> |
| Read-mostly, with few updates  | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i>  | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <sup>[5](#id14)</sup>                           |
| Read-mostly, with many updates | <i class="inline-icon icon-check" aria-hidden="true"></i>  | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i>  | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <sup>[6](#id15)</sup>                           |
| Time Series                    | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <i class="inline-icon icon-cancel" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i>  | <sup>[7](#id16)</sup> and <sup>[8](#id17)</sup> |

The comments below describe the type of amplification each compaction strategy create on each use case, using the following abbreviations:

* SA - Size Amplification
* WA - Write Amplification
* RA - Read Amplification

<a id="id10"></a>

<sub>1</sub> When using Size-tiered with write-only loads it will use approximately 2x peak space -  with Incremental, the SA is much less

<a id="id11"></a>

<sub>2</sub> When using Leveled Compaction with write only loads you will experience high Write Amplification - 

<a id="id12"></a>

<sub>3</sub> When using Size-tired or Incremental with Overwrite loads,  occurs

<a id="id13"></a>

<sub>4</sub> When using Leveled Compaction with overwrite loads,  occurs

<a id="id14"></a>

<sub>5</sub> When using Size-tiered with mostly read loads with little updates,  and  occurs

<a id="id15"></a>

<sub>6</sub> When using Leveled with mostly read loads with many updates,  occurs in excess

<a id="id16"></a>

<sub>7</sub> When using Size-tiered or Incremental with Time Series workloads, , , and   occurs.

<a id="id17"></a>

<sub>8</sub> When using Leveled with Time Series workloads,  and   occurs.

### References

* [Compaction Overview](https://opensource.docs.scylladb.com/branch-6.1/kb/compaction.md) - contains in depth information on all of the strategies
* [Compaction CQL Reference](https://opensource.docs.scylladb.com/branch-6.1/cql/compaction.md) - covers the CQL parameters used for implementing compaction
* ScyllaDB Summit Tech Talk: [How to Ruin Performance by Choosing the Wrong Compaction Strategy](https://www.scylladb.com/tech-talk/ruin-performance-choosing-wrong-compaction-strategy-scylla-summit-2017/)
