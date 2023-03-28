# The Compaction Controller

### Motivation

The Schedulers present in Scylla--namely the I/O Scheduler and the CPU scheduler are fully capable
of enforcing resource usage between classes, like compaction and foreground reads, for instance.
Classes with more shares will use more of the resources while classes with less shares, will use
less.

Before the advent of the Controllers, those shares were statically determined at startup. And while
that would guarantee fairness between classes, it would not guarantee that the classes were
progressing at optimal speed.

Compactions could be too fast: this could be seen in compaction graphs when compaction was a visible
peak. While fast compactions are nice, they take a lot of resources away from the foreground requests
to achieve that speed depressing latencies momentaneously. It would be better to run compactions
more slowly in that case, turning the fast peak into a better distributed plateau.

But blindly making compactions slower can create huge compaction backlogs, depressing future reads.
So we need to try to stay in the sweet spot.

### How does it work

We will keep track of the current backlog for compaction work. The backlog is an estimate of the
amount of work that we still have to do in the future to make the compaction strategy satisfied. Because the various
compaction strategies compact things very differently and have very different ideas about what it
means for the data to be "fully compacted", the backlogs are strategy-dependent.
Each strategy has its own goals on amplification factors, and once those goals are reached, then zero backlog
will be returned for the underlying table.

Each Table (Column Family) keeps track of the backlog that it has, and those backlogs are then added
together to create a system-wide backlog.

It is expected that the backlog measurement is consistent across strategies. What this means is
that if a strategy, at a certain point in time, has more work to do than another strategy, it should
return a higher backlog.

### Per-strategy backlog calculation

Currently unimplemented strategy will return a fixed backlog. Our aim is to soon implement backlog
controls for all classes.

#### Leveled Compaction Strategy

Unimplemented

#### DateTiered Compaction Strategy

Unimplemented

#### TimeWindow Compaction Strategy

Unimplemented

#### SizeTiered Compaction Strategy

Backlog for one SSTable under STCS:


```math
  B_i = E_i \times log_4 \frac{T}{S_i}   \qquad \text{(1)}
```

where $E_i$ is the effective size of the SStable, $S_i$ is the Size of this SSTable, and $T$ is the total
size of the Table.

To calculate the backlog, we can use the logarithm in any base, but we choose 4 as that is the
historical minimum for the number of SSTables being compacted together. Although now that minimum
could be lifted, this is still a good number of SSTables to aim for in a compaction execution.

$T$, the total table size, is defined as

```math
  T = \sum_{i=0}^{N}{S_i}    \qquad \text{(2)}
```

$E_i$, the effective size, is defined as

```math
  E_i = S_i - C_i    \qquad \text{(3)}
```

where $C_i$ is the total amount of bytes already compacted for this table.
For tables that are not under compaction, $C_i = 0$ and $S_i = E_i$.

Using the fact that $\log\frac{a}{b} = \log a - \log b$, we rewrite (1) as:

```math
  B_i = E_i \log_4 T - E_i \log_4 S_{i}
```

For the entire Table, the Aggregate Backlog (A) is

```math
  A = \sum_{i=0}^N E_i \times \log_4 T - E_i * \log_4 S_i
```

which can be expressed as a sum of a table component and a SSTable component:

```math
  A = \left( \sum_{i=0}^N E_i \times \log_4 T \right) - \left( \sum_{i=0}^N E_i \times \log_4 S_{i} \right)
```

and if we define

```math
C = \sum_{i=0}^N C_i
```

then we can write

```math
  A = (T - C) \times \log_4 T - \sum_{i=0}^N (S_i - C_i) \times \log_4 S_{i}
```

Because the SSTable number can be quite big, we'd like to keep iterations to a minimum.  We can do
that if we rewrite the expression above one more time, yielding:

```math
  A = T \times \log_4 T - C \times \log_4 T - \left( \sum_{i=0}^N S_i \times \log_4 S_i - \sum_{i=0}^N C_i \times \log_4 S_i \right)   \qquad \text{(4)}
```

When SSTables are added or removed, we update the static parts of the equation, and every time we
need to compute the backlog we use the most up-to-date estimate of Ci to calculate the compacted
parts, having to iterate only over the SSTables that are compacting, instead of all of them.

For SSTables that are being currently written, we assume that they are a full SSTable in a certain
point in time, whose size is the amount of bytes currently written. So all we need to do is keep
track of them too, and add the current estimate to the static part of (4).
