# Advanced RPC compression modes

## Introduction

For many years, the only mode of compression available for inter-node (RPC) traffic
had been per-message LZ4 compression. This is suboptimal, because it fails to take
advantage of the substantial redundancy across messages.

To exploit cross-message redundancy, a few common techniques are used:
- Streaming compression, in which both sides of the connection keep some messages
  from the recent past, and compress new messages against that context.
- Buffering, in which multiple messages are compressed together.
- Fixed-dictionary compression, in which messages are compressed against some
  representative context, pre-agreed by both sides.

Streaming compression, when used indiscriminately, has some fundamental scaling problems.
Each stream-compressed connection requires its own history buffer, which puts memory
pressure, both on RAM (hundreds of streams might require a significant fraction of a machine's
memory) and on CPU caches (history buffers are randomly accessed for each compressed
byte, and if there are many of them, they are likely to be cold, potentially slowing down
the compression significantly).

Technically, the drawbacks of streaming compression can be worked around (e.g. by limiting
stream compression only to the few most important connections, and/or by using a sparse
network topology), but this requires uncomfortably complex solutions.

Buffering doesn't put as much pressure on memory as streaming, but adds latency,
and has its own fundamental scaling issue -- the more nodes, the lower the utilization
of each connection, which means a smaller number of messages buffered in the same unit
of time. With a sufficiently big cluster, only one message can be gathered during
a reasonably short delay, rendering the mechanism useless. 

A fixed dictionary requires more assumptions about the data to work well
(i.e. that connections are similar and their content is relatively uniform over time),
but it has a constant overhead regardless of the number of connections, so it scales perfectly.
And given that most of the time Scylla clusters are symmetric, a dictionary trained on one
node should work reasonably well on all nodes.

Thus, we chose to employ dictionary compression for Scylla's RPC compressions.

## Feature overview

The general idea is:
1. Some node in the cluster periodically trains a compression dictionary
   on its own RPC traffic, and distributes it.
2. Whenever a new dictionary becomes available on both sides of an RPC connection,
   the connection switches to using the new dictionary.

## Details

### Trainer selection

Given the assumption that shards in the cluster are symmetric, there's no need to involve
more than one shard in the training of any particular dictionary. And since involving only
one shard is simpler than involving many, we only involve one.

In addition, it doesn't make sense to train multiple dictionaries concurrently,
since only the last one will be eventually used, and the earlier ones will only cause
noise.

Given the above, we designate a single shard in the cluster (at a time) to perform the traffic
sampling and dictionary training. As of this writing, we designate shard 0 of the raft leader
for that, just because it's a single shard selection mechanism that's already implemented.

The rest of the design doesn't rely on the single-trainer property for correctness,
and it's fine if multiple shards think they are the trainer (leader) for some time.

However, as is explained later in this doc, it is currently needed for progress
(towards switching connections to new dictionary versions) that the most recent dictionary
version in the cluster remains stable for some time, so a split-brain situation
could theoretically delay progress. But that's acceptable.

### Sampling

Sampling is implemented by merging all outgoing RPC traffic on shard 0 into a single stream,
splitting this stream into pages (of size ~8 kiB), and taking a uniform
random sample (total size ~16 MiB) of all pages seen during the sampling
period, via reservoir sampling. The sample is kept in RAM.

The page size should be big enough for the strings it contains to be worth compressing
(so, at least several bytes), but the number of pages in the sample (inversely proportional
to page size) should be big enough to minimize the chance of all pages falling onto
non-representative parts of the stream. The current page size was chosen based on intuition.

The sample size is based on the recommendations from zstd's documentation -- it says that
~100 kiB is a reasonable dictionary size, and that the sample should be on the order of
100 times larger than the dictionary.

The duration of sampling is configurable by the minimum amount of data that has to be ingested
(to avoid training unrepresentative dictionaries during periods of downtime) and by the minimum
amount of time that has to pass (to get some predictable upper limit on the rate of dictionary
updates).

### Training

When the sampling finishes, a dictionary (~100 kiB) is trained on the (~16 MiB) sample.

For this, we use Zstd's dictionary builder. Since it requires a few seconds of CPU time,
it can't be used directly under the reactor, so we run it in a separate, low-priority OS thread.

Note: LZ4 limits the max size of the dictionary to 64 kiB.
But currently we only train a single ~100 kiB dictionary and
use the first 64 kiB of that as the dict for LZ4.
Perhaps we are getting a suboptimal LZ4 dictionary this way.
Maybe we should do a separate 64 kiB training.

### Distribution

We distribute new dictionaries via a Raft-based Scylla table.
(Local to each node, but with coordinated mutation via Raft).
The trainer inserts new dictionaries to the table via a Raft command.
Other nodes pick it up when the Raft command is applied to them.

The table is:
```
CREATE TABLE system.dicts (
    name text,
    timestamp timestamp,
    origin uuid,
    data blob,
    PRIMARY KEY (name)
);
```

As of now, only a single `name` is used (`"general"`) — because there is only a single
dictionary in the cluster, shared by all RPC connections. In the future, we might want to
have separate dictionaries for different DCs, or for different service levels, etc.
If that comes to be, they will be stored in their own partitions.

The `timestamp` and `origin` fields describe the time and node on which the dictionary
was created. They don't really have to exist, but we have them for observation/debugging purposes.
(And they are used as a part of the dictionary's ID in the negotiation protocol for the same purpose).

The `data` blob contains the actual dictionary contents. Ideally it's a zstd-formatted
dictionary, but any blob should work.

During dictionary negotiation, nodes identify dictionaries by a `<timestamp, origin, content_sha256>` triple.

If two different dictionaries with a different content but the same SHA256 are ever
published with the same `<timestamp, origin>`, the negotiation might result in a corrupted
connection. No countermeasures against this have been implemented as of now.

### Negotiation

Dictionary compression requires that the sender and the receiver both use the same dictionary.
In particular, the sender can only start using a new dictionary after the receiver acquires
it, and the receiver can't forget a dictionary as long as the sender uses it.

Since this is the only part of the entire design which requires some distributed coordination,
this is probably the most complicated part.

There are many possible approaches. For example, the easiest one is to just let the sender
send the new dictionary over the connection whenever it wants to switch. This requires no
inter-node coordination. However, it's relatively expensive -- the number of connections is quadratic
in the size of the cluster, and this method requires each connection to transfer the entire
dictionary. Ideally, each node should only have to download each new dictionary only once.

#### Description

The negotiation algorithm we currently use works like this (it is run for each connection direction independently):

There are two peers: sender (compresses) and receiver (decompresses). They communicate via an ordered
and reliable stream of messages.
There's one peculiarity here: it is assumed that messages can be canceled by newer messages.
i.e. when multiple messages are requested to be sent, only the last one is guaranteed
to eventually be sent, while older ones can be cancelled.
Because the algorithm respects this assumption, it can be run in constant memory --
there never has to be a queue of messages longer than 1.

Each peer references three dicts:
- `recent` -- the target dict. We want to switch to it as soon as our peer also acquires it.
Updates to this reference come asynchronously from outside; it is an input to the algorithm. 
It is assumed that both sides of the connection will eventually have the same `recent`,
and this will hold for a long time.
- `committed` -- the dict we last proposed to our peer. This reference is set to `recent` at
the moment we make the suggestion.
The receiver needs this reference so that it can immediately switch to it when the sender accepts
the suggestion.
- `current` -- the one actually used for compression at the moment. It is set to `committed` at the moment
the proposal is accepted.

The algorithm works in epochs. Each message has an epoch number attached.
Sender monotonically increments the epoch whenever it makes a new proposal.
Receiver always echoes back the last observed epoch.

The flow of each epoch is as follows:
0. Receiver notifies sender about any change in `recent` by sending `<UPDATE>`.
1. When sender gets `<UPDATE>`, or when sender's `recent` changes,
   sender increments the epoch, and requests `<UPDATE, recent>` to be sent. 
   Atomically with the send (i.e. if and only if it isn't cancelled by another message),
   it sets `committed := recent`.
2. When receiver gets `<UPDATE, x> | x == recent`, it sets `committed := recent`,
   and requests `<COMMIT, committed>` to be sent.
3. When sender gets `<COMMIT, x> | x == committed` from the current epoch,
   it requests `<COMMIT, committed>` to be sent. Atomically with the send, it sets `current := committed`.
   (Else when sender gets `<COMMIT, x> | x != committed` from the current epoch,
   it requests `<COMMIT, current>` to be sent. This could be omitted.)
4. When sender receives `<COMMIT, x> | x == committed`, it sets `current := committed`. 
   (When it receives `<COMMIT, x> | x != committed`, it doesn't do anything).

#### Proof sketch

Property 1 (safety): both peers use the same dictionary for every message:

Both sides modify `current` (by setting `current := committed`) only at the point
when they process sender's `COMMIT`. So to prove safety, it suffices to prove
that both sides have the same `committed` at this point.

Sender sends its COMMIT in response to receiver's COMMIT.
But receiver sent the COMMIT only if receiver's `committed` was equal to sender's `committed`
at the time of sender's UPDATE.

But since the epoch hasn't changed, sender couldn't have changed its `committed` since that UPDATE,
and it also hasn't sent anything which could have modified the receiver's `committed`.

So both sender and the receiver have the same `committed`
when they process the sender's COMMIT.

Property 2 (liveness): if both peers have `recent` set to the same dict `x` for sufficiently long,
the algorithm will eventually switch `current` to it:

At the moment receiver acquires `x`, it sends a notification to sender, which will
start a new epoch. At the moment sender acquires `x`, it will also start a new epoch.
Whichever of these two epochs comes later, will eventually succeed.
(Easy to prove).

#### Notes

There are some small differences between the description above here and the implementation,
but they shouldn't alter the algorithm. (E.g. instead of requesting `<COMMIT, current>`,
sender sets `committed := current` and requests `<COMMIT, committed>`. This should be
equivalent.)

The algorithm also negotiates parameters other than the dict.
In particular, it negotiates the available compression libraries (zstd vs lz4).
But these params don't require coordination -- the receiver can decompress messages
regardless of the values chosen by the sender -- so this is easy.
The receiver just communicates its preferences in its COMMIT, and the sender
unilaterally sets the parameters based on these preferences.

There is a TLA specification of the algorithm in
[advanced_rpc_compression_negotiation.tla](advanced_rpc_compression_negotiation.tla).

### Usage

Dictionaries are read-only, so -- to minimize memory pressure -- we take care to have only
one copy of each dictionary, on shard 0, kept alive by foreign shared pointers.

Likewise, we keep only a single compressor and a single decompressor for each algorithm,
and whenever a connection needs to use it, it plugs the correct dictionary in. 
Switching dictionaries should be cheaper than keeping multiple copies of the compressors.

### Wire protocol details

This section describes the layout of a compressed frame produced by `advanced_rpc_compressor::compress()`.

The compression algorithm is selected on per-message basis.
(Rationale: this allows the sender to weaken the compression unilaterally if it doesn't have the resources for the "normal" algorithm.)
The 7 least significant bits of byte 0 of each compressed message
contain an enum value describing the compression algorithm used for this message.

The most significant bit of byte 0 tells if there is a control (negotiation)
header present. If this bit is set, then bytes 1-133 contain a packed `control_protocol_frame` struct,
carrying the negotiation protocol described earlier. See `control_protocol_frame::serialize()` for details.

After byte 0 and the optional negotiation header, the rest of the message contains the actual contents
of the RPC frame, compressed with the selected algorithm. The details of this part are different for each algorithm.
