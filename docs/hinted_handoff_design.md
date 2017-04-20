## Abstract
Hinted Handoff is a feature that allows replaying failed writes. The mutation and the destination replica are saved in a log and replayed later according to the feature configuration. 

## Hinted Handoff Configuration Parameters 
 * _hinted_handoff_enabled_: Enables or disables the Hinted Handoff feature completely or enumerate DCs for which hints are allowed.
 * _max_hint_window_in_ms_: Don't generate hints if the destination Node has been down for more than this value. The hints generation should resume once the Node is seen up.
 * _hints_directory_: Directory where scylla will store hints. By default `$SCYLLA_HOME/hints`
 * _hints_compression_: Compression to apply to hints files. By default, hints files are stored uncompressed.
 
## Future configuration
 * We should define the fairness configuration between the regular WRITES and hints WRITES.
   Since we don't have either CPU scheduler or (and) Network scheduler at the moment we can't give any guarantee regarding the runtime and/or networking bandwidth fairness.
   Once we have tools to define it we should give the hints sender some low runtime priority and some small share of the bandwidth.

## Hints generation
 * Once the WRITE mutation fails with a timeout we create a _hints_queue_ for the target node.
   * The queue is specified by a destination node IP.

 * Each hint is specified by:
   * Mutation.
   * Destination node.

Hints are appended to the _hints_queue_ until (all this should be done using the existing or slightly modified _commitlog_ API):
 * The destination node is DOWN for more than _max_hint_window_in_ms_ time.
 * The total size of hint files is more than 10% of the total disk partition size where _hints_directory_ is located.
 * We are going to ensure storing a new hint to the specific destination node if there are no pending hints to it.

As long as hints are appended to the queue the files are closed and flushed to the disk once they reach the maximum allowed size (32MB) or when the queue is forcefully flushed (see "Hints sending" below).

We are going to reuse the commitlog infrastructure for writing hints to disk - it provides both the internal buffering and the memory consumption control.

Hints to the specific destination are stored under the _hints_directory_/\<shard ID>/\<node IP> directory.

### When new hints may be dropped?
 * A new hint is going to be dropped when there are more than 10MB "in progress" (yet to be stored) hints per-shard and when there are  "in progress" hints to the destination the current hint is aimed to. 
   * If there are no "in progress" hints to the current destination the new hint won't be dropped due to the per-shard memory limitation.
 * A hint is going to be dropped if the disk space quota (to the whole node it's 10% of the total disk space of the disk partition where _hints_directory_ is located) has been depleted and when there are pending (already stored) hints to the current destination.
   * Disk quota is divided equally between all present shards.
   * If there are no pending hints to the current destination a new hint won't be dropped due to a disk space limitation. 
 * If a new hint is dropped the corresponding metrics counter is increased. 

### Redistribution of hints when node boots.
 * When node boots all present hints files are redistributed equally between all present shards.

## Hints sending
 * Hints are sent from each shard by each _hints_queue_ independently.
 * Each shard sends the hints that it owns (according to the hint file location).
 * Hints sending is triggered by the following events: 
   * Timer: every X seconds (every 10s).
     * For each queue:
       * Forcefully close the queues.
     * If the destination node is ALIVE or decommissioned and there are pending hints to it start sending hints to it:
       * If hint's timestamp is older than mutation.gc_grace_seconds() from now() drop this hint. The hint's timestamp is evaluated as _hints_file_ last modification time minus the hints timer period (10s).
       * Hints are sent using a MUTATE verb:
         * Each mutation is sent in a separate message.
           * If the node in the hint is a valid mutation replica - send the mutation to it.
           * Otherwise execute the original mutation with CL=ALL.
       * Once the complete hints file is processed it's deleted and we move to the next file.
       * We are going to limit the parallelism during hints sending. The new hint is going to be sent out unless:
         * The total size of in-flight (being sent) hints is greater or equal to 10% of the total shard memory.
         * The number of in-flight hints is greater or equal to 128 - this is needed to limit the collateral memory consumption in case of small hints (mutations).
       * If there is a hint that is bigger than the memory limit above we are going to send it but won't allow any additional in-flight hints while it's being sent. 
   * Local node is decommissioned (see "When the current node is decommissioned" below).

## When the current node is decommissioned (in the absence of Hints Streaming)
 * Send all pending hints out:
   * If the destination node is not ALIVE or the mutation times out - drop the hint and move on to the next one. 

## Hints streaming (optional)
 * Streaming is performed using a new HINT_STREAMING verb:
   * When node is decommissioned it would stream its hints to other nodes of the cluster (only in a Local data center):
     * Shard X would send its hints to the node[Yx], where Yx = X mod N, where N is number of nodes in the cluster without the node that is being decommissioned.
   * Receiver distributes received hints equally among local shards: pushes them to the corresponding _hint_queue_s (see "Hints generation" above).



