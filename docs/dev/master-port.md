# CQL Master Port

## Overview

The CQL master port is a unified listening port that auto-detects and handles
multiple client protocols on a single TCP endpoint. It eliminates the need for
clients to know the separate plain, TLS, shard-aware, and proxy-protocol ports
by multiplexing all of them onto one port.

When enabled, the server inspects the first byte(s) of each inbound connection
to determine the protocol layer in use, then routes the connection accordingly.

## Architecture

### Seastar layer (`seastar/src/net/posix-stack.cc`)

`posix_server_socket_impl::accept_master_port()` — the core accept loop. It:

1. Accepts a raw TCP connection.
2. Reads the first byte to detect the protocol.
3. For PP v2: reads the full header and extracts the real client address.
4. For shard selection: reads the 4-byte header, extracts shard ID and TLS
   flag. Rejects connections with unknown flags set.
5. For legacy clients: saves the consumed byte as a prefix to be prepended to
   the data stream.
6. Routes the connection to the target shard via `smp::submit_to` or returns
   it directly if already on the correct shard. Failures during cross-shard
   routing are logged.

Invalid or incomplete headers cause the connection to be dropped (with
debug-level logging per connection and rate-limited warnings) and the loop
continues accepting the next connection.

### Generic server layer (`transport/generic_server.cc`)

`server::do_accepts()` handles both regular and master port listeners in a
single unified method via a `bool master_port` parameter. When
`master_port=true`:

- `is_tls` is derived from `accept_result::metadata` (set by Seastar's
  protocol detection) rather than being known at listen time.
- The server wraps the socket with TLS if TLS was detected and credentials
  are available.
- If TLS is detected but no credentials are configured, the connection is
  rejected.

**Design note:** The `bool master_port` parameter approach keeps the two code
paths (regular accept and master port accept) together, which is simpler than a
subclass/strategy pattern for the current level of complexity. If master port
adds significantly more logic, splitting into a strategy may be warranted.

### Controller layer (`transport/controller.cc`)

`controller::start_listening_on_tcp_sockets()` creates the master port
listener when `native_transport_master_port` is set to a non-zero value. It
validates the master port against all other configured CQL ports and passes TLS
credentials (if configured) so that TLS auto-detection can wrap connections.

## Performance Considerations

All connections initially arrive on shard 0's accept loop for protocol
detection, then get forwarded to target shards. With very high connection rates
this could become a bottleneck. The existing `connection_distribution` LBA for
regular ports distributes accept load via `SO_REUSEPORT`, but the master port
cannot use `SO_REUSEPORT` because protocol detection must happen before shard
routing.

**Why `SO_REUSEPORT` is incompatible:** With `SO_REUSEPORT`, the kernel
distributes incoming connections across multiple sockets (one per shard), so
each shard's accept loop receives connections directly. The master port requires
reading the first bytes of each connection to determine the protocol and target
shard *before* routing. If `SO_REUSEPORT` were used, a connection could land on
any shard, but the shard selection header might specify a different shard — the
connection would need to be re-routed anyway. Worse, the protocol detection
read must happen on the accepting shard, and the connection must be forwarded
atomically with the consumed prefix bytes. A single listener on shard 0 avoids
this complexity at the cost of shard 0 being the bottleneck for new connections.

If this becomes a bottleneck in practice, a future optimization could perform
protocol detection on the target shard instead.

## Testing

### Boost unit tests (`test/boost/master_port_test.cc`)

These test the Seastar-level socket accept behavior: header format validation,
protocol detection, and the accept loop's handling of malformed input. They
require `-c1` (single shard) because they rely on deterministic accept ordering.

**Note:** These tests do not exercise full CQL server integration — they verify
that the Seastar layer correctly parses headers and returns the right
`accept_result` metadata.

### Python integration tests (`test/cqlpy/test_master_port.py`)

These test full CQL protocol interaction over the master port. They require a
running ScyllaDB cluster with the master port enabled.

**Running the tests:**
```bash
# Set up a ScyllaDB instance with master port enabled, then:
export MASTER_PORT=29042
pytest test/cqlpy/test_master_port.py --host=127.0.0.1
```

The `MASTER_PORT` environment variable must be set explicitly. If not set, tests
are skipped to avoid false positives from accidentally testing against the
regular CQL port.
