<!--
Copyright (C) 2026-present ScyllaDB
SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
-->

# Unified CQL listener

The unified listener accepts plain CQL, TLS, PROXY protocol v2 (PPv2), and
explicit shard selection on one TCP port. It starts alongside legacy listeners.
It binds to `rpc_address` and uses `rpc_keepalive`.

## Configuration

```yaml
native_transport_unified_port:
  port: 29042  # 0 disables this listener
  encryption: tls-only  # plain-only | tls-only | both
  shard_routing:
    default: balanced  # balanced | source-port
    ppv2:
      mode: optional  # disabled | optional | required
      trusted_proxies:
        interface_subnets: ["*"]  # Or ["eth0", "eth1"]
        addresses: ["192.0.2.10/32"]  # Additional IPs or CIDRs; ["*"] trusts all

client_encryption_options:
  enabled: true
  certificate: /etc/scylla/scylla.crt
  keyfile: /etc/scylla/scylla.key
```

The listener is disabled by default (`port: 0`). Set `encryption` explicitly when
enabling it. TLS modes require valid, enabled `client_encryption_options`.
When TLS client certificates are required, the listener must use `tls-only`.
Choose a port different from legacy CQL ports. A legacy native port set to `0`
can bind to a random port; it does not reliably disable that listener.

| Setting | Behavior |
| --- | --- |
| `encryption: plain-only` | Accept plain CQL and reject TLS. |
| `encryption: tls-only` | Accept TLS and reject plain CQL. |
| `encryption: both` | Accept either transport. |
| `ppv2.mode: disabled` | Reject PPv2 headers. Default mode. |
| `ppv2.mode: optional` | Accept direct connections and PPv2 from trusted peers. |
| `ppv2.mode: required` | Require a PPv2 header from a trusted peer. |
| `shard_routing.default: balanced` | Let the server choose the shard. Default mode. PPv2 still supplies client metadata. |
| `shard_routing.default: source-port` | Choose the shard using the effective source port modulo the shard count. |

### Proxy trust

Both trust fields are lists of strings. The trusted set is their union.
Both default to empty, which trusts no proxies. The listener checks the physical
TCP peer before accepting metadata from its PPv2 header. It rejects headers from
untrusted peers in both `optional` and `required` modes.

- `interface_subnets: ["eth0", "eth1"]`: trust IPv4 and IPv6 subnets assigned to
  these interfaces, using their addresses and netmasks.
- `interface_subnets: ["*"]`: use all active interfaces except loopback. Include
  physical and virtual interfaces. This selects peer address ranges; it does not
  check the interface on which traffic arrives.
- `addresses`: add explicit proxy IPs or CIDRs.
- `addresses: ["*"]`: trust every IPv4 and IPv6 peer. Equivalent to
  `["0.0.0.0/0", "::/0"]`.

IPv4-mapped IPv6 peers have IPv4 identity. A mapped trust entry with a prefix
of 96 or longer selects the corresponding IPv4 range: `::ffff:192.0.2.10/128`
is equivalent to `192.0.2.10/32`. Shorter prefixes remain IPv6 ranges and do
not match IPv4 peers. The IPv6 range `::/0` also excludes IPv4 peers.

Resolve interface subnets at listener startup and log the resulting CIDRs.
Restart the listener to refresh them. Invalid interface names or interfaces
without usable IP prefixes stop listener startup. Quote wildcards in YAML.

For example, combine selected interface subnets and explicit proxy addresses:

```yaml
trusted_proxies:
  interface_subnets: ["eth0", "eth1"]
  addresses: ["192.0.2.10/32", "2001:db8:1::/64"]
```

Trust all proxies:

```yaml
trusted_proxies:
  interface_subnets: []
  addresses: ["*"]
```

These blocks belong under `native_transport_unified_port.shard_routing.ppv2`.
Trusting a subnet permits every matching peer to report another client address.
CQL authentication and authorization still apply.

## Connection setup and routing

Connection order is TCP, optional PPv2, optional shard header, then plain CQL
or TLS followed by CQL. The PPv2 and shard headers precede TLS.

The shard header contains four bytes:

| Offset | Size | Meaning |
| --- | --- | --- |
| 0 | 1 byte | Magic `0xAA`. |
| 1 | 1 byte | Bit 0: TLS follows. Bits 1–7 must be zero. |
| 2 | 2 bytes | Requested shard ID, unsigned, big-endian. |

`AA 00 00 03` selects shard 3 and plain CQL. `AA 01 00 03` selects shard 3
and TLS. The listener applies the requested shard ID modulo the shard count.
Encryption policy applies to both connections with a shard header and legacy
connections without one. TLS failures close the connection.

A shard header always overrides `shard_routing.default`. Without a shard header,
`source-port` uses the source port reported by trusted PPv2 when present;
otherwise it uses the TCP source port. `balanced` distributes connections and
retains PPv2 client metadata independently of shard selection.

The accepting shard detects connection headers and transfers the socket,
TLS flag, physical peer, reported addresses, and any consumed input byte to
the serving shard. The serving shard performs TLS setup before CQL processing.

One acceptor runs up to 128 independent preface detection and socket transfer
tasks per listener. Each accepted socket has one five-second deadline covering
all preface reads, including PPv2 and a following shard header. Idle clients and
clients with partial prefaces occupy individual slots. When all slots are
occupied, new connections remain in the kernel listen backlog until a slot is
released. Each serving shard queues at most 128 completed sockets awaiting
application acceptance; new completions for a full queue are dropped. These
fixed internal limits do not have configuration settings. Abort or listener
destruction cancels pending preface reads and discards queued completions.
Detection and transfer tasks retain their state until cleanup finishes.

Legacy PPv2 listeners keep serial admission. Their complete PPv2 header and
payload must arrive within five seconds; timeout drops the socket. Abort or
listener destruction cancels the pending read, whose state survives until it drains.

## Discovery

`SUPPORTED` includes `SCYLLA_UNIFIED_PORT` with the configured port as a decimal
string when shard-aware driver discovery is enabled. Existing clients can use
this endpoint without a shard header. Existing legacy discovery keys keep their
original meanings. Legacy listeners remain available during migration.

## Tests

- `test/boost/config_test.cc`: YAML validation, trust lists, CIDRs, and interface subnet discovery.
- `test/boost/unified_port_test.cc`: protocol detection, routing policy, PPv2 metadata, and encryption policy.
- `test/cqlpy/test_unified_port.py`: CQL connections and shard discovery against a configured listener.
- `test/cluster/test_unified_port.py`: configured listener policies on multiple shards.
