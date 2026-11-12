#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Cut, degrade or delay the internode network of a test cluster through a TCP proxy.

The nodes of a test cluster are plain processes on the loopback device, one
127.X.Z.W address each, and they talk to each other over TCP (internode RPC on
port 7000). Acting on that traffic with packet filter or traffic control rules
needs CAP_NET_ADMIN, which the harness does not have. A userspace proxy needs
nothing: a proxied node keeps listening on its own address, but advertises the
address of its proxy (``broadcast_address``) to its peers, so every connection
a peer opens to it goes through the proxy. Scylla supports a ``broadcast_address``
that differs from the ``listen_address`` for nodes behind NAT, and binds its own
outgoing connections to the listen address, so the proxy sees every connection
under the real address of the peer that opened it.

That last point is what makes the rules *pair-wise*: a connection from S to D
carries two byte flows, S->D and D->S, and the proxy of D knows both ends. A
rule names a flow by its two real addresses (``*`` for any), and applies to that
flow on whichever proxy sees it: the flow A->B runs through B's proxy on the
connections A opened, and through A's proxy on the connections B opened. So a
cut of A->B stalls A's bytes to B everywhere, exactly like a packet filter rule
``ip saddr A ip daddr B drop``, and a partition of A and B is two such rules.

One ``ProxyServer`` process per cluster, started by the cluster when a test
enables proxying (``ScyllaCluster.enable_network_proxy``), with one listener per
node and proxied port. ``NetworkProxy`` is what a test gets (the
``network_proxy`` fixture): ``cut``/``partition``/``isolate``/``block_clients``/
``blackout``/``heal``, the way a packet filter would cut the links, plus what a
packet filter cannot do: ``delay`` and ``bandwidth``.

What a proxy cannot do, and how this module maps it:

* It sees bytes, not packets. ``mode="drop"`` withholds the bytes of the flow
  (the connections hang, as with lost packets); ``mode="reset"`` closes the
  connections between the two nodes and refuses new ones. A connect to a
  dropped node still succeeds at the TCP level, since the proxy accepts it; the
  RPC handshake behind it then hangs.
* Packet loss cannot be reproduced, only its effect: TCP hides a lost packet
  behind a retransmission. On a LAN most losses are repaired within a couple
  of round trips (fast retransmit, tail loss probe: ~10ms), a loss whose
  repair is lost too costs a retransmission timeout (200ms on Linux), doubled
  for every retransmission lost in turn. ``loss=N`` with ``mode="drop"``
  stalls each chunk of the flow that way with probability N%, each further
  step again with probability N%. With ``mode="reset"`` it is the probability
  that a connection between the two nodes is refused or closed.
* The internode ports (7000, 7001) and the CQL ports (9042, 9142) are proxied;
  the REST API and the metrics are reached at the node's real address. A rule
  without a port set acts on the internode flows; ``block_clients`` acts on
  the CQL flows into a node, whoever the client is. The shard-aware CQL port
  is not advertised by a proxied node (``enable_shard_aware_drivers: false``),
  since the driver picks its shard by source port, which a proxy rewrites.
* Both the peers and the clients know a node by its proxy's address: that is
  what gossip, the failure detector REST API, ``system.peers`` (``peer`` and
  ``rpc_address``), ``nodetool status`` and the driver's ``Host.address``
  report (``address_of`` translates). The harness itself reaches a node's CQL
  at its real address, so its own checks never depend on the rules. Replacing
  a proxied node by address and changing a proxied node's address are refused:
  the peers know the node only by its proxy's address.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import pathlib
import random
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Literal, Optional

from test.pylib.internal_types import IPAddress

logger = logging.getLogger(__name__)

INTERNODE_PORTS = (7000, 7001)
CQL_PORTS = (9042, 9142)
PROXIED_PORTS = INTERNODE_PORTS + CQL_PORTS
CONTROL_PORT = 8474
ANY = "*"

CutMode = Literal["drop", "reset"]
Direction = Literal["upstream", "downstream", "both"]

# How a lost packet shows: a quick repair (fast retransmit or tail loss probe, a
# couple of LAN round trips), then, if the repair is lost too, the first
# retransmission timeout of Linux TCP, doubled for every retransmission lost in
# turn, up to a cap.
REPAIR_S = 0.01
RTO_FIRST_S = 0.2
RTO_MAX_S = 3.2

CHUNK = 64 * 1024
# Chunks a flow may hold back before its reader stops reading: the "window" of a
# stalled or slow flow, 16 MB.
QUEUE_CHUNKS = 256


@dataclass
class Rule:
    """What happens to the byte flow from ``src`` to ``dst`` (real addresses, or ``*``).

    ``action`` is ``pass`` (shape only), ``drop`` (withhold the bytes) or ``reset``
    (close and refuse the connections between the two nodes); ``loss`` in percent
    is the probability of a stall per chunk (drop or pass) or of a refusal per
    connection (reset); ``delay_ms`` +/- ``jitter_ms`` is added to every chunk;
    ``kbps`` caps the flow, 0 for no cap. ``ports`` is the destination ports the
    rule acts on: the internode ports when None.
    """
    src: str
    dst: str
    action: str = "pass"
    loss: int = 0
    delay_ms: int = 0
    jitter_ms: int = 0
    kbps: int = 0
    ports: list[int] | None = None

    def matches(self, src: str, dst: str, port: int) -> bool:
        if self.ports is None:
            if port not in INTERNODE_PORTS:
                return False
        elif port not in self.ports:
            return False
        return self.src in (ANY, src) and self.dst in (ANY, dst)


@dataclass
class Effective:
    """The rules that match one flow, combined: the harshest action, the longest delay, the narrowest cap.

    The two kinds of loss are kept apart, since they mean different things: ``loss`` is
    the stall probability per chunk of the pass rules, ``reset_loss`` the refusal
    probability per connection of the reset rules, 100 for a reset rule without one.
    A reset that does not refuse a connection leaves its bytes alone.
    """
    action: str = "pass"
    loss: int = 0
    reset_loss: int = 0
    delay_ms: int = 0
    jitter_ms: int = 0
    kbps: int = 0

    @classmethod
    def of(cls, rules: Iterable[Rule], src: str, dst: str, port: int) -> Effective:
        eff = cls()
        for rule in rules:
            if not rule.matches(src, dst, port):
                continue
            if rule.action == "reset":
                eff.reset_loss = max(eff.reset_loss, rule.loss or 100)
            elif rule.action == "drop":
                eff.action = "drop"
            else:
                eff.loss = max(eff.loss, rule.loss)
            eff.delay_ms = max(eff.delay_ms, rule.delay_ms)
            eff.jitter_ms = max(eff.jitter_ms, rule.jitter_ms)
            if rule.kbps and (not eff.kbps or rule.kbps < eff.kbps):
                eff.kbps = rule.kbps
        return eff


# ---------------------------------------------------------------------------
# The server: runs in its own process, ``python -m test.pylib.network_proxy``.
# ---------------------------------------------------------------------------

@dataclass(eq=False)
class Connection:
    src: str
    dst: str
    port: int
    client: asyncio.StreamWriter
    upstream: asyncio.StreamWriter
    opened_at: float = field(default_factory=time.monotonic)
    closed: bool = False

    def close(self, graceful: bool = False) -> None:
        """Close both ends: ``graceful`` delivers what is still buffered first, otherwise both are reset."""
        self.closed = True
        for writer in (self.client, self.upstream):
            try:
                if graceful:
                    writer.transport.close()
                else:
                    writer.transport.abort()
            except Exception:  # noqa: BLE001 - closing a dead transport is fine
                pass


class Proxy:
    """The proxies of one cluster: listeners, connections and the rule set."""

    def __init__(self) -> None:
        self.nodes: dict[str, str] = {}             # real address -> proxy address
        self.listeners: dict[str, list[asyncio.Server]] = {}
        self.rules: list[Rule] = []
        # Bumped by every rule change, so that a connection accepted under the old rules is checked again.
        self.generation = 0
        self.connections: set[Connection] = set()
        self.bytes: dict[tuple[str, str, int], int] = {}   # (src, dst, port) -> bytes carried
        self.rng = random.Random()
        self.log = logging.getLogger("proxy")

    # -- control --

    async def add_node(self, node: str, proxy: str, ports: Iterable[int]) -> None:
        await self.remove_node(node)
        servers = []
        for port in ports:
            server = await asyncio.start_server(lambda r, w, node=node, port=port: self.serve(node, port, r, w),
                                                host=proxy, port=port, reuse_address=True)
            servers.append(server)
        self.nodes[node] = proxy
        self.listeners[node] = servers
        self.log.info("proxy %s -> %s on ports %s", proxy, node, list(ports))

    async def remove_node(self, node: str) -> None:
        for server in self.listeners.pop(node, []):
            server.close()
            await server.wait_closed()
        self.nodes.pop(node, None)
        for conn in [c for c in self.connections if node in (c.src, c.dst)]:
            conn.close()

    def set_rules(self, rules: list[Rule]) -> int:
        """Replace the rule set; connections a reset rule now covers are closed. Returns how many."""
        self.rules = rules
        self.generation += 1
        closed = 0
        for conn in list(self.connections):
            if self.refused(conn.src, conn.dst, conn.port):
                conn.close()
                closed += 1
        self.log.info("rules: %s; closed %d connections", [asdict(r) for r in rules], closed)
        return closed

    def state(self) -> dict[str, Any]:
        return {"nodes": dict(self.nodes), "rules": [asdict(r) for r in self.rules],
                "connections": sorted((c.src, c.dst, c.port) for c in self.connections),
                "bytes": [[src, dst, port, n] for (src, dst, port), n in sorted(self.bytes.items())]}

    # -- data path --

    def effective(self, src: str, dst: str, port: int) -> Effective:
        return Effective.of(self.rules, src, dst, port)

    def refused(self, src: str, dst: str, port: int) -> bool:
        """Whether the connections between ``src`` and ``dst`` on ``port`` are reset, either way, with their loss probability."""
        for a, b in ((src, dst), (dst, src)):
            reset_loss = self.effective(a, b, port).reset_loss
            if reset_loss and (reset_loss == 100 or self.rng.randrange(100) < reset_loss):
                return True
        return False

    async def serve(self, node: str, port: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        src = peer[0] if peer else "?"
        generation = self.generation
        if self.refused(src, node, port):
            self.log.info("refused %s -> %s:%d", src, node, port)
            writer.transport.abort()
            return
        try:
            up_reader, up_writer = await asyncio.open_connection(node, port)
        except OSError as exc:
            self.log.debug("%s -> %s:%d: upstream unreachable (%s)", src, node, port, exc)
            writer.transport.abort()
            return
        conn = Connection(src, node, port, writer, up_writer)
        # The rules may have changed while the upstream connection was being opened, and
        # set_rules() did not see this connection then: check it against the new ones.
        if generation != self.generation and self.refused(src, node, port):
            self.log.info("refused %s -> %s:%d (rules changed while connecting)", src, node, port)
            conn.close()
            return
        self.connections.add(conn)
        self.log.debug("opened %s -> %s:%d", src, node, port)
        try:
            await asyncio.gather(self.pump(conn, reader, up_writer, src, node),
                                 self.pump(conn, up_reader, writer, node, src))
        finally:
            self.connections.discard(conn)
            # Both flows ended with an EOF: let the buffered bytes out. Otherwise one of them
            # failed, or a rule reset the connection, and it is closed already.
            conn.close(graceful=not conn.closed)
            self.log.debug("closed %s -> %s:%d", src, node, port)

    async def pump(self, conn: Connection, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                   src: str, dst: str) -> None:
        """Carry the flow ``src`` -> ``dst`` of one connection through its rules.

        A reader task queues the chunks with the time each one is due, a writer
        task delivers them in order: a delay does not throttle the flow, a stall
        or a cap holds every later chunk back as TCP would.

        An EOF is passed on as an EOF, so the other direction carries on (a
        half-closed connection still gets its answer); an error resets the whole
        connection, as the peer's reset would.
        """
        queue: asyncio.Queue[tuple[float, bytes] | None] = asyncio.Queue(QUEUE_CHUNKS)
        flow = (src, dst, conn.port)

        async def read() -> None:
            try:
                while True:
                    data = await reader.read(CHUNK)
                    if not data:
                        break
                    eff = self.effective(src, dst, conn.port)
                    due = time.monotonic()
                    if eff.delay_ms:
                        due += (eff.delay_ms + self.rng.uniform(-eff.jitter_ms, eff.jitter_ms)) / 1000
                    await queue.put((due, data))
            except (OSError, asyncio.IncompleteReadError):
                conn.close()
            # The writer consumes until it sees None, unless it has given up, in which
            # case this task is cancelled: the put cannot block forever.
            await queue.put(None)

        async def write() -> None:
            while (item := await queue.get()) is not None:
                due, data = item
                await asyncio.sleep(max(0.0, due - time.monotonic()))
                while (eff := self.effective(src, dst, conn.port)).action == "drop" and not conn.closed:
                    await asyncio.sleep(0.05)
                if conn.closed:
                    return
                if eff.loss and self.rng.randrange(100) < eff.loss:
                    stall = REPAIR_S
                    while True:
                        await asyncio.sleep(stall)
                        if self.rng.randrange(100) >= eff.loss:
                            break
                        stall = RTO_FIRST_S if stall < RTO_FIRST_S else min(stall * 2, RTO_MAX_S)
                # A cap paces the bytes before they leave, in slices of a twentieth of a
                # second, so that a single chunk, however large, is throttled too.
                step = max(1024, eff.kbps * 1024 // 20) if eff.kbps else len(data)
                for start in range(0, len(data), step):
                    piece = data[start:start + step]
                    if eff.kbps:
                        await asyncio.sleep(len(piece) / (eff.kbps * 1024))
                    if conn.closed:
                        return
                    writer.write(piece)
                    await writer.drain()
                    self.bytes[flow] = self.bytes.get(flow, 0) + len(piece)
            if not conn.closed and writer.can_write_eof():
                writer.write_eof()

        reading = asyncio.create_task(read())
        try:
            await write()
        except (OSError, RuntimeError):
            conn.close()
        finally:
            reading.cancel()
            # Waits for the reader without swallowing a cancellation of this task.
            await asyncio.gather(reading, return_exceptions=True)


async def control(proxy: Proxy, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """One control request: a JSON object on a line, a JSON answer on a line."""
    try:
        line = await reader.readline()
        if not line:
            return
        request = json.loads(line)
        op = request.get("op")
        if op == "ping":
            reply: dict[str, Any] = {}
        elif op == "add_node":
            await proxy.add_node(request["node"], request["proxy"], request.get("ports", INTERNODE_PORTS))
            reply = {}
        elif op == "remove_node":
            await proxy.remove_node(request["node"])
            reply = {}
        elif op == "rules":
            reply = {"closed": proxy.set_rules([Rule(**r) for r in request["rules"]])}
        elif op == "state":
            reply = proxy.state()
        else:
            reply = {"error": f"unknown op {op!r}"}
    except Exception as exc:  # noqa: BLE001 - reported to the caller
        proxy.log.exception("control request failed")
        reply = {"error": f"{type(exc).__name__}: {exc}"}
    writer.write((json.dumps(reply) + "\n").encode())
    try:
        await writer.drain()
    finally:
        writer.close()


async def serve(host: str, port: int) -> None:
    proxy = Proxy()
    server = await asyncio.start_server(lambda r, w: control(proxy, r, w), host=host, port=port)
    proxy.log.info("control on %s:%d", host, port)
    # The parent keeps our stdin open; when it goes away, so do we.
    await asyncio.to_thread(sys.stdin.buffer.read)
    proxy.log.info("stdin closed, exiting")
    server.close()
    for conn in list(proxy.connections):
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="network proxy of a test.py cluster")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=CONTROL_PORT)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(serve(args.host, args.port))


# ---------------------------------------------------------------------------
# The client side: in the harness.
# ---------------------------------------------------------------------------

class ProxyServer:
    """The proxy process of one cluster and its control connection.

    Each node's proxy listens on a separate leased address, on the internode and
    CQL ports, with the node's real address as the upstream; the proxy's address
    is what the node advertises in ``broadcast_address`` and ``broadcast_rpc_address``.
    Requests run in threads,
    so they work from whichever event loop calls them.
    """

    def __init__(self, host: IPAddress, port: int, process: subprocess.Popen, log_file):
        self.host = host
        self.port = port
        self._process = process
        self._log_file = log_file
        # Real address -> advertised (proxy) address of every node added so far, for the logs.
        self.addresses: dict[IPAddress, IPAddress] = {}

    @classmethod
    async def start(cls, host: IPAddress, vardir: pathlib.Path, port: int = CONTROL_PORT,
                    name: Optional[str] = None) -> ProxyServer:
        """Start the proxy process with its control port on ``host:port``, logging under ``vardir``
        (``netproxy-<name>.log``, the host by default)."""
        log_path = vardir / f"netproxy-{name or host}.log"
        log_file = log_path.open("wb")
        repo_root = pathlib.Path(__file__).resolve().parents[2]
        process = subprocess.Popen([sys.executable, "-m", "test.pylib.network_proxy", "--host", host, "--port", str(port)],
                                   cwd=repo_root, stdin=subprocess.PIPE, stdout=log_file, stderr=subprocess.STDOUT)
        server = cls(host, port, process, log_file)
        deadline = time.time() + 30
        while True:
            if process.poll() is not None:
                log_file.close()
                raise RuntimeError(f"the network proxy exited with {process.returncode}, see {log_path}")
            try:
                await server._request({"op": "ping"})
                logger.info("Started the network proxy on %s:%d (pid %d), log in %s", host, port, process.pid, log_path)
                return server
            except OSError as exc:
                if time.time() > deadline:
                    server.stop()
                    raise RuntimeError(f"the network proxy on {host}:{port} did not come up: {exc}") from exc
            await asyncio.sleep(0.05)

    def stop(self) -> None:
        """Close the process's stdin, which makes it exit; kill it if it does not."""
        if self._process.poll() is None:
            if self._process.stdin:
                self._process.stdin.close()
            try:
                self._process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
        self._log_file.close()

    async def _request(self, request: dict[str, Any]) -> Any:
        """One control call; raises RuntimeError on an error reply and OSError when unreachable."""

        def call() -> Any:
            with socket.create_connection((self.host, self.port), timeout=10) as sock:
                sock.sendall((json.dumps(request) + "\n").encode())
                data = b""
                while not data.endswith(b"\n"):
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    data += chunk
            return json.loads(data) if data else {}

        reply = await asyncio.to_thread(call)
        if "error" in reply:
            raise RuntimeError(f"network proxy: {reply['error']}")
        return reply

    async def add_node(self, node: IPAddress, proxy_addr: IPAddress, ports: Iterable[int] = PROXIED_PORTS) -> None:
        """Create the proxy for ``node``: ``proxy_addr`` in front of ``node`` on the internode and CQL ports.

        One left from a server that was replaced on the same address is replaced too.
        """
        await self._request({"op": "add_node", "node": node, "proxy": proxy_addr, "ports": list(ports)})
        self.addresses[node] = proxy_addr
        logger.info("Proxy %s -> %s on ports %s; the nodes advertise (real -> advertised): %s",
                    proxy_addr, node, list(ports), dict(self.addresses))

    async def set_rules(self, rules: list[Rule]) -> int:
        return (await self._request({"op": "rules", "rules": [asdict(r) for r in rules]}))["closed"]

    async def state(self) -> dict[str, Any]:
        return await self._request({"op": "state"})


class NetworkProxy:
    """Cuts and degradations of the links between nodes, as rules on their proxies.

    The nodes are named by their real addresses (``ServerInfo.ip_addr``); ``address_of``
    gives the address the peers know a node by, which is what the gossiper reports for it.

    ``ports`` can name the internode ports (the default) and the CQL ports, or be None
    for all of them; the REST API is not proxied and never cut. ``mode`` is what the
    other node observes: ``drop`` withholds the bytes, so requests hang until they time
    out, like packets lost on the way; ``reset`` closes the connections and refuses new
    ones, like a TCP reset. ``loss`` below 100 turns a drop into TCP-like stalls per
    chunk, with that probability, and a reset into a refusal of that share of the
    connections.

    Each call adds to the rules in place; ``heal`` removes them all. The rule set is
    replaced as a whole on the proxy, so a change takes effect at once on every
    connection it covers.
    """

    def __init__(self, server: ProxyServer):
        self.server = server
        self._rules: list[Rule] = []

    @staticmethod
    def _addresses(addrs: IPAddress | Iterable[IPAddress]) -> list[str]:
        return [str(addrs)] if isinstance(addrs, str) else [str(a) for a in addrs]

    @staticmethod
    def _ports(ports: int | Iterable[int] | None) -> list[int] | None:
        """The port set of a rule: None for the internode ports, a list otherwise (None given: every proxied port)."""
        if ports is None:
            return list(PROXIED_PORTS)
        port_set = {ports} if isinstance(ports, int) else set(ports)
        if not port_set <= set(PROXIED_PORTS):
            raise NotImplementedError(f"only the ports {PROXIED_PORTS} go through the proxy, not {sorted(port_set)}")
        return None if port_set == set(INTERNODE_PORTS) else sorted(port_set)

    @staticmethod
    def _check_mode_and_loss(mode: str, loss: int) -> None:
        if mode not in ("drop", "reset"):
            raise ValueError(f"mode must be drop or reset, got {mode!r}")
        if not 0 < loss <= 100:
            raise ValueError(f"loss must be in 1..100 percent, got {loss}")

    async def _push(self, rules: Iterable[Rule]) -> None:
        self._rules.extend(rules)
        closed = await self.server.set_rules(self._rules)
        logger.info("Network proxy rules: %s (closed %d connections)", [asdict(r) for r in self._rules], closed)

    async def addresses(self) -> dict[IPAddress, IPAddress]:
        """Every proxied node's real address -> the address it advertises to its peers.

        Kept by the harness as it adds the nodes, so this costs no call to the proxy.
        """
        return dict(self.server.addresses)

    async def address_of(self, node: IPAddress) -> IPAddress:
        """The address ``node`` advertises to its peers, i.e. the address of its proxy."""
        nodes = await self.addresses()
        if node not in nodes:
            raise ValueError(f"{node} is not a proxied node; the proxied ones are {sorted(nodes)}")
        return nodes[node]

    @staticmethod
    def _cut_rule(src: str, dst: str, mode: str, loss: int, ports: list[int] | None = None) -> Rule:
        if loss == 100:
            return Rule(src, dst, action=mode, ports=ports)
        if mode == "reset":
            return Rule(src, dst, action="reset", loss=loss, ports=ports)
        return Rule(src, dst, action="pass", loss=loss, ports=ports)

    async def cut(self, src: IPAddress | Iterable[IPAddress], dst: IPAddress | Iterable[IPAddress], *,
                  ports: int | Iterable[int] | None = INTERNODE_PORTS, mode: CutMode = "drop", loss: int = 100) -> None:
        """Cut the flow from every ``src`` to every ``dst``, the other way untouched.

        With ``drop`` the bytes from ``src`` to ``dst`` are withheld on every connection
        between them, whoever opened it, as packets from ``src`` to ``dst`` would be lost:
        neither side's requests to the other complete. With ``reset`` the connections
        between them are closed and refused: a reset cannot be one-way.
        """
        port_set = self._ports(ports)
        self._check_mode_and_loss(mode, loss)
        srcs, dsts = self._addresses(src), self._addresses(dst)
        logger.info("Cutting %s -> %s (%s, loss %d%%, ports %s)", srcs, dsts, mode, loss, port_set or "internode")
        await self._push(self._cut_rule(s, d, mode, loss, port_set) for s in srcs for d in dsts if s != d)

    async def partition(self, side_a: IPAddress | Iterable[IPAddress], side_b: IPAddress | Iterable[IPAddress], *,
                        ports: int | Iterable[int] | None = INTERNODE_PORTS, mode: CutMode = "drop", loss: int = 100) -> None:
        """Cut every flow between the two sides, both ways; nodes within a side stay connected."""
        port_set = self._ports(ports)
        self._check_mode_and_loss(mode, loss)
        a, b = self._addresses(side_a), self._addresses(side_b)
        logger.info("Partitioning %s | %s (%s, loss %d%%, ports %s)", a, b, mode, loss, port_set or "internode")
        await self._push([self._cut_rule(s, d, mode, loss, port_set) for s in a for d in b if s != d]
                         + [self._cut_rule(d, s, mode, loss, port_set) for s in a for d in b if s != d])

    async def isolate(self, node: IPAddress, peers: Iterable[IPAddress], *,
                      ports: int | Iterable[int] | None = INTERNODE_PORTS, mode: CutMode = "drop", loss: int = 100) -> None:
        """Cut ``node`` from each of ``peers``, both ways. The peers stay connected to each other."""
        await self.partition(node, peers, ports=ports, mode=mode, loss=loss)

    async def block_clients(self, node: IPAddress, *, ports: Iterable[int] = CQL_PORTS, mode: CutMode = "drop",
                            loss: int = 100) -> None:
        """Cut the clients off from ``node``'s CQL ports, whoever they are; its peers and the REST API still reach it.

        ``drop`` leaves the clients' requests hanging, ``reset`` closes their connections and
        refuses new ones, so the driver marks the node down at once.
        """
        port_set = sorted({ports} if isinstance(ports, int) else set(ports))
        if not set(port_set) <= set(CQL_PORTS):
            raise NotImplementedError(f"only the CQL ports {CQL_PORTS} are proxied for clients, not {port_set}")
        self._check_mode_and_loss(mode, loss)
        logger.info("Blocking the clients of %s on ports %s (%s, loss %d%%)", node, port_set, mode, loss)
        await self._push([self._cut_rule(ANY, str(node), mode, loss, port_set)])

    async def blackout(self, node: IPAddress, *, mode: CutMode = "drop", loss: int = 100) -> None:
        """Cut ``node`` from every peer, both ways, and from its clients; only the REST API still reaches it."""
        self._check_mode_and_loss(mode, loss)
        logger.info("Blacking out %s from every peer and client (%s, loss %d%%)", node, mode, loss)
        await self._push([self._cut_rule(str(node), ANY, mode, loss), self._cut_rule(ANY, str(node), mode, loss),
                          self._cut_rule(ANY, str(node), mode, loss, list(CQL_PORTS))])

    @staticmethod
    def _flows(node: str, peers: list[str], direction: Direction) -> list[tuple[str, str]]:
        """The flows a degradation of ``node`` applies to: ``upstream`` is what the peers send to it,
        ``downstream`` what it sends to them, ``both`` the two."""
        if direction not in ("upstream", "downstream", "both"):
            raise ValueError(f"direction must be upstream, downstream or both, got {direction!r}")
        flows = []
        for peer in peers:
            if direction in ("upstream", "both"):
                flows.append((peer, node))
            if direction in ("downstream", "both"):
                flows.append((node, peer))
        return flows

    async def delay(self, node: IPAddress, ms: int, jitter_ms: int = 0, *, peers: Iterable[IPAddress] | None = None,
                    direction: Direction = "both") -> None:
        """Delay the bytes between ``node`` and ``peers`` (every peer by default) by ``ms`` +/- ``jitter_ms``,
        per direction: a request to the node and its answer each pay it once."""
        if ms < 0 or jitter_ms < 0 or jitter_ms > ms:
            raise ValueError(f"need 0 <= jitter_ms <= ms, got ms={ms}, jitter_ms={jitter_ms}")
        flows = self._flows(str(node), [ANY] if peers is None else self._addresses(peers), direction)
        logger.info("Delaying %s by %dms +/- %dms (%s)", node, ms, jitter_ms, direction)
        await self._push(Rule(s, d, delay_ms=ms, jitter_ms=jitter_ms) for s, d in flows)

    async def bandwidth(self, node: IPAddress, kbps: int, *, peers: Iterable[IPAddress] | None = None,
                        direction: Direction = "both") -> None:
        """Cap the bytes between ``node`` and ``peers`` at ``kbps`` kilobytes per second, per direction."""
        if kbps <= 0:
            raise ValueError(f"kbps must be positive, got {kbps}")
        flows = self._flows(str(node), [ANY] if peers is None else self._addresses(peers), direction)
        logger.info("Capping %s at %d kB/s (%s)", node, kbps, direction)
        await self._push(Rule(s, d, kbps=kbps) for s, d in flows)

    async def heal(self) -> None:
        """Remove every rule: stalled bytes are delivered, refused connections are accepted again."""
        logger.info("Healing: removing every network proxy rule")
        self._rules = []
        await self.server.set_rules([])


if __name__ == "__main__":
    main()
