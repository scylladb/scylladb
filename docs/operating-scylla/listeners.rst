==================
Protocol Listeners
==================

A ScyllaDB node serves its clients over a set of *listeners*: TCP sockets, each
carrying one protocol, on one address and port, with its own encryption and
proxy-protocol settings. The ``listeners`` :doc:`cluster config </cql/cluster-config>`
option describes that set directly.

It is cluster config rather than a ``scylla.yaml`` setting because a set of
listeners is something you change on a cluster that is already running, for one
node or for all of them, without editing a file on every machine.

.. code-block:: cql

   ALTER CLUSTER WITH listeners = {
       cql: {protocol: 'cql', port: 9042, shard_aware_listener: 'cql_shard_aware'},
       cql_shard_aware: {protocol: 'cql', port: 19042, shard_aware: true},
       cql_ssl: {protocol: 'cql', port: 9142,
                 tls: {certificate: '/etc/scylla/db.crt', keyfile: '/etc/scylla/db.key'}},
       alternator: {protocol: 'alternator', port: 8000, proxy_protocol: true}};

The value is a map of listener name to listener, written as a CQL literal: names
need no quotes, string values do, and numbers, ``true`` and ``false`` do not. It
can also be given as a string holding the same map as YAML in flow style, where
nothing needs quoting:

.. code-block:: cql

   ALTER CLUSTER WITH listeners = '{cql: {protocol: cql, port: 9042}}';

ScyllaDB rewrites the value into a canonical form when it is stored, so two
spellings of the same set of listeners are the same value.

Scopes
======

``listeners`` is a node-oriented option, so it can be set at any of four scopes
and the narrowest one that has a value wins:

.. code-block:: cql

   ALTER CLUSTER WITH listeners = {...};
   ALTER DATACENTER dc1 WITH listeners = {...};
   ALTER RACK dc1 rack1 WITH listeners = {...};
   ALTER NODE <host_id> WITH listeners = {...};

Setting the option to ``NULL`` at a scope removes the value stored there, so the
next scope out takes over again. All four require superuser.

Because a listener's ``address`` defaults to ``rpc_address`` (or
``alternator_address`` for Alternator), a value set at ``CLUSTER`` scope that
leaves ``address`` out works on every node, whatever address each of them
listens on. Spell an address out only at ``NODE`` scope, where it means one
particular machine.

Relationship to the per-protocol options
========================================

If ``listeners`` resolves to a **non-empty** value, it is the sole source of
truth, and the ``scylla.yaml`` per-protocol options are ignored entirely:
``native_transport_port``, ``native_transport_port_ssl``,
``native_shard_aware_transport_port``, ``native_shard_aware_transport_port_ssl``,
their ``_proxy_protocol`` variants, ``alternator_port``,
``alternator_https_port`` and ``alternator_port_proxy_protocol``.

A cluster that has never set ``listeners`` resolves it to an empty value, and so
keeps the listeners those options give it. Both configurations are supported,
but a given node uses one or the other — they are never merged.

Listener names
==============

A listener's name is what *other listeners refer to it by*. It is otherwise
arbitrary and carries no meaning: the node never derives behaviour from it, and
clients never see it.

The reason listeners are a named map rather than a plain list is that one
listener sometimes has to point at another. That is what ``shard_aware_listener``
does: a non-shard-aware CQL listener names the shard-aware listener that its
clients should be handed over to, and the node advertises that listener's port
to connecting clients in the CQL ``SUPPORTED`` message
(``SCYLLA_SHARD_AWARE_PORT``, or ``SCYLLA_SHARD_AWARE_PORT_SSL`` when the
shard-aware listener is encrypted). A shard-aware driver then reconnects to that
port, one connection per shard.

Because the ``SUPPORTED`` message carries only a port, and drivers reconnect to
the address they are already connected to, a listener and the shard-aware
listener it names must be on the same address.

Keys
====

Every listener takes the following keys.

.. list-table::
   :widths: 20 15 65
   :header-rows: 1

   * - Key
     - Default
     - Description
   * - ``protocol``
     - *mandatory*
     - The protocol to serve: ``cql`` or ``alternator``.
   * - ``port``
     - *mandatory*
     - The TCP port to listen on. Must not be zero.
   * - ``address``
     - see description
     - The address to listen on, or a network written as ``10.0.0.0/8``, meaning
       whichever address of the node is in it. A network is what makes one value work
       for every node when the option is set for a whole cluster, a datacenter or a
       rack; a node refuses to start when none of its addresses is in that network, or
       when more than one is. Defaults to ``rpc_address`` for ``cql``, and to
       ``alternator_address`` for ``alternator``.
   * - ``shard_aware``
     - ``false``
     - Serve the shard-aware variant of the protocol. CQL only.
   * - ``shard_aware_listener``
     - none
     - The name of the shard-aware listener that this listener's clients are to
       be handed over to, advertised in the CQL ``SUPPORTED`` message. The named
       listener must have ``shard_aware: true`` and must be on the same address.
       When unset, the clients of this listener are not offered a shard-aware
       port at all. CQL only.
   * - ``proxy_protocol``
     - ``false``
     - Expect a `PROXY protocol <https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt>`_
       header on incoming connections. A listener without it rejects connections
       that send one, and vice versa, so a proxied and a direct endpoint need
       separate listeners.
   * - ``tls``
     - ``false``
     - Encrypt the connections with TLS. Either a boolean, selecting the
       protocol's default encryption options (``client_encryption_options`` for
       ``cql``, ``alternator_encryption_options`` for ``alternator``), or a map
       spelling the encryption options out for this listener alone.
   * - ``keepalive``
     - ``rpc_keepalive``
     - Enable TCP keepalive. CQL only — see :ref:`listeners-keepalive` below.

Per-listener TLS
================

``tls: true`` reuses the protocol-wide encryption options. Spelling the options
out instead gives that listener its own certificate, key and trust settings, so
one node can serve, say, an internal listener with a private CA and an external
one with a public certificate:

.. code-block:: cql

   ALTER NODE <host_id> WITH listeners = {
       cql_internal: {protocol: 'cql', address: '10.0.0.5', port: 9142,
                      tls: {certificate: '/etc/scylla/internal.crt',
                            keyfile: '/etc/scylla/internal.key',
                            truststore: '/etc/scylla/internal-ca.pem'}},
       cql_external: {protocol: 'cql', address: '203.0.113.7', port: 9142,
                      tls: {certificate: '/etc/scylla/public.crt',
                            keyfile: '/etc/scylla/public.key'}}};

Listeners that spell out an identical set of options share one set of
credentials, and certificate files are reloaded in place when they change on
disk, without a restart.

The ``enabled`` key of ``client_encryption_options`` and
``alternator_encryption_options`` has no effect here: whether a listener is
encrypted is decided by its own ``tls`` key, so ``enabled`` is ignored when those
options are used as a listener's TLS settings.

.. _listeners-keepalive:

Limitations
===========

``keepalive`` is accepted for CQL listeners only. Alternator listeners are
served by an HTTP server whose keepalive setting applies to the server as a
whole rather than to an individual listener, so a per-listener value cannot be
honoured; setting ``keepalive`` on an ``alternator`` listener is rejected at
startup rather than silently ignored.

``shard_aware`` and ``shard_aware_listener`` are likewise CQL-only, since
Alternator has no shard-aware variant.

Applying changes
================

Changes to the listeners take effect on a running node, without a restart, for
CQL and Alternator alike. A listener that did not change is left alone, so its
clients notice nothing. A listener that was removed is drained: its connections
receive no further requests, the requests already in flight are answered, and
the connections are then closed. A listener whose settings changed is drained
the same way and reopened with its new settings. The whole new set is resolved
before anything is touched - addresses looked up, certificates read - so a set
that cannot be served leaves the node serving the set it had.
