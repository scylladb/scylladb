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
nothing needs quoting. This sets the same listeners as above:

.. code-block:: cql

   ALTER CLUSTER WITH listeners = '{
       cql: {protocol: cql, port: 9042, shard_aware_listener: cql_shard_aware},
       cql_shard_aware: {protocol: cql, port: 19042, shard_aware: true},
       cql_ssl: {protocol: cql, port: 9142,
                 tls: {certificate: /etc/scylla/db.crt, keyfile: /etc/scylla/db.key}},
       alternator: {protocol: alternator, port: 8000, proxy_protocol: true}}';

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
     - The protocol to serve: ``cql``, ``alternator`` or ``webshell`` (the
       :doc:`Web Shell </operating-scylla/admin-tools/webshell>`).
   * - ``port``
     - *mandatory*
     - The TCP port to listen on. Must not be zero.
   * - ``address``
     - see description
     - The address to listen on, or a network written as ``10.0.0.0/8``, meaning
       whichever address of the node is in it. A network is what makes one value work
       for every node when the option is set for a whole cluster, a datacenter or a
       rack; a node refuses to start when none of its addresses is in that network, or
       when more than one is. Defaults to ``rpc_address`` for ``cql``, to
       ``alternator_address`` for ``alternator``, and to ``localhost`` for
       ``webshell``.
   * - ``shard_aware``
     - ``false``
     - Serve the shard-aware variant of the protocol. CQL only.
   * - ``shard_aware_listener``
     - none
     - The name of the shard-aware listener that this listener's clients are to
       be handed over to, advertised in the CQL ``SUPPORTED`` message. The named
       listener must have ``shard_aware: true``, must be on the same address, and
       must have the same ``proxy_protocol`` setting.
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
       ``cql`` and ``webshell``, ``alternator_encryption_options`` for
       ``alternator``), or a map
       spelling the encryption options out for this listener alone. An option
       name the node does not know is rejected.
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
                            truststore: '/etc/scylla/internal-ca.pem',
                            require_client_auth: 'true'}},
       cql_external: {protocol: 'cql', address: '203.0.113.7', port: 9142,
                      tls: {certificate: '/etc/scylla/public.crt',
                            keyfile: '/etc/scylla/public.key'}}};

A ``tls`` map replaces the protocol-wide encryption options as a whole: nothing
is inherited from them, so an option they set, such as ``require_client_auth``,
has to be repeated in the map for the listener to keep it. In the example, the
internal listener checks client certificates against its private CA only
because it asks for ``require_client_auth``; a ``truststore`` alone does not
make the listener ask clients for a certificate.

Listeners that spell out an identical set of options share one set of
credentials, and certificate files are reloaded in place when they change on
disk, without a restart.

The ``enabled`` key of ``client_encryption_options`` and
``alternator_encryption_options`` has no effect here: whether a listener is
encrypted is decided by its own ``tls`` key, so ``enabled`` is ignored when those
options are used as a listener's TLS settings, and rejected in a ``tls`` map
spelled out for one listener.

.. _listeners-keepalive:

Limitations
===========

``keepalive`` is accepted for CQL listeners only. Alternator and Web Shell
listeners are served by an HTTP server whose keepalive setting applies to the
server as a whole rather than to an individual listener, so a per-listener value
cannot be honoured; setting ``keepalive`` on an ``alternator`` or ``webshell``
listener is rejected at startup rather than silently ignored.

``shard_aware`` and ``shard_aware_listener`` are likewise CQL-only, since
neither Alternator nor the Web Shell has a shard-aware variant.

The Web Shell has no per-protocol options in ``scylla.yaml``: it is served only on
the ``webshell`` listeners of the ``listeners`` option.

Applying changes
================

Changes to the listeners take effect on a running node, without a restart, for
CQL and Alternator alike. A listener that did not change is left alone, so its
clients notice nothing. A listener that was removed is drained: its connections
receive no further requests, the requests already in flight are answered, and
the connections are then closed. A listener whose socket is affected by the
change is drained the same way and reopened with its new settings; other changes
take effect on the open listener. :ref:`listeners-reopen` lists which is which.

Whatever can be checked ahead of time is checked before anything is touched.
Setting the option fails right away if two listeners spell out the same address
and the same port, if two listeners of one protocol both leave the address out
and share a port, or if a ``shard_aware_listener`` names a listener that does
not exist, is not a shard-aware CQL listener, or disagrees about
``proxy_protocol``. Listeners whose addresses are spelled differently, or that
leave it out for different protocols, may still turn out to share an address on
a node; that is only found out when the second one fails to open.

Each node then resolves the new listeners of each protocol - addresses looked
up, certificates read, shard-aware listeners checked to be on the same address -
before touching that protocol's listeners, and if that fails, the node logs the
error and keeps serving that protocol's listeners as they were. The protocols are
resolved and changed one after the other, CQL first, so a change can take effect
for CQL and be rejected for Alternator.

Whether a port can be bound, though, is only known by binding it, and by then
the listeners that were removed or changed have already been closed. So opening
the new set is best effort: a listener that cannot be opened - its port taken by
another process, or its address not one of the node's - is logged as an error
and left out, and the rest of the set is opened regardless. The node does not
retry it until the listeners change again. Choose addresses and ports that the
node can bind.

A protocol that is left with no listeners stops being served altogether, and is
served again once a change gives it a listener. That includes a protocol that had
none when the node started, so Alternator can be enabled on a running node by
adding an ``alternator`` listener. This holds while the option still has a
listener of the other protocol: a change that leaves ``listeners`` with none at
all - set to an empty map, or removed at every scope - brings back the listeners
of the per-protocol options in ``scylla.yaml``, plaintext ones included, rather
than stopping both protocols.

A listener that could not be opened stays in the set the node was asked to
serve. Only the start of a protocol server with the node insists on every
listener opening; when a protocol server is stopped and started again on a
running node, such as with ``nodetool disablebinary`` and
``nodetool enablebinary``, it opens what it can and logs the rest, just as when
the listeners change.

.. _listeners-reopen:

What reopens a listener
-----------------------

A listener is reopened, dropping its connections once they are drained, when
the change alters the socket it listens on, or how that socket accepts
connections:

* its ``address``, as resolved on the node. Spelling the same address
  differently, or giving a network that picks the same address, is not a change;
* its ``port``;
* its ``protocol``;
* ``shard_aware``;
* ``tls``: turning encryption on or off, or changing the encryption options in
  effect - the ``tls`` map spelled out for the listener, or, for ``tls: true``,
  the protocol-wide options as they are when the listeners change. Two ways of arriving at the same options, such as
  ``tls: true`` and a map identical to the protocol-wide options, are not a change;
* ``proxy_protocol``;
* ``keepalive`` (CQL);
* its name (CQL). Renaming a CQL listener removes the listener under its old name
  and adds one under the new name. An Alternator listener is known by its address
  rather than its name, so renaming it changes nothing.

The following take effect without reopening the listener, so its connections
stay open:

* the shard-aware port a CQL listener advertises: the listener named in
  ``shard_aware_listener`` moving to another port or becoming encrypted, or the
  listener naming another shard-aware listener, or none. The new port is offered
  to existing connections and new ones alike;
* certificate, key and truststore files changing on disk: they are reloaded in
  place, and new TLS handshakes use them, while established sessions continue;
* a change to the value that leaves the listeners the same, such as reordered
  keys or a default written out explicitly;
* any change to the other listeners.
