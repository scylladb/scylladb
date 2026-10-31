NTP Configuration for ScyllaDB
==============================
**Topic: System administration**

**Learn: How to configure time synchronization for ScyllaDB**

**Audience: ScyllaDB and Apache Cassandra administrators**

Apache Cassandra and ScyllaDB depend on an accurate system clock. Kyle Kingsbury,
author of the ``jepsen`` distributed systems testing tool,
`writes <https://aphyr.com/posts/299-the-trouble-with-timestamps>`_,

    Apache Cassandra uses wall-clock timestamps provided by the server, or
    optionally by the client, to order writes. It makes several
    guarantees about the monotonicity of writes and reads given
    timestamps. For instance, Cassandra guarantees most of the time that
    if you write successfully to a quorum of nodes, any subsequent read
    from a quorum of nodes will see that write or one with a greater
    timestamp.

So servers need to keep their time in sync. Not a hard problem, since we
all have NTP on our Linux systems, right? Not quite. The way that NTP
ships out of the box is fine for a stand-alone server, but can be a
problem for a distributed data store.

You did WHAT in the Pool?
-------------------------

The default NTP configuration that comes with a typical Linux system
uses “NTP pools”, lists of publicly available time servers contributed
by public-minded Internet timekeeping system administrators. The pools
are a valuable service, but in order to spare the NTP traffic load on
any given server, they’re managed with DNS round robin. One client that
tries to resolve the hostname ``0.pool.ntp.org`` will get a different
result from another client.

As Viliam Holub points out in a two-part series -- `part
1 <https://blog.logentries.com/2014/03/synchronizing-clocks-in-a-cassandra-cluster-pt-1-the-problem/>`__,
`part
2 <https://blog.logentries.com/2014/03/synchronizing-clocks-in-a-cassandra-cluster-pt-2-solutions/>`__
-- if Apache Cassandra nodes in a cluster are independently obtaining their
time from random pool servers out on the Internet, the chances that two
nodes can have widely (by NTP standards) differing time is high. For
example, if a cluster has 10 nodes, 50% of the time some pair of nodes
will have time that differs by more than 10.9ms. The problem only grows
as more nodes are added.

The solution is to be able to take that ntp.conf file that came with
your Linux distribution, and take the default “pool” servers out and put
your data center’s own NTP servers in.

Instead of lines that looks something like:

::

    server 0.fedora.pool.ntp.org iburst
    server 1.fedora.pool.ntp.org iburst

Or

::

    server 0.debian.pool.ntp.org iburst
    server 1.debian.pool.ntp.org iburst

use your own servers. So ntp.conf will have “server” lines pointing to
your own NTP servers, and look more like:

::

    # begin ntp.conf

    # Store clock drift -- see ntp.conf(5)
    driftfile /var/lib/ntp/drift

    # Restrict all access by default
    restrict default nomodify notrap nopeer noquery

    # Allow localhost access and LAN management
    restrict 127.0.0.1
    restrict ::1
    restrict 192.168.1.0 mask 255.255.255.0 nomodify notrap

    # Use our company’s NTP servers only
    server 0.ntp.example.com iburst
    server 1.ntp.example.com iburst
    server 2.ntp.example.com iburst

    # End ntp.conf

The same ntp.conf can be deployed to all the servers in your data
center. Not just Apache Cassandra nodes, but the application servers that use
them. It’s much more important for the time to be in sync throughout the
cluster than for any node to match some random machine out on the
Internet. It’s also helpful to keep the data store time the same as the
application server’s time, for ease in troubleshooting and matching up
log entries.

Dedicated NTP appliances are available, and might be a good choice for
large sites. Otherwise, any standard Linux system should make a good NTP
server.

On the NTP servers, you can go ahead and use the “pool.ntp.org” server
lines that shipped with your Linux distribution if you don’t have a
known good time server. But a good hosting provider or business-class
ISP probably has NTP servers that are close to you on the network, and
that would be better choices to replace the pool entries.

Your NTP servers should peer with each other:

::

    peer 0.ntp.example.com prefer
    peer 1.ntp.example.com
    peer 2.ntp.example.com

Almost done.

Pass the Fudge?
---------------

What happens when the network goes down? In most cases, NTP should just
work. Your NTP servers will establish a new consensus time among
themselves. Old-school NTP documentation had “fudge” lines to let the
NTP server rely on the local system clock if the network connection
failed. On modern versions of NTP, the “fudge” functionality has been
replaced with `Orphan
mode <http://support.ntp.org/bin/view/Support/OrphanMode>`__.

Add an “orphan” line to ntp.conf on each NTP server:

::

    tos orphan 9

And the NTP servers will do the right thing and stay synchronized among
themselves if there’s a problem reaching the servers on the outside.

That’s all it takes. One relatively simple system administration project
can save a bunch of troubleshooting grief later on. Once your NTP
servers are working, have a look at the `instructions for joining the
NTP pool <http://www.pool.ntp.org/join.html>`__ yourself, so that you
can help share the correct time with others

:doc:`Knowledge Base </kb/index>`


.. include:: /rst_include/apache-copyrights.rst
