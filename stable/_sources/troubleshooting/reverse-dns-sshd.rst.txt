ScyllaDB Manager: connection to sshd server is slow or timing out
===================================================================

This troubleshooting guide describes what to do if you experience slow ScyllaDB
Manager behavior or when connections to ScyllaDB nodes over SSH are timing out.

Phenomenon
^^^^^^^^^^

This might affect users of the ScyllaDB Manager when determining REST API status
of the managed clusters. ScyllaDB Manager Client might report certain nodes as
being down even if they are accessible.

Background
^^^^^^^^^^

ScyllaDB Manager manages the ScyllaDB nodes over the HTTP API. Communication
between ScyllaDB Manager server and ScyllaDB nodes is encrypted by tunneling HTTP
traffic over an SSH connection.

Establishing an SSH tunnel requires that ScyllaDB nodes have a running sshd
server and optionally the sshd server can be configured to do reverse DNS
for resolving client IPs.
When resolving takes a long time or it stalls for some reason then connections
and interaction with the sshd server can be impaired.

Solution
^^^^^^^^

There are two options for solving this.

One option is to improve your DNS setup on the ScyllaDB node by changing to a
better DNS resolver. It is recommended to use static nameserver IPs from
`Cloudflare <https://www.cloudflare.com/learning/dns/what-is-1.1.1.1/>`_
or `Google <https://developers.google.com/speed/public-dns/>`_.

Another option is to disable reverse DNS usage for the sshd server. This can
be done by navigating to the sshd configuration file (located in
/etc/ssh/sshd_config on Unix based systems) and setting the UseDNS
configuration option to off.
