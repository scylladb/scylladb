=========================
Run ScyllaDB in Docker
=========================


Each ScyllaDB version is available as a Docker image you can use to create
a ScyllaDB container.

Running ScyllaDB in Docker is the simplest way to experiment with ScyllaDB in
non-production environments. In production environments, additional tuning is
required to run a stateful container and maximize performance; follow
:doc:`Best Practices for Running ScyllaDB on Docker </operating-scylla/procedures/tips/best-practices-scylla-on-docker>`.

Prerequisites
---------------

#. Download and install Docker from the `Docker website <https://docs.docker.com/get-docker/>`_.
#. Download the ScyllaDB image from `DockerHub <https://hub.docker.com/r/scylladb/scylla>`_
   (the latest stable ScyllaDB version will be downloaded):
   
   .. code::
    
    docker pull scylladb/scylla

Examples
-----------

To start a one-node ScyllaDB instance:

.. code::
    
    docker run --name scylla -d scylladb/scylla

To add more nodes:

.. code::
    
    docker run --name scylla-node2 -d scylladb/scylla --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla)"

.. code::
    
    docker run --name scylla-node3 -d scylladb/scylla --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla)"

To verify that the cluster is up and running:

.. code::
    
    docker exec -it scylla nodetool status

To start cqlsh to interact with ScyllaDB:

.. code::
    
    docker exec -it scylla cqlsh


Useful Resources
-------------------

* `Documentation for the ScyllaDB Docker image <https://hub.docker.com/r/scylladb/scylla>`_ on DockerHub.
* :doc:`Best Practices for Running ScyllaDB on Docker </operating-scylla/procedures/tips/best-practices-scylla-on-docker>`
  in the ScyllaDB documentation.
* `Quick Wins Lab <https://university.scylladb.com/courses/scylla-essentials-overview/lessons/quick-wins-install-and-run-scylla/topic/quick-wins-lab/>`_
  using Docker on ScyllaDB University.
* `ScyllaDB on Docker <https://www.scylladb.com/2016/11/09/scylla-on-docker/>`_ blog post.






