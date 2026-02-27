.. tabs::

   .. group-tab:: Supported OS

      .. code-block:: shell

         sudo systemctl stop scylla-server

   .. group-tab:: Docker

      .. code-block:: shell

         docker exec -it some-scylla supervisorctl stop scylla

      (without stopping *some-scylla* container)
