.. tabs::

   .. group-tab:: Supported OS

      .. code-block:: shell

         sudo systemctl restart scylla-server

   .. group-tab:: Docker

      .. code-block:: shell

         docker exec -it some-scylla supervisorctl restart scylla

      (without restarting *some-scylla* container)
