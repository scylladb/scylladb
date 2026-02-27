.. tabs::

   .. group-tab:: Supported OS

      .. code-block:: shell
                            
         sudo systemctl start scylla-server

   .. group-tab:: Docker

      .. code-block:: shell

         docker exec -it some-scylla supervisorctl start scylla

      (with *some-scylla* container already running)
               
