Stopping a local repair
=======================

Issuing a stop to a running nodetool repair does not cause an immediate repair stop.


Issue description
-----------------

I have started a local repair using ``nodetool repair``

I have decided to stop the repair by issuing 

.. code-block:: sh

   curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' 'http://localhost:10000/storage_service/force_terminate_repair'


Checking the repair status, I still see the repair running:

.. code-block:: sh

  curl -X GET --header 'Accept: application/json' 'http://localhost:10000/storage_service/active_repair/'
  [1]


Resolution
----------

**IMPORTANT:** Abort via the http rest api sets the abort flag and the abort does not happen immediately. The repair checks for the abort flag at the beginning of each step, e.g., negotiate_sync_boundary, get_missing_rows_from_follower_nodes, send_missing_rows_to_follower_nodes.

Each step might take a while, so if you abort and check again immediately, the repair in question might still be in RUNNING state.

Please check if the aborted repair stays in RUNNING forever before forcing a stop.

*Force a repair stop:*

.. code-block:: sh

   curl -X POST "http://127.0.0.2:10000/storage_service/force_terminate_repair"


**NOTE:** If you are using `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_ for repairs, a simple stop command via sctool already implements all the needed logic to gracefully stop a repair.
