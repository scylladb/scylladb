Consistency Level Console Demo
==============================
In this demo, we'll bring up 3 nodes and demonstrate how writes and reads look, with tracing enabled in a cluster where our :term:`Replication Factor (RF)<Replication Factor (RF)>` is set to **3**.  We'll change the :term:`Consistency Level (CL)<Consistency Level (CL)>` between operations to show how messages are passed between nodes, and finally take down a few nodes to show failure conditions in a ScyllaDB cluster.

You can also learn more in the `High Availability lesson <https://university.scylladb.com/courses/scylla-essentials-overview/lessons/high-availability/>`_ on ScyllaDB University.


Note:  We use asciinema_ to generate the console casts used in this demo.  These asciicasts are more readable than embedded video, and allow you to copy the text or commands directly from the console to your clipboard.   We suggest viewing console casts in **fullscreen** to see the properly formatted output. 

.. _asciinema: https://asciinema.org


Step one:  bring up the cluster
-------------------------------

First, we'll bring up a 3-node docker cluster.  You'll see all the nodes eventually read  ``UN`` for status. ``U`` means **up**, and ``N`` means **normal**.  Read more about :doc:`Nodetool Status </operating-scylla/nodetool-commands/status/>`.

Once all nodes are up, we can run our CQL shell.

.. raw:: html

   <div class="panel callout radius animated asciinema">
            <div class="row">
                <script type="text/javascript" src="https://asciinema.org/a/SQ5KW0bYTjXRWPHVkGY97WD1A.js" id="asciicast-SQ5KW0bYTjXRWPHVkGY97WD1A" data-speed="3" data-rows="30" autoplay=1 async></script>
              </div>
   </div>           

We've run ``nodetool status`` after we've waited for all nodes to come up:

.. code-block:: console
   :class: hide-copy-button

	Datacenter: datacenter1
	=======================
	Status=Up/Down
	|/ State=Normal/Leaving/Joining/Moving
	--  Address     Load       Tokens       Owns (effective)  Host ID                               Rack
	UN  172.17.0.3  127.42 KB  256          65.8%             66a3bfe7-3e90-4859-a394-becc00af1cf2  rack1
	UN  172.17.0.2  110.58 KB  256          65.5%             f66639bf-3259-47f5-8487-602fcb082624  rack1
	UN  172.17.0.4  118.38 KB  256          68.6%             d978ff65-0331-4f27-a129-9be2d7169ad6  rack1


Step two:  take a single node down, check different consistency levels
----------------------------------------------------------------------

So, what happens when we take a single node down?  Here, we'll take a single node down, check status with ``nodetool``, attempt a write with **ALL** and **QUORUM** consistency levels.

.. raw:: html            

   <div class="panel callout radius animated asciinema">
            <div class="row">
                <script type="text/javascript" src="https://asciinema.org/a/KhUpcnsWgpu119psj5aHjac0N.js" id="asciicast-KhUpcnsWgpu119psj5aHjac0N"  data-speed="3" data-rows="40" async></script>
              </div>
   </div>

Note that we've turned tracing off.  We can observe that our **CL=ALL** writes will always fail because one of the nodes is down.  **CL=QUORUM** operations *could* fail, depending on whether a required partition replica falls on a node that is up or down.


Step three:  take 2 nodes down, read and write with different consistency levels
--------------------------------------------------------------------------------

Let's take a second node down and attempt to read and write with QUORUM and ONE consistency levels.

.. raw:: html            

   <div class="panel callout radius animated  asciinema">
            <div class="row">
                <script type="text/javascript" src="https://asciinema.org/a/dkTt8Lopfa6N1VsajcmjUsduK.js" id="asciicast-dkTt8Lopfa6N1VsajcmjUsduK" data-speed="3" data-rows="40" async></script>
              </div>
   </div>

With 2 nodes down on our 3 node cluster and **RF=3**, a **CL=QUORUM** will always fail as at least one of the required nodes will always be down. One the other hand, a **CL=ONE** will succeed, as it only requires one up node.


Step four: testing different consistency levels with tracing on
---------------------------------------------------------------

Finally, we'll turn on tracing, and with all our nodes up, write to the table with **CL=ALL** and read from it with **CL=QUORUM**.

.. raw:: html            

   <div class="panel callout radius animated  asciinema">
            <div class="row">
                <script type="text/javascript" src="https://asciinema.org/a/LLOZIhQxX96BXsnrMpr50oUSa.js" id="asciicast-LLOZIhQxX96BXsnrMpr50oUSa" data-speed="3" data-t="103" autoplay=1 data-rows="30" async></script>
              </div>
   </div>


We've just written a record with our consistency level set to ALL.

.. code-block:: console

	cqlsh:mykeyspace> CONSISTENCY ALL

Consistency level set to ALL.

.. code-block:: console

	cqlsh:mykeyspace> CREATE TABLE users(user_id int PRIMARY KEY, fname text, lname text);

.. code-block:: console

	cqlsh:mykeyspace> TRACING ON

Now Tracing is enabled

.. code-block:: console
	
	cqlsh:mykeyspace> insert into users(user_id, lname, fname) values (1, 'tzach', 'livyatan');


Here's the output:

.. code-block:: console
   :class: hide-copy-button

	Tracing session: 84deb7a0-9d81-11e7-b770-000000000002

	 activity                                                                | timestamp                  | source     | source_elapsed
	-------------------------------------------------------------------------+----------------------------+------------+---------------
	Execute CQL3 query                                                       | 2017-09-19 21:28:46.362000 | 172.17.0.4 | 0            
	Parsing a statement [shard 0]                                            | 2017-09-19 21:28:46.362223 | 172.17.0.4 | 14
	Processing a statement [shard 0]                                         | 2017-09-19 21:28:46.362604 | 172.17.0.4 | 273
	Creating write handler for token: -4069959284402364209                   | 2017-09-19 21:28:46.362962 | 172.17.0.4 | 755
          natural: {172.17.0.3, 172.17.0.4, 172.17.0.2} pending: {} [shard 0]    |                            |            |
	Creating write handler with live: {172.17.0.2, 172.17.0.3, 172.17.0.4}   | 2017-09-19 21:28:46.362992 | 172.17.0.4 | 784
          dead: {} [shard 0]                                                     |                            |            |
	Sending a mutation to /172.17.0.2 [shard 0]                              | 2017-09-19 21:28:46.363171 | 172.17.0.4 | 959
	Sending a mutation to /172.17.0.3 [shard 0]                              | 2017-09-19 21:28:46.363298 | 172.17.0.4 | 1090
	Executing a mutation locally [shard 0]                                   | 2017-09-19 21:28:46.363316 | 172.17.0.4 | 1109
	Message received from /172.17.0.4 [shard 1]                              | 2017-09-19 21:28:46.363942 | 172.17.0.3 | 16
	Sending mutation_done to /172.17.0.4 [shard 1]                           | 2017-09-19 21:28:46.364225 | 172.17.0.3 | 307
	Message received from /172.17.0.4 [shard 1]                              | 2017-09-19 21:28:46.365395 | 172.17.0.2 | 93
	Sending mutation_done to /172.17.0.4 [shard 1]                           | 2017-09-19 21:28:46.365561 | 172.17.0.2 | 206
	Mutation handling is done [shard 1]                                      | 2017-09-19 21:28:46.366538 | 172.17.0.2 | 1234
	Got a response from /172.17.0.2 [shard 0]                                | 2017-09-19 21:28:46.368064 | 172.17.0.4 | 5858
	Got a response from /172.17.0.4 [shard 0]                                | 2017-09-19 21:28:46.369589 | 172.17.0.4 | 7384
	Mutation handling is done [shard 1]                                      | 2017-09-19 21:28:46.370843 | 172.17.0.3 | 6928
	Got a response from /172.17.0.3 [shard 0]                                | 2017-09-19 21:28:46.372486 | 172.17.0.4 | 9964
	Mutation successfully completed [shard 0]                                | 2017-09-19 21:28:46.372519 | 172.17.0.4 | 10314
	Done processing - preparing a result [shard 0]                           | 2017-09-19 21:28:46.372548 | 172.17.0.4 | 10341
	Request complete                                                         | 2017-09-19 21:28:46.372357 | 172.17.0.4 | 10357


With tracing on, we have verbose output.  Also, with all nodes up, we should have no errors experiencing a **CL=ALL** write.  (Later, we will try different consistency levels with 1 and 2 nodes down.)  With **RF=3**, we see all three nodes involved in the interaction.  Once the co-ordinator (``172.17.0.4``) has received responses from the two other replicas - ``172.17.0.2`` and ``172.17.0.3`` - as well as itself-  the operation is complete.

So what happens when we write under **QUORUM** instead of **ALL**?  **QUORUM** means that we only require responses from (Replication Factor/2) + 1 nodes.  Our replication factor is 3, therefore only a majority of 2 nodes must provide an acknowledgement for the eventually-consistent write to be considered successful.  Below, we observe that our coordinator, after sending two other mutations to replicas ``172.17.0.2`` and ``172.17.0.3``, executes a mutation locally.  However, our coordinator only requires a write acknowledgement from itself and ``172.17.0.3`` before ``Mutation successfully completed`` is returned.  Note that the response from ``172.17.0.2`` actually comes later.

.. code:: console

	cqlsh:mykeyspace> CONSISTENCY QUORUM

Consistency level set to QUORUM.

.. code:: console

	cqlsh:mykeyspace> insert into users (user_id, fname, lname) values (2, 'john', 'hammink');

Output:

.. code-block:: console
   :class: hide-copy-button

	Tracing session: ccb9d1d0-98d7-11e7-b971-000000000003

	 activity                                                                | timestamp                  | source     | source_elapsed
	-------------------------------------------------------------------------+----------------------------+------------+---------------
	Execute CQL3 query                                                       | 2017-09-13 23:03:47.821000 | 172.17.0.4 | 0
	Parsing a statement [shard 3]                                            | 2017-09-13 23:03:47.821879 | 172.17.0.4 | 1
	Processing a statement [shard 3]                                         | 2017-09-13 23:03:47.822050 | 172.17.0.4 | 172
	Creating write handler for token: -3248873570005575792                   | 2017-09-13 23:03:47.822160 | 172.17.0.4 | 283
          natural: {172.17.0.3, 172.17.0.4, 172.17.0.2} pending: {} [shard 3]    |                            |            |
	Creating write handler with live: {172.17.0.2, 172.17.0.3, 172.17.0.4}   | 2017-09-13 23:03:47.822171 | 172.17.0.4 | 293
            dead: {} [shard 3]                                                   |                            |            |
	Sending a mutation to /172.17.0.2 [shard 3]                              | 2017-09-13 23:03:47.822192 | 172.17.0.4 | 314
	Sending a mutation to /172.17.0.3 [shard 3]                              | 2017-09-13 23:03:47.822209 | 172.17.0.4 | 331
	Executing a mutation locally [shard 3]                                   | 2017-09-13 23:03:47.822214 | 172.17.0.4 | 336
	Message received from /172.17.0.4 [shard 1]                              | 2017-09-13 23:03:47.822564 | 172.17.0.2 | 7
	Message received from /172.17.0.4 [shard 1]                              | 2017-09-13 23:03:47.822851 | 172.17.0.3 | 5
	Sending mutation_done to /172.17.0.4 [shard 1]                           | 2017-09-13 23:03:47.823009 | 172.17.0.2 | 452
	Mutation handling is done [shard 1]                                      | 2017-09-13 23:03:47.823067 | 172.17.0.2 | 510
	Sending mutation_done to /172.17.0.4 [shard 1]                           | 2017-09-13 23:03:47.823734 | 172.17.0.3 | 887
	Mutation handling is done [shard 1]                                      | 2017-09-13 23:03:47.823744 | 172.17.0.3 | 898
	Got a response from /172.17.0.4 [shard 3]                                | 2017-09-13 23:03:47.823984 | 172.17.0.4 | 2106
	Got a response from /172.17.0.3 [shard 3]                                | 2017-09-13 23:03:47.824998 | 172.17.0.4 | 3120
	Mutation successfully completed [shard 3]                                | 2017-09-13 23:03:47.825003 | 172.17.0.4 | 3125
	Done processing - preparing a result [shard 3]                           | 2017-09-13 23:03:47.825054 | 172.17.0.4 | 3176
	Got a response from /172.17.0.2 [shard 3]                                | 2017-09-13 23:03:47.825696 | 172.17.0.4 | 3817
	Request complete                                                         | 2017-09-13 23:03:47.824182 | 172.17.0.4 | 3182


We can observe that reading under **QUORUM** consistency works similarly.  In our example below, our coordinator ``172.17.0.4``  waits for only ``read_data`` from ``172.17.0.2`` (as well as itself earlier: ``read_data: querying locally``) before the read is considered successful.  Note that node ``172.17.0.3``   similarly handles ``read_digest`` as ``172.17.0.2`` handles ``read_data``.

.. code:: console

	cqlsh:mykeyspace> CONSISTENCY QUORUM

Consistency level set to QUORUM.

.. code:: console

	cqlsh:mykeyspace> select * from users where user_id = 1;

Output:

.. code-block:: console
   :class: hide-copy-button

	 user_id | fname | lname
	---------+-------+----------
	       1 | tzach | livyatan

	(1 rows)

	Tracing session: b11084e0-9e33-11e7-9f6b-000000000003

	 activity                                                                | timestamp                  | source     | source_elapsed
	-------------------------------------------------------------------------+----------------------------+------------+---------------
	Execute CQL3 query                                                       | 2017-09-20 18:44:10.926000 | 172.17.0.4 | 0
	Parsing a statement [shard 3]                                            | 2017-09-20 18:44:10.926442 | 172.17.0.4 | 1
	Processing a statement [shard 3]                                         | 2017-09-20 18:44:10.926538 | 172.17.0.4 | 97
	Creating read executor for token -4069959284402364209                    | 2017-09-20 18:44:10.926606 | 172.17.0.4 | 165
          with all: {172.17.0.4, 172.17.0.2, 172.17.0.3}                         |                            |            |
          targets: {172.17.0.4, 172.17.0.2, 172.17.0.3}                          |                            |            |
          repair decision: DC_LOCAL [shard 3]                                    |                            |            |
	read_digest: sending a message to /172.17.0.3 [shard 3]                  | 2017-09-20 18:44:10.926634 | 172.17.0.4 | 193
	read_data: querying locally [shard 3]                                    | 2017-09-20 18:44:10.926664 | 172.17.0.4 | 223
	read_data: sending a message to /172.17.0.2 [shard 3]                    | 2017-09-20 18:44:10.926711 | 172.17.0.4 | 270
	read_digest: message received from /172.17.0.4 [shard 3]                 | 2017-09-20 18:44:10.926956 | 172.17.0.3 | 6
	read_data: message received from /172.17.0.4 [shard 3]                   | 2017-09-20 18:44:10.927310 | 172.17.0.2 | 7
	read_digest handling is done, sending a response to /172.17.0.4 [shard 3]| 2017-09-20 18:44:10.927454 | 172.17.0.3 | 505
	read_data handling is done, sending a response to /172.17.0.4 [shard 3]  | 2017-09-20 18:44:10.928248 | 172.17.0.2 | 946
	read_digest: got response from /172.17.0.3 [shard 3]                     | 2017-09-20 18:44:10.929873 | 172.17.0.4 | 3432
	read_data: got response from /172.17.0.2 [shard 3]                       | 2017-09-20 18:44:10.929880 | 172.17.0.4 | 3439
	Done processing - preparing a result [shard 3]                           | 2017-09-20 18:44:10.929934 | 172.17.0.4 | 3493
	Request complete                                                         | 2017-09-20 18:44:10.929503 | 172.17.0.4 | 3503
