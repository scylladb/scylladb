Nodetool proxyhistograms
========================

**proxyhistograms** - Provide the latency request that is recorded by the coordinator. This command is helpful if you encounter slow node operations.

For example:

.. code-block:: shell

   nodetool proxyhistograms

   proxy histograms
   Percentile      Read Latency     Write Latency     Range Latency
                    (micros)          (micros)          (micros)
   50%                   353.50           1214.50           4103.00
   75%                   972.50           2969.25           5073.25
   95%                  4832.85          15394.80          14981.50
   98%                  8181.18          21873.00          27640.50
   99%                  8356.63          21873.00          31843.79
   Min                    22.00            207.00            499.00
   Max                  8365.00          21873.00         208960.00



=============  ===============================
Parameter      Description
=============  ===============================
Percentile     Latency rank
-------------  -------------------------------
Read Latency   Read latency
-------------  -------------------------------
Write Latency  Write latency
-------------  -------------------------------
Range Latency  Range scan latency for the node
=============  ===============================

.. include:: nodetool-index.rst
