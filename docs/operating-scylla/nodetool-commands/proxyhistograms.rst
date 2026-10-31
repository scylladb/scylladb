Nodetool proxyhistograms
========================

**proxyhistograms** - Provide the latency request that is recorded by the coordinator. This command is helpful if you encounter slow node operations.

For example:

.. code-block:: shell

   nodetool proxyhistograms

   proxy histograms
   Percentile       Read Latency      Write Latency      Range Latency   CAS Read Latency  CAS Write Latency View Write Latency
                        (micros)           (micros)           (micros)           (micros)           (micros)           (micros)
   50%                    353.50            1214.50            4103.00               0.00               0.00               0.00
   75%                    972.50            2969.25            5073.25               0.00               0.00               0.00
   95%                   4832.85           15394.80           14981.50               0.00               0.00               0.00
   98%                   8181.18           21873.00           27640.50               0.00               0.00               0.00
   99%                   8356.63           21873.00           31843.79               0.00               0.00               0.00
   Min                     22.00             207.00             499.00               0.00               0.00               0.00
   Max                   8365.00           21873.00          208960.00               0.00               0.00               0.00



==================  ======================================
Parameter           Description
==================  ======================================
Percentile          Latency rank
------------------  --------------------------------------
Read Latency        Read latency
------------------  --------------------------------------
Write Latency       Write latency
------------------  --------------------------------------
Range Latency       Range scan latency for the node
------------------  --------------------------------------
CAS Read Latency    LWT (Paxos) read latency
------------------  --------------------------------------
CAS Write Latency   LWT (Paxos) write latency
------------------  --------------------------------------
View Write Latency  Materialized view update write latency
==================  ======================================

.. include:: nodetool-index.rst
