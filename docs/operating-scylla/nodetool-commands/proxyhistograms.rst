Nodetool proxyhistograms
========================

**proxyhistograms** - Provide the latency request that is recorded by the coordinator. This command is helpful if you encounter slow node operations.

For example:

.. code-block:: shell

   nodetool proxyhistograms

   proxy histograms
   Percentile      Read Latency     Write Latency     Range Latency
                    (micros)          (micros)          (micros)
   50%                   512.00           1024.00           4096.00
   75%                   896.00           2560.00           5120.00
   95%                  4096.00          14336.00          14336.00
   98%                  8192.00          20480.00          24576.00
   99%                  8192.00          20480.00          28672.00
   Min                   512.00            512.00            512.00
   Max                 10240.00          24576.00         229376.00


The latencies cover all operations since the node started. They are kept in a histogram, so a latency percentile is the lower limit of the histogram bucket it falls in, and not an exact value. The lowest bucket starts at 512 microseconds: faster operations are shown as 512. The highest ends at about 33.5 seconds, and slower operations are shown as that.

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
