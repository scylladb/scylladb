Nodetool cfhistograms (tablehistograms) 
=======================================
**cfhistograms** ``<keyspace> <cfname>``- This command provides statistics about a table, including number of SSTables, read/write latency, partition size and column count. `cfhistograms` covers all operations since the node started. Note that this command can also be used for ``tablehistograms``. This command uses the following syntax:

.. code-block:: shell

   nodetool cfhistograms keyspaces cfname

For example: to get statistics about the ``standard1`` table in the ``keyspaces1`` keyspace, use this command:

.. code-block:: shell

   nodetool cfhistograms keyspaces1 standard1

==========  ==============  =============  ============  ==============  ==========
Percentile  SSTables        Write Latency  Read Latency  Partition Size  Cell Count
==========  ==============  =============  ============  ==============  ==========
50%	        10.00           512.00         512.00        210             5
----------  --------------  -------------  ------------  --------------  ----------
75%         20.00           512.00         640.00        280             5
----------  --------------  -------------  ------------  --------------  ----------
95%         25.00           640.00         1024.00       300             5
----------  --------------  -------------  ------------  --------------  ----------
98%         25.00           768.00         2048.00       310             5
----------  --------------  -------------  ------------  --------------  ----------
99%         26.00           1024.00        3584.00       310             5
----------  --------------  -------------  ------------  --------------  ----------
Min         00.00           512.00         512.00        210             5
----------  --------------  -------------  ------------  --------------  ----------
Max         26.00           2560.00        40960.00      310             5
==========  ==============  =============  ============  ==============  ==========

In this example, 95% of the write latency measurements were about 640 microseconds (μs) or less, 95% of the read latencies were about 1024 μs or less, and 95% of the partitions are 300 bytes or less. The SSTables column means that the 95% of all single-key read requests touched at most 25 SSTables.

The latencies are in microseconds and are kept in a histogram, so a latency percentile is the lower limit of the histogram bucket it falls in, and not an exact value. The lowest bucket starts at 512 μs: faster operations are shown as 512. The highest ends at about 33.5 seconds, and slower operations are shown as that.

==============  ========================================
Parameter       Description
==============  ========================================
SSTables        Distribution number of sstable per read
--------------  ----------------------------------------
Write Latency	Distribution of the Write Latency
--------------  ----------------------------------------
Read Latency	Distribution of the Read Latency
--------------  ----------------------------------------
Partition Size	Estimated row size
--------------  ----------------------------------------
Cell Count      Estimated column count
==============  ========================================

.. include:: nodetool-index.rst
