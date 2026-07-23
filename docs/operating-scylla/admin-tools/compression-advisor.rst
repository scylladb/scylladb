Compression Advisor
====================

The Compression Advisor (``tools/compression_advisor.py``) analyzes the
compression configuration of a single table and recommends whether a different
compressor, compression level, or chunk length would meaningfully reduce its
on-disk size.

It reads the table's current compression settings over CQL, asks the node to
estimate compression ratios for a range of candidate configurations through the
``estimate_compression_ratios`` REST API, and prints a color-coded heatmap grid,
a summary table comparing the current configuration against the best LZ4 and
ZSTD candidates, and a single ready-to-run ``ALTER TABLE`` statement for the
best overall recommendation.

The advisor only recommends a change when it improves the estimated compression
ratio by at least 5%, and it prefers the default 4 KB chunk length unless a
larger chunk is more than 5% better. This avoids churning table settings for
negligible gains.

Prerequisites
-------------

* Python 3.
* *(Optional)* The `ScyllaDB Python driver
  <https://pypi.org/project/scylla-driver/>`_ (``scylla-driver``), used to read
  the current compression configuration over CQL. Install it with:

  .. code-block:: console

     pip install scylla-driver

  If the driver is not installed, the tool still runs and prints the full
  estimation heatmap and recommendations — it simply cannot highlight or
  compare against the table's current compression settings.

* Network access from where you run the tool to:

  * the ScyllaDB CQL native transport port (``9042`` by default), and
  * the ScyllaDB Admin REST API port (``10000`` by default).

* The ``SSTABLE_COMPRESSION_DICTS`` cluster feature must be enabled on all
  nodes. The ``estimate_compression_ratios`` REST API depends on it; without it
  the endpoint returns an error and the tool reports that the API returned
  invalid JSON.

Usage
-----

.. code-block:: console

   python3 tools/compression_advisor.py --host <node-ip> --keyspace <ks> --table <tbl> [options]

For example:

.. code-block:: console

   python3 tools/compression_advisor.py --host 127.0.0.1 --keyspace myks --table mytbl

To disable the ANSI color output (for example when redirecting to a file):

.. code-block:: console

   python3 tools/compression_advisor.py --host 127.0.0.1 --keyspace myks --table mytbl --no-color

Options
-------

.. list-table::
   :widths: 20 15 65
   :header-rows: 1

   * - Option
     - Default
     - Description
   * - ``--host``
     - *(required)*
     - IP address of a ScyllaDB node to query.
   * - ``--keyspace``
     - *(required)*
     - Keyspace name of the table to analyze.
   * - ``--table``
     - *(required)*
     - Table name to analyze.
   * - ``--api-port``
     - ``10000``
     - ScyllaDB Admin REST API port.
   * - ``--cql-port``
     - ``9042``
     - ScyllaDB CQL native transport port.
   * - ``--no-color``
     - *off*
     - Disable ANSI color output. Color is also disabled automatically when the
       output is not a terminal.

Run the tool with ``-h`` / ``--help`` for the full list of options.

Output
------

The advisor prints three sections:

* **Heatmap grid** — the estimated compression ratio for every candidate
  compressor / chunk length / level combination, color-coded so the most
  effective configurations stand out. The current configuration is highlighted.
* **Summary table** — the current configuration alongside the best LZ4 and best
  ZSTD candidates, with the estimated size reduction relative to the current
  setting.
* **Recommended change** — a single ``ALTER TABLE`` statement for the best
  overall recommendation (chosen between the best LZ4 and best ZSTD candidate).
  When a dictionary-aware compressor with a retrained dictionary is recommended,
  the tool also prints the corresponding ``retrain_dict`` REST call needed to
  train the compression dictionary.

The recommended ``ALTER TABLE`` statement always quotes the keyspace and table
identifiers, uses the dictionary-capable compressor names returned by the
estimator (for example ``ZstdWithDictsCompressor``), and includes
``compression_level`` for ZSTD. For example:

.. code-block:: cql

   ALTER TABLE "myks"."mytbl" WITH compression = {'sstable_compression': 'ZstdWithDictsCompressor', 'chunk_length_in_kb': '4', 'compression_level': '5'};

If the recommendation uses a retrained dictionary, the tool also prints, as
comments, the REST call to train it:

.. code-block:: console

   -- Then retrain the dictionary (requires SSTABLE_COMPRESSION_DICTS feature):
   -- POST /storage_service/retrain_dict?keyspace=myks&cf=mytbl

When no change improves the estimated ratio by more than 5%, the tool reports
that the current configuration is already optimal instead of printing an
``ALTER TABLE`` statement.

.. note::

   The advisor only estimates and recommends; it never modifies the table.
   Apply any recommended ``ALTER TABLE`` statement yourself after reviewing it.
