============================
ScyllaDB Materialized Views
============================

Occasionally, the same information record needs to be queried based on more than one key.  For example, when:

* user data includes ``name`` and ``ID`` fields, and needs to be queried once by ``name`` and once by ``ID``;

* you need to determine scores, and select the aggregated top scores from within different time ranges.

In ScyllaDB, data is divided into partitions which can be looked up via a partition key. Sometimes the application needs to find a partition or partitions by the value of another column. Doing this efficiently requires indexing. You may also need to index an alternative clustering key.

Prior to the introduction of materialized views, the only way to implement this was using denormalization - creating two entirely separate tables and synchronizing them from within an application. However, ensuring any level of consistency between the data in two or more views this way can require duplicated code along with complex and slow application logic.

ScyllaDB’s materialized views feature, moves this complexity out of the application and into the servers.  With fewer round trips to the applications, this implementation is faster and more reliable.

With materialized views, ScyllaDB automates the process of maintaining separate tables to support different queries of the same data, and allows for fast lookups of data in each view using the standard read path.

A **materialized view** is a global index. It is effectively a new table, populated by a query running against the base table.  You cannot update a materialized view directly;  to update it, you must update the base table.

Each materialized view is a set of rows which corresponds to rows which are present in the underlying, or base, table specified in the materialized view’s ``SELECT`` statement.


Configuration Example
---------------------

Given the following ‘base’ table:

.. code-block:: cql

	CREATE TABLE buildings (
	    name text,
	    city text,
	    built int,
	    meters int,
	    PRIMARY KEY (name)
	);

ScyllaDB can automatically maintain a materialized view table. In the following example, we want to search by city, but show all fields in the original table.

``city`` is the partition key, but since there can be more than one building in each city, we must add ``name`` as the clustering key, so that ``(city, name)`` becomes the primary key:

.. code-block:: cql

	CREATE MATERIALIZED VIEW building_by_city AS
	 	SELECT * FROM buildings
		WHERE city IS NOT NULL
	 	PRIMARY KEY(city, name);

A materialized view may itself be queried just like any other table:

.. code-block:: cql

	SELECT * from building_by_city;

or

.. code-block:: cql

	SELECT name, built, meters from building_by_city LIMIT 1;

A second materialized view can be made by selecting only the primary key and ``meters`` field from the base table:

.. code-block:: cql

	CREATE MATERIALIZED VIEW building_by_city2 AS
	 	SELECT meters FROM buildings
	 	WHERE city IS NOT NULL 
	 	PRIMARY KEY(city, name);

Note that, although each materialized view is a separate table, a user cannot modify a view directly:

.. code-block:: shell
	
	cqlsh:mykeyspace> DELETE FROM building_by_city WHERE city='Taipei';

	InvalidRequest: code=2200 [Invalid query] message="Cannot directly modify a materialized view"

To modify views, remember that you must instead modify the base table associated with the view.

See the :doc:`CQL Reference for Materialized Views </cql/mv>` for more examples.

Compaction Strategies with Materialized Views
---------------------------------------------

Materialized views, just like regular tables, use one of the available :doc:`compaction strategies </architecture/compaction/compaction-strategies>`.
When a materialized view is created, it does not inherit its base table compaction strategy settings, because the data model
of a view does not necessarily have the same characteristics as the one from its base table.
Instead, the default compaction strategy (SizeTieredCompactionStrategy) is used.

A compaction strategy for a new materialized view can be explicitly set during its creation, using the following command:

.. code-block:: cql

   CREATE MATERIALIZED VIEW ks.mv AS SELECT a,b FROM ks.t WHERE
     a IS NOT NULL
     AND b IS NOT NULL
     PRIMARY KEY (a,b)
     WITH COMPACTION = {'class': 'LeveledCompactionStrategy'};

You can also change the compaction strategy of an already existing materialized view, using an ALTER MATERIALIZED VIEW statement.

For example:

.. code-block:: cql

   ALTER MATERIALIZED VIEW ks.mv
     WITH COMPACTION = {'class': 'LeveledCompactionStrategy'} ;

More information 
................

* :doc:`CQL Reference for Materialized Views </cql/mv>`
* Learn more about Materialized Views with ScyllaDB University (Free, registration required)

  - `Materialized Views, Secondary Indexes, and Filtering Lesson <https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/>`_
  - `Hands-on Lab Part 1 <https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/topic/materialized-views-and-indexes-hands-on-lab-1/>`_
  - `Hands-on Lab Part 2 <https://university.scylladb.com/courses/data-modeling/lessons/materialized-views-secondary-indexes-and-filtering/topic/materialized-views-and-secondary-indexes-hands-on-updated/>`_

