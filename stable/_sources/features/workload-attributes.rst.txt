=============================
Defining Workload Attributes
=============================

Introduction
-------------

A typical database has more than one :term:`workload <workload>` running simultaneously with a different acceptable level of latency and 
throughput. By defining the attributes of each workload, you can specify how ScyllaDB will handle requests depending on 
the workload to which they are assigned.

You can define a workload's attribute using the *service level* concept. The service level CQL commands allow you to attach 
attributes to users and roles. When a user logs into the system, all of the attributes attached to that user and to the roles 
granted to that user are combined and become a set of workload attributes.

See `Service Level Management <https://enterprise.docs.scylladb.com/stable/using-scylla/workload-prioritization.html#workload-prioritization-workflow>`_ for more information about service levels.

Prerequisites
---------------

* An :doc:`authenticated </operating-scylla/security/runtime-authentication>` and :doc:`authorized </operating-scylla/security/enable-authorization>` user 
* At least one :ref:`role created <create-role-statement>`.

Procedure
------------

#. Create a service level with the desired attribute.

    .. code-block:: cql

     CREATE SERVICE LEVEL <service_level_name> WITH <attribute> [ AND <attribute>];
	
    For example:

    .. code-block:: cql

	   CREATE SERVICE LEVEL sl2 WITH timeout = 500ms AND workload_type=interactive;
	
	
    See :ref:`Available Attributes <workload-attributes-available-attributes>`.

#. Assign a service level to a role or user:

    .. code-block:: cql

	   ATTACH SERVICE_LEVEL <service_level_name> TO <role_name|user_name>;
	
    For example:

    .. code-block:: cql

	   ATTACH SERVICE LEVEL sl2 TO scylla;


You can modify the service level attributes with the ``ALTER SERVICE LEVEL`` command:

    .. code-block:: cql

     ALTER SERVICE LEVEL <service_level_name> WITH <attribute> [ AND <attribute>];

 For example:

.. code-block:: cql

     ALTER SERVICE LEVEL sl2 WITH timeout = null;


.. _workload-attributes-available-attributes:

Available Attributes
-----------------------

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Attribute
     - Details
   * - ``timeout``
     - :ref:`Specifying Service Level Timeout <workload-attributes-timeout>`
   * - ``workload_type``
     - :ref:`Specifying Workload Type<workload-attributes-workload-type>`


.. _workload-attributes-timeout:

Specifying Service Level Timeout
-----------------------------------

You can specify the timeout for a service level (in milliseconds or seconds) with the ``timeout`` attribute. 

For example:

.. code-block:: cql

   CREATE SERVICE LEVEL primary WITH timeout = 30ms;

Specifying the timeout value is useful when your workloads have different acceptable latency levels.

.. _workload-attributes-workload-type:

Specifying Workload Type
----------------------------

You can specify the workload type for a service level with the ``workload_type`` attribute. 

For example:

.. code-block:: cql

   CREATE SERVICE LEVEL secondary WITH workload_type = 'batch';

Specifying the workload type allows ScyllaDB to handle sessions more efficiently (for example, depending on whether the workload is 
sensitive to latency).


Available Workload Types
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Workload type
     - Description
   * - ``unspecified``
     - A generic workload without any specific characteristics (default).
   * - ``interactive``
     - A workload sensitive to latency, expected to have high/unbounded concurrency, with dynamic characteristics. For example, a workload assigned to users clicking on a website and generating events with their clicks.
   * -  ``batch``
     - A workload for processing large amounts of data, not sensitive to latency, expected to have fixed concurrency. For example, a workload assigned to processing billions of historical sales records to generate statistics.

