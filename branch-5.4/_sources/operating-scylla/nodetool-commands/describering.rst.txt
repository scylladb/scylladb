Nodetool describering
=====================

**describering** - :code:`<keyspace>`- Shows the partition ranges of a given keyspace.

For example:

.. code-block:: shell

   nodetool describering nba

Example output (for three node cluster on AWS):

.. code-block:: shell

   Schema Version:86a67fc7-1d7c-3dc3-9be9-9c86b27e2506
   TokenRange: 
	TokenRange(start_token:1034550345118248712, end_token:-7923576032814755401, endpoints:[54.0.9.1, 54.0.9.2, 54.0.9.3], 
         rpc_endpoints:[54.0.9.1, 54.0.9.2, 54.0.9.3], 
         endpoint_details:[EndpointDetails(host:54.0.9.1, datacenter:us-east1, rack:RACK1), 
         EndpointDetails(host:54.0.9.2, datacenter:us-east-1, rack:RACK1), 
         EndpointDetails(host:54.0.9.3, datacenter:us-east-1, rack:RACK1)])
	TokenRange(start_token:-1034550345118248713, end_token:7923576032814755402, endpoints:[54.0.9.3, 54.0.9.1, 54.0.9.2],          
         rpc_endpoints:[54.0.9.3, 54.0.9.1, 54.0.9.2], 
         endpoint_details:[EndpointDetails(host:127.0.0.3, datacenter:us-east-1, rack:rack1), 
         EndpointDetails(host:127.0.0.1, datacenter:us-east-1, rack:RACK1), 
         EndpointDetails(host:127.0.0.2, datacenter:us-east-1, rack:RACK1)])
	TokenRange(start_token:-1034550345118248714, end_token:-7923576032814755403, endpoints:[54.0.9.2, 54.0.9.3, 54.0.9.1], 
         rpc_endpoints:[54.0.9.2, 54.0.9.3, 54.0.9.1], 
         endpoint_details:[EndpointDetails(host:54.0.9.2, datacenter:us-east-1, rack:RACK1), 
         EndpointDetails(host:54.0.9.3, datacenter:us-east-1, rack:RACK1), 
         EndpointDetails(host:54.0.9.1, datacenter:us-east-1, rack:RACK1)])

.. include:: nodetool-index.rst
