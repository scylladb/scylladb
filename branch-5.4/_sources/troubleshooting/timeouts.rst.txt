Cluster Timeouts 
================

**Issue**: cluster is experiencing timeouts due to server overload

**Possible Root Cause**: aggressive retry policy


Details
-------

When requests fail, it is natural for users to want to retry the requests.
However, if the request fails due to overload conditions, retrying the request can make the situation worse.
Since the server is still processing the old request, when the new retry is attempted, this makes the server process both requests.
The server which was already overloaded is now even more overloaded.

Here are a few situations that can lead to this scenario:

* A mismatch between server and client timeout settings. The client timeout should be equal to or higher than the server-side timeout for the desired request. Say for instance that the server will timeout requests in 5s, but the client times out after 1s. If the client keeps retrying, the server will accumulate up to 5 requests for the same data. If the client timeout is higher than the server’s, this will not happen.
* Client-side speculative retries: Speculative retry is a mechanism that causes the client to speculate that a request may fail, and send a new request (potentially to another node) before the failure occurs. This can reduce latency in the case timeout happens due to transient hardware failures, but will make load-induced timeouts order of magnitudes worse (because the server now has to deal with the requests plus the speculative retries). Speculative retries policies can be configured to kick in more or less aggressively by setting its thresholds.
* Server-side speculative retries: The server can speculate requests too, and that can cause the load to increase for similar reasons to client speculative retry

Actions
-------

If your cluster is having timeouts during overload, check first if you are not making the overload situation worse through retries, and pay attention to the following:

* Make sure the client retries only after the server has already timed out. Depending on the application this may mean increasing the client-side timeout or decreasing the server-side timeout. Client timeouts are configured by the driver, check your :doc:`driver documentation </using-scylla/drivers/index>` about parameters and defaults. For the server-side timeout, the ``/etc/scylla/scylla.yaml`` has request-specific timeout settings like ``read_request_timeout_in_ms`` and ``write_request_timeout_in_ms``
* Make sure the client neither runs a speculative retry nor runs it very aggressively. Client-side speculative retry is configured by the driver, check your :doc:`driver documentation </using-scylla/drivers/index>` about parameters and defaults.
* Make sure the server neither runs speculative retry nor runs it based on percentiles (as those can fluctuate aggressively). Server-side speculative retries are a per-table setting that can be changed with the ALTER TABLE command. See the :ref:`documentation <speculative-retry-options>` for details.




