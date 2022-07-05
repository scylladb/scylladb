Minimal Production System Recommendations
-----------------------------------------

* **CPU** - at least 2 physical cores/ 4vCPUs
* **Memory** - 15GB+ DRAM
* **Disk** - persistent disk storage is proportional to the number of cores and Prometheus retention period (see the following section)
* **Network** - 1GbE/10GbE preferred

Calculating Prometheus Minimal Disk Space requirement
.....................................................

Prometheus storage disk performance requirements: persistent block volume, for example an EC2 EBS volume

Prometheus storage disk volume requirement:  proportional to the number of metrics it holds. The default retention period is 15 days, and the disk requirement is around 200MB per core, assuming the default scraping interval of 15s.

For example, when monitoring a 6 node Scylla cluster, each with 16 CPU cores, and using the default 15 days retention time, you will need **minimal** disk space of

..  code::

   6 * 16 * 200MB ~ 20GB


To account for unexpected events, like replacing or adding nodes, we recommend allocating at least x4-5 space, in this case, ~100GB.
Prometheus Storage disk does not have to be as fast as Scylla disk, and EC2 EBS, for example, is fast enough and provide HA out of the box.
