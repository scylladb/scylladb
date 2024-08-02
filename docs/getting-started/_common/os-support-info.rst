You can `build ScyllaDB from source <https://github.com/scylladb/scylladb#build-prerequisites>`_ on other x86_64 or aarch64 platforms, without any guarantees.

+----------------------------+--------------------+-------+---------------+
| Linux Distributions        |Ubuntu              | Debian| Rocky /       |
|                            |                    |       | RHEL          |
+----------------------------+------+------+------+-------+-------+-------+
| ScyllaDB Version / Version |20.04 |22.04 |24.04 |  11   |   8   |   9   |
+============================+======+======+======+=======+=======+=======+
|   6.1                      | |v|  | |v|  | |v|  | |v|   | |v|   | |v|   |
+----------------------------+------+------+------+-------+-------+-------+
|   6.0                      | |v|  | |v|  | |v|  | |v|   | |v|   | |v|   |
+----------------------------+------+------+------+-------+-------+-------+

* The recommended OS for ScyllaDB Open Source is Ubuntu 22.04.
* All releases are available as a Docker container and EC2 AMI, GCP, and Azure images. 

Supported Architecture
-----------------------------

ScyllaDB Open Source supports x86_64 for all versions and AArch64 starting from ScyllaDB 4.6 and nightly build. 
In particular, aarch64 support includes AWS EC2 Graviton.